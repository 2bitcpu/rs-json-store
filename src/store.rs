use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::error::JsonStoreError;

const INFOS_FILE: &str = "infos.json";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Info {
    pub sequence_field: String,
    pub unique_fields: HashMap<String, Vec<String>>,
    pub capacity: u32,
}

impl Info {
    pub fn new(
        sequence_field: String,
        unique_fields: HashMap<String, Vec<String>>,
        capacity: u32,
    ) -> Self {
        Self {
            sequence_field,
            unique_fields,
            capacity,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Tree {
    sequence: u64,
    data: HashMap<u64, Value>,
    changed: bool,
}

impl Tree {
    pub fn new(sequence: u64, data: HashMap<u64, Value>, changed: bool) -> Self {
        Self {
            sequence,
            data,
            changed,
        }
    }
}

type Trees = HashMap<String, Arc<RwLock<Tree>>>;

#[derive(Debug)]
pub struct JsonStore {
    path: Box<Path>,
    infos: HashMap<String, Info>,
    trees: Trees,
}

impl JsonStore {
    pub async fn create_tree(&mut self, tname: &str, info: Info) -> Result<(), JsonStoreError> {
        if self.infos.contains_key(tname) {
            return Err(JsonStoreError::FoundTree(tname.to_string()));
        }

        self.infos.insert(tname.to_string(), info);

        self.trees.insert(
            tname.to_string(),
            Arc::new(RwLock::new(Tree::new(0, HashMap::default(), true))),
        );

        put_json::<HashMap<String, Info>>(self.path.join(INFOS_FILE), &self.infos).await?;

        self.save_tree(tname).await?;

        Ok(())
    }

    pub async fn drop_tree(&mut self, tname: &str) -> Result<(), JsonStoreError> {
        if !self.infos.contains_key(tname) {
            return Err(JsonStoreError::NotFoundTree(tname.to_string()));
        }
        self.infos.remove(tname);
        self.trees.remove(tname);

        put_json::<HashMap<String, Info>>(self.path.join(INFOS_FILE), &self.infos).await?;

        let path = self.path.join(format!("{}.seq", tname));
        let _ = tokio::fs::remove_file(path).await;

        let path = self.path.join(format!("{}.json", tname));
        let _ = tokio::fs::remove_file(path).await;

        Ok(())
    }

    pub async fn load(path: &Path) -> Result<Self, JsonStoreError> {
        let infos = get_json::<HashMap<String, Info>>(path.join(INFOS_FILE))
            .await?
            .unwrap_or(HashMap::new());

        let mut trees: Trees = HashMap::new();

        for (key, _value) in infos.iter() {
            let file = path.join(format!("{}.seq", key));
            let sequence = get_sequence(file).await?;

            let path = path.join(format!("{}.json", key));
            let data = get_json::<HashMap<u64, Value>>(path)
                .await?
                .unwrap_or(HashMap::new());

            trees.insert(
                key.clone(),
                Arc::new(RwLock::new(Tree::new(sequence, data, false))),
            );
        }

        Ok(Self {
            path: path.into(),
            infos,
            trees,
        })
    }

    // insert tree
    pub async fn insert<T: Serialize>(
        &mut self,
        tname: &str,
        value: &T,
    ) -> Result<u64, JsonStoreError> {
        let info = self
            .infos
            .get(tname)
            .ok_or(JsonStoreError::NotFoundTree(tname.to_string()))?;

        let mut tree = self._write_lock(tname).await?;

        if tree.data.len() >= info.capacity as usize {
            return Err(JsonStoreError::CapacityExceeded(tname.to_string()));
        }

        let mut json_value = serde_json::to_value(value)?;

        for (_, fields) in &info.unique_fields {
            let mut n = json!({});
            for field in fields {
                n.as_object_mut()
                    .ok_or(JsonStoreError::UnObjectValue)?
                    .insert(field.clone(), json_value[field].clone());
            }

            for (_, row) in &tree.data {
                let mut o = json!({});
                for field in fields {
                    o.as_object_mut()
                        .ok_or(JsonStoreError::UnObjectValue)?
                        .insert(field.clone(), row[field].clone());
                }
                if n == o {
                    return Err(JsonStoreError::DuplicateUniqueFields(tname.to_string()));
                }
            }
        }

        let seq = tree.sequence + 1;
        tree.sequence = seq;

        if json_value[info.sequence_field.clone()].is_null() {
            json_value
                .as_object_mut()
                .ok_or(JsonStoreError::UnObjectValue)?
                .insert(info.sequence_field.clone(), serde_json::to_value(seq)?);
        } else {
            *json_value
                .get_mut(info.sequence_field.clone())
                .ok_or(JsonStoreError::UnableToMutValue(tname.to_string()))? =
                serde_json::to_value(seq)?;
        }

        tree.data.insert(seq, json_value);

        tree.changed = true;

        Ok(seq)
    }

    // update tree
    pub async fn update<T: Serialize>(
        &mut self,
        tname: &str,
        value: &T,
    ) -> Result<(), JsonStoreError> {
        let info = self
            .infos
            .get(tname)
            .ok_or(JsonStoreError::NotFoundTree(tname.to_string()))?;

        let mut tree = self._write_lock(tname).await?;

        let json_value = serde_json::to_value(value)?;

        let seq = match json_value[info.sequence_field.clone()].as_u64() {
            Some(n) => n,
            None => return Err(JsonStoreError::SequenceNotExist(tname.to_string())),
        };

        if !tree.data.contains_key(&seq) {
            return Err(JsonStoreError::SequenceNotExist(tname.to_string()));
        }

        for (_, fields) in &info.unique_fields {
            let mut n = json!({});
            for field in fields {
                n.as_object_mut()
                    .ok_or(JsonStoreError::UnObjectValue)?
                    .insert(field.clone(), json_value[field].clone());
            }

            for (key, row) in &tree.data {
                if *key == seq {
                    continue;
                }
                let mut o = json!({});
                for field in fields {
                    o.as_object_mut()
                        .ok_or(JsonStoreError::UnObjectValue)?
                        .insert(field.clone(), row[field].clone());
                }
                if n == o {
                    return Err(JsonStoreError::DuplicateUniqueFields(tname.to_string()));
                }
            }
        }

        tree.data.entry(seq).and_modify(|v| *v = json_value);

        tree.changed = true;

        Ok(())
    }

    pub async fn delete(&mut self, tname: &str, sequence: u64) -> Result<(), JsonStoreError> {
        let mut tree = self._write_lock(tname).await?;

        tree.data
            .remove(&sequence)
            .ok_or(JsonStoreError::SequenceNotExist(tname.to_string()))?;

        tree.changed = true;

        Ok(())
    }

    pub async fn select<T: DeserializeOwned>(
        &self,
        tname: &str,
        sequence: u64,
    ) -> Result<T, JsonStoreError> {
        let tree = self._read_lock(tname).await?;

        Ok(serde_json::from_value::<T>(
            tree.data
                .get(&sequence)
                .ok_or(JsonStoreError::SequenceNotExist(tname.to_string()))?
                .clone(),
        )?)
    }

    pub async fn save(&self) -> Result<(), JsonStoreError> {
        for (key, _value) in self.infos.iter() {
            self.save_tree(key).await?
        }

        Ok(())
    }

    pub async fn save_tree(&self, tname: &str) -> Result<(), JsonStoreError> {
        let mut tree = self._write_lock(tname).await?;

        if !tree.changed {
            return Ok(());
        }

        let file = self.path.join(format!("{}.seq", tname));
        put_sequence(file, tree.sequence).await?;

        let file = self.path.join(format!("{}.json", tname));
        put_json(file, &tree.data).await?;

        tree.changed = false;

        Ok(())
    }

    async fn _write_lock(&self, tname: &str) -> Result<RwLockWriteGuard<'_, Tree>, JsonStoreError> {
        Ok(self
            .trees
            .get(tname)
            .ok_or(JsonStoreError::NotFoundTree(tname.to_string()))?
            .write()
            .await)
    }

    async fn _read_lock(&self, tname: &str) -> Result<RwLockReadGuard<'_, Tree>, JsonStoreError> {
        Ok(self
            .trees
            .get(tname)
            .ok_or(JsonStoreError::NotFoundTree(tname.to_string()))?
            .read()
            .await)
    }

    pub fn show(&self) {
        println!("{:?}", self.infos);
        println!("{:?}", self.trees);
    }
}

async fn get_json<T: DeserializeOwned>(file: PathBuf) -> Result<Option<T>, JsonStoreError> {
    let context = match read_text(file).await? {
        Some(s) => s,
        None => return Ok(None),
    };
    Ok(Some(serde_json::from_str(&context)?))
}

async fn put_json<T: Serialize + Debug>(file: PathBuf, value: &T) -> Result<(), JsonStoreError> {
    write_text(file, serde_json::to_string(value)?).await
}

async fn get_sequence(file: PathBuf) -> Result<u64, JsonStoreError> {
    let line = match read_text(file).await? {
        Some(s) => s,
        None => return Ok(0),
    };

    let seq: u64 = line.parse().unwrap_or_default();

    Ok(seq)
}

async fn put_sequence(file: PathBuf, sequence: u64) -> Result<(), JsonStoreError> {
    write_text(file, sequence.to_string()).await
}

async fn read_text(file: PathBuf) -> Result<Option<String>, JsonStoreError> {
    let file = match tokio::fs::File::open(file).await {
        Ok(f) => f,
        Err(e) if e.kind() == tokio::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut reader = tokio::io::BufReader::new(file);
    let mut context = String::new();
    let mut buf = String::new();
    while let Ok(size) = reader.read_line(&mut buf).await {
        if size == 0 {
            break;
        }
        context.push_str(&buf);
        buf = String::new();
    }

    Ok(Some(context))
}

async fn write_text(file: PathBuf, context: String) -> Result<(), JsonStoreError> {
    let file = tokio::fs::File::create(file).await?;

    let mut writer = tokio::io::BufWriter::new(file);
    writer.write(context.as_bytes()).await?;
    writer.flush().await?;

    Ok(())
}
