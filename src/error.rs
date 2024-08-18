use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsonStoreError {
    // Serde deserialize/serialize error
    #[error("Serde deserialize/serialize error: {0}")]
    DeserializeFromStr(#[from] serde_json::Error),
    // IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Tree at '{0}' in Use")]
    InUseTree(String),

    #[error("Tree at '{0}' not Found")]
    NotFoundTree(String),

    #[error("Tree at '{0}' Found")]
    FoundTree(String),

    #[error("Tree at '{0}' Duplicate Unique Fields")]
    DuplicateUniqueFields(String),

    #[error("Tree at '{0}' Capacity Exceeded")]
    CapacityExceeded(String),

    #[error("Tree at '{0}' Unable to get mut value")]
    UnableToMutValue(String),

    #[error("Tree at '{0}' sequence does not exist")]
    SequenceNotExist(String),

    #[error("Un Object Value")]
    UnObjectValue,

    // Default error
    #[error("An error occurred")]
    DefaultError,
}
