use crate::{MismatchDataField, MismatchMetaField};
use data_mover::{model::FailureClass, storage::StorageRoleFailure};
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    Upstream(#[from] data_mover::error::StorageError),
    #[error("Storage operation error: {0}")]
    OperationError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),
    #[error("transfer cancelled by caller")]
    Cancelled,
    #[error("Data mismatch: {0:?}")]
    MismatchData(Vec<MismatchDataField>),
    #[error("Metadata mismatch: {0:?}")]
    MismatchMeta(Vec<MismatchMetaField>),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TaskJoinError(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Role(StorageRoleFailure),
    #[error(transparent)]
    Transfer(Box<data_mover::transfer::TransferFailure>),
    #[error(transparent)]
    Move(Box<data_mover::transfer::MoveFailure>),
    #[error(transparent)]
    Archive(Box<data_mover::tar_pack::TarFailure>),
}
impl From<StorageRoleFailure> for StorageError {
    fn from(value: StorageRoleFailure) -> Self {
        if let StorageRoleFailure::Entry(failure) = &value {
            match failure.class() {
                FailureClass::NotFound => return Self::FileNotFound(failure.path().as_str().into()),
                FailureClass::Cancelled => return Self::Cancelled,
                _ => {}
            }
        }
        Self::Role(value)
    }
}
impl From<data_mover::transfer::TransferFailure> for StorageError {
    fn from(value: data_mover::transfer::TransferFailure) -> Self {
        Self::Transfer(Box::new(value))
    }
}
impl From<data_mover::model::ModelValueError> for StorageError {
    fn from(value: data_mover::model::ModelValueError) -> Self {
        Self::InvalidPath(value.to_string())
    }
}
impl From<data_mover::storage::CapabilityUnavailable> for StorageError {
    fn from(value: data_mover::storage::CapabilityUnavailable) -> Self {
        Self::OperationError(value.to_string())
    }
}
pub type Result<T> = std::result::Result<T, StorageError>;
pub(crate) fn operation(error: impl std::fmt::Display) -> StorageError {
    StorageError::OperationError(error.to_string())
}

impl From<data_mover::transfer::MoveFailure> for StorageError {
    fn from(value: data_mover::transfer::MoveFailure) -> Self {
        Self::Move(Box::new(value))
    }
}
impl From<data_mover::tar_pack::TarFailure> for StorageError {
    fn from(value: data_mover::tar_pack::TarFailure) -> Self {
        Self::Archive(Box::new(value))
    }
}
