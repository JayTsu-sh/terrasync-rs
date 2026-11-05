use data_mover_model::ModelValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskModelError {
    #[error(transparent)]
    Observation(#[from] ModelValueError),
}

pub type Result<T> = std::result::Result<T, TaskModelError>;
