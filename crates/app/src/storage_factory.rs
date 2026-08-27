use data_mover::{CreateStorageOptions, StorageEnum};

pub(crate) async fn create_storage(
    path: &str, block_size: Option<u64>, ensure_dir: bool,
) -> data_mover::Result<StorageEnum> {
    data_mover::create_storage(path, CreateStorageOptions::new(block_size, ensure_dir)).await
}
