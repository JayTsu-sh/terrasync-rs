//! 需要实验室提供两个同名、内容不同的版本，验证恢复路径不会读取最新版本。
#[path = "../../../tests/support/observed.rs"]
mod observed_fixtures;
use task_model::observation::ObservedItem;
use tempfile::TempDir;
use {
    storage_adapter::{CopyOptions, CreateStorageOptions, StorageContext, WalkOptions, create_storage},
    task_model::{StorageEntryMessage, TransferMode},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires LAB_S3_TEST_URL and LAB_S3_OLD_VERSION; payload v1 is 9 MiB + 137 bytes of A"]
async fn checkpointed_copy_reads_requested_historical_version() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_S3_TEST_URL")?,
        CreateStorageOptions::new(Some(4 * 1024 * 1024), false),
    )
    .await?;
    let destination = TempDir::new()?;
    let target = create_storage(
        destination.path().to_str().ok_or("non UTF-8 test root")?,
        CreateStorageOptions::new(Some(4 * 1024 * 1024), false),
    )
    .await?;
    let old_version = std::env::var("LAB_S3_OLD_VERSION")?;
    let scan = source.walkdir(None, WalkOptions::default()).await?;
    let mut selected = None;
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(entry) = message
            && entry.path().as_str() == "payload"
            && let Some(index) = entry.history().and_then(|history| {
                history
                    .records()
                    .iter()
                    .position(|record| record.version_id() == Some(old_version.as_str()))
            })
        {
            selected = Some(ObservedItem {
                entry,
                version_index: Some(index),
            });
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    let selected = selected.ok_or("requested source version was not listed")?;
    StorageContext::copy_observed_item(
        &source,
        &target,
        &selected,
        &CopyOptions {
            transfer_mode: TransferMode::Checkpoint,
            is_source_reserved: true,
            enable_integrity_check: true,
            ..Default::default()
        },
        None,
    )
    .await?;
    assert_eq!(
        tokio::fs::read(destination.path().join("payload")).await?,
        vec![b'A'; 9 * 1024 * 1024 + 137]
    );
    Ok(())
}
