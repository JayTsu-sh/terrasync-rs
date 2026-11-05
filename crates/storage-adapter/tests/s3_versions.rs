//! 需要实验室提供两个同名、内容不同的版本，验证恢复路径不会读取最新版本。
#[path = "../../../tests/support/observed.rs"]
mod observed_fixtures;
use std::{path::Path, sync::Arc};
use storage_adapter::{CopyOptions, CreateStorageOptions, ResumeContext, StorageEnum, create_storage};
use tempfile::TempDir;

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
    let mut entry = source.get_metadata(Path::new("payload")).await?;
    let record = data_mover::model::VersionRecord::new(
        Some(std::env::var("LAB_S3_OLD_VERSION")?),
        false,
        data_mover::model::RecordKind::Object {
            size: 9 * 1024 * 1024 + 137,
            etag: "fixture".into(),
            modified: entry.modified(),
        },
        entry.source_identity().clone(),
        None,
    )?;
    entry = entry.with_history(Some(Box::new(data_mover::model::ObjectHistory::whole(
        vec![record],
        data_mover::model::HistoryOrder::Server,
    )?)))?;
    StorageEnum::copy_file_resumable(
        &source,
        &target,
        &entry,
        CopyOptions {
            is_source_reserved: true,
            enable_integrity_check: true,
            ..Default::default()
        },
        ResumeContext {
            part_relative_path: "ignored.part".into(),
            missing_intervals: Vec::new(),
            on_committed: Arc::new(|_, _| {}),
        },
    )
    .await?;
    assert_eq!(
        tokio::fs::read(destination.path().join("payload")).await?,
        vec![b'A'; 9 * 1024 * 1024 + 137]
    );
    Ok(())
}
