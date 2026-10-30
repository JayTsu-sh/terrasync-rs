//! 需要实验室提供两个同名、内容不同的版本，验证恢复路径不会读取最新版本。
use std::{path::Path, sync::Arc};
use storage_adapter::{CopyOptions, CreateStorageOptions, EntryEnum, ResumeContext, StorageEnum, create_storage};
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
    let EntryEnum::S3(ref mut object) = entry else {
        return Err("expected S3 entry".into());
    };
    object.version_id = Some(std::env::var("LAB_S3_OLD_VERSION")?);
    object.is_latest = false;
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
