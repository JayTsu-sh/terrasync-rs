//! 真实 S3 请求计数由隔离代理独立核对，不能用本地观察替代协议读取证据。
use crate::migration_plan::MigrationPlan;
use crate::scan::source::WalkOptions;
use crate::storage_factory::{CreateStorageOptions, create_storage};
use data_mover::model::{ObservationMode, ObservationPlan};
use task_model::StorageEntryMessage;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires isolated real S3 and metadata request-counting proxy"]
async fn omitted_directory_object_metadata_does_not_read_denied_tags_or_hide_children() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_METADATA_SCOPE_SOURCE_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let root = tempfile::tempdir()?;
    let destination = create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let plan = MigrationPlan::compile(&source, &destination, false).await?;
    assert!(plan.scope.latest_version_only);
    assert!(!plan.scope.directory_object_output);
    let scan = source
        .walkdir(
            None,
            WalkOptions {
                observation_plan: Some(ObservationPlan::default().with_tags(ObservationMode::Required)),
                metadata_scope: plan.scope,
                ..Default::default()
            },
        )
        .await?;
    let mut names = Vec::new();
    while let Some(message) = scan.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => {
                if entry.path().as_str() == "a/" {
                    assert!(entry.metadata().tags().value().is_none());
                }
                if entry.path().as_str() == "empty" {
                    let facts = entry
                        .directory_marker_object()
                        .map(|marker| marker.metadata())
                        .or_else(|| {
                            entry
                                .history()
                                .and_then(|history| history.current())
                                .and_then(|record| record.metadata())
                        });
                    assert!(
                        facts.is_some_and(|metadata| metadata.tags().value().is_some()),
                        "{entry:?}"
                    );
                }
                names.push(entry.path().as_str().to_owned());
            }
            StorageEntryMessage::Error { path, reason, .. } => {
                return Err(format!("{}: {reason}", path.display()).into());
            }
            _ => {}
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    for expected in ["a/", "a/file", "empty"] {
        assert!(names.iter().any(|name| name == expected), "{names:?}");
    }
    println!("real_s3 omitted_content_metadata=true child_and_marker=preserved errors=0");
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated real 20-version S3 and metadata request-counting proxy"]
async fn latest_version_metadata_scope_keeps_all_history_without_reading_old_tags() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_METADATA_SCOPE_SOURCE_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let destination = create_storage(
        &std::env::var("LAB_MIGRATION_LATEST_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let plan = MigrationPlan::compile(&source, &destination, false).await?;
    assert!(plan.scope.latest_version_only);
    assert!(plan.scope.directory_object_output);
    let history_destination = create_storage(
        &std::env::var("LAB_MIGRATION_HISTORY_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    assert!(
        !MigrationPlan::compile(&source, &history_destination, false)
            .await?
            .scope
            .latest_version_only
    );
    let scan = source
        .walkdir(
            None,
            WalkOptions {
                observation_plan: Some(ObservationPlan::default().with_tags(ObservationMode::Required)),
                metadata_scope: plan.scope,
                ..Default::default()
            },
        )
        .await?;
    let mut versions = 0;
    let mut observed_tags = 0;
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(entry) = message {
            let history = entry.history().ok_or("fixture requires versioned objects")?;
            for record in history.records() {
                versions += 1;
                if record
                    .metadata()
                    .is_some_and(|metadata| metadata.tags().value().is_some())
                {
                    assert!(record.is_latest());
                    observed_tags += 1;
                }
            }
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    assert_eq!(versions, 20);
    assert_eq!(observed_tags, 1);
    println!("real_s3 source_history_records=20 observed_tags=1 old_version_metadata=omitted errors=0");
    Ok(())
}
