//! 检查点只保存完成前缀，半个目录的实际统计可以显示但不能进入持久累计。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::{
    config::{ConsoleConfig, ConsumerConfig, JobType},
    consumer::ConsumerManager,
    storage_factory::{StorageRole, create_storage_for_role},
};
use data_mover::{
    model::StoragePath,
    traversal::{Checkpoint, EntryKindCounts},
};
use db::ClickHouseConfig;
use utils::app_config::DatabaseConfig;

async fn manager() -> Result<ConsumerManager> {
    let config = ConsumerConfig {
        job_id: "prefix".into(),
        job_dir: String::new(),
        job_type: JobType::Scan,
        db_config: DatabaseConfig {
            enabled: false,
            batch_size: 256,
            clickhouse: ClickHouseConfig {
                mode: Default::default(),
                cluster: None,
                dsn: "http://127.0.0.1:8123".into(),
                dial_timeout: 1,
                read_timeout: 1,
                database: "default".into(),
                username: "default".into(),
                password: None,
            },
        },
        console_config: ConsoleConfig {
            raw_command_line: String::new(),
        },
        progress_callback_url: None,
    };
    let manager = ConsumerManager::new(&config).await?;
    let directory = tempfile::tempdir()?;
    let storage =
        create_storage_for_role(&directory.path().to_string_lossy(), None, false, StorageRole::Source).await?;
    manager
        .initialize_scan_statistics(&storage, false, Some(1_700_000_000_000_000_000))
        .await?;
    Ok(manager)
}
fn block(path: &str, count: u64, revision: u64, complete: bool) -> DirectoryStatistics {
    let kinds = EntryKindCounts {
        regular_files: count,
        ..Default::default()
    };
    DirectoryStatistics {
        path: StoragePath::new(path).unwrap(),
        pass_start: revision == 1,
        revision,
        scanned: kinds,
        retained: kinds,
        filtered: EntryKindCounts::default(),
        failed: EntryKindCounts::default(),
        vanished: EntryKindCounts::default(),
        pruned_dirs: 0,
        truncated_dirs: 0,
        regular_file_bytes: count * 10,
        unknown_file_sizes: 0,
        versions: 0,
        delete_markers: 0,
        all_versions_bytes: count * 10,
        outcome: complete.then_some(ListingOutcome::Complete),
        vanished_listing: false,
    }
}

#[tokio::test]
async fn live_300_checkpoint_200_and_resume_to_1000_preserve_new_errors_and_diagnostics() -> Result<()> {
    let manager = manager().await?;
    let statistics = manager.scan_statistics();
    let mut progress = ScanProgress::new(TraversalOrder::Blocks, ScanCounts::default(), None);
    progress.push(block("a", 200, 1, true), &statistics).await?;
    let prefix = progress.checkpoint_counts()?.clone();
    progress.confirm(vec![1, 2, 3]);
    progress.push(block("b", 100, 1, false), &statistics).await?;
    assert!(progress.checkpoint_counts().is_err());
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 300);
    let checkpoint = statistics.checkpoint_for_scan_prefix(&prefix).await?;
    let saved: serde_json::Value = serde_json::from_str(&checkpoint)?;
    assert_eq!(saved["stats"]["Full"]["regular_file_count"], 200);
    assert_eq!(saved["report"]["scan"]["scanned_entries"], 200);
    assert_eq!(
        statistics.snapshot().await.unwrap().scanned_entries,
        300,
        "serializing a prefix must not mutate live counters"
    );
    manager
        .record_task_failures(&["new diagnostic after checkpoint".into()])
        .await;
    let before: serde_json::Value = serde_json::from_str(&statistics.statistics_checkpoint().await?)?;
    progress
        .resumed(
            &TraversalResume {
                attempt: 2,
                checkpoint: Some(Checkpoint::from_bytes(vec![1, 2, 3].into())),
            },
            &statistics,
        )
        .await?;
    let after: serde_json::Value = serde_json::from_str(&statistics.statistics_checkpoint().await?)?;
    assert_eq!(before["callback"], after["callback"]);
    assert_eq!(
        before["stats"]["Full"]["error_stats"],
        after["stats"]["Full"]["error_stats"]
    );
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 200);
    progress.push(block("b", 100, 1, false), &statistics).await?;
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 300);
    progress.push(block("b", 800, 2, true), &statistics).await?;
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 1000);
    assert_eq!(progress.checkpoint_counts()?.scanned.total(), 1000);
    Ok(())
}

#[tokio::test]
async fn unordered_root_restart_drops_attempt_counts_and_does_not_offer_a_checkpoint() -> Result<()> {
    let manager = manager().await?;
    let statistics = manager.scan_statistics();
    let mut progress = ScanProgress::new(TraversalOrder::Unordered, ScanCounts::default(), None);
    progress.push(block("a", 100, 1, false), &statistics).await?;
    progress.push(block("b", 200, 1, false), &statistics).await?;
    progress.push(block("a", 150, 2, true), &statistics).await?;
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 350);
    assert!(progress.checkpoint_counts().is_err());
    progress
        .resumed(
            &TraversalResume {
                attempt: 2,
                checkpoint: None,
            },
            &statistics,
        )
        .await?;
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 0);
    progress.push(block("a", 500, 1, true), &statistics).await?;
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 500);
    assert_eq!(statistics.snapshot().await.unwrap().restarts, 1);
    Ok(())
}

#[tokio::test]
async fn recovery_marker_must_match_the_confirmed_cursor_and_order() -> Result<()> {
    let manager = manager().await?;
    let statistics = manager.scan_statistics();
    let mut progress = ScanProgress::new(TraversalOrder::Blocks, ScanCounts::default(), Some(vec![1]));
    assert!(
        progress
            .resumed(
                &TraversalResume {
                    attempt: 2,
                    checkpoint: None
                },
                &statistics
            )
            .await
            .is_err()
    );
    let marker = TraversalResume {
        attempt: 2,
        checkpoint: Some(Checkpoint::from_bytes(vec![2].into())),
    };
    assert!(progress.resumed(&marker, &statistics).await.is_err());
    let mut unordered = ScanProgress::new(TraversalOrder::Unordered, ScanCounts::default(), None);
    assert!(unordered.resumed(&marker, &statistics).await.is_err());
    progress.push(block("a", 10, 1, false), &statistics).await?;
    assert!(progress.finish().is_err());
    progress.push(block("a", 20, 2, true), &statistics).await?;
    progress.finish()
}
