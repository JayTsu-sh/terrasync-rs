//! 检查点计数恢复与目录重放的公共统计回归。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::{
    config::{ConsoleConfig, ConsumerConfig, JobType},
    consumer::{ConsumerManager, stats::ProgressDetail},
    storage_factory::{StorageRole, create_storage_for_role},
};
use data_mover::{
    model::{BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath},
    traversal::EntryKindCounts,
};
use db::DatabaseConfig;
use std::sync::Arc;

async fn manager() -> Result<ConsumerManager> {
    let config = ConsumerConfig {
        job_id: "accounting".into(),
        job_dir: String::new(),
        job_type: JobType::Scan,
        db_config: DatabaseConfig {
            enabled: false,
            db_type: "clickhouse".into(),
            batch_size: 256,
            clickhouse: None,
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

fn directory(path: &str, count: u64, revision: u64, complete: bool) -> DirectoryStatistics {
    let counts = EntryKindCounts {
        regular_files: count,
        ..Default::default()
    };
    DirectoryStatistics {
        path: StoragePath::new(path).unwrap(),
        pass_start: revision == 1,
        revision,
        scanned: counts,
        filtered: EntryKindCounts::default(),
        retained: counts,
        failed: EntryKindCounts::default(),
        vanished: EntryKindCounts::default(),
        pruned_dirs: 0,
        truncated_dirs: 0,
        regular_file_bytes: count * 10,
        unknown_file_sizes: 0,
        versions: 0,
        delete_markers: 0,
        all_versions_bytes: 0,
        outcome: complete.then_some(ListingOutcome::Complete),
        vanished_listing: false,
    }
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn checkpoint_200_interrupted_300_resumes_to_1000_without_counting_replay() -> Result<()> {
    let original = manager().await?;
    let statistics = original.scan_statistics();
    let mut ledger = DirectoryLedger::new(None, 1).await?;
    let confirmed = directory("", 200, 1, false);
    ledger.push(confirmed.clone(), &statistics).await?;
    ledger.flush(&statistics).await?;
    let checkpoint = original.statistics_checkpoint().await?;
    ledger.push(directory("", 300, 2, false), &statistics).await?;
    ledger.flush(&statistics).await?;
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 300);
    drop(ledger);
    drop(original);

    let resumed = manager().await?;
    resumed.restore_statistics(&checkpoint).await?;
    let statistics = resumed.scan_statistics();
    assert_eq!(statistics.snapshot().await.unwrap().scanned_entries, 200);
    assert!(matches!(
        statistics.raw_progress().await.unwrap().detail,
        ProgressDetail::Full { scanned_files: 200, .. }
    ));
    // 原始交付流绝不能修改任何扫描标量；计数只能来自目录确认事实。
    let backend = BackendIdentity::new(BackendKind::Local, "replay-fixture").unwrap();
    let identity = SourceIdentity::new(backend, IdentityStrength::PathScoped, b"file").unwrap();
    let entry = ObservedEntry::new(
        StoragePath::new("file").unwrap(),
        EntryKind::File,
        Some(10),
        None,
        identity,
    )
    .unwrap();
    resumed.record_observations(&vec![Arc::new(entry); 1000]).await;
    let durable: serde_json::Value = serde_json::from_str(&resumed.statistics_checkpoint().await?)?;
    assert_eq!(durable["stats"]["Full"]["regular_file_count"].as_u64(), Some(200));
    // 模拟从持久库加载已确认的目录贡献；本地 SQLite 已完全丢失。
    let mut ledger = DirectoryLedger::new(None, 2).await?;
    sqlx::query("INSERT INTO contributions VALUES ('',?,1,0,0)")
        .bind(encode(&ScanCounts::from_directory(&confirmed))?)
        .execute(&mut ledger.connection)
        .await?;
    ledger.push(directory("", 300, 1, false), &statistics).await?;
    ledger.flush(&statistics).await?;
    assert_eq!(
        statistics.snapshot().await.unwrap().scanned_entries,
        200,
        "重放未完整重列的目录时冻结已确认贡献"
    );
    ledger.push(directory("", 1000, 2, true), &statistics).await?;
    ledger.flush(&statistics).await?;
    let snapshot = statistics.snapshot().await.unwrap();
    assert_eq!(snapshot.scanned_entries, 1000);
    assert_eq!(snapshot.retained_entries, 1000);
    assert_eq!(snapshot.filtered_entries, 0);
    assert_eq!(snapshot.counts.regular_file_bytes, 10_000);
    assert!(matches!(
        statistics.raw_progress().await.unwrap().detail,
        ProgressDetail::Full {
            scanned_files: 1000,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn replayed_completed_subdirectories_replace_and_filtered_pages_remain_visible() -> Result<()> {
    let manager = manager().await?;
    let stats = manager.scan_statistics();
    let mut ledger = DirectoryLedger::new(None, 1).await?;
    ledger.push(directory("a", 400, 1, true), &stats).await?;
    ledger.push(directory("b", 600, 1, true), &stats).await?;
    ledger.flush(&stats).await?;
    ledger.push(directory("a", 200, 1, false), &stats).await?;
    ledger.flush(&stats).await?;
    assert_eq!(stats.snapshot().await.unwrap().scanned_entries, 1000);
    ledger.push(directory("a", 400, 2, true), &stats).await?;
    let mut filtered = directory("excluded-files", 5, 1, true);
    filtered.filtered = filtered.retained;
    filtered.retained = EntryKindCounts::default();
    filtered.regular_file_bytes = 0;
    ledger.push(filtered, &stats).await?;
    ledger.flush(&stats).await?;
    let snapshot = stats.snapshot().await.unwrap();
    assert_eq!(
        (
            snapshot.scanned_entries,
            snapshot.filtered_entries,
            snapshot.retained_entries
        ),
        (1005, 5, 1000)
    );
    assert_eq!(snapshot.counts.completed_listings, 3);
    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn invalid_revision_does_not_change_the_published_snapshot() -> Result<()> {
    let manager = manager().await?;
    let stats = manager.scan_statistics();
    let mut ledger = DirectoryLedger::new(None, 1).await?;
    ledger.push(directory("a", 10, 1, false), &stats).await?;
    ledger.flush(&stats).await?;
    ledger.push(directory("a", 99, 3, false), &stats).await?;
    ledger.push(directory("a", 200, 2, true), &stats).await?;
    assert!(ledger.flush(&stats).await.is_err());
    assert_eq!(stats.snapshot().await.unwrap().scanned_entries, 10);
    Ok(())
}
