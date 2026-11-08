//! 实际提交后丢确认，也只能恢复同一份目录事实；关闭合并避免 FINAL 掩盖重复写。
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Row;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::Fixture;
use crate::execution::base_client;
use crate::execution::queries::{ScanQueryCancellation, with_scan_cancellation};
use crate::migration_fixtures::request;
use crate::observed_fixtures::observation;
use crate::scan::rows::NativeScanRecord;
use crate::{DatabaseError, DatabaseExecutionMonitor, Result};
use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory};
use task_model::observation::ObservedItem;
use task_model::{CopyCompletion, CopyReadiness};

#[derive(Deserialize)]
struct ProxyStats {
    writes: u64,
    lost: u64,
    tokens: u64,
}

#[derive(Deserialize, Row)]
struct SavedFact {
    version_id: String,
    copy_sequence: u64,
    copy_execution_id: String,
    current_state: u8,
    copy_target_version: Option<String>,
    copy_path_confirmed: bool,
    copy_data_confirmed: bool,
    source_version_id: String,
    entry_snapshot: Vec<u8>,
}

async fn stats(fixture: &Fixture) -> Result<ProxyStats> {
    let mut stream = TcpStream::connect(&fixture.control).await?;
    stream
        .write_all(format!("GET /batch_stats HTTP/1.0\r\nHost: {}\r\n\r\n", fixture.control).as_bytes())
        .await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    assert!(reply.starts_with(b"HTTP/1.0 200"));
    let offset = reply
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .ok_or_else(|| DatabaseError::ConfigError("missing owned proxy headers".into()))?;
    Ok(serde_json::from_slice(&reply[offset + 4..])?)
}

fn completions() -> Result<Vec<CopyCompletion>> {
    let versions: Vec<_> = (1..=3)
        .map(|index| {
            observation(
                BackendKind::S3,
                "payload".into(),
                EntryKind::File,
                index,
                [11; 3],
                None,
                None,
                Some(vec![("raw".into(), format!("tag-{index}"))]),
                Some((Some(format!("v{index}")), index == 3, false)),
            )
        })
        .collect();
    let records = versions
        .iter()
        .flat_map(|entry| {
            entry
                .history()
                .into_iter()
                .flat_map(|history| history.records().iter().cloned())
        })
        .collect();
    let entry = versions.last().ok_or(DatabaseError::CopyExecutionUnavailable)?;
    let entry = Arc::new(
        entry
            .clone()
            .with_history(Some(Box::new(ObjectHistory::whole(records, HistoryOrder::Server)?)))?,
    );
    Ok(ObservedItem::all(&entry)
        .enumerate()
        .map(|(index, item)| CopyCompletion {
            target_version: Some(format!("target-{}", item.view().version_id())),
            item,
            readiness: if index % 2 == 0 {
                CopyReadiness::DataOnly
            } else {
                CopyReadiness::Ready
            },
        })
        .collect())
}

async fn case(fixture: &Fixture, history: bool, mode: &str) -> Result<()> {
    let job = format!("batch_{history}_{mode}");
    let mut database = fixture.open(&job).await?;
    let mut descriptor = request("batch-recovery", true);
    descriptor.binding.target_history = history;
    database.begin_copy_generation(&descriptor).await?;
    let generation = database.current_copy_generation().await?;
    let working = database.get_cached_scan_state().await?;
    let physical = database.catalogue_storage_table(&format!("base_{job}"))?;
    database
        .execute(
            &format!("ALTER TABLE {physical} MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool=1"),
            &[],
        )
        .await?;
    let cancellation = ScanQueryCancellation::new(&fixture.config, fixture.config.cluster_name()?, &job)?;
    database.sync_client = cancellation.data_client(&fixture.config)?;
    database.async_client = cancellation.data_client(&fixture.config)?;
    let execution = cancellation.execution();
    let watch = DatabaseExecutionMonitor::start(cancellation.clone(), CancellationToken::new())?;
    fixture
        .fault(if mode == "cancel" { "batch_stall" } else { mode })
        .await?;
    let stop = cancellation.clone();
    let cancelling = mode == "cancel";
    let watcher = tokio::spawn(async move {
        if cancelling {
            sleep(Duration::from_millis(250)).await;
            stop.begin_cancellation();
        }
    });
    let records = completions()?;
    let start = Instant::now();
    let result = timeout(
        Duration::from_secs(6),
        with_scan_cancellation(
            CancellationToken::new(),
            Some(execution),
            database.batch_insert_copy_completions(&records),
        ),
    )
    .await;
    let elapsed = start.elapsed();
    watcher.await?;
    watch.finish().await?;
    let observed = stats(fixture).await?;
    fixture.fault("normal").await?;
    let result = result.map_err(|_| DatabaseError::ConfigError("copy batch exceeded owned deadline".into()))?;
    let recovered = !matches!(mode, "batch_disconnect" | "batch_stall" | "batch_reject" | "cancel");
    if recovered {
        result?;
    } else {
        assert!(result.is_err());
    }
    let client = base_client(&fixture.config)?;
    let rows = client.query(&format!("SELECT version_id,copy_sequence,copy_execution_id,current_state,copy_target_version,copy_path_confirmed,copy_data_confirmed,source_version_id,entry_snapshot FROM base_{job}"))
        .fetch_all::<SavedFact>().await?;
    assert_eq!(rows.len(), if recovered { if history { 3 } else { 1 } } else { 0 });
    for row in rows {
        let expected = records
            .iter()
            .find(|item| item.item.view().version_id() == row.source_version_id)
            .ok_or_else(|| DatabaseError::ConfigError("unexpected recovered source version".into()))?;
        assert_eq!(
            row.version_id,
            if history { row.source_version_id.as_str() } else { "" }
        );
        assert_eq!(row.copy_sequence, generation.sequence);
        assert_eq!(row.copy_execution_id, descriptor.execution_id);
        assert_eq!(row.current_state, working);
        assert_eq!(row.copy_target_version, expected.target_version);
        assert_eq!(row.copy_path_confirmed, expected.readiness == CopyReadiness::Ready);
        assert!(row.copy_data_confirmed);
        assert_eq!(
            row.entry_snapshot,
            NativeScanRecord::capture(expected.item.view(), working)?
                .entry_snapshot
                .as_bytes()
        );
    }
    assert_eq!(observed.tokens, 1, "every retry must reuse its original batch token");
    if mode == "batch_reject" {
        assert_eq!(observed.writes, 1);
        assert!(elapsed < Duration::from_secs(1));
    }
    if mode == "batch_lost" {
        assert_eq!(observed.lost, 1);
    }
    if matches!(mode, "batch_flash" | "batch_lost" | "batch_recover_stall") {
        assert!(observed.writes >= 2);
    }
    if matches!(mode, "batch_disconnect" | "batch_stall") {
        assert!((Duration::from_millis(2900)..Duration::from_millis(4500)).contains(&elapsed));
    }
    if cancelling {
        assert!(elapsed < Duration::from_secs(1));
    }
    eprintln!(
        "copy-batch cluster={} history={history} mode={mode} recovered={recovered} elapsed_ms={} attempts={} tokens={} lost_acks={} raw_rows_checked=true frozen_facts_checked=true",
        fixture.config.cluster.is_some(),
        elapsed.as_millis(),
        observed.writes,
        observed.tokens,
        observed.lost
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires --catalogue-recovery owned ClickHouse pair and fault proxy"]
async fn real_copy_batches_recover_with_frozen_facts() -> Result<()> {
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            for history in [false, true] {
                for mode in [
                    "normal",
                    "batch_flash",
                    "batch_lost",
                    "batch_recover_stall",
                    "batch_disconnect",
                    "batch_stall",
                    "batch_reject",
                    "cancel",
                ] {
                    case(&fixture, history, mode).await?;
                }
            }
            Ok(())
        }
        .await;
        fixture.fault("normal").await?;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}
