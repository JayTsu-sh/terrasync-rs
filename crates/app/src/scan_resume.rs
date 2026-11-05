//! 检查点扫描：先确认结果与事件持久化，再将 opaque 游标写入 ClickHouse。
use crate::job_execution::JobLease;
use db::DatabaseConfig;
use std::{
    mem::take,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::scan::{
    ScanRequest,
    cancellation::{self, ScanCancellation},
};
use data_mover::model::ObservedEntry;
use data_mover::traversal::{Resumable, TraversalOrder};
use db::scan::ScanResumeStatistics;
use db::scan::{COMPLETED, FINALIZING, RUNNING, ScanResumeState};
use db::scan::{ScanFinalEvent, ScanStore};
use storage_adapter::filter::parse_filter_expression;
use storage_adapter::{ErrorEvent, ScanSourceMode, StorageEntryMessage, StorageEnum, WalkOptions, canonicalize_path};
use tokio::{join, select, task::spawn_blocking};
use tracing::{info, warn};
use utils::app_config::AppConfig;

use crate::{
    config::{JobType, initialize_consumer_config},
    consumer::{ConsumerManager, ScanStatistics},
    error::{AppError, Result},
    storage_factory::{StorageRole, create_storage_for_role},
};

mod input;
mod pipeline;
mod workers;

pub(crate) async fn run_persisted(request: ScanRequest<'_>, config: AppConfig) -> Result<()> {
    let ScanRequest {
        job_id,
        job_dir,
        path,
        depth,
        include,
        exclude,
        command,
        cancel,
        callback_url,
        resume,
        ..
    } = request;
    info!(concurrency = config.scan.concurrency, "Using scan task concurrency");
    if !config.database.enabled || config.database.r#type != "clickhouse" {
        return Err(AppError::ConfigError(
            "Persisted scanning requires enabled ClickHouse storage".into(),
        ));
    }
    tokio::fs::create_dir_all(job_dir).await?;
    let _lease = JobLease::acquire(&config.database.clickhouse, job_id, job_dir)?;
    let source = canonicalize_path(path)?;
    let storage = select! {
        biased;
        _=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
        storage=create_storage_for_role(&source, None, false, StorageRole::Source)=>storage?,
    };
    let options = WalkOptions {
        operation_timeout: Some(Duration::from_secs(config.scan.operation_timeout_secs)),
        recovery_window: Some(Duration::from_secs(config.scan.recovery_window_secs)),
        depth: NonZeroUsize::new(depth as usize).map(NonZeroUsize::get),
        match_expressions: include.as_deref().map(parse_filter_expression).transpose()?,
        exclude_expressions: exclude.as_deref().map(parse_filter_expression).transpose()?,
        concurrency: config.scan.concurrency,
        include_tags: config.scan.include_tags,
        ..Default::default()
    };
    // 只持久化源身份和有效扫描选项的摘要，不保存凭据。
    let identity = storage.storage().identity();
    let request_key = request_key(
        identity.kind().as_str(),
        identity.stable_id(),
        depth,
        include,
        exclude,
        options.include_tags,
        resume,
    )?;
    let database_config = DatabaseConfig::from_app_config(&config.database);
    let mut database = ScanStore::new(
        database_config
            .clickhouse
            .as_ref()
            .ok_or_else(|| AppError::ConfigError("ClickHouse configuration missing".into()))?,
        job_id,
    )?;
    let queries = database.query_cancellation();
    let cancellation = ScanCancellation::new(cancel.clone());
    cancellation::scope(queries, cancellation.clone(), async {
        cancellation.check()?;
        cancellation.operation(database.ping()).await?;
        cancellation.check()?;
        let mut old = cancellation.operation(database.load_resume_state()).await?;
        cancellation.check()?;
        let mut already_committed = false;
        let mut state = if let Some(state) = old.as_ref().filter(|state| state.active() && resume) {
            state.validate(&request_key)?;
            if !cancellation.operation(database.results_exist()).await? {
                return Err(AppError::ScanError(
                    "Saved scan results are missing; refusing to resume".into(),
                ));
            }
            if cancellation
                .operation(database.restore_resume_generation(state))
                .await?
            {
                let mut completed = state.clone();
                completed.phase = COMPLETED;
                completed.checkpoint.clear();
                completed.advance()?;
                cancellation.operation(database.save_resume_state(&completed)).await?;
                cancellation.operation(database.cleanup_scan_snapshots(&completed)).await?;
                if let Err(error) = cancellation.operation(database.cleanup_resume_batch()).await {
                    warn!(%error, "Completed scan; deferred batch-table cleanup");
                }
                info!("Recovered the previous scan's completed generation commit");
                already_committed = true;
            } else {
                cancellation.operation(database.restore_scan_checkpoint(state)).await?;
            }
            info!(
                entries = state.observed_entries,
                checkpoint = !state.checkpoint.is_empty(),
                "Resuming scan from acknowledged ClickHouse progress"
            );
            state.clone()
        } else {
            if let Some(state) = old.as_mut().filter(|state| state.active()) {
                let committed = state.phase == FINALIZING && cancellation.operation(database.restore_resume_generation(state)).await?;
                if committed {
                    state.phase = COMPLETED;
                    state.checkpoint.clear();
                    state.advance()?;
                    cancellation.operation(database.save_resume_state(state)).await?;
                } else {
                    cancellation.operation(database.restore_scan_baseline(state)).await?;
                }
            }
            let incremental = cancellation.operation(database.has_baseline()).await?;
            cancellation.operation(database.initialize()).await?;
            cancellation.operation(database.prepare_incremental()).await?;
            cancellation.operation(database.prepare_native_schema()).await?;
            let baseline_snapshot = cancellation.operation(database.capture_scan_baseline()).await?;
            let generation = cancellation.operation(database.begin_scan_generation()).await?;
            // begin 可能写入旧轮次的 ABANDONED manifest；新轮版本必须严格超过它。
            let previous = cancellation.operation(database.load_resume_state()).await?;
            let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
            let event_time = cancellation.operation(database.resume_event_epoch(now)).await?;
            let mut fresh = ScanResumeState {
                id: 1,
                schema_version: 1,
                version: previous.as_ref().map_or(0, |value| value.version),
                request_key,
                generation,
                incremental,
                phase: RUNNING,
                event_time,
                observed_entries: 0,
                checkpoint: Vec::new(),
                statistics: String::new(),
                checkpoint_snapshot: baseline_snapshot.clone(),
                baseline_snapshot,
            };
            fresh.advance()?;
            cancellation.operation(database.save_resume_state(&fresh)).await?;
            cancellation.operation(database.cleanup_scan_snapshots(&fresh)).await?;
            info!("Starting a scan with durable ClickHouse checkpoints");
            fresh
        };
        if state.incremental {
            cancellation.operation(database.prepare_incremental()).await?;
        }
        cancellation.operation(database.prepare_native_schema()).await?;
        let kind = if state.incremental {
            JobType::IncrementalScan
        } else {
            JobType::Scan
        };
        let mut consumer_config = initialize_consumer_config(job_id, job_dir, kind, command, &config, callback_url)?;
        // 本管道负责数据库持久化确认；manager 只消费统计信息。
        consumer_config.db_config.enabled = false;
        let mut consumers = ConsumerManager::new(&consumer_config).await?;
        // 累计数与目录贡献必须来自同一已确认检查点；旧检查点无法补出过滤计数。
        consumers.restore_statistics(&state.statistics).await?;
        if (!state.checkpoint.is_empty() || state.observed_entries > 0)
            && !consumers.scan_statistics().snapshot().await.is_some_and(|scan| scan.valid_checkpoint(state.observed_entries)) {
            return Err(AppError::ScanError("Saved scan checkpoint lacks consistent directory statistics; reset this job's scan records before starting a new round".into()));
        }
        consumers
            .initialize_scan_statistics(&storage, options.include_tags, Some(state.event_time))
            .await?;
        consumers.begin_lifecycle().await;
        let result = if already_committed {
            Ok(())
        } else {
            run(
                &storage,
                &options,
                &mut database,
                &mut state,
                &consumers,
                config.database.batch_size.max(1) as usize,
                cancellation.clone(), resume,
            )
            .await
        };
        if let Err(error) = &result
            && !cancel.is_cancelled()
        {
            consumers
                .record_statistics([StorageEntryMessage::Error {
                    event: ErrorEvent::Scan,
                    path: Default::default(),
                    entry: None,
                    reason: error.to_string(),
                }])
                .await;
        }
        consumers.set_generation_committed(result.is_ok()).await;
        if cancellation.token().is_cancelled() {
            consumers.end_cancelled_lifecycle().await;
        } else {
            consumers.end_lifecycle().await;
        }
        result
    })
    .await
}

fn request_key(
    kind: &str, identity: &str, depth: u32, include: &Option<String>, exclude: &Option<String>, tags: bool,
    resume: bool,
) -> Result<String> {
    let bytes = serde_json::to_vec(&(2, kind, identity, depth, include, exclude, tags, resume))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn checkpoint_interval() -> Duration {
    #[cfg(feature = "lab-fault-injection")]
    if let Ok(value) = std::env::var("TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS")
        && let Ok(value) = value.parse::<u64>()
    {
        return Duration::from_millis(value);
    }
    Resumable::DEFAULT_EVERY
}

#[allow(clippy::too_many_arguments)]
async fn run(
    storage: &StorageEnum, options: &WalkOptions, database: &mut ScanStore, state: &mut ScanResumeState,
    consumers: &ConsumerManager, batch_size: usize, cancel: ScanCancellation, resume: bool,
) -> Result<()> {
    if state.phase == RUNNING {
        let mode = if resume {
            ScanSourceMode::Resumable {
                checkpoint: (!state.checkpoint.is_empty()).then(|| state.checkpoint.clone()),
                every: checkpoint_interval(),
            }
        } else {
            ScanSourceMode::Streaming {
                order: TraversalOrder::Unordered,
            }
        };
        let source = storage.scan_source(options, mode, cancel.token().clone())?;
        pipeline::consume(source, database, state, consumers, batch_size, cancel.clone(), resume).await?;
    }
    cancel.check()?;
    let statistics = final_statistics(database, state, consumers, batch_size, &cancel).await?;
    cancel.check()?;
    consumers.reconcile_scan_statistics(statistics).await;
    cancel.check()?;
    state.statistics = consumers.statistics_checkpoint().await?;
    state.advance()?;
    if state.incremental {
        cancel.operation(database.seal_resume_state(state)).await?;
    } else {
        // 全量汇总只读；源末屏障已确认 FINALIZING 快照，只更新统计 manifest。
        cancel.operation(database.save_resume_state(state)).await?;
    }
    cancel.check()?;
    cancel.operation(database.commit_scan_generation()).await?;
    cancel.check()?;
    state.statistics = consumers.statistics_checkpoint().await?;
    state.phase = COMPLETED;
    state.checkpoint.clear();
    state.advance()?;
    cancel.operation(database.save_resume_state(state)).await?;
    cancel.operation(database.cleanup_scan_snapshots(state)).await?;
    if let Err(error) = cancel.operation(database.cleanup_resume_batch()).await {
        warn!(%error, "Completed scan; deferred batch-table cleanup");
    }
    Ok(())
}

async fn flush_batch(
    database: &mut ScanStore, state: &ScanResumeState, consumers: &ScanStatistics, batch: &mut Vec<Arc<ObservedEntry>>,
    cancel: &ScanCancellation,
) -> Result<()> {
    cancel.check()?;
    if batch.is_empty() {
        return Ok(());
    }
    // 与 base 的主键顺序一致；排序只调整本批落盘顺序，不改变检查点推进边界。
    if batch.len() >= 4096 {
        // 大批次排序独占 CPU，将其移出 Tokio 的遍历与网络调度线程。
        let mut sorting = take(batch);
        let mut sorted = spawn_blocking(move || {
            sort_batch(&mut sorting);
            sorting
        });
        *batch = select! {
            biased;
            ()=cancel.cancelled()=>{
                // 排队中的计算可撤销，已经运行的计算仍等待退出。
                sorted.abort();
                let _=sorted.await;
                return Err(AppError::ObservationScanCancelled);
            }
            result=&mut sorted=>result?,
        };
    } else {
        sort_batch(batch);
    }
    cancel.check()?;
    let mut events = Vec::new();
    if state.incremental {
        database.begin_resume_batch().await?;
        cancel.check()?;
        database.write_observation_keys(batch).await?;
        cancel.check()?;
        events = database.detect_resume_batch_events(batch).await?;
        cancel.check()?;
        database.write_resume_events(&events, state.event_time).await?;
        cancel.check()?;
        #[cfg(feature = "lab-fault-injection")]
        if !events.is_empty() && std::env::var_os("TERRASYNC_LAB_SCAN_FAIL_AFTER_EVENTS").is_some() {
            return Err(AppError::ScanError(
                "Lab failure after durable events, before base update".into(),
            ));
        }
        database.write_candidate_observations(batch, &events).await?;
    } else {
        cancel.check()?;
        database.write_observations(batch, false).await?;
    }
    consumers.record_observed_events(&events).await;
    batch.clear();
    Ok(())
}

fn sort_batch(batch: &mut [Arc<ObservedEntry>]) {
    // 相同路径保留源交付顺序，保证重放或路径复用时最后观察到的记录生效。
    batch.sort_by(|a, b| a.path().as_str().cmp(b.path().as_str()));
}

async fn final_statistics(
    database: &ScanStore, state: &ScanResumeState, consumers: &ConsumerManager, batch_size: usize,
    cancel: &ScanCancellation,
) -> Result<ScanResumeStatistics> {
    if !state.incremental {
        return cancel
            .operation(database.resume_statistics(state.generation, None))
            .await;
    }
    // 所有观察已确认；最终化只删除旧代次，当前快照可并发只读汇总。两个分支均等待退出。
    let (events, statistics) = join!(
        cancel.operation(finalize_events(database, state, consumers, batch_size, cancel)),
        cancel.operation(database.resume_statistics(state.generation, None)),
    );
    events?;
    let mut statistics = statistics?;
    cancel.check()?;
    // 公共事件全部确认后再读本轮计数，包含中断前已落盘的最终化决定。
    statistics.events = cancel
        .operation(database.resume_event_statistics(state.event_time))
        .await?;
    Ok(statistics)
}

async fn finalize_events(
    database: &ScanStore, state: &ScanResumeState, consumers: &ConsumerManager, batch_size: usize,
    cancel: &ScanCancellation,
) -> Result<()> {
    let deleted = database.resolve_final_events(state.event_time).await?;
    let mut events = Vec::with_capacity(batch_size);
    for entry in deleted {
        cancel.check()?;
        events.push(entry);
        if events.len() >= batch_size {
            flush_final_events(database, state, consumers, &mut events, cancel).await?;
        }
    }
    flush_final_events(database, state, consumers, &mut events, cancel).await?;
    cancel.check()?;
    database.cleanup_candidates(state.event_time).await?;
    Ok(())
}

async fn flush_final_events(
    database: &ScanStore, state: &ScanResumeState, consumers: &ConsumerManager, events: &mut Vec<ScanFinalEvent>,
    cancel: &ScanCancellation,
) -> Result<()> {
    cancel.check()?;
    database.write_final_events(events, state.event_time).await?;
    cancel.check()?;
    let keys: Vec<_> = events.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    database.delete_resume_entries(&keys).await?;
    consumers.record_final_events(events).await;
    events.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_mover::model::{BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath};

    #[test]
    #[allow(clippy::unwrap_used)]
    fn batch_sort_preserves_delivery_order_when_a_path_is_replayed() {
        let backend = BackendIdentity::new(BackendKind::Nfs, "replayed-path").unwrap();
        let mut batch: Vec<_> = (0_u64..1000)
            .map(|index| {
                let path = if index % 2 == 0 { "a" } else { "b" };
                let identity = SourceIdentity::new(
                    backend.clone(),
                    IdentityStrength::StableWithinBackend,
                    index.to_le_bytes(),
                )
                .unwrap();
                Arc::new(
                    ObservedEntry::new(
                        StoragePath::new(path).unwrap(),
                        EntryKind::File,
                        Some(index),
                        None,
                        identity,
                    )
                    .unwrap(),
                )
            })
            .collect();
        sort_batch(&mut batch);
        for path in ["a", "b"] {
            let observed: Vec<_> = batch
                .iter()
                .filter(|entry| entry.path().as_str() == path)
                .map(|entry| entry.size().unwrap())
                .collect();
            assert!(
                observed.windows(2).all(|pair| pair[0] < pair[1]),
                "same-path observations changed order"
            );
        }
    }

    #[test]
    fn binds_source_depth_filters_and_tag_policy_but_not_secrets_in_plaintext() -> Result<()> {
        let key = request_key("local", "root", 0, &None, &None, false, true)?;
        assert_eq!(key.len(), 64);
        assert_ne!(key, request_key("local", "other", 0, &None, &None, false, true)?);
        assert_ne!(key, request_key("local", "root", 1, &None, &None, false, true)?);
        assert_ne!(
            key,
            request_key("local", "root", 0, &Some("type==file".into()), &None, false, true)?
        );
        assert_ne!(key, request_key("local", "root", 0, &None, &None, true, true)?);
        Ok(())
    }
}

/// 清理一个扫描作业的数据；表布局及其他作业隔离由数据库层负责。
pub async fn clear_scan(job_id: &str) -> Result<()> {
    let config = AppConfig::fetch()?;
    if !config.database.enabled {
        return Err(AppError::ConfigError("ClickHouse is not enabled".into()));
    }
    let store = ScanStore::new(&config.database.clickhouse, job_id)?;
    store.clear_job().await?;
    Ok(())
}

/// 根据持久化轮次识别执行类型，不能用作业目录是否存在来区分全量续扫和增量。
pub async fn scan_execution_is_incremental(job_id: &str) -> Result<bool> {
    let config = AppConfig::fetch()?;
    let database = DatabaseConfig::from_app_config(&config.database);
    let store = ScanStore::new(
        database
            .clickhouse
            .as_ref()
            .ok_or_else(|| AppError::ConfigError("ClickHouse configuration missing".into()))?,
        job_id,
    )?;
    Ok(store.resume_execution_is_incremental().await?)
}
