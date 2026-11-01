//! 检查点扫描：先确认结果与事件持久化，再将 opaque 游标写入 ClickHouse。
use db::DatabaseConfig;
use std::{
    fs::OpenOptions,
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use self::cancellation::ScanCancellation;
use data_mover::model::ObservedEntry;
use data_mover::traversal::Resumable;
use db::clickhouse::scan_resume::{COMPLETED, RUNNING, ScanResumeState};
use db::scan_store::{ScanFinalEvent, ScanStore};
#[cfg(feature = "license")]
use licensing::verify::quick_verify;
use storage_adapter::filter::parse_filter_expression;
use storage_adapter::{ErrorEvent, StorageEntryMessage, StorageEnum, WalkOptions, canonicalize_path};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use utils::app_config::AppConfig;

use crate::{
    config::{JobType, initialize_consumer_config},
    consumer::{ConsumerManager, ScanStatistics},
    error::{AppError, Result},
    storage_factory::{StorageRole, create_storage_for_role},
};

mod cancellation;
mod input;
mod pipeline;
mod workers;

#[allow(clippy::too_many_arguments, clippy::ref_option)]
pub async fn scan_with_resume(
    job_id: &str, job_dir: &str, path: &str, depth: u32, include: &Option<String>, exclude: &Option<String>,
    command: String, cancel: CancellationToken,
) -> Result<()> {
    scan_with_resume_and_progress(job_id, job_dir, path, depth, include, exclude, command, cancel, None).await
}

#[allow(clippy::too_many_arguments, clippy::ref_option)]
pub async fn scan_with_resume_and_progress(
    job_id: &str, job_dir: &str, path: &str, depth: u32, include: &Option<String>, exclude: &Option<String>,
    command: String, cancel: CancellationToken, callback_url: Option<String>,
) -> Result<()> {
    scan_with_resume_settings(
        job_id,
        job_dir,
        path,
        depth,
        include,
        exclude,
        command,
        cancel,
        callback_url,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::ref_option)]
pub async fn scan_with_resume_settings(
    job_id: &str, job_dir: &str, path: &str, depth: u32, include: &Option<String>, exclude: &Option<String>,
    command: String, cancel: CancellationToken, callback_url: Option<String>, concurrency: Option<usize>,
) -> Result<()> {
    #[cfg(feature = "license")]
    if let Ok(license) = licensing::get_global_license() {
        quick_verify(license)?;
    }
    let mut config = AppConfig::fetch()?;
    if let Some(value) = concurrency {
        if value == 0 {
            return Err(AppError::ConfigError("scan concurrency must be positive".into()));
        }
        config.scan.concurrency = value;
    }
    info!(concurrency = config.scan.concurrency, "Using scan task concurrency");
    if !config.database.enabled || config.database.r#type != "clickhouse" {
        return Err(AppError::ConfigError(
            "scan --resume requires enabled ClickHouse storage".into(),
        ));
    }
    tokio::fs::create_dir_all(job_dir).await?;
    let lease = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(Path::new(job_dir).join("scan-resume.lock"))?;
    lease.try_lock().map_err(|source| AppError::ScanJobLock {
        job_id: job_id.into(),
        source,
    })?;
    let source = canonicalize_path(path)?;
    let storage = select! {
        biased;
        _=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
        storage=create_storage_for_role(&source, None, false, StorageRole::Source)=>storage?,
    };
    let options = WalkOptions {
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
    )?;
    let database_config = DatabaseConfig::from_app_config(&config.database);
    let mut database = ScanStore::new(
        database_config
            .clickhouse
            .as_ref()
            .ok_or_else(|| AppError::ConfigError("ClickHouse configuration missing".into()))?,
        config.database.clickhouse.cluster.as_deref(),
        job_id,
    )?;
    let queries = database.query_cancellation();
    let cancellation = ScanCancellation::new(cancel.clone());
    cancellation::scope(queries, cancellation.clone(), async {
        cancellation.check()?;
        cancellation.operation(database.ping()).await?;
        cancellation.check()?;
        let old = cancellation.operation(database.load_resume_state()).await?;
        cancellation.check()?;
        let mut already_committed = false;
        let mut state = if let Some(state) = old.as_ref().filter(|state| state.active()) {
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
                if let Err(error) = cancellation.operation(database.cleanup_resume_batch()).await {
                    warn!(%error, "Completed scan; deferred batch-table cleanup");
                }
                info!("Recovered the previous scan's completed generation commit");
                already_committed = true;
            }
            info!(
                entries = state.observed_entries,
                checkpoint = !state.checkpoint.is_empty(),
                "Resuming scan from acknowledged ClickHouse progress"
            );
            state.clone()
        } else {
            let incremental = cancellation.operation(database.has_baseline()).await?;
            cancellation.operation(database.initialize()).await?;
            let generation = cancellation.operation(database.begin_scan_generation()).await?;
            let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
            let event_time = cancellation.operation(database.resume_event_epoch(now)).await?;
            let mut fresh = ScanResumeState {
                id: 1,
                schema_version: 1,
                version: old.as_ref().map_or(0, |value| value.version),
                request_key,
                generation,
                incremental,
                phase: RUNNING,
                event_time,
                observed_entries: 0,
                checkpoint: Vec::new(),
                statistics: String::new(),
            };
            fresh.advance()?;
            cancellation.operation(database.save_resume_state(&fresh)).await?;
            info!("Starting a scan with durable ClickHouse checkpoints");
            fresh
        };
        if state.incremental {
            cancellation.operation(database.prepare_incremental()).await?;
        }
        let kind = if state.incremental {
            JobType::IncrementalScan
        } else {
            JobType::Scan
        };
        let mut consumer_config = initialize_consumer_config(job_id, job_dir, kind, command, &config, callback_url)?;
        // 本管道负责数据库持久化确认；manager 只消费统计信息。
        consumer_config.db_config.enabled = false;
        let mut consumers = ConsumerManager::new(&consumer_config).await?;
        // 续扫展示上次确认检查点的累计统计，再叠加本次实际扫描流；整轮完成后校准去重。
        consumers.restore_statistics(&state.statistics).await?;
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
                cancellation.child(),
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
        if cancel.is_cancelled() {
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
) -> Result<String> {
    let bytes = serde_json::to_vec(&(1, kind, identity, depth, include, exclude, tags))?;
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
    consumers: &ConsumerManager, batch_size: usize, cancel: ScanCancellation,
) -> Result<()> {
    if state.phase == RUNNING {
        let source = storage.resumable_scan(
            options,
            (!state.checkpoint.is_empty()).then(|| state.checkpoint.clone()),
            cancel.token().clone(),
            checkpoint_interval(),
        )?;
        pipeline::consume(source, database, state, consumers, batch_size, cancel.clone()).await?;
    }
    cancel.check()?;
    if state.incremental {
        cancel
            .operation(finalize_events(database, state, consumers, batch_size, &cancel))
            .await?;
    }
    cancel.check()?;
    consumers
        .reconcile_scan_statistics(
            cancel
                .operation(database.resume_statistics(state.generation, state.incremental.then_some(state.event_time)))
                .await?,
        )
        .await;
    cancel.check()?;
    state.statistics = consumers.statistics_checkpoint().await?;
    state.advance()?;
    cancel.operation(database.save_resume_state(state)).await?;
    cancel.check()?;
    cancel.operation(database.commit_scan_generation()).await?;
    cancel.check()?;
    state.statistics = consumers.statistics_checkpoint().await?;
    state.phase = COMPLETED;
    state.checkpoint.clear();
    state.advance()?;
    cancel.operation(database.save_resume_state(state)).await?;
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
    sort_batch(batch);
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
        database.write_observations(batch, false).await?;
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
        let key = request_key("local", "root", 0, &None, &None, false)?;
        assert_eq!(key.len(), 64);
        assert_ne!(key, request_key("local", "other", 0, &None, &None, false)?);
        assert_ne!(key, request_key("local", "root", 1, &None, &None, false)?);
        assert_ne!(
            key,
            request_key("local", "root", 0, &Some("type==file".into()), &None, false)?
        );
        assert_ne!(key, request_key("local", "root", 0, &None, &None, true)?);
        Ok(())
    }
}

/// 清理集群中一个扫描作业的数据分区，保留共享表和其他作业。
pub async fn clear_cluster_scan(job_id: &str) -> Result<()> {
    let config = AppConfig::fetch()?;
    let cluster = config
        .database
        .clickhouse
        .cluster
        .as_deref()
        .ok_or_else(|| AppError::ConfigError("ClickHouse cluster is not configured".into()))?;
    let db_config = DatabaseConfig::from_app_config(&config.database);
    db::clear_cluster_scan_data(
        db_config
            .clickhouse
            .as_ref()
            .ok_or_else(|| AppError::ConfigError("ClickHouse configuration missing".into()))?,
        cluster,
        job_id,
    )
    .await?;
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
        config.database.clickhouse.cluster.as_deref(),
        job_id,
    )?;
    Ok(store.resume_execution_is_incremental().await?)
}
