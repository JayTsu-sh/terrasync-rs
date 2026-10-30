//! 检查点扫描：先确认结果与事件持久化，再将 opaque 游标写入 ClickHouse。
use db::DatabaseConfig;
use std::{
    fs::OpenOptions,
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use data_mover::traversal::{Resumable, TraversalOutcome};
use db::clickhouse::scan_resume::{COMPLETED, FINALIZING, RUNNING, ScanResumeState};
use db::{ClickHouseDatabase, Database, DeletionStatus, INCREMENTAL_SCAN_TABLE_BASE_NAME};
#[cfg(feature = "license")]
use licensing::verify::quick_verify;
use storage_adapter::filter::parse_filter_expression;
use storage_adapter::{
    EntryEnum, ErrorEvent, ResumableScanItem, StorageEntryMessage, StorageEnum, WalkOptions, canonicalize_path,
};
#[cfg(feature = "lab-fault-injection")]
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use utils::app_config::AppConfig;

use crate::{
    broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY},
    config::{JobType, initialize_consumer_config},
    consumer::ConsumerManager,
    error::{AppError, Result},
    storage_factory::{StorageRole, create_storage_for_role},
};

#[allow(clippy::too_many_arguments, clippy::ref_option)]
pub async fn scan_with_resume(
    job_id: &str, job_dir: &str, path: &str, depth: u32, include: &Option<String>, exclude: &Option<String>,
    command: String, cancel: CancellationToken,
) -> Result<()> {
    #[cfg(feature = "license")]
    if let Ok(license) = licensing::get_global_license() {
        quick_verify(license)?;
    }
    let config = AppConfig::fetch()?;
    if !config.database.enabled || config.database.r#type != "clickhouse" {
        return Err(AppError::ConfigError(
            "scan --resume requires enabled ClickHouse storage".into(),
        ));
    }
    let lease = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(Path::new(job_dir).join("scan-resume.lock"))?;
    lease.try_lock().map_err(|source| AppError::ScanJobLock {
        job_id: job_id.into(),
        source,
    })?;
    let storage = create_storage_for_role(&canonicalize_path(path)?, None, false, StorageRole::Source).await?;
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
    let mut database = ClickHouseDatabase::new(
        database_config
            .clickhouse
            .as_ref()
            .ok_or_else(|| AppError::ConfigError("ClickHouse configuration missing".into()))?,
        job_id,
    );
    database.ping().await?;
    let old = database.load_resume_state().await?;
    let mut state = if let Some(state) = old.as_ref().filter(|state| state.active()) {
        state.validate(&request_key)?;
        if !database.table_exists(&db::base_table_name_for_job(job_id)).await? {
            return Err(AppError::ScanError(
                "Saved scan results are missing; refusing to resume".into(),
            ));
        }
        if database.restore_resume_generation(state).await? {
            let mut completed = state.clone();
            completed.phase = COMPLETED;
            completed.checkpoint.clear();
            completed.advance()?;
            database.save_resume_state(&completed).await?;
            if let Err(error) = database.cleanup_resume_batch().await {
                warn!(%error, "Completed scan; deferred batch-table cleanup");
            }
            info!("Recovered the previous scan's completed generation commit");
            return Ok(());
        }
        info!(
            entries = state.observed_entries,
            checkpoint = !state.checkpoint.is_empty(),
            "Resuming scan from acknowledged ClickHouse progress"
        );
        state.clone()
    } else {
        let incremental = database.table_exists(&db::base_table_name_for_job(job_id)).await?;
        database.initialize().await?;
        let generation = database.begin_scan_generation().await?;
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        let event_time = database.resume_event_epoch(now).await?;
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
        };
        fresh.advance()?;
        database.save_resume_state(&fresh).await?;
        info!("Starting a scan with durable ClickHouse checkpoints");
        fresh
    };
    if state.incremental {
        database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
    }
    let kind = if state.incremental {
        JobType::IncrementalScan
    } else {
        JobType::Scan
    };
    let mut consumer_config = initialize_consumer_config(job_id, job_dir, kind, command, &config, None)?;
    // 本管道负责数据库持久化确认；manager 只消费统计信息。
    consumer_config.db_config.enabled = false;
    let mut consumers = ConsumerManager::new(&consumer_config).await?;
    consumers.begin_lifecycle().await;
    let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
    let handles = consumers.start_consumers(&mut broadcaster).await?;
    let result = run(
        &storage,
        &options,
        &mut database,
        &mut state,
        &broadcaster,
        config.database.batch_size.max(1) as usize,
        cancel,
    )
    .await;
    if let Err(error) = &result {
        broadcaster
            .broadcast(StorageEntryMessage::Error {
                event: ErrorEvent::Scan,
                path: Default::default(),
                entry: None,
                reason: error.to_string(),
            })
            .await;
    }
    drop(broadcaster);
    for handle in handles {
        handle.await??;
    }
    consumers.set_generation_committed(result.is_ok()).await;
    consumers.end_lifecycle().await;
    drop(lease);
    result
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
    storage: &StorageEnum, options: &WalkOptions, database: &mut ClickHouseDatabase, state: &mut ScanResumeState,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>, batch_size: usize, cancel: CancellationToken,
) -> Result<()> {
    if state.phase == RUNNING {
        let mut scan = storage.resumable_scan(
            options,
            (!state.checkpoint.is_empty()).then(|| state.checkpoint.clone()),
            cancel.clone(),
            checkpoint_interval(),
        )?;
        let mut batch = Vec::with_capacity(batch_size);
        let mut failure = None;
        let mut observed = state.observed_entries;
        while let Some(item) = scan.next().await {
            if failure.is_some() {
                continue;
            }
            match item {
                Ok(ResumableScanItem::Entries(entries)) => {
                    for entry in entries {
                        observed = observed.saturating_add(1);
                        batch.push(Arc::new(entry));
                        if batch.len() >= batch_size {
                            flush_batch(database, state, broadcaster, &mut batch).await?;
                        }
                    }
                }
                Ok(ResumableScanItem::Checkpoint(checkpoint)) => {
                    // 前序条目和事件全部持久化后，才能推进游标。
                    flush_batch(database, state, broadcaster, &mut batch).await?;
                    state.checkpoint = checkpoint;
                    state.observed_entries = observed;
                    state.advance()?;
                    database.save_resume_state(state).await?;
                    info!(
                        version = state.version,
                        entries = observed,
                        "Scan checkpoint saved in ClickHouse"
                    );
                    #[cfg(feature = "lab-fault-injection")]
                    if let Ok(value) = std::env::var("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS")
                        && let Ok(value) = value.parse::<u64>()
                    {
                        sleep(Duration::from_millis(value)).await;
                    }
                    #[cfg(feature = "lab-fault-injection")]
                    if std::env::var_os("TERRASYNC_LAB_SCAN_STOP_AFTER_CHECKPOINT").is_some() {
                        cancel.cancel();
                    }
                }
                Err(error) => {
                    failure = Some(error);
                    cancel.cancel();
                }
            }
        }
        if let Some(error) = failure {
            return Err(error.into());
        }
        flush_batch(database, state, broadcaster, &mut batch).await?;
        let outcome = scan.finish().await?;
        match outcome {
            TraversalOutcome::Completed(completion)
                if completion.totals.failed_children == 0 && completion.totals.listing_failures == 0 => {}
            _ => {
                return Err(AppError::ScanError(
                    "Scan paused or incomplete; checkpoint retained, generation not committed".into(),
                ));
            }
        }
        state.phase = FINALIZING;
        state.observed_entries = observed;
        state.advance()?;
        database.save_resume_state(state).await?;
    }
    if state.incremental {
        finalize_deletions(database, state, broadcaster, batch_size).await?;
    }
    database.commit_scan_generation().await?;
    state.phase = COMPLETED;
    state.checkpoint.clear();
    state.advance()?;
    database.save_resume_state(state).await?;
    if let Err(error) = database.cleanup_resume_batch().await {
        warn!(%error, "Completed scan; deferred batch-table cleanup");
    }
    Ok(())
}

async fn flush_batch(
    database: &mut ClickHouseDatabase, state: &ScanResumeState, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
    batch: &mut Vec<Arc<EntryEnum>>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut events = Vec::new();
    if state.incremental {
        database.begin_resume_batch().await?;
        database.write_resume_temp_records(batch).await?;
        events.extend(
            database
                .detect_new_items()
                .await?
                .map(|entry| StorageEntryMessage::New(Arc::new(entry))),
        );
        events.extend(
            database
                .detect_changed_items()
                .await?
                .map(|(entry, kind)| StorageEntryMessage::Changed {
                    entry: Arc::new(entry),
                    kind,
                }),
        );
        database.write_resume_events(&events, state.event_time).await?;
        #[cfg(feature = "lab-fault-injection")]
        if !events.is_empty() && std::env::var_os("TERRASYNC_LAB_SCAN_FAIL_AFTER_EVENTS").is_some() {
            return Err(AppError::ScanError(
                "Lab failure after durable events, before base update".into(),
            ));
        }
        database.insert_temp_to_base_table(&[]).await?;
    } else {
        database.write_resume_base_records(batch).await?;
    }
    for entry in batch.drain(..) {
        broadcaster.broadcast(StorageEntryMessage::Scanned(entry)).await;
    }
    for event in events {
        broadcaster.broadcast(event).await;
    }
    Ok(())
}

async fn finalize_deletions(
    database: &ClickHouseDatabase, state: &ScanResumeState, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
    batch_size: usize,
) -> Result<()> {
    let deleted = database.detect_deleted_items().await?;
    let mut events = Vec::with_capacity(batch_size);
    for entry in deleted {
        events.push(match entry {
            DeletionStatus::Deleted(entry) => StorageEntryMessage::Deleted(Arc::new(entry)),
            DeletionStatus::Renamed(from, to) => StorageEntryMessage::Renamed((Arc::new(*from), Arc::new(*to))),
        });
        if events.len() >= batch_size {
            flush_deletions(database, state, broadcaster, &mut events).await?;
        }
    }
    flush_deletions(database, state, broadcaster, &mut events).await
}

async fn flush_deletions(
    database: &ClickHouseDatabase, state: &ScanResumeState, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
    events: &mut Vec<StorageEntryMessage>,
) -> Result<()> {
    database.write_resume_events(events, state.event_time).await?;
    let keys: Vec<(String, String)> = events
        .iter()
        .filter_map(|event| match event {
            StorageEntryMessage::Deleted(entry) | StorageEntryMessage::Renamed((entry, _)) => Some((
                entry.get_relative_path().to_string_lossy().into_owned(),
                entry.get_version_id().unwrap_or_default().to_owned(),
            )),
            _ => None,
        })
        .collect();
    database.delete_resume_entries(&keys).await?;
    for event in events.drain(..) {
        broadcaster.broadcast(event).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
