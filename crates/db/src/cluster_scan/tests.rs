//! 真正经过六个副本的读写，验证共享表不会跨 job 比较或删除。
use super::*;
use crate::clickhouse::scan_resume::{COMPLETED, RUNNING};
use crate::scan_store::ScanFinalEvent;
use crate::scan_store::with_scan_cancellation;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
    StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding, TimePrecision,
};
use std::sync::atomic::AtomicUsize;
use storage_adapter::ChangeKind;
use storage_adapter::observed_scan::ObservedScanEvent;
use tokio::{sync::Barrier, task::JoinSet};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cancelled_quorum_write_does_not_start_another_attempt() {
    let cancel = CancellationToken::new();
    let attempts = AtomicUsize::new(0);
    let result = with_scan_cancellation(
        cancel.clone(),
        retry_quorum(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            cancel.cancel();
            async {
                Err(DatabaseError::ClickHouseError(ClickHouseError::BadResponse(
                    "UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE".into(),
                )))
            }
        }),
    )
    .await;
    assert!(matches!(result, Err(DatabaseError::ScanCancelled)));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

struct DatabaseGuard {
    config: ClickHouseConfig,
    name: String,
    cluster: String,
}
impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        let config = self.config.clone();
        let name = self.name.clone();
        let cluster = self.cluster.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                let admin = build_base_client(&config);
                let _ = runtime.block_on(
                    admin
                        .query(&format!("DROP DATABASE IF EXISTS {name} ON CLUSTER {cluster} SYNC"))
                        .execute(),
                );
            }
        })
        .join();
    }
}
fn entry(path: &str, size: u64, handle: Option<&str>) -> Arc<EntryEnum> {
    Arc::new(
        StorageEntryRecord {
            name: path.into(),
            relative_path: path.into(),
            size,
            ext: None,
            ctime: 1_000_000_000,
            mtime: 1_000_000_000,
            atime: 1_000_000_000,
            mode: Some(420),
            storage_type: "nas".into(),
            is_symlink: false,
            is_dir: false,
            is_regular_file: true,
            hard_links: 1,
            current_state: 0,
            uid: Some(1000),
            gid: Some(1000),
            ino: None,
            file_handle: handle.map(str::to_string),
            version_id: String::new(),
            tags: None,
            version_count: None,
        }
        .to_entry_enum(),
    )
}
fn state(epoch: i64, incremental: bool, generation: u8) -> ScanResumeState {
    ScanResumeState {
        id: 1,
        schema_version: 1,
        version: epoch as u64,
        request_key: "fixture".into(),
        generation,
        incremental,
        phase: RUNNING,
        event_time: epoch,
        observed_entries: 0,
        checkpoint: vec![],
        statistics: String::new(),
    }
}

async fn fixture() -> Result<(ClickHouseConfig, Client, DatabaseGuard, String, i64)> {
    let epoch = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DatabaseError::ConfigError(e.to_string()))?
            .as_nanos(),
    )
    .map_err(|e| DatabaseError::ConfigError(e.to_string()))?;
    let name = format!("ts_cluster_scan_{epoch}");
    let config = ClickHouseConfig {
        dsn: std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
        database: name.clone(),
        username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
        dial_timeout: 10,
        read_timeout: 60,
    };
    let cluster = std::env::var("LAB_CLICKHOUSE_CLUSTER").unwrap_or_else(|_| "clickhouse".into());
    identifier(&cluster)?;
    let admin = build_base_client(&ClickHouseConfig {
        database: "default".into(),
        ..config.clone()
    });
    admin
        .query(&format!("CREATE DATABASE {name} ON CLUSTER {cluster}"))
        .execute()
        .await?;
    let _guard = DatabaseGuard {
        config: ClickHouseConfig {
            database: "default".into(),
            ..config.clone()
        },
        name: name.clone(),
        cluster: cluster.clone(),
    };
    Ok((config, admin, _guard, cluster, epoch))
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_shared_tables_isolate_jobs_and_resume_rounds() -> Result<()> {
    let (config, admin, _guard, cluster, epoch) = fixture().await?;
    let name = config.database.clone();
    let a = ClusterScanStore::new(&config, &cluster, "a")?;
    assert!(a.load_resume_state().await?.is_none());
    let b = ClusterScanStore::new(&config, &cluster, "b")?;
    assert!(b.load_resume_state().await?.is_none());
    assert!(!a.has_baseline().await? && !b.has_baseline().await?);
    let mut full = state(epoch, false, 1);
    a.save_resume_state(&full).await?;
    a.write_resume_base_records(&[
        entry("same", 100, None),
        entry("victim", 9, None),
        entry("old/renamed", 5, Some("5453493401")),
    ])
    .await?;
    b.save_resume_state(&state(epoch + 1, false, 1)).await?;
    b.write_resume_base_records(&[entry("same", 999, None), entry("victim", 888, None)])
        .await?;
    let mut a = ClusterScanStore::new(&config, &cluster, "a")?;
    let saved = a
        .load_resume_state()
        .await?
        .ok_or_else(|| DatabaseError::ConfigError("checkpoint missing".into()))?;
    assert!(!saved.incremental);
    assert!(!a.restore_resume_generation(&saved).await?);
    assert!(!a.has_baseline().await?, "an incomplete full scan is not a baseline");
    a.write_resume_base_records(&[entry("same", 100, None)]).await?;
    full.phase = FINALIZING;
    full.version += 1;
    a.save_resume_state(&full).await?;
    a.commit_scan_generation().await?;
    assert!(
        a.restore_resume_generation(&full).await?,
        "recover committed finalization"
    );
    full.phase = COMPLETED;
    full.version += 1;
    a.save_resume_state(&full).await?;
    assert!(a.has_baseline().await?);
    let mut incremental = state(epoch + 10, true, 0);
    a.save_resume_state(&incremental).await?;
    a.begin_resume_batch().await?;
    let entries = vec![entry("same", 101, None), entry("added", 1, None)];
    a.write_resume_temp_records(&entries).await?;
    let events = a.detect_resume_batch_events(&entries).await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, StorageEntryMessage::New(_)))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                StorageEntryMessage::Changed {
                    kind: ChangeKind::DataOnly,
                    ..
                }
            ))
            .count(),
        1
    );
    a.write_resume_events(&events, incremental.event_time).await?;
    // 模拟 events 已确认、base 未更新即崩溃后的重放。
    a.write_resume_events(&events, incremental.event_time).await?;
    a.insert_temp_to_base_table(&[]).await?;
    a.cleanup_resume_batch().await?;
    let mut resumed = ClusterScanStore::new(&config, &cluster, "a")?;
    let saved = resumed
        .load_resume_state()
        .await?
        .ok_or_else(|| DatabaseError::ConfigError("incremental checkpoint missing".into()))?;
    assert!(saved.incremental);
    assert_eq!(saved.event_time, incremental.event_time);
    assert!(!resumed.restore_resume_generation(&saved).await?);
    resumed.begin_resume_batch().await?;
    resumed.write_resume_temp_records(&entries).await?;
    assert!(
        resumed.detect_resume_batch_events(&entries).await?.is_empty(),
        "base replay should be unchanged"
    );
    resumed.insert_temp_to_base_table(&[]).await?;
    let deleted: Vec<_> = resumed.detect_deleted_items().await?.collect();
    assert_eq!(deleted.len(), 2);
    let mut keys = Vec::new();
    let mut messages = Vec::new();
    for deletion in deleted {
        if let DeletionStatus::Deleted(entry) = deletion {
            keys.push((entry.get_relative_path().to_string_lossy().into_owned(), String::new()));
            messages.push(StorageEntryMessage::Deleted(Arc::new(entry)));
        }
    }
    resumed.write_resume_events(&messages, incremental.event_time).await?;
    resumed.delete_resume_entries(&keys).await?;
    let summary = resumed.resume_statistics(0, Some(incremental.event_time)).await?;
    assert_eq!(summary.summary.files, 2);
    let events: u64 = resumed
        .client
        .query("SELECT count() FROM incremental FINAL WHERE job_id='a'")
        .fetch_one()
        .await?;
    assert_eq!(events, 4, "new + changed + 2 deleted, without replay duplicates");
    let other_files: u64 = b
        .client
        .query("SELECT count() FROM base FINAL WHERE job_id='b' AND is_deleted=0")
        .fetch_one()
        .await?;
    let other_size: u64 = b
        .client
        .query("SELECT sum(size) FROM base FINAL WHERE job_id='b' AND is_deleted=0")
        .fetch_one()
        .await?;
    assert_eq!((other_files, other_size), (2, 1887));
    incremental.phase = FINALIZING;
    incremental.version += 1;
    resumed.save_resume_state(&incremental).await?;
    resumed.commit_scan_generation().await?;
    resumed.cleanup_resume_batch().await?;
    let tables: u64 = admin
        .clone()
        .with_database(name.clone())
        .query("SELECT count() FROM clusterAllReplicas(?,system.tables) WHERE database=? AND name='base_local'")
        .bind(&cluster)
        .bind(&name)
        .fetch_one()
        .await?;
    assert_eq!(tables, 6, "one replicated local table on every node");
    eprintln!(
        "shared cluster: 6 replicas, two isolated jobs, full/incremental resume, replay deduplication and final statistics verified"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_shared_batch_prunes_base_and_supports_concurrent_jobs() -> Result<()> {
    let (config, admin, _guard, cluster, epoch) = fixture().await?;
    let name = &config.database;
    let mut store = ClusterScanStore::new(&config, &cluster, "a")?;
    store.load_resume_state().await?;
    store.client.query(&format!("INSERT INTO {name}.base (job_id,epoch,ver,name,relative_path,size,mtime,mode,is_regular_file,current_state) SELECT if(number<500000,'a','b'),{epoch},fromUnixTimestamp64Nano({epoch}),toString(n),leftPad(toString(n),9,'0'),if(number<500000,100,999),toDateTime64(1,9),420,1,1 FROM (SELECT number,number%500000 AS n FROM numbers(1000000))")).execute().await?;
    store.save_resume_state(&state(epoch + 10, true, 0)).await?;
    store.begin_resume_batch().await?;
    let batch = store.batch_id;
    store.client.query(&format!("INSERT INTO {name}.scan_resume_batch (job_id,epoch,ver,batch_id,name,relative_path,size,mtime,mode,is_regular_file,current_state) SELECT 'a',{},fromUnixTimestamp64Nano({}),{batch},toString(n),leftPad(toString(n),9,'0'),101,toDateTime64(1,9),420,1,0 FROM (SELECT number+100000 AS n FROM numbers(200))",epoch+10,epoch+10)).execute().await?;
    let records = store
        .client
        .query(&format!(
            "SELECT {} FROM {name}.scan_resume_batch WHERE job_id='a' AND batch_id={batch}",
            *FILE_SCAN_COLUMNS_LIST
        ))
        .fetch_all::<StorageEntryRecord>()
        .await?;
    let entries: Vec<_> = records.iter().map(|record| Arc::new(record.to_entry_enum())).collect();
    // 一百万行基线下，相关批次的读取必须少于十万行；恢复全表聚合会直接触发限制。
    store.client = store.client.clone().with_option("max_rows_to_read", "100000");
    let started = Instant::now();
    let events = store.detect_resume_batch_events(&entries).await?;
    assert_eq!(events.len(), 200);
    assert!(events.iter().all(|event| matches!(
        event,
        StorageEntryMessage::Changed {
            kind: ChangeKind::DataOnly,
            ..
        }
    )));
    eprintln!(
        "cluster bounded compare: 200 entries against 1,000,000 baseline rows in {:.3}s, hard read limit 100,000",
        started.elapsed().as_secs_f64()
    );
    let native_entries = records
        .iter()
        .map(|row| native_entry(&row.relative_path, row.size, None))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        store.compare_native(&native_entries).await?.len(),
        200,
        "native comparison must also remain within the 100,000-row read limit"
    );
    store.cleanup_resume_batch().await?;

    // rename 源路径在范围外，另一个 job 的相同身份也不能吞掉本 job 的新增。
    let mut nfs = ClusterScanStore::new(&config, &cluster, "nfs")?;
    let other = ClusterScanStore::new(&config, &cluster, "other_nfs")?;
    nfs.save_resume_state(&state(epoch + 20, false, 1)).await?;
    nfs.write_resume_base_records(&[entry("a/old", 1, Some("5453493401"))])
        .await?;
    other.save_resume_state(&state(epoch + 21, false, 1)).await?;
    other
        .write_resume_base_records(&[entry("outside", 1, Some("5453493402"))])
        .await?;
    nfs.save_resume_state(&state(epoch + 22, true, 0)).await?;
    nfs.begin_resume_batch().await?;
    let entries = vec![
        entry("z/renamed", 1, Some("5453493401")),
        entry("z/added", 1, Some("5453493402")),
    ];
    nfs.write_resume_temp_records(&entries).await?;
    let events = nfs.detect_resume_batch_events(&entries).await?;
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0],StorageEntryMessage::New(entry) if entry.get_relative_path().to_string_lossy()=="z/added")
    );
    nfs.insert_temp_to_base_table(&[]).await?;
    let renamed: Vec<_> = nfs.detect_deleted_items().await?.collect();
    assert_eq!(renamed.len(), 1);
    assert!(matches!(&renamed[0], DeletionStatus::Renamed(..)));
    nfs.begin_resume_batch().await?;
    let mut hardlink = entry("z/hardlink", 1, Some("5453493401")).as_ref().clone();
    if let EntryEnum::NAS(entry) = &mut hardlink {
        entry.hard_links = Some(2);
    }
    let entries = vec![Arc::new(hardlink)];
    nfs.write_resume_temp_records(&entries).await?;
    assert!(matches!(
        nfs.detect_resume_batch_events(&entries).await?.as_slice(),
        [StorageEntryMessage::New(_)]
    ));
    nfs.cleanup_resume_batch().await?;

    let jobs = admin
        .clone()
        .with_database(name.clone())
        .query("SELECT concat('parallel_',toString(number)) AS job FROM numbers(100) WHERE cityHash64(job)%2=0 LIMIT 2")
        .fetch_all::<String>()
        .await?;
    let left = ClusterScanStore::new(&config, &cluster, &jobs[0])?;
    let right = ClusterScanStore::new(&config, &cluster, &jobs[1])?;
    left.save_resume_state(&state(epoch + 30, false, 1)).await?;
    right.save_resume_state(&state(epoch + 31, false, 1)).await?;
    for round in 0..5 {
        let entries: Vec<_> = (0..5000)
            .map(|n| entry(&format!("parallel/{n:06}"), 100 + round, None))
            .collect();
        let (a, b) = tokio::join!(
            left.write_resume_base_records(&entries),
            right.write_resume_base_records(&entries)
        );
        a?;
        b?;
    }
    let rows: u64 = left
        .client
        .query(&format!(
            "SELECT count() FROM {name}.base FINAL WHERE job_id IN ? AND is_deleted=0"
        ))
        .bind(&jobs)
        .fetch_one()
        .await?;
    assert_eq!(rows, 10000, "concurrent quorum writes cannot lose either job");
    Ok(())
}

#[derive(Deserialize, clickhouse::Row)]
struct NativeSnapshotRow {
    relative_path: String,
    entry_snapshot: Vec<u8>,
    identity_key: String,
}

fn native_entry(path: &str, size: u64, stable: Option<&[u8]>) -> Result<Arc<ObservedEntry>> {
    let backend = BackendIdentity::new(BackendKind::Nfs, "native-cluster-fixture")?;
    let identity = SourceIdentity::new(
        backend,
        if stable.is_some() {
            IdentityStrength::StableWithinBackend
        } else {
            IdentityStrength::PathScoped
        },
        stable.unwrap_or(path.as_bytes()),
    )?;
    Ok(Arc::new(ObservedEntry::new(
        StoragePath::new(path)?,
        EntryKind::File,
        Some(size),
        None,
        identity,
    )?))
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_native_key_stage_replays_events_before_direct_base_write() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "native_keys")?;
    store.load_resume_state().await?;
    let mut full = state(epoch, false, 1);
    store.save_resume_state(&full).await?;
    store
        .write_native(
            &[
                native_entry("same", 10, None)?,
                native_entry("changed", 20, None)?,
                native_entry("old", 30, Some(b"rename"))?,
            ],
            false,
        )
        .await?;
    full.phase = FINALIZING;
    full.version += 1;
    store.save_resume_state(&full).await?;
    store.commit_scan_generation().await?;
    let incremental = state(epoch + 10, true, 0);
    store.save_resume_state(&incremental).await?;
    let entries = vec![
        native_entry("same", 10, None)?,
        native_entry("changed", 21, None)?,
        native_entry("added", 1, None)?,
        native_entry("renamed", 30, Some(b"rename"))?,
    ];
    store.begin_resume_batch().await?;
    store.write_native_keys(&entries).await?;
    let snapshots: u64 = store
        .client
        .query("SELECT sum(length(entry_snapshot)) FROM scan_resume_batch WHERE job_id='native_keys'")
        .fetch_one()
        .await?;
    assert_eq!(snapshots, 0);
    let events = store.compare_native(&entries).await?;
    assert_eq!(events.len(), 3);
    store.events_native(&events, incremental.event_time).await?;
    let old_size: u64 = store
        .client
        .query("SELECT size FROM base FINAL WHERE job_id='native_keys' AND relative_path='changed'")
        .fetch_one()
        .await?;
    assert_eq!(old_size, 20);

    // 模拟事件已持久化、base 尚未更新时重启；检查点之前的批次必须可幂等重放。
    let mut resumed = ClusterScanStore::new(&config, &cluster, "native_keys")?;
    let loaded = resumed.load_resume_state().await?;
    assert!(loaded.is_some_and(|saved| saved.incremental && saved.event_time == incremental.event_time));
    resumed.begin_resume_batch().await?;
    resumed.write_native_keys(&entries).await?;
    let replayed = resumed.compare_native(&entries).await?;
    assert_eq!(replayed.len(), events.len());
    resumed.events_native(&replayed, incremental.event_time).await?;
    resumed.write_native(&entries, false).await?;
    assert!(resumed.compare_native(&entries).await?.is_empty());
    let final_events = resumed.resolve_final_events_native(incremental.event_time).await?;
    assert_eq!(final_events.len(), 1);
    assert!(
        matches!(&final_events[0], ScanFinalEvent::Renamed { from_path, to, .. } if from_path=="old" && to.relative_path=="renamed")
    );
    resumed
        .write_final_events_native(&final_events, incremental.event_time)
        .await?;
    let keys: Vec<_> = final_events.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    resumed.delete_resume_entries(&keys).await?;
    let rows = resumed.client.query("SELECT relative_path,entry_snapshot,identity_key FROM base FINAL WHERE job_id='native_keys' AND is_deleted=0 ORDER BY relative_path").fetch_all::<NativeSnapshotRow>().await?;
    assert_eq!(rows.len(), entries.len());
    for row in rows {
        let observation = ObservedEntry::decode_snapshot(&row.entry_snapshot, entries[0].source_identity().backend())?;
        assert!(entries.iter().any(|entry| entry.as_ref() == &observation));
    }
    let changes: u64 = resumed.client.query("SELECT count() FROM incremental FINAL WHERE job_id='native_keys' AND operation_type IN ('new','data_changed','rename')").fetch_one().await?;
    assert_eq!(changes, 3);
    Ok(())
}

async fn apply_native_key_batch(store: &mut ClusterScanStore, entries: &[Arc<ObservedEntry>], time: i64) -> Result<()> {
    store.begin_resume_batch().await?;
    store.write_native_keys(entries).await?;
    let events = store.compare_native(entries).await?;
    store.events_native(&events, time).await?;
    store.write_native(entries, false).await
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_native_parallel_unchanged_batches_never_create_candidates() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let store = ClusterScanStore::new(&config, &cluster, "parallel_unchanged")?;
    store.load_resume_state().await?;
    store.save_resume_state(&state(epoch, false, 1)).await?;
    let entries = (0..40_000)
        .map(|index| {
            let key = format!("path/{index:08}");
            native_entry(&key, 17, Some(key.as_bytes()))
        })
        .collect::<Result<Vec<_>>>()?;
    store.write_native(&entries, false).await?;
    store.commit_scan_generation().await?;
    store.save_resume_state(&state(epoch + 10, true, 0)).await?;
    let barrier = Arc::new(Barrier::new(4));
    let mut workers: JoinSet<Result<()>> = JoinSet::new();
    for worker in 0..4 {
        let mut writer = store.batch_writer();
        let entries: Vec<_> = entries.iter().skip(worker).step_by(4).cloned().collect();
        let barrier = Arc::clone(&barrier);
        workers.spawn(async move {
            for round in 0..64 {
                // 每次写入独立 epoch，避免服务端幂等去重使并发压力退化为空写入。
                writer
                    .epoch
                    .store(epoch as u64 + 20 + round * 4 + worker as u64, Ordering::Relaxed);
                writer.begin_resume_batch().await?;
                writer.write_native_keys(&entries).await?;
                barrier.wait().await;
                let events = writer.compare_native(&entries).await?;
                assert!(
                    events.is_empty(),
                    "unchanged batch produced {} events in worker {worker}, round {round}",
                    events.len()
                );
                writer.write_native(&entries, false).await?;
            }
            Ok(())
        });
    }
    while let Some(worker) = workers.join_next().await {
        worker??;
    }
    store.cleanup_resume_batch().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_native_parallel_batches_preserve_reused_paths_and_hardlinks() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "parallel_native")?;
    store.load_resume_state().await?;
    let mut full = state(epoch, false, 1);
    store.save_resume_state(&full).await?;
    store
        .write_native(
            &[
                native_entry("reuse", 10, Some(b"old-object"))?,
                native_entry("rename_old", 30, Some(b"rename"))?,
                native_entry("keep_link", 40, Some(b"hardlink"))?,
                native_entry("remove_link", 40, Some(b"hardlink"))?,
            ],
            false,
        )
        .await?;
    full.phase = FINALIZING;
    full.version += 1;
    store.save_resume_state(&full).await?;
    store.commit_scan_generation().await?;
    let incremental = state(epoch + 10, true, 0);
    store.save_resume_state(&incremental).await?;
    let first = vec![
        native_entry("reuse", 11, Some(b"replacement"))?,
        native_entry("keep_link", 40, Some(b"hardlink"))?,
        native_entry("added", 3, Some(b"new-object"))?,
    ];
    let second = vec![
        native_entry("rename_new", 30, Some(b"rename"))?,
        native_entry("rescued", 10, Some(b"old-object"))?,
        native_entry("new_link", 40, Some(b"hardlink"))?,
    ];
    let mut a = store.batch_writer();
    let mut b = store.batch_writer();
    let (first_result, second_result) = tokio::join!(
        apply_native_key_batch(&mut a, &first, incremental.event_time),
        apply_native_key_batch(&mut b, &second, incremental.event_time),
    );
    first_result?;
    second_result?;
    assert_ne!(a.batch_id, b.batch_id);
    let events = store.resolve_final_events_native(incremental.event_time).await?;
    assert_eq!(events.len(), 4);
    assert!(events.iter().any(|event|matches!(event,ScanFinalEvent::Renamed{from_path,to,..} if from_path=="reuse" && to.relative_path=="rescued")));
    assert!(events.iter().any(|event|matches!(event,ScanFinalEvent::Renamed{from_path,to,..} if from_path=="rename_old" && to.relative_path=="rename_new")));
    assert!(
        events
            .iter()
            .any(|event| matches!(event,ScanFinalEvent::Added(row) if row.relative_path=="new_link"))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event,ScanFinalEvent::Deleted(row) if row.relative_path=="remove_link"))
    );
    store.write_final_events_native(&events, incremental.event_time).await?;
    let keys: Vec<_> = events.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    store.delete_resume_entries(&keys).await?;
    let rows = store.client.query("SELECT relative_path,entry_snapshot,identity_key FROM base FINAL WHERE job_id='parallel_native' AND is_deleted=0").fetch_all::<NativeSnapshotRow>().await?;
    assert_eq!(rows.len(), first.len() + second.len());
    for row in rows {
        let observation = ObservedEntry::decode_snapshot(&row.entry_snapshot, first[0].source_identity().backend())?;
        assert!(first.iter().chain(&second).any(|entry| entry.as_ref() == &observation));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_native_replayed_path_restoring_original_identity_is_not_deleted() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "restored_identity")?;
    store.load_resume_state().await?;
    let mut full = state(epoch, false, 1);
    store.save_resume_state(&full).await?;
    store
        .write_native(&[native_entry("p", 10, Some(b"original"))?], false)
        .await?;
    full.phase = FINALIZING;
    full.version += 1;
    store.save_resume_state(&full).await?;
    store.commit_scan_generation().await?;
    let incremental = state(epoch + 10, true, 0);
    store.save_resume_state(&incremental).await?;
    let entries = vec![
        native_entry("p", 10, Some(b"temporary"))?,
        native_entry("p", 10, Some(b"original"))?,
    ];
    store.begin_resume_batch().await?;
    store.write_native_keys(&entries).await?;
    let events = store.compare_native(&entries).await?;
    store.events_native(&events, incremental.event_time).await?;
    store.write_native(&entries, false).await?;
    let final_events = store.resolve_final_events_native(incremental.event_time).await?;
    assert!(
        final_events.is_empty(),
        "restored original identity must not generate a deletion"
    );
    assert!(
        events.is_empty(),
        "the final same-key observation matches the original baseline"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_shared_native_observations_roundtrip_merge_replay_and_legacy_baseline() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "native_a")?;
    let other = ClusterScanStore::new(&config, &cluster, "native_b")?;
    store.load_resume_state().await?;
    let mut full = state(epoch, false, 1);
    store.save_resume_state(&full).await?;
    let initial = vec![
        native_entry("same", 100, None)?,
        native_entry("changed", 100, None)?,
        native_entry("victim", 7, None)?,
        native_entry("a/old", 5, Some(b"rename"))?,
    ];

    store.write_native(&initial, false).await?;
    other.save_resume_state(&state(epoch + 1, false, 1)).await?;
    other.write_native(&[native_entry("same", 999, None)?], false).await?;
    // 旧 GUI 结果没有 snapshot；升级后的增量删除必须仍能读取。
    store.client.query("INSERT INTO base (job_id,epoch,ver,name,relative_path,size,is_regular_file,current_state) VALUES ('native_a',?,fromUnixTimestamp64Nano(?),'legacy','legacy',3,1,1)").bind(epoch as u64).bind(epoch).execute().await?;
    full.phase = FINALIZING;
    full.version += 1;
    store.save_resume_state(&full).await?;
    store.commit_scan_generation().await?;
    let incremental = state(epoch + 10, true, 0);
    store.save_resume_state(&incremental).await?;
    store.begin_resume_batch().await?;
    let entries = vec![
        native_entry("same", 100, None)?,
        native_entry("changed", 101, None)?,
        native_entry("added", 1, None)?,
        native_entry("z/new", 5, Some(b"rename"))?,
    ];
    store.write_native(&entries, true).await?;
    let events = store.compare_native(&entries).await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| !matches!(event, ObservedScanEvent::Candidate(_)))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ObservedScanEvent::Candidate(_)))
            .count(),
        1
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event,ObservedScanEvent::New(item) if item.view().path()=="added"))
    );
    store.events_native(&events, incremental.event_time).await?;
    store.events_native(&events, incremental.event_time).await?;
    store.merge_native().await?;
    assert!(store.compare_native(&entries).await?.is_empty());
    let deleted = store.resolve_final_events_native(incremental.event_time).await?;
    assert_eq!(deleted.len(), 3);
    assert!(deleted.iter().any(|event|matches!(event,ScanFinalEvent::Renamed{from_path,to,..} if from_path=="a/old" && to.relative_path=="z/new")));
    store
        .write_final_events_native(&deleted, incremental.event_time)
        .await?;
    let keys: Vec<_> = deleted.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    store.delete_resume_entries(&keys).await?;
    let rows=store.client.query("SELECT relative_path,entry_snapshot,identity_key FROM base FINAL WHERE job_id='native_a' AND is_deleted=0 ORDER BY relative_path").fetch_all::<NativeSnapshotRow>().await?;
    assert_eq!(rows.len(), 4);
    for row in rows {
        let entry = ObservedEntry::decode_snapshot(&row.entry_snapshot, entries[0].source_identity().backend())?;
        assert_eq!(entry.path().as_str(), row.relative_path);
        assert_eq!(hex::encode(entry.identity_key().as_bytes()), row.identity_key);
    }
    store.cleanup_candidates(incremental.event_time).await?;
    let event_count: u64 = store
        .client
        .query("SELECT count() FROM incremental FINAL WHERE job_id='native_a'")
        .fetch_one()
        .await?;
    assert_eq!(
        event_count, 5,
        "new + changed + 2 deleted + renamed; replay remains idempotent"
    );
    assert_eq!(
        store
            .resume_statistics(0, Some(incremental.event_time))
            .await?
            .summary
            .files,
        4
    );
    let other_size: u64 = other
        .client
        .query("SELECT sum(size) FROM base FINAL WHERE job_id='native_b' AND is_deleted=0")
        .fetch_one()
        .await?;
    assert_eq!(other_size, 999);
    store.cleanup_resume_batch().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_shared_native_hardlinks_survive_finalization_replay() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "native_links")?;
    store.load_resume_state().await?;
    store.save_resume_state(&state(epoch, false, 1)).await?;
    store
        .write_native(
            &[
                native_entry("a", 1, Some(b"shared"))?,
                native_entry("b", 1, Some(b"shared"))?,
                native_entry("c", 1, Some(b"shared"))?,
            ],
            false,
        )
        .await?;
    let round = epoch + 10;
    store.save_resume_state(&state(round, true, 0)).await?;
    store.begin_resume_batch().await?;
    let entries = vec![
        native_entry("a", 1, Some(b"shared"))?,
        native_entry("new", 1, Some(b"shared"))?,
    ];
    store.write_native(&entries, true).await?;
    let events = store.compare_native(&entries).await?;
    assert!(matches!(events.as_slice(), [ObservedScanEvent::Candidate(_)]));
    store.events_native(&events, round).await?;
    store.merge_native().await?;
    let final_events = store.resolve_final_events_native(round).await?;
    assert_eq!(final_events.len(), 3);
    assert!(!final_events.iter().any(|event| event.operation() == "rename"));
    let first: Vec<_> = final_events
        .iter()
        .filter(|event| event.operation() == "new" || event.deletion_key().is_some_and(|(path, _)| path == "b"))
        .cloned()
        .collect();
    store.write_final_events_native(&first, round).await?;
    let keys: Vec<_> = first.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    store.delete_resume_entries(&keys).await?;
    // 模拟只删掉一条旧别名后中断；重启仍是新增链接和删除，不能漂移成 rename。
    let replay = store.resolve_final_events_native(round).await?;
    assert_eq!(replay.len(), 2);
    assert!(!replay.iter().any(|event| event.operation() == "rename"));
    store.write_final_events_native(&replay, round).await?;
    let keys: Vec<_> = replay.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    store.delete_resume_entries(&keys).await?;
    store.cleanup_candidates(round).await?;
    assert!(store.resolve_final_events_native(round).await?.is_empty());
    let summary = store.resume_statistics(0, Some(round)).await?;
    assert_eq!(summary.summary.files, 2);
    let count: u64 = store
        .client
        .query("SELECT count() FROM incremental FINAL WHERE job_id='native_links'")
        .fetch_one()
        .await?;
    assert_eq!(count, 3);
    // 两个剩余别名同时消失：都必须删除，不能互相当成 rename 的目标。
    store.save_resume_state(&state(round + 10, true, 1)).await?;
    let removed = store.resolve_final_events_native(round + 10).await?;
    assert_eq!(removed.len(), 2);
    assert!(removed.iter().all(|event| event.operation() == "deleted"));
    store.cleanup_resume_batch().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_shared_native_large_identity_lookup_keeps_sql_bounded() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "native_bounded")?;
    store.load_resume_state().await?;
    store.save_resume_state(&state(epoch, false, 1)).await?;
    let mut initial = vec![native_entry("old", 1, Some(b"old-identity"))?];
    initial.extend(
        (0..1_000)
            .map(|n| native_entry(&format!("old/{n:04}"), 1, Some(format!("identity-{n}").as_bytes())))
            .collect::<Result<Vec<_>>>()?,
    );
    store.write_native(&initial, false).await?;
    store.save_resume_state(&state(epoch + 10, true, 0)).await?;
    store.begin_resume_batch().await?;
    let entries = (0..1_000)
        .map(|n| native_entry(&format!("new/{n:04}"), 1, Some(format!("new-identity-{n}").as_bytes())))
        .collect::<Result<Vec<_>>>()?;
    store.write_native(&entries, true).await?;
    store.client = store.client.clone().with_option("max_query_size", "50000");
    let events = store.compare_native(&entries).await?;
    assert_eq!(events.len(), 1_000);
    assert!(events.iter().all(|event| matches!(event, ObservedScanEvent::New(_))));
    store.begin_resume_batch().await?;
    let renamed = (0..1_000)
        .map(|n| native_entry(&format!("renamed/{n:04}"), 1, Some(format!("identity-{n}").as_bytes())))
        .collect::<Result<Vec<_>>>()?;
    store.write_native(&renamed, true).await?;
    let candidates = store.compare_native(&renamed).await?;
    assert_eq!(candidates.len(), 1_000);
    assert!(
        candidates
            .iter()
            .all(|event| matches!(event, ObservedScanEvent::Candidate(_)))
    );
    store.events_native(&candidates, epoch + 10).await?;
    store.merge_native().await?;
    let final_events = store.resolve_final_events_native(epoch + 10).await?;
    assert_eq!(
        final_events
            .iter()
            .filter(|event| event.operation() == "rename")
            .count(),
        1_000
    );
    assert_eq!(
        final_events
            .iter()
            .filter(|event| event.operation() == "deleted")
            .count(),
        1
    );
    store.cleanup_resume_batch().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_shared_native_rename_keeps_recreated_source_path() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let mut store = ClusterScanStore::new(&config, &cluster, "native_reused")?;
    store.load_resume_state().await?;
    store.save_resume_state(&state(epoch, false, 1)).await?;
    store
        .write_native(&[native_entry("a", 17, Some(b"original"))?], false)
        .await?;
    let round = epoch + 10;
    store.save_resume_state(&state(round, true, 0)).await?;
    store.begin_resume_batch().await?;
    let entries = vec![
        native_entry("a", 29, Some(b"replacement"))?,
        native_entry("b", 17, Some(b"original"))?,
    ];
    let mut events = Vec::new();
    // 先覆盖 a，再在后一批观察 b；判定不能依赖两个路径恰好落在同批。
    for entry in entries {
        store.begin_resume_batch().await?;
        let batch = vec![entry];
        store.write_native(&batch, true).await?;
        let batch_events = store.compare_native(&batch).await?;
        store.events_native(&batch_events, round).await?;
        store.merge_native().await?;
        events.extend(batch_events);
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ObservedScanEvent::New(_)))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ObservedScanEvent::Candidate(_)))
            .count(),
        1
    );
    // 原路径已被新身份覆盖；旧事实必须仍可从已确认日志恢复。
    let final_events = store.resolve_final_events_native(round).await?;
    assert!(
        matches!(final_events.as_slice(),[ScanFinalEvent::Renamed{from_path,to,..}] if from_path=="a" && to.relative_path=="b")
    );
    store.write_final_events_native(&final_events, round).await?;
    let keys: Vec<_> = final_events.iter().filter_map(ScanFinalEvent::deletion_key).collect();
    store.delete_resume_entries(&keys).await?;
    let current=store.client.query("SELECT relative_path,size FROM base FINAL WHERE job_id='native_reused' AND is_deleted=0 ORDER BY relative_path").fetch_all::<(String,u64)>().await?;
    assert_eq!(
        current,
        vec![("a".into(), 29), ("b".into(), 17)],
        "old source cleanup must not remove its replacement"
    );
    // 最终化中断后重放同一决定，公开事件不重复。
    let replay = store.resolve_final_events_native(round).await?;
    store.write_final_events_native(&replay, round).await?;
    store.delete_resume_entries(&keys).await?;
    store.cleanup_candidates(round).await?;
    assert!(store.resolve_final_events_native(round).await?.is_empty());
    let count: u64 = store
        .client
        .query("SELECT count() FROM incremental FINAL WHERE job_id='native_reused'")
        .fetch_one()
        .await?;
    assert_eq!(count, 2);
    store.cleanup_resume_batch().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn aggregate_statistics_keep_unique_rows_histograms_and_job_scope() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let store = ClusterScanStore::new(&config, &cluster, "aggregate_stats")?;
    store.initialize().await?;
    store.save_resume_state(&state(epoch, false, 0)).await?;
    let mut entries = vec![
        native_entry("a.TXT", 1, None)?,
        native_entry("b.txt", 4096, None)?,
        native_entry("no-extension", 65536, None)?,
    ];
    let backend = entries[0].source_identity().backend().clone();
    for (path, kind, size) in [
        ("nested/dir", EntryKind::Directory, 1024),
        ("link", EntryKind::Symlink, 0),
    ] {
        let identity = SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, path.as_bytes())?;
        let entry = if kind == EntryKind::Symlink {
            ObservedEntry::new_symlink(
                StoragePath::new(path)?,
                None,
                identity,
                SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"target".to_vec())?,
            )?
        } else {
            ObservedEntry::new(StoragePath::new(path)?, kind, Some(size), None, identity)?
        };
        entries.push(Arc::new(entry));
    }
    store.write_native(&entries, false).await?;
    store.write_native(&entries, false).await?;
    let other = ClusterScanStore::new(&config, &cluster, "other_stats")?;
    other.save_resume_state(&state(epoch, false, 0)).await?;
    other
        .write_native(&[native_entry("other.txt", 999999, None)?], false)
        .await?;
    let statistics = store.resume_statistics(0, None).await?;
    assert_eq!((statistics.summary.files, statistics.summary.file_bytes), (3, 69633));
    assert_eq!((statistics.summary.dirs, statistics.summary.dir_bytes), (1, 1024));
    assert_eq!((statistics.summary.links, statistics.summary.link_bytes), (1, 0));
    assert_eq!((statistics.summary.total_depth, statistics.summary.max_depth), (2, 2));
    assert_eq!(
        (statistics.summary.total_name_length, statistics.summary.max_name_length),
        (29, 12)
    );
    assert_eq!(statistics.extensions, vec![("txt".into(), 4097, 2)]);
    assert_eq!(statistics.ages, vec![(5, 70657, 5)]);
    assert_eq!(statistics.sizes, vec![(0, 1, 1), (1, 4096, 1), (2, 65536, 1)]);
    assert!(statistics.events.is_empty());

    // 新轮次只汇总当前 epoch，未被再次观察的旧路径不能混入实时校准。
    store.save_resume_state(&state(epoch + 10, true, 1)).await?;
    store.write_native(&entries[..1], false).await?;
    let next = store.resume_statistics(0, None).await?;
    assert_eq!(
        (next.summary.files, next.summary.file_bytes, next.summary.dirs),
        (1, 1, 0)
    );
    assert_eq!(next.extensions, vec![("txt".into(), 1, 1)]);
    let empty = ClusterScanStore::new(&config, &cluster, "empty_stats")?;
    empty.save_resume_state(&state(epoch + 20, false, 0)).await?;
    let empty = empty.resume_statistics(0, None).await?;
    assert_eq!(
        (empty.summary.files, empty.summary.dirs, empty.summary.links),
        (0, 0, 0)
    );
    assert!(empty.extensions.is_empty() && empty.ages.is_empty() && empty.sizes.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn aggregate_statistics_cover_every_size_and_age_bucket() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let store = ClusterScanStore::new(&config, &cluster, "histogram_stats")?;
    store.initialize().await?;
    store.save_resume_state(&state(epoch, false, 0)).await?;
    let backend = BackendIdentity::new(BackendKind::Nfs, "histogram-fixture")?;
    let sizes = [
        0_u64, 4096, 65536, 131072, 262144, 524288, 1048576, 2097152, 16777216, 67108864, 134217728, 536870912,
    ];
    let days = [0_i128, 3, 14, 90, 270, 730];
    let mut entries = Vec::with_capacity(sizes.len());
    for (index, size) in sizes.into_iter().enumerate() {
        let path = format!("bucket_{index}.dat");
        let identity = SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, path.as_bytes())?;
        let modified = StorageTimestamp::new(
            i128::from(epoch) - days[index % days.len()] * 86400000000000,
            TimePrecision::Nanoseconds,
        )?;
        entries.push(Arc::new(ObservedEntry::new(
            StoragePath::new(path)?,
            EntryKind::File,
            Some(size),
            Some(modified),
            identity,
        )?));
    }
    store.write_native(&entries, false).await?;
    let statistics = store.resume_statistics(0, None).await?;
    let expected_sizes: Vec<_> = sizes
        .into_iter()
        .enumerate()
        .map(|(bucket, size)| (bucket as u8, size as i64, 1))
        .collect();
    assert_eq!(statistics.sizes, expected_sizes);
    let expected_ages: Vec<_> = (0..6)
        .map(|bucket| (bucket as u8, (sizes[bucket] + sizes[bucket + 6]) as i64, 2))
        .collect();
    assert_eq!(statistics.ages, expected_ages);
    assert_eq!(
        statistics.extensions,
        vec![("dat".into(), sizes.iter().sum::<u64>() as i64, 12)]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real multi-shard ClickHouse cluster"]
async fn cluster_native_path_scoped_unchanged_batches_never_create_events() -> Result<()> {
    let (config, _admin, _guard, cluster, epoch) = fixture().await?;
    let store = ClusterScanStore::new(&config, &cluster, "path_scoped_unchanged")?;
    store.load_resume_state().await?;
    store.save_resume_state(&state(epoch, false, 1)).await?;
    let entries = (0..1_024)
        .map(|index| {
            let key = format!("path/{index:08}");
            native_entry(&key, 17, None)
        })
        .collect::<Result<Vec<_>>>()?;
    store.write_native(&entries, false).await?;
    store.commit_scan_generation().await?;
    store.save_resume_state(&state(epoch + 10, true, 0)).await?;
    let mut workers: JoinSet<Result<()>> = JoinSet::new();
    for worker in 0..4 {
        let mut writer = store.batch_writer();
        let entries: Vec<_> = entries.iter().skip(worker).step_by(4).cloned().collect();
        workers.spawn(async move {
            for round in 0..128 {
                // 每次写入独立 epoch，避免服务端幂等去重使并发压力退化为空写入。
                writer
                    .epoch
                    .store(epoch as u64 + 20 + round * 4 + worker as u64, Ordering::Relaxed);
                writer.begin_resume_batch().await?;
                writer.write_native_keys(&entries).await?;
                let events = writer.compare_native(&entries).await?;
                assert!(
                    events.is_empty(),
                    "unchanged batch produced {} events in worker {worker}, round {round}",
                    events.len()
                );
                writer.write_native(&entries, false).await?;
            }
            Ok(())
        });
    }
    while let Some(worker) = workers.join_next().await {
        worker??;
    }
    store.cleanup_resume_batch().await?;
    Ok(())
}
