//! 对真实单机或集群验证扫描元数据的 cut 恢复和 job 隔离。
use clickhouse::{Client, sql::Identifier};
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, SpecialFileKind,
    StoragePath, SymlinkTarget, SymlinkTargetEncoding,
};
use db::{
    ClickHouseConfig, DatabaseConfig, DatabaseFactory, Result,
    clickhouse::scan_resume::{RUNNING, ScanResumeState},
    scan_store::ScanStore,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storage_adapter::observed_scan::{ObservedItem, ObservedScanEvent};
use tokio::time::sleep;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

struct Fixture {
    config: ClickHouseConfig,
    client: Client,
    admin: Client,
    cluster: Option<String>,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let cluster = std::env::var("LAB_CLICKHOUSE_CLUSTER").ok();
        let config = ClickHouseConfig {
            mode: if cluster.is_some() {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.clone(),
            dsn: std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into()),
            database: format!(
                "{}{}",
                if cluster.is_some() {
                    "ts_scan_cuts_"
                } else {
                    "ts-scan-cuts?`_"
                },
                Uuid::new_v4().simple()
            ),
            username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
            password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
            dial_timeout: 5,
            read_timeout: 60,
        };
        let admin = Client::default()
            .with_url(&config.dsn)
            .with_user(&config.username)
            .with_password(config.password.as_deref().unwrap_or_default())
            .with_option("async_insert", "0")
            .with_option("insert_distributed_sync", "1")
            .with_option("insert_quorum", "auto")
            .with_option("insert_quorum_parallel", "0")
            .with_option("insert_quorum_timeout", "1000");
        let on_cluster = cluster
            .as_ref()
            .map_or(String::new(), |name| format!(" ON CLUSTER `{name}`"));
        admin
            .query(&format!("CREATE DATABASE ?{on_cluster}"))
            .bind(Identifier(&config.database))
            .execute()
            .await?;
        Ok(Self {
            client: admin.clone().with_database(&config.database),
            admin,
            config,
            cluster,
        })
    }
    fn table(&self, name: &str, job: &str) -> String {
        if self.cluster.is_some() {
            name.into()
        } else {
            format!("{name}_{job}")
        }
    }
    async fn write(&self, job: &str, path: &str, size: u64) -> Result<()> {
        let (columns, values) = if self.cluster.is_some() {
            (
                "job_id,relative_path,name,size,current_state",
                format!("'{job}','{path}','{path}',{size},1"),
            )
        } else {
            (
                "relative_path,name,size,current_state",
                format!("'{path}','{path}',{size},1"),
            )
        };
        self.client
            .query(&format!(
                "INSERT INTO {} ({columns}) VALUES ({values})",
                self.table("base", job)
            ))
            .execute()
            .await?;
        Ok(())
    }
    async fn event(&self, job: &str, path: &str) -> Result<()> {
        let (columns, values) = if self.cluster.is_some() {
            (
                "job_id,relative_path,operation_type",
                format!("'{job}','{path}','data_changed'"),
            )
        } else {
            ("relative_path,operation_type", format!("'{path}','data_changed'"))
        };
        self.client
            .query(&format!(
                "INSERT INTO {} ({columns}) VALUES ({values})",
                self.table("incremental", job)
            ))
            .execute()
            .await?;
        Ok(())
    }
    async fn rows(&self, table: &str, job: &str) -> Result<Vec<String>> {
        let scope = if self.cluster.is_some() {
            format!(" WHERE job_id='{job}'")
        } else {
            String::new()
        };
        Ok(self
            .client
            .query(&format!(
                "SELECT relative_path FROM {} FINAL{scope} ORDER BY relative_path",
                self.table(table, job)
            ))
            .fetch_all()
            .await?)
    }
    async fn cleanup(&self) -> Result<()> {
        let cluster = self
            .cluster
            .as_ref()
            .map_or(String::new(), |name| format!(" ON CLUSTER `{name}`"));
        self.admin
            .query(&format!("DROP DATABASE IF EXISTS ?{cluster} SYNC"))
            .bind(Identifier(&self.config.database))
            .execute()
            .await?;
        Ok(())
    }
    async fn store(&self, job: &str) -> Result<ScanStore> {
        let store = ScanStore::new(&self.config, job)?;
        store.load_resume_state().await?;
        store.initialize().await?;
        store.prepare_incremental().await?;
        store.prepare_native_schema().await?;
        Ok(store)
    }
}

#[tokio::test]
#[ignore = "requires an isolated writable ClickHouse database"]
async fn idle_scan_monitor_stays_healthy_in_the_configured_deployment() -> Result<()> {
    let fixture = Fixture::new().await?;
    let execution = ScanStore::new(&fixture.config, "healthy_probe")?
        .query_cancellation()
        .execution();
    let monitor = execution.start_monitor()?;
    // 没有业务 ACK，至少跨过一次完整的探测 + 恢复窗口，不能把 NULL 标记读成 UInt8 零。
    sleep(Duration::from_millis(4500)).await;
    let failure = execution.failure();
    let drained = monitor.finish().await;
    let cleanup = fixture.cleanup().await;
    drained?;
    cleanup?;
    assert!(
        failure.is_none(),
        "healthy ClickHouse was reported unavailable: {failure:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated writable ClickHouse database"]
async fn checkpoint_restores_metadata_and_events_after_merges_and_keeps_other_jobs() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise(&fixture).await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup
}

async fn exercise(fixture: &Fixture) -> Result<()> {
    let job = "scan_a";
    let store = fixture.store(job).await?;
    let _other = fixture.store("scan_b").await?;
    eprintln!("stage: empty baseline");
    let empty = store.capture_scan_baseline().await?;
    fixture.write(job, "old", 10).await?;
    fixture.event(job, "old").await?;
    eprintln!("stage: populated baseline");
    let baseline = store.capture_scan_baseline().await?;
    let mut state = ScanResumeState {
        id: 1,
        schema_version: 1,
        version: 1,
        request_key: "cut-test".into(),
        generation: 1,
        incremental: false,
        phase: RUNNING,
        event_time: 1,
        observed_entries: 0,
        checkpoint: vec![],
        statistics: String::new(),
        baseline_snapshot: baseline.clone(),
        checkpoint_snapshot: baseline,
    };
    store.save_resume_state(&state).await?;
    fixture.write(job, "prefix", 20).await?;
    fixture.event(job, "prefix").await?;
    state.advance()?;
    eprintln!("stage: seal");
    store.seal_resume_state(&mut state).await?;
    fixture.write(job, "old", 999).await?;
    fixture.write(job, "ghost", 30).await?;
    fixture.event(job, "ghost").await?;
    fixture.write("scan_b", "unrelated", 40).await?;
    let local = if fixture.cluster.is_some() {
        "base_local".into()
    } else {
        fixture.table("base", job)
    };
    let cluster = fixture
        .cluster
        .as_ref()
        .map_or(String::new(), |name| format!(" ON CLUSTER `{name}`"));
    fixture
        .client
        .query(&format!("OPTIMIZE TABLE {local}{cluster} FINAL"))
        .execute()
        .await?;
    eprintln!("stage: restore checkpoint");
    store.restore_scan_checkpoint(&state).await?;
    assert_eq!(fixture.rows("base", job).await?, ["old", "prefix"]);
    assert_eq!(fixture.rows("incremental", job).await?, ["old", "prefix"]);
    assert_eq!(fixture.rows("base", "scan_b").await?, ["unrelated"]);
    let scope = if fixture.cluster.is_some() {
        " AND job_id='scan_a'"
    } else {
        ""
    };
    let old_size: u64 = fixture
        .client
        .query(&format!(
            "SELECT size FROM {} FINAL WHERE relative_path='old'{scope}",
            fixture.table("base", job)
        ))
        .fetch_one()
        .await?;
    assert_eq!(old_size, 10);
    if fixture.cluster.is_some() {
        eprintln!("stage: outstanding old DDL");
        // 模拟发起查询已经返回/被取消，但旧 DELETE 仍在分布式 DDL 队列里执行。
        let client = fixture
            .client
            .clone()
            .with_option("distributed_ddl_task_timeout", "0")
            .with_option("allow_nondeterministic_mutations", "1")
            .with_option("mutations_sync", "2");
        client
            .query(&format!(
                "ALTER TABLE `{}`.base_local ON CLUSTER clickhouse DELETE WHERE job_id='scan_a' AND sleep(2)=0",
                fixture.config.database
            ))
            .execute()
            .await?;
        let pending: u64 = fixture.client.query("SELECT count() FROM system.distributed_ddl_queue WHERE position(query,?)>0 AND position(query,'sleep')>0 AND (status IS NULL OR status!='Finished')")
            .bind(&fixture.config.database).fetch_one().await?;
        assert!(pending > 0, "the fixture must leave an old destructive DDL outstanding");
        eprintln!("stage: restore checkpoint");
        store.restore_scan_checkpoint(&state).await?;
        fixture.write(job, "after_restore", 1).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert_eq!(fixture.rows("base", job).await?, ["after_restore", "old", "prefix"]);
    }
    let config = DatabaseConfig {
        enabled: true,
        db_type: "clickhouse".into(),
        batch_size: 1,
        clickhouse: Some(fixture.config.clone()),
    };
    assert!(DatabaseFactory::new_database(&config, job).await.is_err());
    store.restore_scan_baseline(&state).await?;
    assert_eq!(fixture.rows("base", job).await?, ["old"]);
    assert_eq!(fixture.rows("incremental", job).await?, ["old"]);
    // 合法空快照必须清除尾部；快照缺失与空快照不能混为一谈。
    let mut empty_state = state.clone();
    empty_state.checkpoint_snapshot = empty;
    // empty 已被 seal 后的 GC 回收，缺失必须 fail closed，且不能改动 working。
    assert!(store.restore_scan_checkpoint(&empty_state).await.is_err());
    assert_eq!(fixture.rows("base", job).await?, ["old"]);
    let empty_store = fixture.store("empty_job").await?;
    let empty = empty_store.capture_scan_baseline().await?;
    fixture.write("empty_job", "ghost", 1).await?;
    empty_state.checkpoint_snapshot = empty;
    empty_store.restore_scan_checkpoint(&empty_state).await?;
    assert!(fixture.rows("base", "empty_job").await?.is_empty());
    assert_eq!(fixture.rows("base", "scan_b").await?, ["unrelated"]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated writable ClickHouse database"]
async fn special_entries_remain_persisted_without_inflating_regular_statistics() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise_special_statistics(&fixture).await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup
}

async fn exercise_special_statistics(fixture: &Fixture) -> Result<()> {
    let store = fixture.store("special_types").await?;
    let epoch = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let state = ScanResumeState {
        id: 1,
        schema_version: 1,
        version: 1,
        request_key: "typed-statistics".into(),
        generation: 1,
        incremental: true,
        phase: RUNNING,
        event_time: epoch,
        observed_entries: 0,
        checkpoint: vec![],
        statistics: String::new(),
        baseline_snapshot: String::new(),
        checkpoint_snapshot: String::new(),
    };
    store.save_resume_state(&state).await?;
    store.begin_scan_generation().await?;
    let backend = BackendIdentity::new(BackendKind::Nfs, "special-statistics")?;
    let mut entries = Vec::new();
    for (index, kind) in [
        EntryKind::File,
        EntryKind::Directory,
        EntryKind::Symlink,
        EntryKind::Special(SpecialFileKind::BlockDevice),
        EntryKind::Special(SpecialFileKind::CharacterDevice),
        EntryKind::Special(SpecialFileKind::Fifo),
        EntryKind::Special(SpecialFileKind::Socket),
    ]
    .into_iter()
    .enumerate()
    {
        let path = format!("entry_{index}.dat");
        let identity = SourceIdentity::new(backend.clone(), IdentityStrength::StableWithinBackend, path.as_bytes())?;
        let entry = if kind == EntryKind::Symlink {
            ObservedEntry::new_symlink(
                StoragePath::new(path)?,
                None,
                identity,
                SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"target".to_vec())?,
            )?
        } else {
            ObservedEntry::new(StoragePath::new(path)?, kind, Some(17), None, identity)?
        };
        entries.push(Arc::new(entry));
    }
    store.write_observations(&entries, false).await?;
    let events = entries
        .into_iter()
        .map(|entry| {
            ObservedScanEvent::New(ObservedItem {
                entry,
                version_index: None,
            })
        })
        .collect::<Vec<_>>();
    store.write_resume_events(&events, epoch).await?;
    let statistics = store.resume_statistics(1, Some(epoch)).await?;
    assert_eq!((statistics.summary.files, statistics.summary.file_bytes), (1, 17));
    assert_eq!((statistics.summary.dirs, statistics.summary.links), (1, 1));
    assert_eq!(statistics.sizes.iter().map(|(_, _, count)| count).sum::<u64>(), 1);
    assert_eq!(statistics.events.iter().map(|event| event.entries).sum::<u64>(), 7);
    assert_eq!(
        statistics
            .events
            .iter()
            .filter(|event| !event.is_dir && !event.is_symlink && !event.is_regular_file)
            .map(|event| event.entries)
            .sum::<u64>(),
        4
    );
    for table in ["base", "incremental"] {
        let actual = fixture
            .client
            .query(&format!(
                "SELECT count(),countIf(is_regular_file) FROM {} FINAL",
                fixture.table(table, "special_types")
            ))
            .fetch_one::<(u64, u64)>()
            .await?;
        assert_eq!(actual, (7, 1), "all typed entries must remain in persisted facts");
    }
    Ok(())
}
