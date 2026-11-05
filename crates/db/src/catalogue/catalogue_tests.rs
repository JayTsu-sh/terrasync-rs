//! 目录集群适配经由公开连接和通用 Database 契约验证，不借用 native scan 入口。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::execution::base_client;
use data_mover::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use std::path::PathBuf;
use std::time::Duration;
use storage_adapter::ObservedEntryExt;
use tokio::runtime::Builder;
use tokio::time::timeout;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

struct Fixture {
    config: ClickHouseConfig,
    admin: Client,
    source: PathBuf,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let dsn = std::env::var("LAB_CLICKHOUSE_DSN").map_err(|_| {
            DatabaseError::ConfigError("LAB_CLICKHOUSE_DSN must explicitly select a test server".into())
        })?;
        let cluster = if std::env::var("LAB_CATALOGUE_SINGLE").as_deref() == Ok("1") {
            None
        } else {
            Some(std::env::var("LAB_CLICKHOUSE_CLUSTER").map_err(|_| {
                DatabaseError::ConfigError(
                    "LAB_CLICKHOUSE_CLUSTER must explicitly select a six-replica test cluster".into(),
                )
            })?)
        };
        let database = format!("ts_catalogue_{}", Uuid::new_v4().simple());
        let mut config = ClickHouseConfig {
            mode: if cluster.is_some() {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.clone(),
            dsn,
            database: "default".into(),
            username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
            password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
            dial_timeout: 10,
            read_timeout: 60,
        };
        let admin = base_client(&config).unwrap();
        // 本 fixture 只操作自己的随机数据库及临时本地文件，不访问远程源。
        if let Some(cluster) = &cluster {
            let replicas = admin
                .query("SELECT count() FROM system.clusters WHERE cluster=?")
                .bind(cluster)
                .fetch_one::<u64>()
                .await?;
            if replicas != 6 {
                return Err(DatabaseError::ConfigError(format!(
                    "catalogue integration fixture requires six replicas, found {replicas}"
                )));
            }
            admin
                .query(&format!("CREATE DATABASE {database} ON CLUSTER `{cluster}`"))
                .execute()
                .await?;
        } else {
            admin.query(&format!("CREATE DATABASE {database}")).execute().await?;
        }
        eprintln!("catalogue fixture database: {database}");
        config.database = database.clone();
        let source = std::env::temp_dir().join(database);
        std::fs::create_dir_all(&source)?;
        Ok(Self { config, admin, source })
    }

    async fn database(&self, job: &str) -> Result<Box<dyn Database>> {
        let database: Box<dyn Database> = Box::new(ClickHouseDatabase::connect(&self.config, job).await?);
        database.initialize().await?;
        Ok(database)
    }

    fn entry(&self, path: &str, size: u64, handle: &str) -> Arc<ObservedEntry> {
        std::fs::write(self.source.join(path), vec![0u8; usize::try_from(size).unwrap()]).unwrap();
        Arc::new(
            {
                let fixture_path: std::path::PathBuf = path.into();
                let fixture_storage: String = "nas".into();
                let fixture_version: String = String::new();
                let fixture_handle: Option<String> = Some(format!("54534934{handle}"));
                let fixture_tags: Option<String> = None;
                let entry = crate::observed_fixtures::observation(
                    if fixture_storage == "s3" {
                        data_mover::model::BackendKind::S3
                    } else {
                        data_mover::model::BackendKind::Nfs
                    },
                    fixture_path,
                    data_mover::model::EntryKind::File,
                    size,
                    [1_000_000_000, 1_000_000_000, 1_000_000_000],
                    Some(data_mover::model::OwnershipMode {
                        mode: 420,
                        uid: 1000,
                        gid: 1000,
                    }),
                    fixture_handle.map(|handle| hex::decode(handle).unwrap()),
                    fixture_tags.map(|tags| {
                        serde_json::from_str::<Vec<storage_adapter::Tag>>(&tags)
                            .unwrap()
                            .into_iter()
                            .map(|tag| (tag.key, tag.value))
                            .collect()
                    }),
                    (!fixture_version.is_empty()).then_some((Some(fixture_version), true, false)),
                );
                StorageEntryRecord::capture(&entry, 0).unwrap()
            }
            .to_observation()
            .unwrap(),
        )
    }

    async fn rows(&self, job: &str) -> Result<Vec<StorageEntryRecord>> {
        let table = get_scan_base_table_name(job);
        Ok(base_client(&self.config)
            .unwrap()
            .query(&format!(
                "SELECT {} FROM {table} FINAL ORDER BY relative_path, version_id",
                *FILE_SCAN_COLUMNS_LIST
            ))
            .fetch_all()
            .await?)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let database = self.config.database.clone();
        let cluster = self.config.cluster.clone();
        let config = ClickHouseConfig {
            database: "default".into(),
            ..self.config.clone()
        };
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = Builder::new_current_thread().enable_all().build() {
                let admin = base_client(&config).unwrap();
                let statement = match cluster {
                    Some(cluster) => format!("DROP DATABASE IF EXISTS {database} ON CLUSTER `{cluster}` SYNC"),
                    None => format!("DROP DATABASE IF EXISTS {database} SYNC"),
                };
                let _ = runtime.block_on(admin.query(&statement).execute());
            }
        })
        .join();
        let _ = std::fs::remove_dir_all(&self.source);
    }
}

fn paths(entries: impl Iterator<Item = ObservedEntry>) -> Vec<String> {
    entries
        .map(|entry| entry.get_relative_path().to_string_lossy().into_owned())
        .collect()
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn cluster_catalogue_copy_sync_and_delete_are_job_isolated() -> Result<()> {
    let fixture = Fixture::new().await?;
    let mut a = fixture.database("a").await?;
    let b = fixture.database("b").await?;
    let original = [
        fixture.entry("gone", 10, "aa"),
        fixture.entry("oldname", 11, "bb"),
        fixture.entry("keep", 12, "cc"),
    ];
    a.begin_scan_generation().await?;
    a.batch_insert_base_record(&original).await?;
    a.commit_scan_generation().await?;
    b.begin_scan_generation().await?;
    b.batch_insert_base_record(&[fixture.entry("gone", 99, "dd")]).await?;
    b.commit_scan_generation().await?;
    let next = [
        fixture.entry("newname", 11, "bb"),
        fixture.entry("keep", 13, "cc"),
        fixture.entry("added", 14, "ee"),
    ];
    a.begin_scan_generation().await?;
    a.create_scan_temporary_table().await?;
    a.batch_insert_temp_record(&next).await?;
    assert_eq!(paths(a.detect_new_items().await?), ["added"]);
    let changed: Vec<_> = a.detect_changed_items().await?.collect();
    assert!(
        matches!(changed.as_slice(), [(entry, ChangeKind::DataOnly)] if entry.get_relative_path().to_string_lossy()=="keep")
    );
    a.insert_temp_to_base_table(&[("added".into(), String::new())]).await?;
    let final_events: Vec<_> = a.detect_deleted_items().await?.collect();
    assert!(final_events.iter().any(
        |event| matches!(event, DeletionStatus::Deleted(entry) if entry.get_relative_path().to_string_lossy()=="gone")
    ));
    assert!(final_events.iter().any(|event| matches!(event, DeletionStatus::Renamed(from,to) if from.get_relative_path().to_string_lossy()=="oldname" && to.get_relative_path().to_string_lossy()=="newname")));
    assert_eq!(final_events.len(), 2);
    a.batch_delete_base_record(&["gone".into(), "oldname".into()]).await?;
    a.commit_scan_generation().await?;
    a.drop_scan_temporary_table().await?;
    assert_eq!(a.get_count("base").await?, 2);
    assert_eq!(b.get_count("base").await?, 1);
    assert_eq!(fixture.rows("b").await?[0].size, 99);
    let (tx, mut rx) = mpsc::channel(8);
    a.query_storage_entry(None, None, None, tx).await?;
    let mut observed = Vec::new();
    while let Some(row) = rx.recv().await {
        observed.push(row.get_relative_path().to_string_lossy().into_owned());
    }
    observed.sort();
    assert_eq!(observed, ["keep", "newname"]);
    // 工厂 clone 仍写同一部署和 generation，而不是退回本机表。
    let clone = a.clone_box();
    clone.update_base_record(&fixture.entry("keep", 15, "cc")).await?;
    assert_eq!(fixture.rows("a").await?[0].size, 15);
    a.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
    a.batch_insert_incremental_record(&[
        StorageEntryMessage::New(next[2].clone().into()),
        StorageEntryMessage::Changed {
            entry: next[1].clone().into(),
            kind: ChangeKind::DataOnly,
        },
        StorageEntryMessage::Renamed((original[1].clone(), next[0].clone())),
        StorageEntryMessage::Deleted(original[0].clone()),
    ])
    .await?;
    let events: Vec<(String, String, Option<String>)> = base_client(&fixture.config)
        .unwrap()
        .query("SELECT operation_type, relative_path, comment FROM incremental_a FINAL ORDER BY operation_type")
        .fetch_all()
        .await?;
    assert_eq!(events.len(), 4);
    assert!(
        events.iter().any(|(kind, path, comment)| kind == "rename"
            && path == "newname"
            && comment.as_deref() == Some("oldname"))
    );
    a.create_tar_manifest_table().await?;
    a.batch_insert_tar_manifest(&[TarManifestRecord::from_entry(next[0].as_ref(), "bundle.tar")])
        .await?;
    let manifest = base_client(&fixture.config)
        .unwrap()
        .query("SELECT entry_path FROM tar_manifest_a FINAL")
        .fetch_one::<String>()
        .await?;
    assert_eq!(manifest, "newname");
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn cluster_catalogue_versions_survive_reopening_an_uncommitted_generation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let version = |id: &str, size| {
        let _ = fixture.entry("object", size, "aa");
        Arc::new(crate::observed_fixtures::observation(
            data_mover::model::BackendKind::S3,
            "object".into(),
            data_mover::model::EntryKind::File,
            size,
            [1_000_000_000; 3],
            None,
            None,
            None,
            Some((Some(id.into()), id == "v2", false)),
        ))
    };
    let first = version("v1", 11);
    let second = version("v2", 12);
    let database = fixture.database("versions").await?;
    database.begin_scan_generation().await?;
    database.batch_insert_base_record(&[first, second]).await?;
    database.commit_scan_generation().await?;
    database.begin_scan_generation().await?;
    database.update_base_record(&version("v2", 13)).await?;
    drop(database);
    let mut reopened = fixture.database("versions").await?;
    let working = reopened.begin_scan_generation().await?;
    let rows = fixture.rows("versions").await?;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.current_state != working));
    assert!(rows.iter().any(|row| row.version_id == "v2" && row.size == 13));
    reopened.create_scan_temporary_table().await?;
    reopened.batch_insert_temp_record(&[version("v2", 13)]).await?;
    reopened.insert_temp_to_base_table(&[]).await?;
    let deleted: Vec<_> = reopened.detect_deleted_items().await?.collect();
    assert!(matches!(deleted.as_slice(), [DeletionStatus::Deleted(entry)] if entry.get_version_id()==Some("v1")));
    reopened.commit_scan_generation().await?;
    reopened.drop_scan_temporary_table().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn incomplete_copy_replays_metadata_that_preceded_a_failed_rename_copy() -> Result<()> {
    let fixture = Fixture::new().await?;
    let mut database = ClickHouseDatabase::new(&fixture.config, "copy_retry")?;
    database.initialize().await?;
    let original = fixture.entry("old", 10, "aa");
    database.batch_insert_base_record(&[original]).await?;
    database.insert_scan_state(0).await?;
    assert_eq!(
        database.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Fresh
    );
    let changed = fixture.entry("new", 20, "aa");
    database.create_scan_temporary_table().await?;
    database.batch_insert_temp_record(&[changed.clone()]).await?;
    assert_eq!(database.detect_new_items().await?.count(), 0);
    assert_eq!(database.detect_changed_items().await?.count(), 0);
    // Phase A has observed the new source facts, but Phase B has not acknowledged
    // its rename+copy. The target may already have moved while retaining old bytes.
    database.insert_temp_to_base_table(&[]).await?;
    database.drop_scan_temporary_table().await?;
    drop(database);
    let mut retry = ClickHouseDatabase::new(&fixture.config, "copy_retry")?;
    assert_eq!(
        retry.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Recovering
    );
    retry.create_scan_temporary_table().await?;
    retry.batch_insert_temp_record(&[changed.clone()]).await?;
    let replay: Vec<_> = retry.detect_new_items().await?.collect();
    assert_eq!(
        replay.len(),
        1,
        "unconfirmed source metadata must not suppress the retry copy"
    );
    assert_eq!(replay[0].get_relative_path().to_string_lossy(), "new");
    assert_eq!(replay[0].get_size(), 20);
    assert_eq!(retry.detect_changed_items().await?.count(), 0);
    // Replay is excluded from Phase A promotion; only the physical-copy ACK writes it.
    retry
        .insert_temp_to_base_table(&[("new".into(), String::new())])
        .await?;
    retry.update_base_record(&changed).await?;
    let deleted: Vec<_> = retry.detect_deleted_items().await?.collect();
    assert!(
        matches!(deleted.as_slice(), [DeletionStatus::Deleted(entry)] if entry.get_relative_path().to_string_lossy()=="old")
    );
    retry.batch_delete_base_record(&["old".into()]).await?;
    retry.commit_scan_generation().await?;
    retry.drop_scan_temporary_table().await?;
    assert_eq!(retry.get_count("base").await?, 1);
    drop(retry);
    let fresh = ClickHouseDatabase::new(&fixture.config, "copy_retry")?;
    assert_eq!(
        fresh.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Fresh
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn empty_unconfirmed_copy_remains_dirty_until_commit() -> Result<()> {
    let fixture = Fixture::new().await?;
    let database = ClickHouseDatabase::new(&fixture.config, "empty_copy")?;
    database.initialize().await?;
    assert_eq!(
        database.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Fresh
    );
    assert_eq!(database.get_count("base").await?, 0);
    drop(database);
    // A physical move may already have removed its source before an empty base's
    // first DB acknowledgement. The caller must see Recovering and reject replay.
    let retry = ClickHouseDatabase::new(&fixture.config, "empty_copy")?;
    assert!(matches!(
        retry.begin_copy_generation("changed-source-or-move-policy").await,
        Err(DatabaseError::ConfigError(_))
    ));
    assert_eq!(
        retry.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Recovering
    );
    assert_eq!(retry.get_count("base").await?, 0);
    retry.commit_scan_generation().await?;
    drop(retry);
    let clean = ClickHouseDatabase::new(&fixture.config, "empty_copy")?;
    assert_eq!(
        clean.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Fresh
    );
    clean.commit_scan_generation().await?;
    // The same two generation values and identical payload recur across rounds.
    // Replica block deduplication must not suppress the new started marker/commit.
    for _ in 0..4 {
        let next = ClickHouseDatabase::new(&fixture.config, "empty_copy")?;
        assert_eq!(
            next.begin_copy_generation("catalogue-fixture-v1").await?,
            CopyGenerationStatus::Fresh
        );
        let marker = next
            .sync_client
            .query("SELECT copy_in_progress FROM state_empty_copy FINAL WHERE id=1")
            .fetch_quorum_one::<bool>()
            .await?;
        assert!(marker);
        next.commit_scan_generation().await?;
        let marker = next
            .sync_client
            .query("SELECT copy_in_progress FROM state_empty_copy FINAL WHERE id=1")
            .fetch_quorum_one::<bool>()
            .await?;
        assert!(!marker);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn history_copy_recovery_keeps_the_group_and_does_not_delete_the_replayed_path() -> Result<()> {
    let fixture = Fixture::new().await?;
    use data_mover::model::{HistoryOrder, ObjectHistory};
    use storage_adapter::observed_scan::ObservedItem;
    // 源顺序不能由版本 ID、变更类型或时间戳排序推断；真实分段必须保留位置。
    let versions: Vec<_> = [("z-old", false), ("m-delete", true), ("a-new", false)]
        .into_iter()
        .map(|(id, deleted)| {
            crate::observed_fixtures::observation(
                BackendKind::S3,
                "object".into(),
                EntryKind::File,
                10,
                [1_000_000_000; 3],
                None,
                None,
                None,
                Some((Some(id.into()), id == "a-new", deleted)),
            )
        })
        .collect();
    let records = versions
        .iter()
        .map(|entry| entry.history().unwrap().records()[0].clone())
        .collect();
    let history = ObjectHistory::whole(records, HistoryOrder::Server)?;
    let whole = versions
        .into_iter()
        .last()
        .unwrap()
        .with_history(Some(Box::new(history)))?;
    let segments: Vec<_> = whole.into_segments(1).into_iter().map(Arc::new).collect();
    let mut database = ClickHouseDatabase::new(&fixture.config, "history_copy")?;
    database.initialize().await?;
    database.begin_copy_generation("catalogue-fixture-v1").await?;
    database.batch_insert_base_record(&[Arc::clone(&segments[0])]).await?;
    database.commit_scan_generation().await?;
    database.begin_copy_generation("catalogue-fixture-v1").await?;
    drop(database); // A later source/metadata failure left this round uncommitted.
    database = ClickHouseDatabase::new(&fixture.config, "history_copy")?;
    assert_eq!(
        database.begin_copy_generation("catalogue-fixture-v1").await?,
        CopyGenerationStatus::Recovering
    );
    database.create_scan_temporary_table().await?;
    database.batch_insert_temp_record(&segments).await?;
    let replay: Vec<_> = database.clone_box().detect_new_items().await?.collect();
    assert_eq!(
        replay.iter().map(|entry| entry.get_version_id()).collect::<Vec<_>>(),
        [Some("z-old"), Some("m-delete"), Some("a-new")]
    );
    assert_eq!(
        replay.iter().map(|entry| entry.get_is_latest()).collect::<Vec<_>>(),
        [false, false, true]
    );
    assert_eq!(
        replay
            .iter()
            .map(|entry| entry.get_is_delete_marker())
            .collect::<Vec<_>>(),
        [false, true, false]
    );
    for (entry, source) in replay.iter().zip(&segments) {
        assert_eq!(
            entry.identity_key(),
            source.history().unwrap().records()[0].identity().identity_key()
        );
    }
    let before_ack = fixture.rows("history_copy").await?;
    assert_eq!(before_ack.len(), 1);
    assert_eq!(before_ack[0].version_id, "z-old");
    assert_eq!(database.detect_changed_items().await?.count(), 0);
    database
        .insert_temp_to_base_table(&[
            ("object".into(), "z-old".into()),
            ("object".into(), "m-delete".into()),
            ("object".into(), "a-new".into()),
        ])
        .await?;
    // A non-history destination acknowledges only the latest item of this group.
    assert_eq!(fixture.rows("history_copy").await?.len(), 1);
    database
        .batch_insert_completed_records(&[ObservedItem {
            entry: Arc::clone(&segments[2]),
            version_index: Some(0),
        }])
        .await?;
    assert_eq!(database.detect_deleted_items().await?.count(), 0);
    let records = fixture.rows("history_copy").await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].version_id, "a-new");
    database.commit_scan_generation().await?;
    database.drop_scan_temporary_table().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn cluster_catalogue_snapshot_can_exchange_repeatedly_without_reusing_keeper_paths() -> Result<()> {
    let fixture = Fixture::new().await?;
    let backend = BackendIdentity::new(BackendKind::Local, "catalogue-local-fixture")?;
    let mut database = ClickHouseDatabase::new(&fixture.config, "snapshot")?;
    database.initialize().await?;
    let mut previous_paths = HashSet::new();
    for (index, path) in ["first", "renamed", "third"].into_iter().enumerate() {
        std::fs::write(fixture.source.join(path), b"snapshot").unwrap();
        let observation = ObservedEntry::new(
            StoragePath::new(path)?,
            EntryKind::File,
            Some(8),
            None,
            SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, path.as_bytes())?,
        )?;
        let generation = database.begin_observation_snapshot().await?;
        database
            .batch_insert_temp_observations(&[ObservedEntryRecord::capture(&observation, generation)])
            .await?;
        assert_eq!(database.publish_observation_snapshot().await?, u64::from(index != 0));
        assert_eq!(
            database
                .load_observation(&hex::encode(observation.identity_key().as_bytes()), &backend)
                .await?,
            Some(observation)
        );
        let records: Vec<(String, String)> = fixture.admin.query(
            "SELECT hostName(), zookeeper_path FROM clusterAllReplicas(?, system.replicas) WHERE database=? AND table='__catalogue_base_snapshot'"
        ).bind(fixture.config.cluster.as_deref().unwrap()).bind(&fixture.config.database).fetch_all().await?;
        assert_eq!(records.len(), 6);
        for (host, path) in records {
            assert!(
                previous_paths.insert((host, path)),
                "a retired snapshot Keeper path was reused"
            );
        }
        assert_eq!(database.get_count("base").await?, 1);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn cluster_catalogue_rejects_existing_single_table_without_modifying_it() -> Result<()> {
    let fixture = Fixture::new().await?;
    let cluster = fixture.config.cluster.as_deref().unwrap();
    let database = &fixture.config.database;
    fixture.admin.query(&format!(
        "CREATE TABLE `{database}`.base_old ON CLUSTER `{cluster}` ({FILE_SCAN_COLUMNS_DEFINITION}) ENGINE=ReplacingMergeTree ORDER BY (relative_path,version_id)"
    )).execute().await?;
    fixture
        .admin
        .query(&format!(
            "INSERT INTO `{database}`.base_old (relative_path,size) VALUES ('untouched',7)"
        ))
        .execute()
        .await?;
    let catalogue = ClickHouseDatabase::new(&fixture.config, "old")?;
    assert!(matches!(
        catalogue.initialize().await,
        Err(DatabaseError::ConfigError(_))
    ));
    let (count, size) = fixture
        .admin
        .query("SELECT count(),sum(size) FROM clusterAllReplicas(?, ?, 'base_old') WHERE relative_path='untouched'")
        .bind(cluster)
        .bind(database)
        .fetch_one::<(u64, u64)>()
        .await?;
    assert_eq!((count, size), (1, 7));
    let old_engines = fixture
        .admin
        .query("SELECT engine FROM clusterAllReplicas(?,system.tables) WHERE database=? AND name='base_old'")
        .bind(cluster)
        .bind(database)
        .fetch_all::<String>()
        .await?;
    assert_eq!(old_engines, vec!["ReplacingMergeTree"; 6]);
    Ok(())
}

#[test]
fn invalid_catalogue_mode_is_rejected_before_a_connection() {
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: Some("six_nodes".into()),
        dsn: "http://127.0.0.1:1".into(),
        database: "default".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    assert!(matches!(
        ClickHouseDatabase::new(&config, "invalid"),
        Err(DatabaseError::UtilsError(_))
    ));
}

#[tokio::test]
async fn catalogue_stream_does_not_hide_a_terminal_query_error() -> Result<()> {
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: "http://127.0.0.1:1".into(),
        database: "default".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    let database = ClickHouseDatabase::new(&config, "unreachable")?;
    let (tx, _rx) = mpsc::channel(1);
    assert!(
        timeout(
            Duration::from_secs(1),
            database.query_storage_entry(None, None, None, tx)
        )
        .await
        .unwrap()
        .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn copy_comparison_binds_new_and_changed_versions_in_source_order() -> Result<()> {
    use data_mover::model::{HistoryOrder, ObjectHistory};
    use storage_adapter::EntryChange;
    let fixture = Fixture::new().await?;
    let observe = |id: Option<&str>, size, latest| {
        crate::observed_fixtures::observation(
            BackendKind::S3,
            "object".into(),
            EntryKind::File,
            size,
            [1_000_000_000; 3],
            None,
            None,
            None,
            Some((id.map(str::to_string), latest, false)),
        )
    };
    let mut database = ClickHouseDatabase::new(&fixture.config, "mixed_history")?;
    database.initialize().await?;
    database.begin_copy_generation("mixed-history").await?;
    database
        .batch_insert_base_record(&[Arc::new(observe(None, 10, true))])
        .await?;
    database.commit_scan_generation().await?;
    database.begin_copy_generation("mixed-history").await?;
    // suspended bucket 的 null 被覆盖，随后恢复版本化并写入新版本。
    let null = observe(None, 20, false);
    let latest = observe(Some("a-new"), 30, true);
    let history = ObjectHistory::whole(
        vec![
            null.history().unwrap().records()[0].clone(),
            latest.history().unwrap().records()[0].clone(),
        ],
        HistoryOrder::Server,
    )?;
    let source = Arc::new(latest.with_history(Some(Box::new(history)))?);
    let changes = database
        .compare_catalogue_batch(&[Arc::clone(&source)], ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert_eq!(
        changes.iter().map(|change| change.kind).collect::<Vec<_>>(),
        [EntryChange::Changed(ChangeKind::DataOnly), EntryChange::New]
    );
    assert_eq!(
        changes
            .iter()
            .map(|change| change.item.view().version_id())
            .collect::<Vec<_>>(),
        ["null", "a-new"]
    );
    assert_eq!(
        changes
            .iter()
            .map(|change| change.item.version_index)
            .collect::<Vec<_>>(),
        [Some(0), Some(1)]
    );
    assert!(changes.iter().all(|change| Arc::ptr_eq(&change.item.entry, &source)));
    let baseline = fixture.rows("mixed_history").await?;
    assert_eq!(baseline.len(), 1);
    assert_eq!(baseline[0].size, 10, "分类不能提前确认尚未复制的版本");
    database
        .batch_insert_completed_records(&changes.into_iter().map(|change| change.item).collect::<Vec<_>>())
        .await?;
    assert_eq!(database.detect_deleted_items().await?.count(), 0);
    database.commit_scan_generation().await?;
    let rows = fixture.rows("mixed_history").await?;
    assert_eq!(
        rows.iter()
            .map(|row| (row.version_id.as_str(), row.size))
            .collect::<Vec<_>>(),
        [("a-new", 30), ("null", 20)]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn copy_comparison_distinguishes_unmapped_principals_from_numeric_zero() -> Result<()> {
    use data_mover::model::{PermissionFacts, PrincipalIdentity};
    use storage_adapter::EntryChange;
    let fixture = Fixture::new().await?;
    let observe = |uid: Option<u32>| -> Result<Arc<ObservedEntry>> {
        let entry = crate::observed_fixtures::observation(
            BackendKind::Nfs,
            "file".into(),
            EntryKind::File,
            10,
            [1_000_000_000; 3],
            None,
            Some(b"stable-handle".to_vec()),
            None,
            None,
        )
        .with_permissions(Some(Box::new(PermissionFacts {
            owner: uid.map(PrincipalIdentity::Numeric),
            group: None,
            mode: Some(0o640),
        })))?;
        Ok(Arc::new(entry))
    };
    let mut database = ClickHouseDatabase::new(&fixture.config, "nullable_principals")?;
    database.initialize().await?;
    database.begin_scan_generation().await?;
    database.batch_insert_base_record(&[observe(None)?]).await?;
    database.commit_scan_generation().await?;
    for uid in [Some(0), None] {
        database.begin_scan_generation().await?;
        let source = observe(uid)?;
        let changes = database
            .compare_catalogue_batch(&[Arc::clone(&source)], ComparisonMode::AwaitCopyAcknowledgement)
            .await?;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, EntryChange::Changed(ChangeKind::MetadataOnly));
        assert!(Arc::ptr_eq(&changes[0].item.entry, &source));
        database
            .batch_insert_completed_records(&[changes.into_iter().next().unwrap().item])
            .await?;
        database.commit_scan_generation().await?;
        assert_eq!(fixture.rows("nullable_principals").await?[0].uid, uid);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly selected six-replica ClickHouse cluster; isolated database and local fixture only"]
async fn copy_comparison_deduplicates_repeated_identical_observations() -> Result<()> {
    let fixture = Fixture::new().await?;
    let mut database = ClickHouseDatabase::new(&fixture.config, "duplicate_source")?;
    database.initialize().await?;
    database.begin_scan_generation().await?;
    let source = fixture.entry("same", 10, "aa");
    let changes = database
        .compare_catalogue_batch(
            &[Arc::clone(&source), Arc::clone(&source)],
            ComparisonMode::AwaitCopyAcknowledgement,
        )
        .await?;
    assert_eq!(changes.len(), 1);
    assert!(Arc::ptr_eq(&changes[0].item.entry, &source));
    assert_eq!(fixture.rows("duplicate_source").await?.len(), 0);
    database
        .batch_insert_completed_records(&[changes.into_iter().next().unwrap().item])
        .await?;
    database.commit_scan_generation().await?;
    assert_eq!(fixture.rows("duplicate_source").await?.len(), 1);
    Ok(())
}
