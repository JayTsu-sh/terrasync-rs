// 集成测试代码：豁免 unwrap/expect deny
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[cfg(test)]
mod clickhouse_integration_tests {
    // 标准库
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // 外部crate
    use bytes::Bytes;
    use clickhouse::Client;
    // 内部模块
    use data_mover::{EntryEnum, NASEntry, S3Entry};
    use db::clickhouse::ClickHouseDatabase;
    use db::{ClickHouseConfig, Database, DatabaseError, DeletionStatus};

    // 使用原子计数器确保每个测试用例都有唯一的job_id
    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn generate_unique_job_id(prefix: &str) -> String {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{}_{}_{}", prefix, counter, timestamp)
    }

    fn setup_test_db_with_job_id(job_id: &str) -> ClickHouseDatabase {
        let config = ClickHouseConfig {
            dsn: std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".to_string()),
            dial_timeout: 10,
            read_timeout: 30,
            database: std::env::var("LAB_CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string()),
            username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string()),
            password: std::env::var("LAB_CLICKHOUSE_PASSWORD")
                .ok()
                .filter(|value| !value.is_empty()),
        };

        ClickHouseDatabase::new(&config, job_id)
    }

    fn lab_clickhouse_config(database: String) -> ClickHouseConfig {
        ClickHouseConfig {
            dsn: std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".to_string()),
            dial_timeout: 10,
            read_timeout: 30,
            database,
            username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string()),
            password: std::env::var("LAB_CLICKHOUSE_PASSWORD")
                .ok()
                .filter(|value| !value.is_empty()),
        }
    }

    fn clickhouse_client(config: &ClickHouseConfig) -> Client {
        let mut client = Client::default()
            .with_url(&config.dsn)
            .with_database(config.database.clone())
            .with_user(config.username.clone());
        if let Some(password) = &config.password {
            client = client.with_password(password);
        }
        client
    }

    struct IsolatedDatabase {
        name: String,
        active: bool,
    }

    impl IsolatedDatabase {
        async fn cleanup(mut self) {
            self.active = false;
            cleanup_isolated_database(&self.name).await.unwrap();
        }
    }

    impl Drop for IsolatedDatabase {
        fn drop(&mut self) {
            if !self.active {
                return;
            }
            let database = self.name.clone();
            let _ = std::thread::spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
                    return;
                };
                let _ = runtime.block_on(cleanup_isolated_database(&database));
            })
            .join();
        }
    }

    async fn setup_isolated_state_db(prefix: &str) -> (ClickHouseDatabase, ClickHouseConfig, String, IsolatedDatabase) {
        let database = generate_unique_job_id(prefix);
        let job_id = generate_unique_job_id("state_job");
        let admin_config = lab_clickhouse_config("default".to_string());
        clickhouse_client(&admin_config)
            .query(&format!("CREATE DATABASE `{database}`"))
            .execute()
            .await
            .unwrap();

        let config = lab_clickhouse_config(database.clone());
        let db = ClickHouseDatabase::new(&config, &job_id);
        (
            db,
            config,
            job_id,
            IsolatedDatabase {
                name: database,
                active: true,
            },
        )
    }

    async fn cleanup_isolated_database(database: &str) -> std::result::Result<(), clickhouse::error::Error> {
        let config = lab_clickhouse_config("default".to_string());
        clickhouse_client(&config)
            .query(&format!("DROP DATABASE IF EXISTS `{database}`"))
            .execute()
            .await
    }

    // 测试清理辅助函数
    async fn cleanup_test_tables(db: &ClickHouseDatabase, job_id: &str) -> Result<(), DatabaseError> {
        let base_table = format!("{}_{}", db::SCAN_BASE_TABLE_BASE_NAME, job_id);
        let state_table = format!("{}_{}", db::SCAN_STATE_TABLE_BASE_NAME, job_id);
        let incremental_table = format!("{}_{}", db::INCREMENTAL_SCAN_TABLE_BASE_NAME, job_id);

        let _ = db.drop_table_by_name(&base_table).await;
        let _ = db.drop_table_by_name(&state_table).await;
        let _ = db.drop_table_by_name(&incremental_table).await;

        // 清理临时表（如果有）
        let _ = db
            .drop_tables_with_prefix(&format!("{}_{}", db::SCAN_TEMP_TABLE_BASE_NAME, job_id))
            .await;
        let _ = db.drop_tables_with_prefix("temp_files_").await;
        let _ = db.drop_tables_with_prefix("exclude_").await;

        Ok(())
    }

    /// 构造 NASEntry 的 EntryEnum，只需指定差异字段
    fn make_nas(rel_path: &str, size: u64, mtime: i64, fh: Option<&[u8]>) -> EntryEnum {
        EntryEnum::NAS(NASEntry {
            name: rel_path.rsplit('/').next().unwrap_or(rel_path).to_string(),
            relative_path: PathBuf::from(rel_path),
            extension: None,
            is_dir: false,
            size,
            atime: mtime,
            ctime: mtime,
            mtime,
            mode: 0o644,
            is_symlink: false,
            hard_links: Some(1),
            uid: Some(1000),
            gid: Some(1000),
            ino: Some(12345),
            file_handle: fh.map(Bytes::copy_from_slice),
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        })
    }

    /// 构造 S3Entry 的 EntryEnum
    fn make_s3(rel_path: &str, size: u64, mtime: i64, version_id: Option<&str>) -> EntryEnum {
        EntryEnum::S3(S3Entry {
            name: rel_path.rsplit('/').next().unwrap_or(rel_path).to_string(),
            relative_path: rel_path.to_string(),
            extension: None,
            size,
            mtime,
            tags: None,
            version_id: version_id.map(String::from),
            is_latest: true,
            is_delete_marker: false,
            version_count: None,
            is_dir: false,
        })
    }

    /// 批量生成 NASEntry（用于模拟大 base 表场景）
    fn make_nas_batch(prefix: &str, count: usize, mtime: i64) -> Vec<Arc<EntryEnum>> {
        (0..count)
            .map(|i| Arc::new(make_nas(&format!("{}/{}.dat", prefix, i), 1024, mtime, None)))
            .collect()
    }

    // ═══ 2.1 基础 CRUD ═══

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn query_scan_state_returns_none_when_state_table_is_empty() {
        let (db, _, _, database) = setup_isolated_state_db("state_empty").await;
        db.create_scan_state_table().await.unwrap();

        assert_eq!(db.query_scan_state().await.unwrap(), None);

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn query_scan_state_returns_persisted_zero_and_one_from_new_adapters() {
        let (db, config, job_id, database) = setup_isolated_state_db("state_values").await;
        db.create_scan_state_table().await.unwrap();
        db.insert_scan_state(0).await.unwrap();

        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(0));
        assert_eq!(fresh_db.begin_scan_generation().await.unwrap(), 1);

        db.insert_scan_state(1).await.unwrap();
        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(1));

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn begin_from_committed_one_returns_zero_without_persisting() {
        let (db, config, job_id, database) = setup_isolated_state_db("begin_one").await;
        db.create_scan_base_table().await.unwrap();
        db.create_scan_state_table().await.unwrap();
        db.insert_scan_state(1).await.unwrap();

        assert_eq!(db.begin_scan_generation().await.unwrap(), 0);
        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(1));

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn uncommitted_adapter_retry_reuses_working_generation() {
        let (adapter_a, config, job_id, database) = setup_isolated_state_db("begin_retry").await;
        adapter_a.initialize().await.unwrap();
        assert_eq!(adapter_a.begin_scan_generation().await.unwrap(), 1);
        drop(adapter_a);

        let adapter_b = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(adapter_b.begin_scan_generation().await.unwrap(), 1);
        drop(adapter_b);
        let adapter_c = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(adapter_c.query_scan_state().await.unwrap(), Some(0));
        let physical_rows = clickhouse_client(&config)
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(physical_rows, 1);

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn begun_batch_uses_working_generation_and_commit_is_idempotent() {
        let (mut db, config, job_id, database) = setup_isolated_state_db("begin_batch_commit").await;
        db.initialize().await.unwrap();
        assert_eq!(db.begin_scan_generation().await.unwrap(), 1);

        let record = Arc::new(make_nas("working.dat", 9, 1_700_000_000, None));
        db.batch_insert_base_record(&[record]).await.unwrap();
        let client = clickhouse_client(&config);
        let stamped_generation = client
            .query(&format!(
                "SELECT current_state FROM base_{job_id} FINAL WHERE relative_path = 'working.dat'"
            ))
            .fetch_one::<u8>()
            .await
            .unwrap();
        assert_eq!(stamped_generation, 1);
        db.create_scan_temporary_table().await.unwrap();
        let temp_table = db.scan_temp_table_name.clone().unwrap();
        let temp_record = Arc::new(make_nas("temp-working.dat", 11, 1_700_000_001, None));
        db.batch_insert_temp_record(&[temp_record]).await.unwrap();
        let temp_generation = client
            .query(&format!(
                "SELECT current_state FROM {temp_table} WHERE relative_path = 'temp-working.dat'"
            ))
            .fetch_one::<u8>()
            .await
            .unwrap();
        assert_eq!(temp_generation, 1);
        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(0));

        let rows_before_commit = client
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        let (first_commit, second_commit) = tokio::join!(db.commit_scan_generation(), db.commit_scan_generation());
        first_commit.unwrap();
        second_commit.unwrap();
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(1));
        let rows_after_commit = client
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(rows_after_commit, rows_before_commit + 1);
        db.commit_scan_generation().await.unwrap();
        let rows_after_repeat = client
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(rows_after_repeat, rows_after_commit);

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn commit_before_begin_is_rejected_without_persisting() {
        let (db, config, job_id, database) = setup_isolated_state_db("commit_without_begin").await;
        db.initialize().await.unwrap();

        let error = db.commit_scan_generation().await.unwrap_err();
        assert!(matches!(error, DatabaseError::TransactionError(_)));
        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(0));
        let physical_rows = clickhouse_client(&config)
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(physical_rows, 1);

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn invalid_persisted_generation_rejects_begin_and_commit() {
        let (db, config, job_id, database) = setup_isolated_state_db("begin_invalid_generation").await;
        db.create_scan_state_table().await.unwrap();
        db.insert_scan_state(2).await.unwrap();

        let error = db.begin_scan_generation().await.unwrap_err();
        assert!(matches!(error, DatabaseError::ConversionError(_)));
        let error = db.commit_scan_generation().await.unwrap_err();
        assert!(matches!(error, DatabaseError::TransactionError(_)));
        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(2));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn failed_begin_does_not_enable_commit() {
        let config = ClickHouseConfig {
            dsn: "http://127.0.0.1:1".to_string(),
            dial_timeout: 1,
            read_timeout: 1,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        };
        let db = ClickHouseDatabase::new(&config, "failed_begin");

        assert!(matches!(
            db.begin_scan_generation().await.unwrap_err(),
            DatabaseError::QueryError(_)
        ));
        assert!(matches!(
            db.commit_scan_generation().await.unwrap_err(),
            DatabaseError::TransactionError(_)
        ));
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn failed_rebegin_clears_previous_working_generation() {
        let (db, _, job_id, database) = setup_isolated_state_db("failed_rebegin").await;
        db.initialize().await.unwrap();
        assert_eq!(db.begin_scan_generation().await.unwrap(), 1);
        db.drop_table_by_name(&format!("state_{job_id}")).await.unwrap();

        assert!(matches!(
            db.begin_scan_generation().await.unwrap_err(),
            DatabaseError::QueryError(_)
        ));
        assert!(matches!(
            db.commit_scan_generation().await.unwrap_err(),
            DatabaseError::TransactionError(_)
        ));

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn query_scan_state_preserves_missing_table_as_database_error() {
        let (db, _, _, database) = setup_isolated_state_db("state_missing_table").await;

        let error = db.query_scan_state().await.unwrap_err();
        assert!(matches!(error, DatabaseError::QueryError(_)));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn query_scan_state_preserves_connection_error() {
        let config = ClickHouseConfig {
            dsn: "http://127.0.0.1:1".to_string(),
            dial_timeout: 1,
            read_timeout: 1,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        };
        let db = ClickHouseDatabase::new(&config, "unreachable");

        let error = db.query_scan_state().await.unwrap_err();
        assert!(matches!(error, DatabaseError::QueryError(_)));
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn query_scan_state_preserves_authentication_error() {
        let mut config = lab_clickhouse_config("default".to_string());
        config.password = Some("definitely-wrong-password".to_string());
        let db = ClickHouseDatabase::new(&config, "auth_failure");

        let error = db.query_scan_state().await.unwrap_err();
        assert!(matches!(error, DatabaseError::QueryError(_)));
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn failed_query_does_not_create_default_state() {
        let (db, config, job_id, database) = setup_isolated_state_db("state_recovery").await;
        let record = Arc::new(make_nas("recovery.dat", 7, 1_700_000_000, None));

        assert!(db.batch_insert_base_record(&[record.clone()]).await.is_err());

        db.create_scan_state_table().await.unwrap();
        db.create_scan_base_table().await.unwrap();
        db.insert_scan_state(1).await.unwrap();
        db.batch_insert_base_record(&[record]).await.unwrap();

        let stored_state = clickhouse_client(&config)
            .query(&format!(
                "SELECT current_state FROM base_{job_id} FINAL WHERE relative_path = 'recovery.dat'"
            ))
            .fetch_one::<u8>()
            .await
            .unwrap();
        assert_eq!(stored_state, 1, "失败查询不得把默认状态 0 写入缓存");

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn initialize_preserves_existing_generation_one_without_appending_state() {
        let (db, config, job_id, database) = setup_isolated_state_db("initialize_existing_one").await;
        db.create_scan_state_table().await.unwrap();
        db.insert_scan_state(1).await.unwrap();

        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        fresh_db.initialize().await.unwrap();

        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(1));
        let physical_rows = clickhouse_client(&config)
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(physical_rows, 1, "已有 generation 时初始化不得追加默认状态");

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn initialize_creates_one_zero_state_and_is_idempotent_across_adapters() {
        let (db, config, job_id, database) = setup_isolated_state_db("initialize_empty").await;

        db.initialize().await.unwrap();
        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(0));

        fresh_db.initialize().await.unwrap();
        let newest_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(newest_db.query_scan_state().await.unwrap(), Some(0));
        let physical_rows = clickhouse_client(&config)
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(physical_rows, 1, "重复初始化不得追加 generation 0");

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn initialize_propagates_state_decode_error_without_writing_default() {
        let (db, config, job_id, database) = setup_isolated_state_db("initialize_invalid_state").await;
        let client = clickhouse_client(&config);
        client
            .query(&format!(
                "CREATE TABLE state_{job_id} (id UInt8, scan_state String) \
                 ENGINE = ReplacingMergeTree() ORDER BY id"
            ))
            .execute()
            .await
            .unwrap();
        client
            .query(&format!(
                "INSERT INTO state_{job_id} (id, scan_state) VALUES (1, 'broken')"
            ))
            .execute()
            .await
            .unwrap();

        let error = db.initialize().await.unwrap_err();
        assert!(matches!(error, DatabaseError::QueryError(_)));

        let stored_values = client
            .query(&format!("SELECT scan_state FROM state_{job_id} FINAL WHERE id = 1"))
            .fetch_all::<String>()
            .await
            .unwrap();
        assert_eq!(stored_values, vec!["broken"]);

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn begin_from_committed_zero_returns_one_without_persisting() {
        let (db, config, job_id, database) = setup_isolated_state_db("begin_zero").await;
        db.initialize().await.unwrap();

        assert_eq!(db.begin_scan_generation().await.unwrap(), 1);

        let fresh_db = ClickHouseDatabase::new(&config, &job_id);
        assert_eq!(fresh_db.query_scan_state().await.unwrap(), Some(0));
        let physical_rows = clickhouse_client(&config)
            .query(&format!("SELECT count() FROM state_{job_id}"))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(physical_rows, 1);

        database.cleanup().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_initialize_creates_tables() {
        let job_id = generate_unique_job_id("init");
        let db = setup_test_db_with_job_id(&job_id);

        let result = db.initialize().await;
        assert!(result.is_ok());

        let count = db.get_count(db::SCAN_BASE_TABLE_BASE_NAME).await;
        assert!(count.is_ok());

        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_batch_insert_and_count() {
        let job_id = generate_unique_job_id("insert");
        let db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        let batch = make_nas_batch("dir", 1000, 1000000);
        db.batch_insert_base_record(&batch).await.unwrap();

        let count = db.get_count(db::SCAN_BASE_TABLE_BASE_NAME).await.unwrap();
        assert_eq!(count, 1000);

        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_temp_table_lifecycle() {
        let job_id = generate_unique_job_id("temp");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        db.create_scan_temporary_table().await.unwrap();
        assert!(db.scan_temp_table_name.is_some());

        let batch: Vec<Arc<EntryEnum>> = (0..100)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.txt", i), 512, 100, None)))
            .collect();
        db.batch_insert_temp_record(&batch).await.unwrap();

        db.drop_scan_temporary_table().await.unwrap();
        assert!(db.scan_temp_table_name.is_none());

        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    // ═══ 2.2 detect_new_items（优化 1: NOT IN）═══

    #[tokio::test]
    #[ignore]
    async fn test_detect_new_path_join() {
        let job_id = generate_unique_job_id("new_path");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // base: 1000 条
        let base_batch = make_nas_batch("dir", 1000, 1000000);
        db.batch_insert_base_record(&base_batch).await.unwrap();

        // temp: 前 90 条已有 + 10 条全新
        db.create_scan_temporary_table().await.unwrap();
        let mut temp_items: Vec<Arc<EntryEnum>> = (0..90)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, None)))
            .collect();
        for i in 0..10 {
            temp_items.push(Arc::new(make_nas(&format!("new/{}.dat", i), 2048, 2000000, None)));
        }
        db.batch_insert_temp_record(&temp_items).await.unwrap();

        let new_items: Vec<_> = db.detect_new_items().await.unwrap().collect();
        assert_eq!(new_items.len(), 10);

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_detect_new_file_handle_join() {
        let job_id = generate_unique_job_id("new_fh");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // base: 1000 条带 fh
        let base_batch: Vec<Arc<EntryEnum>> = (0..1000)
            .map(|i| {
                let fh = format!("{:04x}", i);
                Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, Some(fh.as_bytes())))
            })
            .collect();
        db.batch_insert_base_record(&base_batch).await.unwrap();

        // temp: 90 条已有 fh + 10 条新 fh
        db.create_scan_temporary_table().await.unwrap();
        let mut temp_items: Vec<Arc<EntryEnum>> = (0..90)
            .map(|i| {
                let fh = format!("{:04x}", i);
                Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, Some(fh.as_bytes())))
            })
            .collect();
        for i in 0..10 {
            let fh = format!("ff{:02x}", i);
            temp_items.push(Arc::new(make_nas(
                &format!("new/{}.dat", i),
                2048,
                2000000,
                Some(fh.as_bytes()),
            )));
        }
        db.batch_insert_temp_record(&temp_items).await.unwrap();

        let new_items: Vec<_> = db.detect_new_items().await.unwrap().collect();
        assert_eq!(new_items.len(), 10);

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    // ═══ 2.3 detect_changed_items（优化 1: LIMIT 1 BY）═══

    #[tokio::test]
    #[ignore]
    async fn test_detect_changed_path_join() {
        let job_id = generate_unique_job_id("changed_path");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // base: 1000 条 (size=1024)
        let base_batch = make_nas_batch("dir", 1000, 1000000);
        db.batch_insert_base_record(&base_batch).await.unwrap();

        // temp: 100 条（其中 5 条 size=9999 变更）
        db.create_scan_temporary_table().await.unwrap();
        let mut temp_items: Vec<Arc<EntryEnum>> = (0..95)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, None)))
            .collect();
        for i in 95..100 {
            temp_items.push(Arc::new(make_nas(&format!("dir/{}.dat", i), 9999, 1000000, None)));
        }
        db.batch_insert_temp_record(&temp_items).await.unwrap();

        let changed: Vec<_> = db.detect_changed_items().await.unwrap().collect();
        assert_eq!(changed.len(), 5);

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    // ═══ 2.4 detect_deleted_items（优化 2: 批量查询）═══

    #[tokio::test]
    #[ignore]
    async fn test_detect_deleted_simple() {
        let job_id = generate_unique_job_id("deleted");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // 第一轮: base 插入 1000 条 (state=0)
        let base_batch = make_nas_batch("dir", 1000, 1000000);
        db.batch_insert_base_record(&base_batch).await.unwrap();

        // switch_scan_state
        db.switch_scan_state().await.unwrap();

        // 第二轮: temp 插入 990 条（10 条消失）
        db.create_scan_temporary_table().await.unwrap();
        let temp_batch: Vec<Arc<EntryEnum>> = (0..990)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, None)))
            .collect();
        db.batch_insert_temp_record(&temp_batch).await.unwrap();
        db.insert_temp_to_base_table(&[]).await.unwrap();
        db.drop_scan_temporary_table().await.unwrap();

        // detect_deleted
        let deleted: Vec<_> = db.detect_deleted_items().await.unwrap().collect();
        assert_eq!(deleted.len(), 10);
        for status in &deleted {
            assert!(matches!(status, DeletionStatus::Deleted(_)));
        }

        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    // ═══ 2.5 insert_temp_to_base_table（优化 3: Memory 排除表）═══

    #[tokio::test]
    #[ignore]
    async fn test_insert_no_exclusions() {
        let job_id = generate_unique_job_id("no_excl");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        db.create_scan_temporary_table().await.unwrap();
        let batch: Vec<Arc<EntryEnum>> = (0..100)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.txt", i), 512, 100, None)))
            .collect();
        db.batch_insert_temp_record(&batch).await.unwrap();

        db.insert_temp_to_base_table(&[]).await.unwrap();

        let count = db.get_count(db::SCAN_BASE_TABLE_BASE_NAME).await.unwrap();
        assert_eq!(count, 100);

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_with_exclusions() {
        let job_id = generate_unique_job_id("with_excl");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        db.create_scan_temporary_table().await.unwrap();
        let batch: Vec<Arc<EntryEnum>> = (0..100)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.txt", i), 512, 100, None)))
            .collect();
        db.batch_insert_temp_record(&batch).await.unwrap();

        // 排除前 20 条
        let excluded: Vec<(String, String)> = (0..20).map(|i| (format!("dir/{}.txt", i), String::new())).collect();
        db.insert_temp_to_base_table(&excluded).await.unwrap();

        let count = db.get_count(db::SCAN_BASE_TABLE_BASE_NAME).await.unwrap();
        assert_eq!(count, 80);

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_all_excluded() {
        let job_id = generate_unique_job_id("all_excl");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        db.create_scan_temporary_table().await.unwrap();
        let batch: Vec<Arc<EntryEnum>> = (0..100)
            .map(|i| Arc::new(make_nas(&format!("dir/{}.txt", i), 512, 100, None)))
            .collect();
        db.batch_insert_temp_record(&batch).await.unwrap();

        let excluded: Vec<(String, String)> = (0..100).map(|i| (format!("dir/{}.txt", i), String::new())).collect();
        db.insert_temp_to_base_table(&excluded).await.unwrap();

        let count = db.get_count(db::SCAN_BASE_TABLE_BASE_NAME).await.unwrap();
        assert_eq!(count, 0);

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_s3_version_id_exclusion() {
        let job_id = generate_unique_job_id("s3_excl");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        db.create_scan_temporary_table().await.unwrap();
        let batch: Vec<Arc<EntryEnum>> = vec![
            Arc::new(make_s3("a.txt", 100, 100, Some("v1"))),
            Arc::new(make_s3("a.txt", 100, 100, Some("v2"))),
            Arc::new(make_s3("a.txt", 100, 100, Some("v3"))),
        ];
        db.batch_insert_temp_record(&batch).await.unwrap();

        // 只排除 v2
        let excluded = vec![("a.txt".to_string(), "v2".to_string())];
        db.insert_temp_to_base_table(&excluded).await.unwrap();

        let count = db.get_count(db::SCAN_BASE_TABLE_BASE_NAME).await.unwrap();
        assert_eq!(count, 2); // v1 和 v3

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    // ═══ 2.6 端到端增量流程 ═══

    #[tokio::test]
    #[ignore]
    async fn test_incremental_scan_e2e() {
        let job_id = generate_unique_job_id("inc_scan");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // 第一轮：base 插入 1000 条 NASEntry
        let base_batch = make_nas_batch("dir", 1000, 1000000);
        db.batch_insert_base_record(&base_batch).await.unwrap();

        // switch_scan_state
        db.switch_scan_state().await.unwrap();

        // 第二轮：temp 100 条（80 已有其中 5 变更 + 20 新增）
        db.create_scan_temporary_table().await.unwrap();
        let mut temp_items: Vec<Arc<EntryEnum>> = Vec::with_capacity(100);
        // 75 条未变更
        for i in 0..75 {
            temp_items.push(Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, None)));
        }
        // 5 条变更（size 不同）
        for i in 75..80 {
            temp_items.push(Arc::new(make_nas(&format!("dir/{}.dat", i), 9999, 1000000, None)));
        }
        // 20 条全新
        for i in 0..20 {
            temp_items.push(Arc::new(make_nas(&format!("new/{}.dat", i), 2048, 2000000, None)));
        }
        db.batch_insert_temp_record(&temp_items).await.unwrap();

        // detect_new
        let new_items: Vec<_> = db.detect_new_items().await.unwrap().collect();
        assert_eq!(new_items.len(), 20);

        // detect_changed
        let changed_items: Vec<_> = db.detect_changed_items().await.unwrap().collect();
        assert_eq!(changed_items.len(), 5);

        // insert_temp_to_base（增量扫描：全量插入）
        db.insert_temp_to_base_table(&[]).await.unwrap();
        db.drop_scan_temporary_table().await.unwrap();

        // detect_deleted: base 中 920 条 old-state 未被覆盖
        let deleted: Vec<_> = db.detect_deleted_items().await.unwrap().collect();
        assert_eq!(deleted.len(), 920);

        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_incremental_copy_e2e() {
        let job_id = generate_unique_job_id("inc_copy");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // 第一轮
        let base_batch = make_nas_batch("dir", 1000, 1000000);
        db.batch_insert_base_record(&base_batch).await.unwrap();

        db.switch_scan_state().await.unwrap();

        // 第二轮
        db.create_scan_temporary_table().await.unwrap();
        let mut temp_items: Vec<Arc<EntryEnum>> = Vec::with_capacity(100);
        for i in 0..75 {
            temp_items.push(Arc::new(make_nas(&format!("dir/{}.dat", i), 1024, 1000000, None)));
        }
        for i in 75..80 {
            temp_items.push(Arc::new(make_nas(&format!("dir/{}.dat", i), 9999, 1000000, None)));
        }
        for i in 0..20 {
            temp_items.push(Arc::new(make_nas(&format!("new/{}.dat", i), 2048, 2000000, None)));
        }
        db.batch_insert_temp_record(&temp_items).await.unwrap();

        // detect
        let new_items: Vec<_> = db.detect_new_items().await.unwrap().collect();
        let changed_items: Vec<_> = db.detect_changed_items().await.unwrap().collect();

        // 收集 excluded_paths（模拟 app 层 keep_item=false 行为）
        let mut excluded: Vec<(String, String)> = Vec::new();
        for item in &new_items {
            excluded.push((
                item.get_relative_path().to_string_lossy().into_owned(),
                item.get_version_id().unwrap_or_default().to_string(),
            ));
        }
        for (item, _kind) in &changed_items {
            excluded.push((
                item.get_relative_path().to_string_lossy().into_owned(),
                item.get_version_id().unwrap_or_default().to_string(),
            ));
        }

        assert_eq!(excluded.len(), 25); // 20 new + 5 changed
        db.insert_temp_to_base_table(&excluded).await.unwrap();
        db.drop_scan_temporary_table().await.unwrap();

        cleanup_test_tables(&db, &job_id).await.unwrap();
    }

    // ═══ 2.7 version_count JOIN（优化 4）═══

    #[tokio::test]
    #[ignore]
    async fn test_version_count_deduction() {
        let job_id = generate_unique_job_id("vc");
        let mut db = setup_test_db_with_job_id(&job_id);
        db.initialize().await.unwrap();

        // base: S3Entry("a.txt", vid="v1") + ("a.txt", vid="v2")
        let base_batch = vec![
            Arc::new(make_s3("a.txt", 100, 100, Some("v1"))),
            Arc::new(make_s3("a.txt", 100, 100, Some("v2"))),
        ];
        db.batch_insert_base_record(&base_batch).await.unwrap();

        // temp: S3Entry("a.txt", vid="v3", version_count=5)
        db.create_scan_temporary_table().await.unwrap();
        let mut s3_new = make_s3("a.txt", 100, 100, Some("v3"));
        if let EntryEnum::S3(ref mut e) = s3_new {
            e.version_count = Some(5);
        }
        let temp_batch = vec![Arc::new(s3_new)];
        db.batch_insert_temp_record(&temp_batch).await.unwrap();

        // detect_new → version_count = 5 - 2 = 3
        let new_items: Vec<_> = db.detect_new_items().await.unwrap().collect();
        assert_eq!(new_items.len(), 1);
        assert_eq!(new_items[0].get_version_count(), Some(3));

        db.drop_scan_temporary_table().await.unwrap();
        cleanup_test_tables(&db, &job_id).await.unwrap();
    }
}
