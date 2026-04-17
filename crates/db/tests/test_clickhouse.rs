#[cfg(test)]
mod clickhouse_integration_tests {
    // 标准库
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // 外部crate
    use bytes::Bytes;
    // 内部模块
    use db::clickhouse::ClickHouseDatabase;
    use db::{ClickHouseConfig, Database, DeletionStatus};
    use storage_v2::{EntryEnum, NASEntry, S3Entry};

    // 使用原子计数器确保每个测试用例都有唯一的job_id
    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn generate_unique_job_id(prefix: &str) -> String {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{}_{}_{}", prefix, counter, timestamp)
    }

    fn setup_test_db_with_job_id(job_id: &str) -> ClickHouseDatabase {
        let config = ClickHouseConfig {
            dsn: "http://10.128.133.213:8123".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        };

        ClickHouseDatabase::new(&config, job_id)
    }

    // 测试清理辅助函数
    async fn cleanup_test_tables(db: &ClickHouseDatabase, job_id: &str) -> Result<(), db::error::DatabaseError> {
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
        for item in &changed_items {
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
