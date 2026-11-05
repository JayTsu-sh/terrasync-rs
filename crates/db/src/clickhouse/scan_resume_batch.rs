//! 检查点批次增量比较：按路径裁剪基线，只对未命中的身份补查跨范围 rename。
use super::*;

/// 只取比较所需列，不把 ACL、标签等无关数据从 base 回传。
#[derive(Deserialize, clickhouse::Row)]
struct ResumeBaseEntry {
    relative_path: String,
    version_id: String,
    size: u64,
    mtime: i64,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    is_dir: bool,
}

impl ResumeBaseEntry {
    fn change_kind(&self, entry: &EntryEnum) -> Option<ChangeKind> {
        if self.is_dir {
            return None;
        }
        let data = self.size != entry.get_size() || self.mtime != entry.get_mtime();
        let metadata = self.mode != entry.get_mode() || self.uid != entry.get_uid() || self.gid != entry.get_gid();
        match (data, metadata) {
            (true, true) => Some(ChangeKind::Both),
            (true, false) => Some(ChangeKind::DataOnly),
            (false, true) => Some(ChangeKind::MetadataOnly),
            (false, false) => None,
        }
    }
}

impl ClickHouseDatabase {
    /// 输入须已同步写入当前 staging；这里只做比较，不推进 base 或检查点。
    pub async fn detect_resume_batch_events(&mut self, entries: &[Arc<EntryEnum>]) -> Result<Vec<StorageEntryMessage>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        // 多版本对象仍走原有版本计数逻辑；NAS 不应为 NULL version_count 做整表聚合。
        if entries.iter().any(|entry| entry.get_version_count().is_some()) {
            return self.detect_versioned_resume_events().await;
        }
        let temp = self
            .scan_temp_table_name
            .clone()
            .ok_or_else(|| DatabaseError::TableNotFound("scan resume batch table".into()))?;
        let base = get_scan_base_table_name(&self.job_id);
        let mut keys: Vec<_> = entries
            .iter()
            .map(|entry| entry.get_relative_path().to_string_lossy().into_owned())
            .collect();
        keys.sort_unstable();
        let handles: Vec<_> = entries
            .iter()
            .filter_map(|entry| entry.get_file_handle().map(hex::encode))
            .collect();
        let typed_batch = !handles.is_empty() && handles.iter().all(|handle| handle.starts_with("54534934"));
        let by_handle = if typed_batch {
            match self.resume_base_has_typed_handles {
                Some(typed) => typed,
                None => {
                    // 升级过渡代（legacy/raw handle）沿用 Path 语义。只在本次扫描首次需要时查一次。
                    let typed: bool = self.sync_client.query(&format!(
                        "SELECT count(file_handle)>0 AND countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle,'54534934'))=0 FROM {base}"
                    )).fetch_one().await?;
                    self.resume_base_has_typed_handles = Some(typed);
                    typed
                }
            }
        } else {
            false
        };
        let membership = if by_handle {
            "(relative_path,file_handle)"
        } else {
            "(relative_path,version_id)"
        };
        // BETWEEN 显式下推主键范围；IN 再裁剪批内离散键，避免无序 readdir 的宽范围放大。
        // LIMIT 1 BY 保留原比较的去重口径，不要求后台 merge 已完成。
        let rows = self
            .sync_client
            .query(&format!(
                "SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir FROM {base} \
             WHERE relative_path BETWEEN ? AND ? AND {membership} IN (SELECT {membership} FROM {temp}) \
             ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id"
            ))
            .bind(&keys[0])
            .bind(&keys[keys.len() - 1])
            .fetch_all::<ResumeBaseEntry>()
            .await?;
        let mut events = Vec::new();
        let mut absent = Vec::new();
        for entry in entries {
            let path = entry.get_relative_path().to_string_lossy();
            let version = entry.get_version_id().unwrap_or_default();
            // base 已按主键排序，用二分查找避免复制路径构建另一份哈希表。
            if let Ok(index) = rows.binary_search_by(|old| {
                old.relative_path
                    .as_str()
                    .cmp(path.as_ref())
                    .then_with(|| old.version_id.as_str().cmp(version))
            }) {
                let old = &rows[index];
                if let Some(kind) = old.change_kind(entry) {
                    events.push(StorageEntryMessage::Changed {
                        entry: Arc::clone(entry),
                        kind,
                    });
                }
            } else if by_handle {
                if let Some(handle) = entry.get_file_handle() {
                    absent.push((Arc::clone(entry), hex::encode(handle)));
                }
            } else {
                events.push(StorageEntryMessage::New(Arc::clone(entry)));
            }
        }
        if !absent.is_empty() {
            // 只补查本批路径未命中的身份。不能加路径范围：rename 源路径可以在范围之外。
            let handles: Vec<_> = absent.iter().map(|(_, handle)| handle.as_str()).collect();
            let found: HashSet<String> = self
                .sync_client
                .query(&format!(
                    "SELECT DISTINCT assumeNotNull(file_handle) FROM {base} WHERE file_handle IN ?"
                ))
                .bind(&handles)
                .fetch_all::<String>()
                .await?
                .into_iter()
                .collect();
            events.extend(
                absent
                    .into_iter()
                    .filter(|(_, handle)| !found.contains(handle))
                    .map(|(entry, _)| StorageEntryMessage::New(entry)),
            );
        }
        Ok(events)
    }

    async fn detect_versioned_resume_events(&self) -> Result<Vec<StorageEntryMessage>> {
        let mut events: Vec<_> = self
            .detect_new_items()
            .await?
            .map(|entry| StorageEntryMessage::New(Arc::new(entry)))
            .collect();
        events.extend(
            self.detect_changed_items()
                .await?
                .map(|(entry, kind)| StorageEntryMessage::Changed {
                    entry: Arc::new(entry),
                    kind,
                }),
        );
        Ok(events)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    #[ignore = "requires ClickHouse; checks incremental classifications and database read amplification"]
    async fn resume_diff_scales_with_batch_instead_of_repeated_global_aggregates() -> Result<()> {
        let name = format!(
            "ts_resume_diff_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| DatabaseError::ConfigError(e.to_string()))?
                .as_nanos()
        );
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
            database: name.clone(),
            username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
            password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
            dial_timeout: 10,
            read_timeout: 60,
        };
        let admin = build_base_client(&ClickHouseConfig {
            database: "default".into(),
            ..config.clone()
        })?;
        admin.query(&format!("CREATE DATABASE {name}")).execute().await?;
        let result: Result<([usize;4],u64,u64)>=async {
            let mut database=ClickHouseDatabase::new(&config,"fixture")?;
            database.initialize().await?;
            database.begin_resume_batch().await?;
            database.sync_client.query("INSERT INTO base_fixture (name,relative_path,size,mtime,mode,uid,gid,is_regular_file,file_handle,current_state) SELECT concat('file-',toString(number)),concat('file-',leftPad(toString(number),9,'0')),100,toDateTime64(1,9),420,1000,1000,1,concat('54534934',leftPad(lower(hex(number)),16,'0')),0 FROM numbers(100000)").execute().await?;
            database.sync_client.query("INSERT INTO scan_resume_batch_fixture (name,relative_path,size,mtime,mode,uid,gid,is_regular_file,file_handle,current_state) SELECT concat('file-',toString(n)),concat('file-',leftPad(toString(n),9,'0')),if(number%5 IN(1,3),101,100),toDateTime64(1,9),if(number%5 IN(2,3),384,420),1000,1000,1,concat('54534934',leftPad(lower(hex(n)),16,'0')),1 FROM (SELECT number,if(number%5=4,100000+number,(number+1)*400) AS n FROM numbers(200))").execute().await?;
            // 同身份的新路径应留给后续 rename 判定，不能误判成新增。
            database.sync_client.query("INSERT INTO scan_resume_batch_fixture SELECT * REPLACE ('renamed' AS name,'renamed' AS relative_path) FROM base_fixture WHERE relative_path='file-000000000'").execute().await?;
            let query=format!("SELECT {} FROM scan_resume_batch_fixture ORDER BY relative_path",*FILE_SCAN_COLUMNS_LIST);
            let records=database.sync_client.query(&query).fetch_all::<StorageEntryRecord>().await?;
            let entries:Vec<_>=records.iter().map(|record|Arc::new(record.to_entry_enum())).collect();
            // 基线身份格式探测每次扫描只做一次；热路径预算不含这一次初始化。
            database.detect_resume_batch_events(&entries).await?;
            let tag=format!("resume_diff_{name}");
            database.sync_client=database.sync_client.clone().with_option("log_comment",tag.clone()).with_option("log_queries","1");
            let started=std::time::Instant::now();
            let events=database.detect_resume_batch_events(&entries).await?;
            let elapsed=started.elapsed();
            let mut counts=[0_usize;4];
            for event in events {
                match event {
                    StorageEntryMessage::New(entry)=>{assert_ne!(entry.get_relative_path().to_string_lossy(),"renamed");counts[0]+=1;},
                    StorageEntryMessage::Changed {kind,..}=>counts[match kind {ChangeKind::DataOnly=>1,ChangeKind::MetadataOnly=>2,ChangeKind::Both=>3}]+=1,
                    _=>continue,
                }
            }

            admin.query("SYSTEM FLUSH LOGS").execute().await?;
            let (calls,read_rows):(u64,u64)=admin.query("SELECT count(),sum(read_rows) FROM system.query_log WHERE type='QueryFinish' AND query_kind='Select' AND log_comment=?").bind(&tag).fetch_one().await?;
            eprintln!("resume diff: {calls} SELECTs, {read_rows} rows read, {:.3}s",elapsed.as_secs_f64());
            Ok((counts,calls,read_rows))
        }.await;
        admin
            .query(&format!("DROP DATABASE IF EXISTS {name}"))
            .execute()
            .await?;
        let (counts, calls, read_rows) = result?;
        assert_eq!(counts, [40, 40, 40, 40]);
        assert!(
            calls <= 2 && read_rows <= 400_000,
            "small batch caused repeated full-table work: queries={calls}, rows={read_rows}"
        );
        Ok(())
    }

    fn signature(events: Vec<StorageEntryMessage>) -> Vec<(String, String, Option<u32>)> {
        let mut result: Vec<_> = events
            .into_iter()
            .filter_map(|event| {
                let (entry, kind) = match event {
                    StorageEntryMessage::New(entry) => (entry, "new"),
                    StorageEntryMessage::Changed { entry, kind } => (entry, kind.as_state_str()),
                    _ => return None,
                };
                Some((
                    entry.get_relative_path().to_string_lossy().into_owned(),
                    kind.to_string(),
                    entry.get_version_count(),
                ))
            })
            .collect();
        result.sort_unstable();
        result
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse; compares optimized classification with legacy behavior"]
    async fn resume_diff_matches_legacy_for_local_legacy_handles_and_versions() -> Result<()> {
        let name = format!(
            "ts_resume_diff_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| DatabaseError::ConfigError(e.to_string()))?
                .as_nanos()
        );
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
            database: name.clone(),
            username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
            password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
            dial_timeout: 10,
            read_timeout: 60,
        };
        let admin = build_base_client(&ClickHouseConfig {
            database: "default".into(),
            ..config.clone()
        })?;
        admin.query(&format!("CREATE DATABASE {name}")).execute().await?;
        let result: Result<Vec<_>> = async {
            let mut comparisons = Vec::new();
            for (job, base_handle, temp_handle, version_count, storage_type) in [
                ("local", "NULL", "NULL", "NULL", "nas"),
                ("legacy", "'01'", "'5453493401'", "NULL", "nas"),
                ("s3", "NULL", "NULL", "NULL", "s3"),
                ("versioned", "NULL", "NULL", "2", "s3"),
            ] {
                let version_id = if storage_type == "s3" { "v1" } else { "" };
                let base_mode = if storage_type == "s3" { "NULL" } else { "if(number=5,NULL,420)" };
                let temp_mode = if storage_type == "s3" { "NULL" } else { "if(number=5,NULL,if(number IN(2,3),384,420))" };
                let mut database = ClickHouseDatabase::new(&config, job)?;
                database.initialize().await?;
                database.begin_resume_batch().await?;
                database.sync_client.query(&format!(
                    "INSERT INTO base_{job} (name,relative_path,size,mtime,mode,uid,gid,is_regular_file,file_handle,storage_type,version_id,version_count) \
                     SELECT toString(number),toString(number),100,toDateTime64(1,9),{base_mode},NULL,NULL,1,{base_handle},'{storage_type}','{version_id}',{version_count} FROM numbers(6)"
                )).execute().await?;
                database.sync_client.query(&format!(
                    "INSERT INTO scan_resume_batch_{job} (name,relative_path,size,mtime,mode,uid,gid,is_regular_file,file_handle,storage_type,version_id,version_count) \
                     SELECT toString(n),toString(n),if(number IN(1,3,5),101,100),toDateTime64(1,9),{temp_mode},NULL,NULL,1,{temp_handle},'{storage_type}','{version_id}',{version_count} FROM (SELECT number,if(number=4,10,number) AS n FROM numbers(6))"
                )).execute().await?;
                let records = database.sync_client.query(&format!("SELECT {} FROM scan_resume_batch_{job}", *FILE_SCAN_COLUMNS_LIST))
                    .fetch_all::<StorageEntryRecord>().await?;
                let entries: Vec<_> = records.iter().map(|record|Arc::new(record.to_entry_enum())).collect();
                let expected = signature(database.detect_versioned_resume_events().await?);
                let actual = signature(database.detect_resume_batch_events(&entries).await?);
                comparisons.push((job, expected, actual));
            }
            Ok(comparisons)
        }.await;
        admin
            .query(&format!("DROP DATABASE IF EXISTS {name}"))
            .execute()
            .await?;
        for (job, expected, actual) in result? {
            assert_eq!(actual, expected, "classification changed for {job}");
        }
        Ok(())
    }
}
