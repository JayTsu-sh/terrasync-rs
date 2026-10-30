//! 持久化扫描游标：结果与事件写入确认后才能推进游标。
use super::*;
use clickhouse::error::Error as ClickHouseError;

pub const RUNNING: u8 = 0;
pub const FINALIZING: u8 = 1;
pub const COMPLETED: u8 = 2;
pub const ABANDONED: u8 = 3;

#[derive(Serialize, clickhouse::Row)]
struct ResumeDeleteKey {
    relative_path: String,
    version_id: String,
}

#[derive(Serialize, clickhouse::Row)]
struct ResumeEventPath {
    relative_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct ScanResumeState {
    pub id: u8,
    pub schema_version: u8,
    pub version: u64,
    pub request_key: String,
    pub generation: u8,
    pub incremental: bool,
    pub phase: u8,
    /// 本轮幂等事件日志的时间戳下界，不包含边界。
    pub event_time: i64,
    pub observed_entries: u64,
    pub checkpoint: Vec<u8>,
}

impl ScanResumeState {
    fn validate_envelope(&self) -> Result<()> {
        if self.schema_version != 1 || self.id != 1 || self.generation > 1 || self.phase > ABANDONED {
            return Err(DatabaseError::ConversionError(
                "Unsupported or invalid scan resume state".into(),
            ));
        }
        Ok(())
    }

    pub fn validate(&self, request_key: &str) -> Result<()> {
        self.validate_envelope()?;
        if self.request_key != request_key {
            return Err(DatabaseError::ConfigError(
                "Scan resume source/options differ from the saved scan; use the original source/options or omit --resume to start a fresh scan".into(),
            ));
        }
        Ok(())
    }

    pub fn advance(&mut self) -> Result<()> {
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| DatabaseError::ConversionError("Scan resume version overflow".into()))?;
        Ok(())
    }

    pub const fn active(&self) -> bool {
        self.phase == RUNNING || self.phase == FINALIZING
    }
}

impl ClickHouseDatabase {
    fn resume_table(&self) -> String {
        format!("scan_resume_{}", sanitize_job_id(&self.job_id))
    }

    pub async fn load_resume_state(&self) -> Result<Option<ScanResumeState>> {
        let table = self.resume_table();
        self.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table} (id UInt8, schema_version UInt8, version UInt64, \
             request_key String, generation UInt8, incremental Bool, phase UInt8, event_time Int64, \
             observed_entries UInt64, checkpoint Array(UInt8)) \
             ENGINE = ReplacingMergeTree(version) ORDER BY id"
            ),
            &[],
        )
        .await?;
        match self
            .sync_client
            .query(&format!(
                "SELECT id, schema_version, version, request_key, generation, incremental, phase, \
             event_time, observed_entries, checkpoint FROM {table} ORDER BY version DESC LIMIT 1"
            ))
            .fetch_one::<ScanResumeState>()
            .await
        {
            Ok(row) => {
                row.validate_envelope()?;
                Ok(Some(row))
            }
            Err(ClickHouseError::RowNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn save_resume_state(&self, state: &ScanResumeState) -> Result<()> {
        state.validate_envelope()?;
        let mut insert = self.sync_client.insert::<ScanResumeState>(&self.resume_table()).await?;
        insert.write(state).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn invalidate_resume_state(&self) -> Result<()> {
        if !self.check_table_exists(&self.resume_table()).await? {
            return Ok(());
        }
        if let Some(mut state) = self.load_resume_state().await?
            && state.active()
        {
            state.advance()?;
            state.phase = ABANDONED;
            state.checkpoint.clear();
            self.save_resume_state(&state).await?;
            self.cleanup_resume_batch().await?;
        }
        Ok(())
    }

    pub async fn begin_resume_batch(&mut self) -> Result<()> {
        let table = format!("scan_resume_batch_{}", sanitize_job_id(&self.job_id));
        self.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {table} ({FILE_SCAN_COLUMNS_DEFINITION}) \
             ENGINE = MergeTree() ORDER BY (relative_path, version_id, identity_key) SETTINGS allow_nullable_key = 1"
            ),
            &[],
        )
        .await?;
        self.execute(&format!("TRUNCATE TABLE {table}"), &[]).await?;
        self.scan_temp_table_name = Some(table);
        Ok(())
    }

    pub async fn write_resume_base_records(&self, entries: &[Arc<EntryEnum>]) -> Result<()> {
        self.batch_insert(&get_scan_base_table_name(&self.job_id), entries, false)
            .await
    }

    pub async fn write_resume_temp_records(&self, entries: &[Arc<EntryEnum>]) -> Result<()> {
        let table = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("scan resume batch table".into()))?;
        self.batch_insert(table, entries, false).await
    }

    pub async fn cleanup_resume_batch(&self) -> Result<()> {
        for prefix in ["scan_resume_batch", "scan_resume_event_keys", "scan_resume_delete_keys"] {
            self.execute(
                &format!("DROP TABLE IF EXISTS {prefix}_{}", sanitize_job_id(&self.job_id)),
                &[],
            )
            .await?;
        }
        Ok(())
    }

    /// 恢复现有工作代次，保留已扫描条目的代次。
    /// 返回 true 表示前一个进程退出前已提交最终代次。
    pub async fn restore_resume_generation(&self, state: &ScanResumeState) -> Result<bool> {
        let committed = self
            .query_scan_state()
            .await?
            .ok_or_else(|| DatabaseError::TransactionError("Scan resume generation is missing".into()))?;
        if committed == state.generation {
            if state.phase != FINALIZING {
                return Err(DatabaseError::TransactionError(
                    "Saved running scan conflicts with the committed generation".into(),
                ));
            }
            return Ok(true);
        }
        if committed != 1 - state.generation {
            return Err(DatabaseError::TransactionError("Invalid scan resume generation".into()));
        }
        *self.generation_lifecycle.lock().await = ScanGenerationLifecycle {
            working: Some(state.generation),
            ..Default::default()
        };
        self.cached_scan_state.store(state.generation, Ordering::Relaxed);
        Ok(false)
    }

    /// 即使运行节点时钟回拨，本轮也从历史事件之后开始。
    pub async fn resume_event_epoch(&self, now: i64) -> Result<i64> {
        let table = get_incremental_scan_base_table_name(&self.job_id);
        if !self.check_table_exists(&table).await? {
            return Ok(now);
        }
        let last = self
            .sync_client
            .query(&format!(
                "SELECT ifNull(toUnixTimestamp64Nano(max(create_at)), 0) FROM {table}"
            ))
            .fetch_one::<i64>()
            .await?;
        Ok(now.max(
            last.checked_add(1)
                .ok_or_else(|| DatabaseError::ConversionError("Scan event timestamp overflow".into()))?,
        ))
    }

    /// 以路径与版本联合键删除，避免删除历史版本时误删同名的存活版本。
    pub async fn delete_resume_entries(&self, entries: &[(String, String)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let keys = format!("scan_resume_delete_keys_{}", sanitize_job_id(&self.job_id));
        self.execute(
            &format!("CREATE TABLE IF NOT EXISTS {keys} (relative_path String, version_id String) ENGINE = Memory"),
            &[],
        )
        .await?;
        self.execute(&format!("TRUNCATE TABLE {keys}"), &[]).await?;
        let mut insert = self.sync_client.insert::<ResumeDeleteKey>(&keys).await?;
        for (relative_path, version_id) in entries {
            insert
                .write(&ResumeDeleteKey {
                    relative_path: relative_path.clone(),
                    version_id: version_id.clone(),
                })
                .await?;
        }
        insert.end().await?;
        let base = get_scan_base_table_name(&self.job_id);
        // 等待 mutation 完成后才能覆盖下一个批次的辅助键表。
        self.sync_client
            .clone()
            .with_option("lightweight_deletes_sync", "1")
            .query(&format!(
                "DELETE FROM {base} WHERE (relative_path, version_id) IN (SELECT relative_path, version_id FROM {keys})"
            ))
            .execute()
            .await?;
        Ok(())
    }

    async fn write_resume_keys(&self, paths: HashSet<String>) -> Result<String> {
        let keys = format!("scan_resume_event_keys_{}", sanitize_job_id(&self.job_id));
        self.execute(
            &format!("CREATE TABLE IF NOT EXISTS {keys} (relative_path String) ENGINE = Memory"),
            &[],
        )
        .await?;
        self.execute(&format!("TRUNCATE TABLE {keys}"), &[]).await?;
        let mut insert = self.sync_client.insert::<ResumeEventPath>(&keys).await?;
        for path in paths {
            insert.write(&ResumeEventPath { relative_path: path }).await?;
        }
        insert.end().await?;
        Ok(keys)
    }

    /// 事件先于基表更新；重放复用已有时间戳，不同 S3 版本在原排序键中保持独立。
    pub async fn write_resume_events(&self, messages: &[StorageEntryMessage], event_time: i64) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let table = get_incremental_scan_base_table_name(&self.job_id);
        let paths: HashSet<String> = messages
            .iter()
            .map(|message| IncrementalStorageEntryRecord::from_message(message).relative_path)
            .collect();
        // 使用 RowBinary 辅助键表，避免大批次生成超长 SQL IN 字面量。
        let keys = self.write_resume_keys(paths).await?;
        let existing = self
            .sync_client
            .query(&format!(
                "SELECT operation_type, relative_path, version_id, create_at FROM {table} FINAL \
             WHERE toUnixTimestamp64Nano(create_at) >= ? AND relative_path IN (SELECT relative_path FROM {keys})"
            ))
            .bind(event_time)
            .fetch_all::<(String, String, String, i64)>()
            .await?;
        let mut next_time = existing
            .iter()
            .map(|row| row.3)
            .max()
            .unwrap_or(event_time)
            .checked_add(1)
            .ok_or_else(|| DatabaseError::ConversionError("Scan event timestamp overflow".into()))?;
        let mut timestamps: HashMap<_, _> = existing
            .into_iter()
            .map(|(op, path, version, time)| ((op, path, version), time))
            .collect();
        let mut insert = self.sync_client.insert::<IncrementalStorageEntryRecord>(&table).await?;
        for message in messages {
            let mut record = IncrementalStorageEntryRecord::from_message(message);
            let key = (
                record.operation_type.clone(),
                record.relative_path.clone(),
                record.version_id.clone(),
            );
            record.create_at = if let Some(time) = timestamps.get(&key) {
                *time
            } else {
                let time = next_time;
                next_time = next_time
                    .checked_add(1)
                    .ok_or_else(|| DatabaseError::ConversionError("Scan event timestamp overflow".into()))?;
                timestamps.insert(key, time);
                time
            };
            insert.write(&record).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ScanResumeState {
        ScanResumeState {
            id: 1,
            schema_version: 1,
            version: 7,
            request_key: "source-options".into(),
            generation: 1,
            incremental: true,
            phase: RUNNING,
            event_time: 42,
            observed_entries: 100,
            checkpoint: vec![1, 2, 3],
        }
    }

    #[test]
    fn refuses_other_sources_filters_and_unknown_envelopes() {
        let mut value = state();
        assert!(value.validate("source-options").is_ok());
        assert!(value.validate("other-options").is_err());
        value.schema_version = 2;
        assert!(value.validate("source-options").is_err());
    }

    #[test]
    fn retries_keep_event_keys_but_advance_checkpoint_versions() -> Result<()> {
        let mut value = state();
        value.advance()?;
        assert_eq!(value.version, 8);
        assert_eq!(value.event_time, 42);
        value.version = u64::MAX;
        assert!(value.advance().is_err());
        Ok(())
    }
}
