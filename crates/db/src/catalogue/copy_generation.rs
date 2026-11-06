//! 复制目录仅在物理操作与数据库确认全部完成后结束轮次；失败轮次保守重放。

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use clickhouse::error::Error as ClickHouseError;
use serde::Deserialize;
use serde_json::Value;

use super::ClickHouseDatabase;
use super::lifecycle::ScanGenerationLifecycle;
use crate::DeletionStatus;
use crate::error::{DatabaseError, Result};
use crate::execution::read::QuorumRead;
use crate::get_scan_base_table_name;
use crate::get_scan_state_table_name;
use crate::schema::columns::CATALOGUE_COLUMNS as FILE_SCAN_COLUMNS_LIST;
use crate::schema::records::StorageEntryRecord;

/// Whether an unfinished copy generation must replay unacknowledged target operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyGenerationStatus {
    /// 首轮或上一轮全部确认成功，沿用正常增量比较。
    Fresh,
    /// 上轮复制没有确认完成，必须重放源项或拒绝不安全的恢复。
    Recovering,
}

#[derive(Deserialize, clickhouse::Row)]
struct CopyState {
    scan_state: u8,
    baseline_committed: bool,
    copy_in_progress: bool,
    copy_request_key: String,
}

impl ClickHouseDatabase {
    pub async fn begin_copy_generation(&self, request_key: &str) -> Result<CopyGenerationStatus> {
        if request_key.is_empty() {
            return Err(DatabaseError::ConfigError(
                "copy generation requires a source/destination recovery binding".into(),
            ));
        }
        let mut lifecycle = self.generation_lifecycle.lock().await;
        let saved = self.copy_generation_state().await?;
        let previous = saved.scan_state;
        let recovering = saved.copy_in_progress || (!saved.baseline_committed && self.copy_base_has_rows().await?);
        if recovering && (saved.copy_request_key.is_empty() || saved.copy_request_key != request_key) {
            return Err(DatabaseError::ConfigError("incomplete copy belongs to unknown or different source/destination/options; restore the original settings or choose a new job id".into()));
        }
        let working = 1 - previous;
        if recovering {
            let base = self.catalogue_storage_table(&get_scan_base_table_name(&self.job_id))?;
            self.execute(&format!("ALTER TABLE {base} UPDATE current_state={previous} WHERE current_state!={previous} SETTINGS mutations_sync=2"), &[]).await?;
        }
        let table = get_scan_state_table_name(&self.job_id);
        self.execute(&format!(
            "INSERT INTO {table} (id,scan_state,baseline_committed,copy_in_progress,copy_request_key) VALUES (1,{working},false,true,?)"
        ), &[Value::from(request_key)]).await?;
        *lifecycle = ScanGenerationLifecycle {
            working: Some(working),
            copy_recovery: recovering,
            ..Default::default()
        };
        self.cached_scan_state.store(working, Ordering::Relaxed);
        Ok(if recovering {
            CopyGenerationStatus::Recovering
        } else {
            CopyGenerationStatus::Fresh
        })
    }

    async fn copy_generation_state(&self) -> Result<CopyState> {
        let table = get_scan_state_table_name(&self.job_id);
        self.add_catalogue_column(&table, "copy_in_progress Bool DEFAULT false")
            .await?;
        self.add_catalogue_column(&table, "copy_request_key String DEFAULT ''")
            .await?;
        let state = self.sync_client.query(&format!(
            "SELECT scan_state, baseline_committed, copy_in_progress, copy_request_key FROM {table} FINAL WHERE id=1"
        )).fetch_quorum_one::<CopyState>().await?;
        if state.scan_state > 1 {
            return Err(DatabaseError::ConversionError(format!(
                "Invalid copy generation {}; expected 0 or 1",
                state.scan_state
            )));
        }
        Ok(state)
    }

    async fn copy_base_has_rows(&self) -> Result<bool> {
        let table = get_scan_base_table_name(&self.job_id);
        match self
            .sync_client
            .query(&format!("SELECT 1 FROM {table} LIMIT 1"))
            .fetch_quorum_one::<u8>()
            .await
        {
            Ok(_) => Ok(true),
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn is_copy_recovery(&self) -> bool {
        self.generation_lifecycle.lock().await.copy_recovery
    }

    pub(super) async fn copy_replay_items(&self) -> Result<Vec<StorageEntryRecord>> {
        let table = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("copy comparison staging".into()))?;
        // 按源端历史位置返回逐版本快照；只有复制完成 ACK 才能确认对应版本。
        self.sync_client
            .query(&format!(
                "SELECT {} FROM {table} ORDER BY relative_path, history_segment, history_index, version_id",
                *FILE_SCAN_COLUMNS_LIST
            ))
            .fetch_quorum_all()
            .await
    }

    pub(super) async fn copy_recovery_deletions(
        &self, rows: Vec<StorageEntryRecord>, working: u8,
    ) -> Result<Vec<DeletionStatus>> {
        let active = self.replayed_paths(&rows, working).await?;
        let mut retired = Vec::new();
        let mut deleted = Vec::new();
        let mut emitted = HashSet::new();
        for row in rows {
            if active.contains(&row.relative_path) {
                retired.push((row.relative_path, row.version_id));
            } else if emitted.insert(row.relative_path.clone()) {
                deleted.push(DeletionStatus::Deleted(row.to_observation()?));
            }
        }
        self.retire_copy_versions(&retired, working).await?;
        Ok(deleted)
    }

    async fn replayed_paths(&self, old: &[StorageEntryRecord], working: u8) -> Result<HashSet<String>> {
        let table = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;
        let paths: HashSet<_> = old.iter().map(|row| row.relative_path.as_str()).collect();
        let paths: Vec<_> = paths.into_iter().collect();
        let mut active = HashSet::new();
        for paths in bounded_keys(&paths, |path| path.len()) {
            let placeholders = vec!["?"; paths.len()].join(",");
            let mut query = self.sync_client.query(&format!("SELECT DISTINCT relative_path FROM {table} FINAL WHERE current_state={working} AND relative_path IN ({placeholders})"));
            for path in paths {
                query = query.bind(*path);
            }
            active.extend(query.fetch_quorum_all::<String>().await?);
        }
        Ok(active)
    }

    async fn retire_copy_versions(&self, keys: &[(String, String)], working: u8) -> Result<()> {
        let table = self.catalogue_storage_table(&get_scan_base_table_name(&self.job_id))?;
        for keys in bounded_keys(keys, |(path, version)| path.len().saturating_add(version.len())) {
            let placeholders = vec!["(?,?)"; keys.len()].join(",");
            let values: Vec<_> = keys
                .iter()
                .flat_map(|(path, version)| [Value::from(path.clone()), Value::from(version.clone())])
                .collect();
            self.execute(&format!("ALTER TABLE {table} DELETE WHERE current_state!={working} AND (relative_path,version_id) IN ({placeholders}) SETTINGS mutations_sync=2"), &values).await?;
        }
        Ok(())
    }
}

// SQL 参数经客户端转义后成为文本；同时约束键数和字节数，覆盖合法长路径。
fn bounded_keys<T>(keys: &[T], size: impl Fn(&T) -> usize) -> Vec<&[T]> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < keys.len() {
        let mut end = start;
        let mut bytes = 0usize;
        while end < keys.len() && end - start < 256 {
            let next = size(&keys[end]).saturating_mul(2).saturating_add(32);
            if end > start && bytes.saturating_add(next) > 16_384 {
                break;
            }
            bytes = bytes.saturating_add(next);
            end += 1;
        }
        chunks.push(&keys[start..end]);
        start = end;
    }
    chunks
}
