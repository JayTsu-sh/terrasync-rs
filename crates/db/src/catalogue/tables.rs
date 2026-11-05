//! 目录表的创建、校验和清理；物理部署细节由 layout 处理。
use super::ClickHouseDatabase;
use super::layout::CatalogueEngine;
use crate::error::{DatabaseError, Result};
use crate::execution::read::QuorumRead;
use crate::schema::columns::{FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION, FILE_SCAN_COLUMNS_DEFINITION};
use crate::traits::Database;
use crate::{
    get_incremental_scan_base_table_name, get_scan_base_table_name, get_scan_state_table_name,
    get_tar_manifest_table_name,
};
use clickhouse::error::Error as ClickHouseError;
use tracing::debug;

impl ClickHouseDatabase {
    /// 创建主扫描表（ReplacingMergeTree，按 `relative_path` + `version_id` 排序）
    pub async fn create_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            FILE_SCAN_COLUMNS_DEFINITION,
            "(relative_path, version_id)",
            CatalogueEngine::Replacing,
        )
        .await?;
        self.validate_scan_base_table().await
    }

    /// 检查已有快照键，拒绝续写旧表而保留所有记录。
    pub(crate) async fn validate_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        let sorting_key = self
            .sync_client
            .query("SELECT sorting_key FROM system.tables WHERE database=currentDatabase() AND name=?")
            .bind(self.catalogue_sorting_table(&table_name))
            .fetch_quorum_one::<String>()
            .await?;
        let normalized = sorting_key.replace([' ', '(', ')', '`'], "");
        if normalized.strip_prefix("tuple").unwrap_or(&normalized) != "relative_path,version_id" {
            return Err(DatabaseError::ConfigError(format!(
                "ClickHouse scan table {table_name} uses an incompatible sorting key; clear this scan job and run a full scan again"
            )));
        }
        Ok(())
    }

    /// 创建扫描状态表（id=1, `scan_state` 0/1）
    pub async fn create_scan_state_table(&self) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            "id UInt8, scan_state UInt8, baseline_committed Bool DEFAULT true",
            "id",
            CatalogueEngine::Replacing,
        )
        .await?;
        self.ensure_baseline_committed_column().await?;

        Ok(())
    }

    pub(super) async fn ensure_baseline_committed_column(&self) -> Result<()> {
        let table = get_scan_state_table_name(&self.job_id);
        // 兼容旧状态行的已有基线；新任务在首次成功提交前显式写入 false。
        self.add_catalogue_column(&table, "baseline_committed Bool DEFAULT true")
            .await?;
        Ok(())
    }

    /// 建表不代表首次全量已完成，只有成功提交代次才发布基线。
    pub async fn has_committed_baseline(&self) -> Result<bool> {
        let table = get_scan_state_table_name(&self.job_id);
        if !self.check_table_exists(&table).await? {
            return Ok(false);
        }
        self.ensure_baseline_committed_column().await?;
        match self
            .sync_client
            .query(&format!("SELECT baseline_committed FROM {table} FINAL WHERE id=1"))
            .fetch_quorum_one::<bool>()
            .await
        {
            Ok(committed) => Ok(committed),
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// 创建增量扫描表（按 `operation_type` + `relative_path` + `create_at` 排序）
    pub(super) async fn create_incremental_scan_table(&self) -> Result<()> {
        let table_name = get_incremental_scan_base_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION,
            "(operation_type, relative_path, create_at)",
            CatalogueEngine::Replacing,
        )
        .await?;

        Ok(())
    }

    /// 创建 tar manifest 表
    ///
    /// 记录每个 .tar 文件内部包含的条目清单
    pub(super) async fn create_tar_manifest_table_impl(&self) -> Result<()> {
        let table_name = get_tar_manifest_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            "tar_path String, entry_path String, size UInt64, ext Nullable(String), mtime Int64, mode Nullable(UInt32),
             storage_type String, is_dir Bool, is_symlink Bool, uid Nullable(UInt32), gid Nullable(UInt32),
             version_id String, tags Nullable(String)",
            "(tar_path, entry_path, version_id)",
            CatalogueEngine::Replacing,
        )
        .await?;

        Ok(())
    }

    /// 删除所有以指定前缀开头的表，返回已删除表名列表
    pub async fn drop_tables_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        // 转义 LIKE 通配符，防止 prefix 中含有 % 或 _ 匹配到计划外的表
        let escaped = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let query = format!(
            "SELECT name FROM system.tables WHERE name LIKE '{escaped}%' ESCAPE '\\\\' AND database = currentDatabase()"
        );

        let table_names: Vec<String> = self.sync_client.query(&query).fetch_quorum_all::<String>().await?;

        let mut dropped_tables = Vec::new();
        for table_name in table_names {
            self.drop_table_by_name(&table_name).await?;
            dropped_tables.push(table_name);
        }

        debug!("Dropped {} tables with prefix '{}'", dropped_tables.len(), prefix);
        Ok(dropped_tables)
    }

    /// 检查指定表名是否存在于当前数据库中
    pub async fn check_table_exists(&self, table_name: &str) -> Result<bool> {
        let count: u64 = self
            .sync_client
            .query("SELECT count() FROM system.tables WHERE name = ? AND database = currentDatabase()")
            .bind(table_name)
            .fetch_quorum_one()
            .await?;
        Ok(count > 0)
    }
}
