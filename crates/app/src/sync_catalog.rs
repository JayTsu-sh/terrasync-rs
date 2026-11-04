//! 复制／同步的目录元数据比对，不承载扫描轮次或扫描取消策略。
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, Result},
    scan::ScanType,
};
use db::traits::Database;
use std::sync::Arc;
use storage_adapter::{EntryEnum, StorageEntryMessage};
use tokio::task::JoinHandle;
use tracing::{error, trace, warn};

pub(crate) async fn determine_sync_type(
    db: Option<&dyn Database>, job_id: &str, job_dir_pre_existing: bool,
) -> ScanType {
    if let Some(db) = db {
        let table_name = db::base_table_name_for_job(job_id);
        match db.table_exists(&table_name).await {
            Ok(true) => return ScanType::Incremental,
            Ok(false) => return ScanType::Full,
            Err(e) => {
                warn!("数据库查询失败，fallback 到文件系统快照判定: {}", e);
            }
        }
    }

    // Fallback：调用方注入的 job_dir 预存在快照
    if job_dir_pre_existing {
        ScanType::Incremental
    } else {
        ScanType::Full
    }
}

#[allow(clippy::borrowed_box)]
pub(crate) async fn batch_processing_to_generate_message(
    database: &Box<dyn Database>, batch: &[Arc<EntryEnum>], broadcaster: BroadcastForwarder<StorageEntryMessage>,
    is_final_batch: bool, keep_item: bool,
) -> Result<()> {
    // 由于database是不可变引用，但我们需要可变引用，这里进行clone
    let mut db_mut = database.clone();

    batch_insert_temp_record(0, &mut db_mut, batch, is_final_batch).await?;

    // 使用现有的generate_storage_entry_message_events辅助函数生成事件
    let generation_result =
        generate_storage_entry_message_events(0, &mut db_mut, broadcaster, is_final_batch, keep_item).await;

    // 清理临时表
    trace!("Dropping temporary table for batch processing");
    let cleanup_result = db_mut.drop_scan_temporary_table().await;

    generation_result?;
    cleanup_result.map_err(AppError::DatabaseError)?;

    Ok(())
}

/// 处理批次数据的辅助函数: 创建临时表，插入当前批次的数据到临时表里
async fn batch_insert_temp_record(
    task_id: usize, database: &mut Box<dyn Database>, batch: &[Arc<EntryEnum>], is_final_batch: bool,
) -> Result<()> {
    trace!(task_id, is_final_batch, "Preparing catalogue comparison batch");
    database.create_scan_temporary_table().await?;
    if let Err(source) = database.batch_insert_temp_record(batch).await {
        if let Err(cleanup) = database.drop_scan_temporary_table().await {
            warn!(%cleanup, "Failed to clean up incomplete catalogue batch");
        }
        return Err(source.into());
    }
    Ok(())
}

/// 排空同一阶段的所有任务后返回首个类型化错误；调用方仅在成功后推进删除阶段。
pub(crate) async fn finish_phase(tasks: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    let mut failure = None;
    for task in tasks {
        let result = match task.await {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            error!(%error, "Catalogue phase task failed");
            failure.get_or_insert(error);
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// 处理查询结果的辅助函数
async fn generate_storage_entry_message_events(
    task_id: usize, database: &mut Box<dyn Database>, broadcaster: BroadcastForwarder<StorageEntryMessage>,
    is_final_batch: bool, keep_item: bool,
) -> Result<()> {
    // 根据是否是最后一批数据，设置不同的日志消息
    let batch_type = if is_final_batch { "final batch" } else { "batch" };

    let mut excluded_paths: Vec<(String, String)> = Vec::new();

    // 查询新增文件
    trace!("Task {} querying new items for {}", task_id, batch_type);
    let new_items = database.detect_new_items().await.map_err(AppError::DatabaseError)?;
    let items: Vec<EntryEnum> = new_items.collect();
    if !keep_item {
        for item in &items {
            excluded_paths.push((
                item.get_relative_path().to_string_lossy().into_owned(),
                item.get_version_id().unwrap_or_default().to_string(),
            ));
        }
    }
    for item in items {
        broadcaster.broadcast(StorageEntryMessage::New(Arc::new(item))).await;
    }

    // 查询修改文件（区分 Data/Metadata/Both 三种变更）
    trace!("Task {} querying changed items for {}", task_id, batch_type);
    let changed_items = database.detect_changed_items().await.map_err(AppError::DatabaseError)?;
    let items: Vec<(EntryEnum, storage_adapter::ChangeKind)> = changed_items.collect();
    if !keep_item {
        for (item, _) in &items {
            excluded_paths.push((
                item.get_relative_path().to_string_lossy().into_owned(),
                item.get_version_id().unwrap_or_default().to_string(),
            ));
        }
    }
    for (item, kind) in items {
        broadcaster
            .broadcast(StorageEntryMessage::Changed {
                entry: Arc::new(item),
                kind,
            })
            .await;
    }

    // 将临时表数据插入到主表（带排除过滤）
    trace!(
        "Task {} inserting temporary table to base table for {} (excluded: {} paths)",
        task_id,
        batch_type,
        excluded_paths.len()
    );
    database
        .insert_temp_to_base_table(&excluded_paths)
        .await
        .map_err(AppError::DatabaseError)?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use db::error::DatabaseError;
    use db::traits::{Database, TarManifestRecord};
    use db::{CopyGenerationStatus, DeletionStatus, Result as DatabaseResult};
    use storage_adapter::{ChangeKind, EntryEnum, StorageEntryMessage};
    use tokio::sync::mpsc;

    use super::{batch_processing_to_generate_message, finish_phase};
    use crate::broadcast::BroadcastForwarder;
    use crate::error::AppError;
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Clone, Copy)]
    enum ScanDatabaseFault {
        BatchInsertOnce,
        DetectNew,
        DetectChanged,
    }

    #[derive(Clone)]
    struct FaultDatabase {
        fault: ScanDatabaseFault,
        fired: Arc<AtomicBool>,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    fn fault_database(fault: ScanDatabaseFault) -> Box<dyn Database> {
        fault_database_with_calls(fault).0
    }

    fn fault_database_with_calls(fault: ScanDatabaseFault) -> (Box<dyn Database>, Arc<Mutex<Vec<&'static str>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let database = FaultDatabase {
            fault,
            fired: Arc::new(AtomicBool::new(false)),
            calls: Arc::clone(&calls),
        };
        (Box::new(database), calls)
    }

    fn dummy_entry(name: &str) -> EntryEnum {
        EntryEnum::NAS(storage_adapter::NASEntry {
            name: name.to_string(),
            relative_path: PathBuf::from(name),
            extension: None,
            is_dir: false,
            size: 1,
            atime: 0,
            ctime: 0,
            mtime: 0,
            mode: Some(0o644),
            is_symlink: false,
            hard_links: None,
            uid: None,
            gid: None,
            ino: None,
            file_handle: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        })
    }

    impl FaultDatabase {
        fn record(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }

        fn fires_once(&self, fault: ScanDatabaseFault) -> bool {
            std::mem::discriminant(&self.fault) == std::mem::discriminant(&fault)
                && !self.fired.swap(true, Ordering::AcqRel)
        }
    }

    #[async_trait]
    impl Database for FaultDatabase {
        fn clone_box(&self) -> Box<dyn Database> {
            Box::new(self.clone())
        }

        async fn initialize(&self) -> DatabaseResult<()> {
            Ok(())
        }
        async fn ping(&self) -> DatabaseResult<()> {
            Ok(())
        }
        async fn create_table(&self, _table_name: &str) -> DatabaseResult<()> {
            Ok(())
        }
        async fn create_scan_temporary_table(&mut self) -> DatabaseResult<()> {
            self.record("create_temp");
            Ok(())
        }
        async fn drop_scan_temporary_table(&mut self) -> DatabaseResult<()> {
            self.record("drop_temp");
            Ok(())
        }
        async fn drop_table_by_name(&self, _table_name: &str) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_insert_base_record(&self, _records: &[Arc<EntryEnum>]) -> DatabaseResult<()> {
            Ok(())
        }
        async fn update_base_record(&self, _record: &Arc<EntryEnum>) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_delete_base_record(&self, _deleted_paths: &[String]) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_insert_temp_record(&self, _records: &[Arc<EntryEnum>]) -> DatabaseResult<()> {
            self.record("insert_temp");
            if self.fires_once(ScanDatabaseFault::BatchInsertOnce) {
                return Err(DatabaseError::QueryError("injected batch insert failure".into()));
            }
            Ok(())
        }
        async fn batch_insert_incremental_record(&self, _records: &[StorageEntryMessage]) -> DatabaseResult<()> {
            Ok(())
        }
        async fn begin_copy_generation(&self, _request_key: &str) -> DatabaseResult<CopyGenerationStatus> {
            Ok(CopyGenerationStatus::Fresh)
        }
        async fn begin_scan_generation(&self) -> DatabaseResult<u8> {
            Ok(1)
        }
        async fn commit_scan_generation(&self) -> DatabaseResult<()> {
            Ok(())
        }

        async fn detect_new_items(&self) -> DatabaseResult<Box<dyn Iterator<Item = EntryEnum> + Send>> {
            self.record("detect_new");
            match self.fault {
                ScanDatabaseFault::DetectNew => Err(DatabaseError::QueryError("injected new detection failure".into())),
                ScanDatabaseFault::DetectChanged => Ok(Box::new(vec![dummy_entry("new.txt")].into_iter())),
                ScanDatabaseFault::BatchInsertOnce => Ok(Box::new(std::iter::empty())),
            }
        }

        async fn detect_changed_items(
            &self,
        ) -> DatabaseResult<Box<dyn Iterator<Item = (EntryEnum, ChangeKind)> + Send>> {
            self.record("detect_changed");
            if matches!(self.fault, ScanDatabaseFault::DetectChanged) {
                return Err(DatabaseError::QueryError("injected changed detection failure".into()));
            }
            Ok(Box::new(std::iter::empty()))
        }

        async fn detect_deleted_items(&self) -> DatabaseResult<Box<dyn Iterator<Item = DeletionStatus> + Send>> {
            Ok(Box::new(std::iter::empty()))
        }

        async fn insert_temp_to_base_table(&self, _excluded_paths: &[(String, String)]) -> DatabaseResult<()> {
            self.record("merge_temp");
            Ok(())
        }
        async fn get_count(&self, _table_name: &str) -> DatabaseResult<u64> {
            Ok(0)
        }
        async fn query_storage_entry(
            &self, _is_dir: Option<bool>, _is_symlink: Option<bool>, _extension: Option<String>,
            _tx: mpsc::Sender<EntryEnum>,
        ) -> DatabaseResult<()> {
            Ok(())
        }
        async fn create_tar_manifest_table(&self) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_insert_tar_manifest(&self, _records: &[TarManifestRecord]) -> DatabaseResult<()> {
            Ok(())
        }
        async fn table_exists(&self, _table_name: &str) -> DatabaseResult<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn failed_catalogue_batch_keeps_its_typed_error_and_drains_other_phase_tasks() {
        let database = fault_database(ScanDatabaseFault::BatchInsertOnce);
        let completed = Arc::new(AtomicBool::new(false));
        let done = completed.clone();
        let work = tokio::spawn(async move {
            batch_processing_to_generate_message(&database, &[], BroadcastForwarder::new(8), true, false).await
        });
        let other = tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            done.store(true, Ordering::Release);
            Ok(())
        });
        let result = finish_phase(vec![work, other]).await;
        assert!(
            matches!(result, Err(AppError::DatabaseError(DatabaseError::QueryError(message))) if message=="injected batch insert failure")
        );
        assert!(
            completed.load(Ordering::Acquire),
            "all Phase A work must finish before returning the failure"
        );
    }

    #[tokio::test]
    async fn a_panicked_phase_worker_prevents_success() {
        let failed = tokio::spawn(async {
            panic!("injected phase panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        assert!(matches!(
            finish_phase(vec![failed]).await,
            Err(AppError::TaskJoinError(_))
        ));
    }

    #[tokio::test]
    async fn new_detection_failure_is_returned_without_emitting_delta_events() {
        let database = fault_database(ScanDatabaseFault::DetectNew);
        let mut broadcaster = BroadcastForwarder::new(8);
        let mut events = broadcaster.subscribe();

        let result = batch_processing_to_generate_message(&database, &[], broadcaster, true, true).await;

        assert!(result.is_err());
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn failed_batch_does_not_prevent_a_later_batch_from_finishing() {
        let (database, calls) = fault_database_with_calls(ScanDatabaseFault::BatchInsertOnce);
        let broadcaster = BroadcastForwarder::new(8);

        let first = batch_processing_to_generate_message(&database, &[], broadcaster.clone(), false, true).await;
        let second = batch_processing_to_generate_message(&database, &[], broadcaster, true, true).await;

        assert!(first.is_err());
        assert!(second.is_ok());
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                "create_temp",
                "insert_temp",
                "drop_temp",
                "create_temp",
                "insert_temp",
                "detect_new",
                "detect_changed",
                "merge_temp",
                "drop_temp"
            ]
        );
    }

    #[tokio::test]
    async fn changed_detection_failure_preserves_new_output_and_skips_merge() {
        let (database, calls) = fault_database_with_calls(ScanDatabaseFault::DetectChanged);
        let mut broadcaster = BroadcastForwarder::new(8);
        let mut events = broadcaster.subscribe();

        let result = batch_processing_to_generate_message(&database, &[], broadcaster, true, true).await;
        let event = events.recv().await;

        assert!(result.is_err());
        assert!(matches!(event, Some(StorageEntryMessage::New(_))));
        assert!(events.recv().await.is_none());
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                "create_temp",
                "insert_temp",
                "detect_new",
                "detect_changed",
                "drop_temp"
            ]
        );
    }
}
