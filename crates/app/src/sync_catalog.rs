//! 复制／同步的目录元数据比对，不承载扫描轮次或扫描取消策略。
use crate::{broadcast::BroadcastForwarder, error::Result, scan::ScanType};
use db::catalogue::ComparisonMode;
use db::traits::Database;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, trace, warn};
use {storage_adapter::ObservedEntry, task_model::StorageEntryMessage};

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
    database: &Box<dyn Database>, batch: &[Arc<ObservedEntry>], broadcaster: BroadcastForwarder<StorageEntryMessage>,
    is_final_batch: bool, keep_item: bool,
) -> Result<()> {
    trace!(is_final_batch, "Comparing catalogue batch");
    let mode = if keep_item {
        ComparisonMode::PublishObserved
    } else {
        ComparisonMode::AwaitCopyAcknowledgement
    };
    let changes = database.compare_catalogue_batch(batch, mode).await?;
    for change in changes {
        broadcaster.broadcast(change.kind.message(change.item)).await;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use db::error::DatabaseError;
    use db::traits::Database;
    use db::{CopyGenerationStatus, DeletionStatus, Result as DatabaseResult};
    use tokio::sync::mpsc;
    use {
        storage_adapter::ObservedEntry,
        task_model::{ChangeKind, StorageEntryMessage},
    };

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

    fn dummy_entry(name: &str) -> ObservedEntry {
        {
            let fixture_path: std::path::PathBuf = PathBuf::from(name);
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::Nfs,
                fixture_path,
                data_mover::model::EntryKind::File,
                1,
                [0, 0, 0],
                Some(data_mover::model::OwnershipMode {
                    mode: 0o644,
                    uid: 0u32,
                    gid: 0u32,
                }),
                None,
                None,
                None,
            )
        }
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
        async fn batch_insert_base_record(&self, _records: &[Arc<ObservedEntry>]) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_insert_completed_records(
            &self, _records: &[task_model::observation::ObservedItem],
        ) -> DatabaseResult<()> {
            Ok(())
        }
        async fn update_base_record(&self, _record: &Arc<ObservedEntry>) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_delete_base_record(&self, _deleted_paths: &[String]) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_insert_temp_record(&self, _records: &[Arc<ObservedEntry>]) -> DatabaseResult<()> {
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

        async fn classify_catalogue_items(&self) -> DatabaseResult<Vec<db::catalogue::CatalogueDifference>> {
            drop(self.detect_new_items().await?);
            drop(self.detect_changed_items().await?);
            Ok(Vec::new())
        }

        async fn detect_new_items(&self) -> DatabaseResult<Box<dyn Iterator<Item = ObservedEntry> + Send>> {
            self.record("detect_new");
            match self.fault {
                ScanDatabaseFault::DetectNew => Err(DatabaseError::QueryError("injected new detection failure".into())),
                ScanDatabaseFault::DetectChanged => Ok(Box::new(vec![dummy_entry("new.txt")].into_iter())),
                ScanDatabaseFault::BatchInsertOnce => Ok(Box::new(std::iter::empty())),
            }
        }

        async fn detect_changed_items(
            &self,
        ) -> DatabaseResult<Box<dyn Iterator<Item = (ObservedEntry, ChangeKind)> + Send>> {
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
            _tx: mpsc::Sender<ObservedEntry>,
        ) -> DatabaseResult<()> {
            Ok(())
        }
        async fn create_tar_manifest_table(&self) -> DatabaseResult<()> {
            Ok(())
        }
        async fn batch_insert_tar_manifest(
            &self, _tar_path: &str, _entries: &[Arc<ObservedEntry>],
        ) -> DatabaseResult<()> {
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
    async fn changed_detection_failure_emits_no_partial_plan_and_skips_merge() {
        let (database, calls) = fault_database_with_calls(ScanDatabaseFault::DetectChanged);
        let mut broadcaster = BroadcastForwarder::new(8);
        let mut events = broadcaster.subscribe();

        let result = batch_processing_to_generate_message(&database, &[], broadcaster, true, true).await;
        let event = events.recv().await;

        assert!(result.is_err());
        assert!(event.is_none());
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
