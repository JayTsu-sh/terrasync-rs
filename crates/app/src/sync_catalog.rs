//! 复制／同步的目录元数据比对，不承载扫描轮次或扫描取消策略。

use std::sync::Arc;

use data_mover::model::ObservedEntry;
use db::ClickHouseDatabase;
use db::catalogue::ComparisonMode;
use task_model::StorageEntryMessage;
use tokio::task::JoinHandle;
use tracing::{error, trace, warn};

use crate::{broadcast::BroadcastForwarder, error::Result, scan::ScanType};

pub(crate) async fn determine_sync_type(db: Option<&ClickHouseDatabase>, job_dir_pre_existing: bool) -> ScanType {
    if let Some(db) = db {
        match db.copy_catalogue_exists().await {
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

pub(crate) async fn batch_processing_to_generate_message(
    database: &ClickHouseDatabase, batch: &[Arc<ObservedEntry>], broadcaster: BroadcastForwarder<StorageEntryMessage>,
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
mod tests {
    use super::*;
    use crate::error::AppError;
    use clickhouse::error::Error as ClickHouseError;
    use clickhouse::test::{Mock, handlers};
    use db::{ClickHouseConfig, DatabaseError};
    use reqwest::StatusCode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn failed_catalogue_batch_stays_typed_emits_no_events_and_drains_phase_tasks() {
        let mock = Mock::new();
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: mock.url().into(),
            database: "default".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let database = ClickHouseDatabase::new(&config, "failed_phase")
            .unwrap_or_else(|error| panic!("fixture catalogue: {error}"));
        mock.add(handlers::failure(StatusCode::SERVICE_UNAVAILABLE));
        let mut broadcaster = BroadcastForwarder::new(8);
        let mut events = broadcaster.subscribe();
        let completed = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&completed);
        let work = tokio::spawn(async move {
            batch_processing_to_generate_message(&database, &[], broadcaster, true, false).await
        });
        let other = tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            done.store(true, Ordering::Release);
            Ok(())
        });
        let result = finish_phase(vec![work, other]).await;
        assert!(matches!(
            result,
            Err(AppError::DatabaseError(DatabaseError::ClickHouseError(
                ClickHouseError::BadResponse(_)
            )))
        ));
        assert!(completed.load(Ordering::Acquire));
        assert!(events.recv().await.is_none());
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
}
