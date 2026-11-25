//! One task's catalogue coordination. Only explicit catalogue clones share this state.
use task_model::migration::CopyGeneration;
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard};

use super::ClickHouseDatabase;
use crate::{DatabaseError, Result};

#[derive(Default)]
pub(super) struct ScanGenerationLifecycle {
    pub working: Option<u8>,
    pub committed: bool,
    pub copy_recovery: bool,
    pub copy_generation_recovering: bool,
    pub copy_execution_id: Option<String>,
    pub copy_generation: Option<CopyGeneration>,
    pub copy_fact_range: Option<CopyFactRange>,
}

/// 最近一次全量到本次执行的复制事实。全量清理之后到达的旧写入也要排除。
#[derive(Clone, Copy)]
pub(super) struct CopyFactRange {
    pub first: u64,
    pub last: u64,
}

impl CopyFactRange {
    pub fn predicate(self) -> String {
        format!(
            "copy_sequence BETWEEN {} AND {} AND copy_execution_id!=''",
            self.first, self.last
        )
    }

    pub fn and_predicate(range: Option<Self>) -> String {
        range.map_or_else(String::new, |range| format!(" AND {}", range.predicate()))
    }
}

#[derive(Default)]
pub(super) struct CatalogueLifecycle {
    state: Mutex<ScanGenerationLifecycle>,
    pub writes: Mutex<()>,
    /// 只约束 base 事实；通知、代次记录和最终报告不受清理屏障限制。
    pub base_closed: RwLock<bool>,
}

impl ClickHouseDatabase {
    pub(super) async fn copy_fact_range(&self) -> Result<Option<CopyFactRange>> {
        let lifecycle = self.generation_lifecycle.lock().await;
        match (lifecycle.copy_generation.as_ref(), lifecycle.copy_fact_range) {
            (Some(_), None) => Err(DatabaseError::CopyExecutionUnavailable),
            (_, range) => Ok(range),
        }
    }

    pub(super) async fn base_write_guard(&self) -> Result<RwLockReadGuard<'_, bool>> {
        let guard = self.generation_lifecycle.base_closed.read().await;
        if *guard {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "base_writes_closed",
            });
        }
        Ok(guard)
    }
}

impl CatalogueLifecycle {
    pub async fn lock(&self) -> MutexGuard<'_, ScanGenerationLifecycle> {
        self.state.lock().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{ClickHouseConfig, ClickHouseDatabase, Result};

    fn config() -> ClickHouseConfig {
        ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: "http://127.0.0.1:8123".into(),
            database: "default".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        }
    }

    #[tokio::test]
    async fn catalogue_clones_share_generation_but_independent_tasks_do_not() -> Result<()> {
        let config = config();
        let first = ClickHouseDatabase::new(&config, "task_scope")?;
        let cloned = first.clone();
        let independent = ClickHouseDatabase::new(&config, "task_scope")?;
        first.generation_lifecycle.lock().await.working = Some(1);
        assert_eq!(cloned.generation_lifecycle.lock().await.working, Some(1));
        assert_eq!(independent.generation_lifecycle.lock().await.working, None);
        Ok(())
    }

    #[tokio::test]
    async fn catalogue_clones_serialize_writes_inside_their_task_scope() -> Result<()> {
        let config = config();
        let first = ClickHouseDatabase::new(&config, "write_scope")?;
        let cloned = first.clone();
        let independent = ClickHouseDatabase::new(&config, "write_scope")?;
        let _write = first.generation_lifecycle.writes.lock().await;
        assert!(cloned.generation_lifecycle.writes.try_lock().is_err());
        assert!(independent.generation_lifecycle.writes.try_lock().is_ok());
        Ok(())
    }
}
