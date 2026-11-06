//! One task's catalogue coordination. Only explicit catalogue clones share this state.
use tokio::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub(super) struct ScanGenerationLifecycle {
    pub working: Option<u8>,
    pub committed: bool,
    pub copy_recovery: bool,
}

#[derive(Default)]
pub(super) struct CatalogueLifecycle {
    state: Mutex<ScanGenerationLifecycle>,
    pub writes: Mutex<()>,
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
