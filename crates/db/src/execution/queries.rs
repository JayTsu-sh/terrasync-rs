//! 用执行实例独有的标记停止查询，不依赖任务名、数据库名或数据库用户。
use super::recovery::{RequestSafety, ScanDatabaseExecution};
use crate::execution::transport::build_client;
use crate::{ClickHouseConfig, DatabaseError, Result, execution::base_client, schema::identifier};
use clickhouse::{Client, error::Error, query::Query};
use std::{future::Future, panic::Location, time::Duration};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

tokio::task_local! {
    static SCAN_CANCELLATION: CancellationToken;
    static SCAN_EXECUTION: Option<ScanDatabaseExecution>;
}

/// 仅作用于当前执行 future；并发 worker 各自进入此作用域，不影响其他任务。
pub async fn with_scan_cancellation<T>(
    cancel: CancellationToken, execution: Option<ScanDatabaseExecution>, work: impl Future<Output = T>,
) -> T {
    SCAN_CANCELLATION
        .scope(cancel, SCAN_EXECUTION.scope(execution, work))
        .await
}

pub(crate) fn execution_active() -> bool {
    current_execution().is_some()
}

pub(crate) fn current_execution() -> Option<ScanDatabaseExecution> {
    SCAN_EXECUTION.try_with(Clone::clone).ok().flatten()
}

pub(crate) async fn execute_request<T, F, Fut>(safety: RequestSafety, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    check_scan_cancellation()?;
    let execution = current_execution();
    match execution {
        Some(execution) => execution.request(safety, operation).await,
        None => operation().await,
    }
}

pub(crate) fn idempotent_insert_client(client: &Client) -> Client {
    client
        .clone()
        .with_option("deduplicate_insert", "enable")
        .with_option("insert_deduplication_token", Uuid::new_v4().simple().to_string())
}

pub(crate) fn check_scan_cancellation() -> Result<()> {
    if SCAN_CANCELLATION
        .try_with(CancellationToken::is_cancelled)
        .unwrap_or(false)
    {
        Err(DatabaseError::ScanCancelled)
    } else {
        Ok(())
    }
}

pub(crate) async fn scan_retry_delay(duration: Duration) -> Result<()> {
    if let Ok(cancel) = SCAN_CANCELLATION.try_with(Clone::clone) {
        select! {
            biased;
            _=cancel.cancelled()=>Err(DatabaseError::ScanCancelled),
            _=sleep(duration)=>Ok(()),
        }
    } else {
        sleep(duration).await;
        Ok(())
    }
}

pub(crate) trait ScanExecute {
    #[track_caller]
    fn execute_scan(self) -> impl Future<Output = Result<()>> + Send;
    async fn execute_idempotent_scan(self) -> Result<()>;
}

pub(crate) fn job_query_prefix(database: &str, job: &str) -> String {
    let job = utils::sanitize_job_id(job);
    let identity = format!("{database}\0{job}");
    format!("terrasync-scan-{}-", &blake3::hash(identity.as_bytes()).to_hex()[..16])
}

impl ScanExecute for Query {
    #[track_caller]
    fn execute_scan(self) -> impl Future<Output = Result<()>> + Send {
        // 记录语句调用位置，既能定位未确认阶段，又不输出 SQL、URL 或认证配置。
        let origin = Location::caller();
        async move {
            check_scan_cancellation()?;
            let result = execute_request(RequestSafety::UnconfirmedWrite, || {
                let query = self.clone();
                async move { query.execute().await.map_err(DatabaseError::from) }
            })
            .await;
            if let Err(error) = &result
                && !error.is_confirmed_scan_cancellation()
            {
                let (error_class, server_code) = error.diagnostic_class();
                warn!(
                    operation_file = origin.file(),
                    operation_line = origin.line(),
                    error_class,
                    server_code,
                    "ClickHouse statement did not complete successfully"
                );
            }
            result
        }
    }

    async fn execute_idempotent_scan(self) -> Result<()> {
        execute_request(RequestSafety::IdempotentWrite, || {
            let query = self.clone();
            async move { query.execute().await.map_err(DatabaseError::from) }
        })
        .await
    }
}

#[derive(Clone)]
pub struct ScanQueryCancellation {
    client: Client,
    comment: String,
    execution: ScanDatabaseExecution,
    cluster: Option<String>,
}

impl ScanQueryCancellation {
    pub(crate) fn new(config: &ClickHouseConfig, cluster: Option<&str>, job: &str) -> Result<Self> {
        Ok(Self {
            client: {
                let mut control = config.clone();
                control.dial_timeout = control.dial_timeout.min(1);
                control.read_timeout = control.read_timeout.min(1);
                base_client(&control)?
            },
            execution: ScanDatabaseExecution::new(config)?,
            comment: format!("{}{}", job_query_prefix(&config.database, job), Uuid::new_v4()),
            cluster: cluster.filter(|value| !value.is_empty()).map(identifier).transpose()?,
        })
    }

    pub(crate) fn data_client(&self, config: &ClickHouseConfig) -> Result<Client> {
        Ok(build_client(config, Some(self.execution.transport()))?.with_option("log_comment", &self.comment))
    }

    /// 先给精确 KILL 留短暂机会，再以同一执行截止时间终止所有本地 HTTP 等待。
    pub fn begin_cancellation(&self) {
        self.execution.begin_stop();
    }

    pub fn execution(&self) -> ScanDatabaseExecution {
        self.execution.clone()
    }

    pub(crate) fn comment(&self) -> &str {
        &self.comment
    }

    /// 终止当前已注册的查询；调用方仍须等待所有已发出的请求返回，空匹配不构成完成证据。
    pub async fn cancel(&self) -> Result<()> {
        let cluster = self
            .cluster
            .as_ref()
            .map_or(String::new(), |name| format!(" ON CLUSTER {name}"));
        let result = self
            .client
            .query(&format!("KILL QUERY{cluster} WHERE Settings['log_comment']=? SYNC"))
            .bind(&self.comment)
            .execute()
            .await;
        match result {
            // 不同版本对于空匹配的行为不同；没有查询需要停止也是成功。
            Err(Error::BadResponse(message)) if message.contains("NOTHING_TO_KILL") => Ok(()),
            result => Ok(result?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse::test::{Mock, handlers};

    #[test]
    fn query_tags_isolate_databases_and_jobs_without_distinguishing_credentials() {
        assert_eq!(job_query_prefix("db", "job"), job_query_prefix("db", "job"));
        assert_ne!(job_query_prefix("db", "job"), job_query_prefix("other_db", "job"));
        assert_ne!(job_query_prefix("db", "job"), job_query_prefix("db", "other_job"));
    }

    #[tokio::test]
    async fn cancellation_is_scoped_to_one_execution_and_all_replicas() -> Result<()> {
        let mock = Mock::new();
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: mock.url().into(),
            database: "fixture".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let first = ScanQueryCancellation::new(&config, Some("clickhouse"), "fixture")?;
        let other = ScanQueryCancellation::new(&config, Some("clickhouse"), "fixture")?;
        assert_ne!(first.comment(), other.comment());
        let captured = mock.add(handlers::record_ddl());
        first.cancel().await?;
        let query = captured.query().await;
        assert!(query.contains("ON CLUSTER `clickhouse`"));
        assert!(query.contains(first.comment()));
        assert!(!query.contains(other.comment()));
        assert!(query.contains("Settings['log_comment']="));
        assert!(query.ends_with("SYNC"));
        Ok(())
    }
}
