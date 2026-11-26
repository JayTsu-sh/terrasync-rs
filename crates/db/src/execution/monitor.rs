//! 数据库执行的监测及查询取消收尾；业务结果判定由调用方负责。
use std::time::Duration;

use tokio::{
    select,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::warn;

use super::{ConnectivityWatch, ScanQueryCancellation};
use crate::Result;

/// 同一执行只有一个 monitor；finish 必须在所有业务请求确认结束后调用。
/// Drop 收束管理任务，不把空匹配 KILL 当作业务请求已经完成的证据。
pub struct DatabaseExecutionMonitor {
    finished: CancellationToken,
    watcher: AbortOnDropHandle<()>,
    connectivity: ConnectivityWatch,
}

impl DatabaseExecutionMonitor {
    /// 启动本执行唯一的探测和查询取消管理任务。
    ///
    /// # Errors
    /// 同一执行重复启动连通性监测时返回配置错误。
    pub fn start(
        queries: ScanQueryCancellation, cancel: CancellationToken, phase: Option<&'static str>,
    ) -> Result<Self> {
        let mut database = queries.execution();
        if let Some(phase) = phase {
            database = database.with_request_phase(phase);
        }
        let connectivity = database.start_monitor()?;
        let finished = CancellationToken::new();
        let done = finished.clone();
        let terminal = database.clone();
        let watcher = AbortOnDropHandle::new(tokio::spawn(async move {
            select! {
                biased;
                ()=cancel.cancelled()=>queries.begin_cancellation(),
                ()=done.cancelled()=>return,
                failure=terminal.failed()=>{
                    if failure.is_none() { return; }
                    cancel.cancel();
                    queries.begin_cancellation();
                }
            }
            let mut warned = false;
            loop {
                // 不把管理请求的网络等待无限叠加到取消耗时；失败时仍等原请求自然退出。
                let response = timeout(Duration::from_secs(2), queries.cancel()).await;
                if !matches!(response, Ok(Ok(()))) && !warned {
                    warn!("Query cancellation unavailable; waiting for current requests to complete");
                    warned = true;
                }
                if done.is_cancelled() {
                    break;
                }
                select! {
                    ()=done.cancelled()=>break,
                    ()=sleep(Duration::from_millis(100))=>{},
                }
            }
        }));

        Ok(Self {
            finished,
            watcher,
            connectivity,
        })
    }

    /// 业务请求已经收束后，停止并等待本执行的管理任务。
    ///
    /// # Errors
    /// 连通性监测任务异常退出时保留其有类型错误。
    pub async fn finish(self) -> Result<()> {
        self.finished.cancel();
        // 业务请求已结束，仅终止本执行的管理轮询。
        self.watcher.abort();
        let _ = self.watcher.await;
        self.connectivity.finish().await
    }
}
