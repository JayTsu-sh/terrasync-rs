use std::sync::Arc;

use data_mover::{
    storage,
    traversal::{
        ScanFailureSignal, TraversalReconnect, TraversalRecoveryPolicy, TraversalRecoveryState,
        TraversalRecoveryStatus, TraversalRequest, TraversalSession, TraversalSource, recoverable_traversal,
    },
};

use crate::{ConnectedStorage, Result, WalkOptions};

pub(crate) struct TraversalExecution {
    pub session: TraversalSession,
    pub failure: ScanFailureSignal,
    pub recovery: Option<TraversalRecoveryStatus>,
}

impl ConnectedStorage {
    pub(crate) fn start_traversal(
        &self, request: TraversalRequest, options: &WalkOptions,
    ) -> Result<TraversalExecution> {
        let Some((config, recovery_window)) = self.reconnect_config.clone().zip(options.recovery_window) else {
            let (source, failure) = ScanFailureSignal::source(&self.roles, options.operation_timeout)?;
            return Ok(TraversalExecution {
                session: source.traverse(request),
                failure,
                recovery: None,
            });
        };
        let reconnect: TraversalReconnect = Arc::new(move || {
            let config = (*config).clone();
            Box::pin(storage::connect_backend(config))
        });
        let policy = TraversalRecoveryPolicy {
            operation_timeout: options.operation_timeout,
            recovery_window,
        };
        let (session, recovery) = recoverable_traversal(&self.roles, request, policy, reconnect)?;
        log_recovery(recovery.clone());
        Ok(TraversalExecution {
            session,
            failure: recovery.failure_signal(),
            recovery: Some(recovery),
        })
    }
}

fn log_recovery(status: TraversalRecoveryStatus) {
    let mut updates = status.subscribe();
    tokio::spawn(async move {
        while updates.changed().await.is_ok() {
            let state = updates.borrow_and_update().clone();
            match state {
                TraversalRecoveryState::Recovering { first_failure, attempt } => tracing::warn!(
                    backend = ?first_failure.backend, path = %first_failure.path, attempt,
                    "存储连接暂时中断，正在恢复遍历"
                ),
                TraversalRecoveryState::Running { recoveries } if recoveries > 0 => {
                    tracing::info!(recoveries, "存储连接已恢复，继续遍历");
                }
                _ => {}
            }
        }
    });
}

#[cfg(test)]
#[path = "traversal_recovery_tests.rs"]
mod tests;
