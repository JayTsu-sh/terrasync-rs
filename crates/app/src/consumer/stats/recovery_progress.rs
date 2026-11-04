//! watch 只通知当前状态；累计数字直接读取恢复生产方，结束前再采样一次。
use super::{callback_state::ScanRuntimePhase, consumer::StatisticConsumer, diagnostics::safe_diagnostic_text};
use data_mover::traversal::{TraversalRecoveryCounters, TraversalRecoveryState, TraversalRecoveryStatus};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::task::AbortOnDropHandle;

type Consumer = Arc<Mutex<StatisticConsumer>>;

pub(crate) struct RecoveryProgress {
    consumer: Consumer,
    status: TraversalRecoveryStatus,
    baseline: TraversalRecoveryCounters,
    task: AbortOnDropHandle<()>,
}

impl RecoveryProgress {
    pub(crate) async fn start(consumer: Consumer, status: TraversalRecoveryStatus) -> Self {
        let baseline = {
            let consumer = consumer.lock().await;
            let value = &consumer.callback.runtime;
            TraversalRecoveryCounters {
                network_faults: value.network_faults,
                reconnect_attempts: value.recovery_attempts,
                successful_recoveries: value.successful_recoveries,
            }
        };
        let actor = consumer.clone();
        let watched = status.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut updates = watched.subscribe();
            loop {
                let state = updates.borrow_and_update().clone();
                sample(&actor, &watched, baseline, state).await;
                if updates.changed().await.is_err() {
                    break;
                }
            }
        }));
        Self {
            consumer,
            status,
            baseline,
            task,
        }
    }

    pub(crate) async fn finish(self) {
        let Self {
            consumer,
            status,
            baseline,
            task,
        } = self;
        task.abort();
        let _ = task.await;
        let state = status.subscribe().borrow().clone();
        sample(&consumer, &status, baseline, state).await;
    }
}

async fn sample(
    consumer: &Consumer, status: &TraversalRecoveryStatus, baseline: TraversalRecoveryCounters,
    state: TraversalRecoveryState,
) {
    let counters = status.counters();
    let mut consumer = consumer.lock().await;
    apply(&mut consumer, baseline, counters, state);
}

fn apply(
    consumer: &mut StatisticConsumer, baseline: TraversalRecoveryCounters, counters: TraversalRecoveryCounters,
    state: TraversalRecoveryState,
) {
    let callback = &mut consumer.callback;
    let faults = baseline.network_faults.saturating_add(counters.network_faults);
    let new_fault = callback.runtime.network_faults < faults;
    callback.runtime.network_faults = faults;
    callback.runtime.recovery_attempts = baseline.reconnect_attempts.saturating_add(counters.reconnect_attempts);
    callback.runtime.successful_recoveries = baseline
        .successful_recoveries
        .saturating_add(counters.successful_recoveries);
    match state {
        TraversalRecoveryState::Running { .. } => {
            if new_fault {
                callback.outbox.enqueue(
                    "storage",
                    "network_recovered",
                    "Network recovery completed; cumulative counts are available in scan_runtime",
                    None,
                );
            }
            callback.runtime.state = ScanRuntimePhase::Running;
            callback.runtime.current_error = None;
        }
        TraversalRecoveryState::Recovering {
            first_failure: failure, ..
        } => {
            if new_fault {
                callback.outbox.enqueue(
                    "storage",
                    "network_fault",
                    &failure.to_string(),
                    Some(failure.path.as_str()),
                );
            }
            callback.runtime.state = ScanRuntimePhase::Recovering;
            callback.runtime.current_error = Some(safe_diagnostic_text(&failure.to_string()));
        }
        TraversalRecoveryState::Failed { failure } => {
            if new_fault {
                callback.outbox.enqueue(
                    "storage",
                    "network_fault",
                    &failure.to_string(),
                    Some(failure.path.as_str()),
                );
            }
            callback.runtime.state = ScanRuntimePhase::Failed;
            callback.runtime.current_error = Some(safe_diagnostic_text(&failure.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::JobType,
        consumer::stats::{FullStats, ProgressBar, StatsKind},
    };

    #[test]
    fn coalesced_notifications_do_not_lose_or_double_add_cumulative_recovery_facts() {
        let mut consumer = StatisticConsumer {
            stats: StatsKind::Full(FullStats::new(
                JobType::Scan,
                "recovery".into(),
                String::new(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::Scan),
            job_dir: String::new(),
            callback_url: None,
            generation_committed: None,
            scan_snapshot: None,
            callback: Default::default(),
            pb_handle: None,
        };
        let baseline = TraversalRecoveryCounters {
            network_faults: 5,
            reconnect_attempts: 10,
            successful_recoveries: 4,
        };
        let current = TraversalRecoveryCounters {
            network_faults: 1,
            reconnect_attempts: 3,
            successful_recoveries: 1,
        };
        for _ in 0..2 {
            apply(
                &mut consumer,
                baseline,
                current,
                TraversalRecoveryState::Running { recoveries: 1 },
            );
            let runtime = &consumer.callback.runtime;
            assert_eq!(
                (
                    runtime.network_faults,
                    runtime.recovery_attempts,
                    runtime.successful_recoveries
                ),
                (6, 13, 5)
            );
            assert_eq!(runtime.state, ScanRuntimePhase::Running);
        }
    }
}
