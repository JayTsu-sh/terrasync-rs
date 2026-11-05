#![allow(clippy::unwrap_used)]
use super::*;
use crate::http_transport::tests::Blackhole;
use clickhouse::error::Error as ClickHouseError;
use std::{
    future::pending,
    io::{Error as IoError, ErrorKind},
    sync::atomic::AtomicUsize,
};
use tokio::{sync::Barrier, task::yield_now, time::advance};

fn disconnected() -> DatabaseError {
    DatabaseError::from(ClickHouseError::Network(
        IoError::new(ErrorKind::ConnectionReset, "link reset").into(),
    ))
}

#[tokio::test(start_paused = true)]
async fn transient_reads_recover_and_release_the_shared_write_gate() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let attempts = AtomicUsize::new(0);
    let value = execution
        .request(RequestSafety::ReadOnly, || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move { if attempt < 2 { Err(disconnected()) } else { Ok(7) } }
        })
        .await
        .unwrap();
    assert_eq!(value, 7);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(!execution.inner.recovering.load(Ordering::Acquire));
    assert!(execution.failure().is_none());
    execution
        .request(RequestSafety::UnconfirmedWrite, || async { Ok(()) })
        .await
        .unwrap();
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn nested_steps_retry_as_one_logical_operation_without_deadlocking() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let attempts = AtomicUsize::new(0);
    execution
        .request(RequestSafety::IsolatedRestart, || async {
            execution
                .request(RequestSafety::UnconfirmedWrite, || async {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(disconnected())
                    } else {
                        Ok(())
                    }
                })
                .await
        })
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(!execution.inner.recovering.load(Ordering::Acquire));
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_nested_failure_starts_the_budget_before_its_sibling_finishes() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let running = execution.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        running
            .request(RequestSafety::IsolatedRestart, || async {
                let (failed, drained) = tokio::join!(
                    running.request(RequestSafety::UnconfirmedWrite, || async {
                        Err::<(), _>(disconnected())
                    }),
                    async {
                        running.failed().await;
                        true
                    },
                );
                assert!(drained);
                failed
            })
            .await
    }));
    while !execution.inner.recovering.load(Ordering::Acquire) {
        yield_now().await;
    }
    assert!(!task.is_finished());
    execution.recovered(0).await;
    advance(RECOVERY_WINDOW).await;
    execution.expire_if_due().await;
    assert!(execution.failure().is_some());
    assert!(task.await.unwrap().is_err());
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn permission_failure_does_not_enter_the_network_recovery_window() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let started = Instant::now();
    let attempts = AtomicUsize::new(0);
    let result = execution
        .request(RequestSafety::ReadOnly, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), _>(DatabaseError::from(ClickHouseError::BadResponse(
                    "Code: 497. ACCESS_DENIED".into(),
                )))
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(!execution.inner.recovering.load(Ordering::Acquire));
    server.stop().await;
}

#[tokio::test]
async fn a_short_probe_outage_recovers_without_stopping_the_execution() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let watch = execution.start_monitor().unwrap();
    timeout(Duration::from_secs(4), async {
        while !execution.inner.recovering.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    server.responsive.store(true, Ordering::SeqCst);
    timeout(Duration::from_secs(3), async {
        while execution.inner.recovering.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(execution.failure().is_none());
    watch.finish().await.unwrap();
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn concurrent_failures_share_the_first_deadline() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let both_started = Arc::new(Barrier::new(2));
    let started = Instant::now();
    let mut tasks = Vec::new();
    for delay in [Duration::ZERO, Duration::from_secs(1)] {
        let execution = execution.clone();
        let both_started = both_started.clone();
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            let mut first = true;
            execution
                .request(RequestSafety::ReadOnly, || {
                    let first_attempt = std::mem::replace(&mut first, false);
                    let both_started = both_started.clone();
                    async move {
                        if first_attempt {
                            both_started.wait().await;
                            sleep(delay).await;
                        }
                        Err::<(), _>(disconnected())
                    }
                })
                .await
        })));
    }
    for task in tasks {
        assert!(task.await.unwrap().is_err());
    }
    assert!(started.elapsed() >= RECOVERY_WINDOW);
    assert!(started.elapsed() <= RECOVERY_WINDOW + RETRY_DELAY);
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_successful_probe_cannot_clear_an_unconfirmed_write() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = attempts.clone();
    let running = execution.clone();
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        running
            .request(RequestSafety::UnconfirmedWrite, || {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(disconnected()) }
            })
            .await
    }));
    while !execution.inner.recovering.load(Ordering::Acquire) {
        yield_now().await;
    }
    execution.recovered(0).await;
    assert!(execution.inner.recovering.load(Ordering::Acquire));
    advance(Duration::from_millis(2900)).await;
    assert!(!task.is_finished());
    advance(Duration::from_millis(100)).await;
    assert!(matches!(
        timeout(Duration::from_millis(50), &mut task).await.unwrap().unwrap(),
        Err(DatabaseError::ScanRequestFailed { .. })
    ));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "unconfirmed writes must not be blindly replayed"
    );
    server.stop().await;
}

#[tokio::test]
async fn silent_frontend_failure_is_detected_without_any_business_batch() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let watch = execution.start_monitor().unwrap();
    let detected = timeout(Duration::from_secs(4), async {
        loop {
            let changed = execution.inner.changed.notified();
            if let Some(started) = execution.inner.recovery.lock().await.started {
                break started;
            }
            changed.await;
        }
    })
    .await
    .unwrap();
    let failure = timeout(Duration::from_secs(4), execution.failed()).await.unwrap();
    assert!(failure.is_some());
    assert!(detected.elapsed() >= Duration::from_millis(2900));
    assert!(detected.elapsed() < Duration::from_millis(3400));
    watch.finish().await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn frequent_business_acks_avoid_additional_probe_requests() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let watch = execution.start_monitor().unwrap();
    for _ in 0..5 {
        execution.acknowledged();
        sleep(Duration::from_millis(450)).await;
    }
    assert_eq!(server.requests.load(Ordering::SeqCst), 0);
    watch.finish().await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn monitor_exit_failure_is_not_silently_detached() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    let guard = MonitorExit {
        execution: execution.clone(),
        done: CancellationToken::new(),
    };
    drop(guard);
    assert!(matches!(
        execution.failure().as_deref(),
        Some(DatabaseError::ConnectivityMonitorStopped)
    ));
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_releases_new_requests_waiting_at_the_recovery_gate() {
    let server = Blackhole::start(false).await;
    let execution = ScanDatabaseExecution::new(&server.config).unwrap();
    execution.failed_operation(99, Arc::new(disconnected())).await;
    let running = execution.clone();
    let mut request = AbortOnDropHandle::new(tokio::spawn(async move {
        running
            .request(RequestSafety::ReadOnly, || pending::<Result<()>>())
            .await
    }));
    yield_now().await;
    execution.begin_stop();
    assert!(matches!(
        timeout(Duration::from_millis(50), &mut request).await.unwrap().unwrap(),
        Err(DatabaseError::ScanCancelled)
    ));
    server.stop().await;
}
