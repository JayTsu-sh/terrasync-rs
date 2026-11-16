//! 私有时钟与等待边界验证，不用紧毫秒阈值推断唤醒是否生效。
#![allow(clippy::unwrap_used)]

use std::{
    cell::Cell,
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle, Thread},
    time::{Duration, Instant},
};

use super::{DirectoryMetadataProgressBar, FINISH_POLL_MS, ProgressBar, sleep_interruptible_with};

const DEADLOCK_GUARD: Duration = Duration::from_secs(5);

pub(crate) struct ParkedReporter {
    pub(crate) handle: Option<JoinHandle<()>>,
    thread: Thread,
    stopped: Receiver<()>,
}

impl ParkedReporter {
    /// 超时只作死锁保险；救援属于此 fixture 的线程，避免红测试遗留阻塞 join。
    pub(crate) fn stopped_without_rescue(&self) -> bool {
        let stopped = self.stopped.recv_timeout(DEADLOCK_GUARD).is_ok();
        self.thread.unpark();
        stopped
    }
}

fn parked_reporter(is_finished: impl Fn() -> bool + Send + 'static) -> ParkedReporter {
    let (ready, waiting) = mpsc::sync_channel(0);
    let (done, stopped) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready.send(()).unwrap();
        loop {
            thread::park();
            if is_finished() {
                break;
            }
        }
        done.send(()).unwrap();
    });
    waiting.recv_timeout(DEADLOCK_GUARD).unwrap();
    let thread = handle.thread().clone();
    ParkedReporter {
        handle: Some(handle),
        thread,
        stopped,
    }
}

pub(crate) fn parked_main_reporter(bar: &ProgressBar) -> ParkedReporter {
    let bar = bar.pb.clone();
    parked_reporter(move || bar.is_finished())
}

pub(crate) fn parked_directory_reporter(bar: &DirectoryMetadataProgressBar) -> ParkedReporter {
    let bar = bar.pb.clone();
    parked_reporter(move || bar.is_finished())
}

#[test]
fn progress_wakeup_already_finished_never_waits() {
    sleep_interruptible_with(20, || true, Instant::now, |_| panic!("finished display must not wait"));
}

#[test]
fn progress_wakeup_early_and_repeated_wakes_keep_both_refresh_deadlines() {
    for checks in [20, 5] {
        let start = Instant::now();
        let now = Cell::new(start);
        let calls = Cell::new(0);
        sleep_interruptible_with(
            checks,
            || false,
            || now.get(),
            |duration| {
                assert!(duration <= Duration::from_millis(FINISH_POLL_MS));
                calls.set(calls.get() + 1);
                // 前三次模拟重复 permit/伪唤醒；随后每次只过去 1ms。
                if calls.get() > 3 {
                    now.set(now.get() + duration.min(Duration::from_millis(1)));
                }
                assert!(calls.get() < 3000, "wait must terminate at its fixed deadline");
            },
        );
        assert_eq!(now.get() - start, Duration::from_millis(checks * FINISH_POLL_MS));
    }
}

#[test]
fn progress_wakeup_finish_between_check_and_park_uses_an_existing_permit() {
    let finished = Cell::new(false);
    let start = Instant::now();
    let calls = Cell::new(0);
    sleep_interruptible_with(
        20,
        || finished.get(),
        || start,
        |_| {
            calls.set(calls.get() + 1);
            finished.set(true);
            thread::current().unpark();
            thread::park_timeout(DEADLOCK_GUARD);
        },
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn progress_wakeup_finish_after_parking_and_poll_without_unpark_are_observed() {
    let start = Instant::now();
    let now = Cell::new(start);
    let finished = Cell::new(false);
    let calls = Cell::new(0);
    sleep_interruptible_with(
        20,
        || finished.get(),
        || now.get(),
        |duration| {
            calls.set(calls.get() + 1);
            assert_eq!(duration, Duration::from_millis(FINISH_POLL_MS));
            now.set(now.get() + duration);
            finished.set(true);
        },
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(now.get() - start, Duration::from_millis(FINISH_POLL_MS));
}

#[test]
fn progress_wakeup_a_long_pause_does_not_restart_the_refresh_interval() {
    let start = Instant::now();
    let now = Cell::new(start);
    let calls = Cell::new(0);
    sleep_interruptible_with(
        20,
        || false,
        || now.get(),
        |_| {
            calls.set(calls.get() + 1);
            now.set(start + Duration::from_secs(3));
        },
    );
    assert_eq!(calls.get(), 1);
}
