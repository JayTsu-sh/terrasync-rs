//! 三条所有权路径在发布 finished 后唤醒 reporter；Drop 的异步回收保持不变。
#![allow(clippy::unwrap_used)]

use std::thread;

use super::StatisticConsumer;
use crate::config::JobType;
use crate::consumer::stats::{FullStats, ProgressBar, StatsKind, progress_bar::wakeup_tests::parked_main_reporter};

fn consumer() -> StatisticConsumer {
    StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Copy,
            "wakeup".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Copy),
        job_dir: String::new(),
        callback_url: None,
        generation_commit_result: None,
        migration: None,
        scan_snapshot: None,
        callback: Default::default(),
        pb_handle: None,
    }
}

#[test]
fn progress_wakeup_finalize_publishes_finish_then_wakes_and_joins_once() {
    let mut consumer = consumer();
    let mut reporter = parked_main_reporter(&consumer.progress_bar);
    consumer.pb_handle = reporter.handle.take();
    let owner = thread::spawn(move || {
        consumer.finalize();
        assert!(consumer.pb_handle.is_none());
        consumer.finalize();
        drop(consumer);
    });
    let woke = reporter.stopped_without_rescue();
    owner.join().unwrap();
    assert!(woke, "finalize must unpark the finished reporter before joining");
}

#[test]
fn progress_wakeup_consumer_drop_wakes_before_asynchronous_reaping() {
    let mut consumer = consumer();
    let mut reporter = parked_main_reporter(&consumer.progress_bar);
    consumer.pb_handle = reporter.handle.take();
    drop(consumer);
    assert!(
        reporter.stopped_without_rescue(),
        "Drop must wake the finished reporter before reaping"
    );
}
