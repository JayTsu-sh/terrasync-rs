//! 单进程复制的目标写入进度与已发布累计。

use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicU64, Ordering},
};

use data_mover::storage::{WriteProgress, WriteProgressObserver};

use crate::byte_resume::IntervalSet;

#[derive(Debug, Default)]
pub struct CopyBytes {
    published: AtomicU64,
    in_flight: Mutex<u64>,
}

impl CopyBytes {
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    pub(crate) fn restore_published(&self, bytes: u64) {
        self.published.store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_published(&self, bytes: u64) {
        self.published.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> (u64, u64) {
        let in_flight = lock(&self.in_flight);
        (self.published(), *in_flight)
    }

    pub(crate) fn track(self: &Arc<Self>) -> FileWrites {
        FileWrites {
            totals: Arc::clone(self),
            active: Arc::new(Mutex::new(Some(ConfirmedRanges::default()))),
        }
    }
}

#[derive(Default)]
struct ConfirmedRanges {
    ranges: IntervalSet,
    bytes: u64,
}

/// 文件从在途量转入已发布量时使用和报告相同的锁。
pub(crate) struct FileWrites {
    totals: Arc<CopyBytes>,
    active: Arc<Mutex<Option<ConfirmedRanges>>>,
}

impl FileWrites {
    pub(crate) fn observer(&self) -> WriteProgressObserver {
        let active = Arc::clone(&self.active);
        let totals = Arc::clone(&self.totals);
        WriteProgressObserver::new(move |progress| match progress {
            WriteProgress::Confirmed { offset, bytes } => {
                if let Some(current) = lock(&active).as_mut() {
                    let newly_confirmed = current.ranges.insert(offset, offset.saturating_add(bytes));
                    if newly_confirmed != 0 {
                        current.bytes += newly_confirmed;
                        *lock(&totals.in_flight) += newly_confirmed;
                    }
                }
            }
            WriteProgress::Published { newly_written_bytes } => settle(&totals, &active, newly_written_bytes),
            WriteProgress::Checkpoint { .. } => {}
        })
    }
}

fn settle(totals: &CopyBytes, active: &Mutex<Option<ConfirmedRanges>>, published: u64) {
    if let Some(active) = lock(active).take() {
        let mut in_flight = lock(&totals.in_flight);
        *in_flight -= active.bytes;
        totals.add_published(published);
    }
}

impl Drop for FileWrites {
    fn drop(&mut self) {
        settle(&self.totals, &self.active, 0);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use crate::config::JobType;
    use crate::consumer::stats::ProgressBar;
    use crate::report::ProgressDetail;

    #[test]
    fn report_moves_confirmed_ranges_to_published_once() {
        let progress = ProgressBar::new(JobType::Copy);
        let totals = progress.get_real_time_bytes_counter();
        let file = totals.track();
        let observer = file.observer();
        observer.confirmed(40, 10);
        observer.confirmed(35, 10);
        observer.confirmed(40, 10);
        observer.checkpoint(50);
        let report = progress.to_report("job");
        assert_eq!(report.in_flight_bytes, 15);
        assert!(matches!(
            report.detail,
            ProgressDetail::Full {
                transferred_bytes: 0,
                ..
            }
        ));

        observer.published(15);
        observer.published(15);
        observer.confirmed(50, 5);
        let report = progress.to_report("job");
        assert_eq!(report.in_flight_bytes, 0);
        assert!(matches!(
            report.detail,
            ProgressDetail::Full {
                transferred_bytes: 15,
                ..
            }
        ));
        drop(file);

        let cancelled = totals.track();
        cancelled.observer().confirmed(0, 20);
        drop(cancelled);
        let report = progress.to_report("job");
        assert_eq!(report.in_flight_bytes, 0);
        assert!(matches!(
            report.detail,
            ProgressDetail::Full {
                transferred_bytes: 15,
                ..
            }
        ));
    }
}
