//! Blocks 以完整目录前缀提交；Unordered 仅保留活跃目录，恢复时从根重新开始。
use super::statistics::ScanCounts;
use crate::{
    consumer::ScanStatistics,
    error::{AppError, Result},
};
use data_mover::traversal::{Checkpoint, DirectoryStatistics, ListingOutcome, TraversalOrder, TraversalResume};
use std::collections::BTreeMap;

pub(crate) struct ScanProgress {
    order: TraversalOrder,
    active: BTreeMap<String, (ScanCounts, u64)>,
    complete_prefix: ScanCounts,
    confirmed: ScanCounts,
    cursor: Option<Vec<u8>>,
}

impl ScanProgress {
    pub(crate) fn new(order: TraversalOrder, initial: ScanCounts, cursor: Option<Vec<u8>>) -> Self {
        Self {
            order,
            active: BTreeMap::new(),
            complete_prefix: initial.clone(),
            confirmed: initial,
            cursor,
        }
    }
    pub(crate) async fn push(&mut self, value: DirectoryStatistics, statistics: &ScanStatistics) -> Result<()> {
        let key = value.path.as_str().to_owned();
        if self.order == TraversalOrder::Blocks && self.active.keys().any(|path| path != &key) {
            return Err(AppError::ScanError(
                "Blocks traversal interleaved two directory blocks".into(),
            ));
        }
        let previous = self.active.remove(&key);
        if value.pass_start != previous.is_none()
            || previous
                .as_ref()
                .is_some_and(|(_, revision)| value.revision <= *revision)
        {
            return Err(AppError::ScanError(
                "Directory statistics revision is out of order".into(),
            ));
        }
        let old = previous.map_or_else(ScanCounts::default, |(counts, _)| counts);
        let new = ScanCounts::from_directory(&value);
        statistics.replace_directory_contributions(&[(old, new.clone())]).await;
        match value.outcome {
            Some(ListingOutcome::Complete) if value.failed.total() == 0 => {
                self.complete_prefix.replace(&ScanCounts::default(), &new);
            }
            None => {
                self.active.insert(key, (new, value.revision));
            }
            _ => {
                return Err(AppError::ScanError(
                    "Incomplete directory cannot advance the scan checkpoint".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn checkpoint_counts(&self) -> Result<&ScanCounts> {
        if self.order != TraversalOrder::Blocks {
            return Err(AppError::ScanError(
                "Unordered scan cannot publish a resumable checkpoint".into(),
            ));
        }
        if !self.active.is_empty() {
            return Err(AppError::ScanError(
                "Checkpoint arrived inside a directory block".into(),
            ));
        }
        Ok(&self.complete_prefix)
    }
    pub(crate) fn confirm(&mut self, cursor: Vec<u8>) {
        self.cursor = Some(cursor);
        self.confirmed = self.complete_prefix.clone();
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if !self.active.is_empty() {
            return Err(AppError::ScanError(
                "Traversal ended with an unfinished directory block".into(),
            ));
        }
        Ok(())
    }
    pub(crate) async fn resumed(&mut self, event: &TraversalResume, statistics: &ScanStatistics) -> Result<()> {
        let selected = event.checkpoint.as_ref().map(Checkpoint::as_bytes);
        let counts = if self.order == TraversalOrder::Blocks {
            if selected != self.cursor.as_deref() {
                return Err(AppError::ScanError(
                    "Recovery checkpoint differs from confirmed scan statistics".into(),
                ));
            }
            self.confirmed.clone()
        } else {
            if selected.is_some() {
                return Err(AppError::ScanError(
                    "Unordered recovery must restart from the root".into(),
                ));
            }
            ScanCounts::default()
        };
        self.active.clear();
        self.complete_prefix = counts.clone();
        self.confirmed = counts.clone();
        statistics.restore_scan_counts(counts).await;
        Ok(())
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
