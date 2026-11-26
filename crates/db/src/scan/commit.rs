//! 发布和回查使用同一个请求恢复窗口。

use task_model::GenerationCommitResult;
use tracing::info;

use super::{FINALIZING, RECOVERING, ScanRecovery, ScanResumeState, ScanStore, Store};
use crate::execution::commit::commit_with_readback;
use crate::{DatabaseError, Result};

impl ScanStore {
    /// 先更新 manifest 阻止迟到请求，再等已读取旧 manifest 的请求退出后复核。
    pub async fn recover_scan_generation(
        &self, state: &mut ScanResumeState, recovery: ScanRecovery, outcome: &mut GenerationCommitResult,
    ) -> Result<()> {
        if !state.active() {
            return Err(DatabaseError::TransactionError("Scan round is not active".into()));
        }
        if state.phase == FINALIZING {
            let mut recovering = state.clone();
            recovering.phase = RECOVERING;
            recovering.advance()?;
            self.save_resume_state(&recovering).await?;
            *state = recovering;
        }
        if state.phase == RECOVERING {
            self.cuts.await_prior_operations().await?;
        }
        let committed = match &self.inner {
            Store::Single(store) => store.restore_resume_generation(state).await?,
            Store::Cluster(store) => store.restore_resume_generation(state).await?,
        };
        if committed {
            *outcome = GenerationCommitResult::Committed;
            return Ok(());
        }
        *outcome = GenerationCommitResult::NotCommitted;
        let snapshot = match recovery {
            ScanRecovery::Resume => &state.checkpoint_snapshot,
            ScanRecovery::Restart => &state.baseline_snapshot,
        };
        self.cuts.restore(snapshot).await
    }

    /// 即使调用方随后观察到取消，结果仍保留已经取得的提交证据。
    pub async fn commit_scan_generation(
        &self, state: &ScanResumeState, outcome: &mut GenerationCommitResult,
    ) -> Result<()> {
        state.validate_envelope()?;
        if state.phase != FINALIZING {
            return Err(DatabaseError::TransactionError(
                "Cannot publish a scan generation before finalization".into(),
            ));
        }
        let query = match &self.inner {
            Store::Single(store) => store.persisted_scan_commit_query(state).await?,
            Store::Cluster(store) => store.persisted_scan_commit_query(state),
        };
        commit_with_readback(
            query,
            async || {
                let committed = self.matches_commit(state).await?;
                if committed {
                    info!(
                        scan_epoch = state.event_time,
                        "Confirmed scan generation after losing the commit response"
                    );
                }
                Ok(committed)
            },
            outcome,
        )
        .await
    }

    async fn matches_commit(&self, state: &ScanResumeState) -> Result<bool> {
        match &self.inner {
            Store::Single(store) => {
                let (generation, epoch) = store.persisted_scan_state().await?;
                state.matches_commit(generation, epoch)
            }
            Store::Cluster(store) => store.persisted_scan_matches_commit(state).await,
        }
    }
}
