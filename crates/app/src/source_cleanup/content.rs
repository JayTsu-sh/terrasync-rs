//! 数据校验复用原生完整性比较；所需元数据已按复制计划独立核验。
use crate::error::storage::operation;
use data_mover::integrity::{self, IntegrityMode, IntegrityOptions, IntegrityPlan, IntegrityRequest};
use data_mover::model::{HistoryOrder, ObjectHistory, ObservedEntry, RecordKind, SourceVersion, VersionRecord};
use data_mover::storage::SourceDescriptor;

use super::SourceCleanupCheck;
use crate::error::{Result, StorageError};
use crate::integrity_check::{IntegritySide, MismatchDataField};
use crate::storage_factory::StorageContext;

pub(super) async fn verify(
    source: &StorageContext, destination: &StorageContext, expected: &ObservedEntry, target: &SourceDescriptor,
    check: &SourceCleanupCheck, historical_target: bool,
) -> Result<()> {
    // 绑定刚通过属性校验和持久化确认的目标身份；版本化目标的描述会固定实际版本。
    let mut target_observation = ObservedEntry::new(
        target.path.clone(),
        target.kind,
        target.size,
        target.inline_timestamps.and_then(|times| times.modified),
        target.source_identity.clone(),
    )
    .map_err(StorageError::from)?;
    if historical_target && let SourceVersion::Id(id) = &target.version {
        let etag = target
            .content_version
            .as_ref()
            .ok_or_else(|| operation("pinned target lacks content-change evidence"))?;
        let etag = String::from_utf8(etag.to_vec())
            .map_err(|_| operation("pinned target content evidence is not representable"))?;
        // 单记录容器只表达精确读取选择，不声称该目标版本仍为当前项；null 也固定 Id。
        let record = VersionRecord::new(
            (id != "null").then(|| id.clone()),
            false,
            RecordKind::Object {
                size: target.size.unwrap_or(0),
                etag,
                modified: target.inline_timestamps.and_then(|times| times.modified),
            },
            target.source_identity.clone(),
            None,
        )
        .map_err(StorageError::from)?;
        target_observation = target_observation
            .with_history(Some(Box::new(
                ObjectHistory::whole(vec![record], HistoryOrder::Server).map_err(StorageError::from)?,
            )))
            .map_err(StorageError::from)?;
    }
    let comparison = integrity::compare(IntegrityRequest {
        source: source.storage().clone(),
        source_path: expected.clone(),
        destination: destination.storage().clone(),
        destination_path: target_observation,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: check.cancel.clone(),
    });
    let report = tokio::select! {
        biased;
        () = check.cancel.cancelled() => return Err(StorageError::Cancelled.into()),
        result = comparison => result.map_err(|failure| StorageError::SourceCleanupContentUnverified {
            path: expected.path().clone(),
            side: failure.side,
            source: failure.failure,
        })?,
    };
    // 原生比较报告实际源读取量，不能把双方同时短读且摘要相同当作完整校验。
    if expected.size() != Some(report.compared_bytes()) {
        return Err(StorageError::MismatchData(vec![MismatchDataField::ReadLength {
            side: IntegritySide::Source,
            expected: expected.size().unwrap_or(0),
            actual: report.compared_bytes(),
        }])
        .into());
    }
    if report.digests().is_none_or(|(source, target)| source != target) {
        return Err(StorageError::MismatchData(vec![MismatchDataField::Content { offset: 0 }]).into());
    }
    Ok(())
}
