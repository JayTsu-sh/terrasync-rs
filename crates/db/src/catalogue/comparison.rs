//! 比对批次的暂存、分类、发布及清理。分类结果重新绑定输入观察，保留源端版本顺序。
use super::{ClickHouseDatabase, JoinStrategy};
use crate::{
    error::{DatabaseError, Result},
    execution::read::QuorumRead,
    get_scan_base_table_name,
    traits::Database,
};
use data_mover::model::ObservedEntry;
use serde::Deserialize;
use std::sync::Arc;
use storage_adapter::{
    ChangeKind, EntryChange,
    observed_scan::{ObservedItem, ObservedView},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonMode {
    PublishObserved,
    AwaitCopyAcknowledgement,
}

/// 只携带分类所需的主键，不重新解码或复制输入观察。
pub struct CatalogueDifference {
    pub relative_path: String,
    pub version_id: String,
    pub kind: EntryChange,
}
pub struct CatalogueChange {
    pub item: ObservedItem,
    pub kind: EntryChange,
}

#[derive(Deserialize, clickhouse::Row)]
struct DifferenceRow {
    relative_path: String,
    version_id: String,
    change_code: u8,
}

impl ClickHouseDatabase {
    pub(super) async fn classify_batch(&self) -> Result<Vec<CatalogueDifference>> {
        let temporary = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("catalogue comparison staging".into()))?;
        let temporary = self.catalogue_query_table(temporary)?;
        let query = if self.is_copy_recovery().await {
            format!("SELECT relative_path,version_id,toUInt8(0) AS change_code FROM {temporary}")
        } else {
            let base = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;
            let (temporary_handles, baseline_handles) = self.check_file_handle_status(&temporary, &base).await?;
            let strategy = JoinStrategy::from_file_handle_status(temporary_handles, baseline_handles);
            let queries = [
                strategy.build_detect_new_sql(&temporary, &base),
                strategy.build_detect_changed_sql(&temporary, &base, ChangeKind::DataOnly),
                strategy.build_detect_changed_sql(&temporary, &base, ChangeKind::MetadataOnly),
                strategy.build_detect_changed_sql(&temporary, &base, ChangeKind::Both),
            ];
            queries
                .into_iter()
                .enumerate()
                .map(|(code, query)| {
                    format!("SELECT relative_path,version_id,toUInt8({code}) AS change_code FROM ({query})")
                })
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        };
        self.sync_client
            .query(&query)
            .fetch_quorum_all::<DifferenceRow>()
            .await?
            .into_iter()
            .map(|row| {
                let kind = match row.change_code {
                    0 => EntryChange::New,
                    1 => EntryChange::Changed(ChangeKind::DataOnly),
                    2 => EntryChange::Changed(ChangeKind::MetadataOnly),
                    3 => EntryChange::Changed(ChangeKind::Both),
                    code => {
                        return Err(DatabaseError::ConversionError(format!(
                            "Unknown catalogue classification {code}"
                        )));
                    }
                };
                Ok(CatalogueDifference {
                    relative_path: row.relative_path,
                    version_id: row.version_id,
                    kind,
                })
            })
            .collect()
    }
}

fn bind_input_order(
    entries: &[Arc<ObservedEntry>], differences: &mut Vec<CatalogueDifference>,
) -> Result<Vec<CatalogueChange>> {
    if differences.is_empty() {
        return Ok(Vec::new());
    }
    differences.sort_unstable_by(|left, right| {
        (left.relative_path.as_str(), left.version_id.as_str())
            .cmp(&(right.relative_path.as_str(), right.version_id.as_str()))
    });
    differences.dedup_by(|left, right| {
        left.relative_path == right.relative_path && left.version_id == right.version_id && left.kind == right.kind
    });
    if differences
        .windows(2)
        .any(|pair| pair[0].relative_path == pair[1].relative_path && pair[0].version_id == pair[1].version_id)
    {
        return Err(DatabaseError::ObservationProjectionMismatch {
            field: "conflicting catalogue classifications",
        });
    }
    let mut consumed = vec![usize::MAX; differences.len()];
    let mut changes: Vec<CatalogueChange> = Vec::with_capacity(differences.len());
    for entry in entries {
        let view = ObservedView {
            entry,
            version_index: entry.history().map(|_| 0),
        };
        let path = view.sql_path();
        for view in ObservedView::all(entry) {
            let version = view.version_id();
            if let Ok(index) = differences.binary_search_by(|row| {
                (row.relative_path.as_str(), row.version_id.as_str()).cmp(&(path.as_ref(), version))
            }) {
                if consumed[index] != usize::MAX {
                    let previous = changes[consumed[index]].item.view();
                    let same = match (previous.version(), view.version()) {
                        (Some(left), Some(right)) => left == right,
                        (None, None) => previous.entry == view.entry,
                        _ => false,
                    };
                    if !same {
                        return Err(DatabaseError::ObservationProjectionMismatch {
                            field: "conflicting source observations",
                        });
                    }
                    continue;
                }
                consumed[index] = changes.len();
                changes.push(CatalogueChange {
                    item: ObservedItem {
                        entry: Arc::clone(entry),
                        version_index: view.version_index,
                    },
                    kind: differences[index].kind,
                });
            }
        }
    }
    if consumed.contains(&usize::MAX) {
        return Err(DatabaseError::ObservationProjectionMismatch {
            field: "catalogue classification key",
        });
    }
    Ok(changes)
}

pub(crate) async fn compare_catalogue_batch(
    mut database: Box<dyn Database>, entries: &[Arc<ObservedEntry>], mode: ComparisonMode,
) -> Result<Vec<CatalogueChange>> {
    database.create_scan_temporary_table().await?;
    let operation = async {
        database.batch_insert_temp_record(entries).await?;
        let mut differences = database.classify_catalogue_items().await?;
        let changes = bind_input_order(entries, &mut differences)?;
        // 复用分类查询拥有的主键；不再次复制观察中的路径／版本字符串。
        let excluded = match mode {
            ComparisonMode::PublishObserved => Vec::new(),
            ComparisonMode::AwaitCopyAcknowledgement => differences
                .into_iter()
                .map(|row| (row.relative_path, row.version_id))
                .collect(),
        };
        database.insert_temp_to_base_table(&excluded).await?;
        Ok(changes)
    }
    .await;
    let cleanup = database.drop_scan_temporary_table().await;
    match (operation, cleanup) {
        (Ok(changes), Ok(())) => Ok(changes),
        (Err(source), Ok(())) | (Ok(_), Err(source)) => Err(source),
        (Err(source), Err(cleanup)) => Err(DatabaseError::CatalogueBatchCleanupFailed {
            source: Box::new(source),
            cleanup: Box::new(cleanup),
        }),
    }
}
