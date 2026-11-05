//! `ClickHouse` 目录行及事件行的编码；查询和任务入口不暴露 SQL 行 DTO。
use crate::error::{DatabaseError, Result};
use crate::scan::rows::{capture_summary, incremental_record};
use crate::schema::entry::EntrySummary;
use data_mover::model::{BackendIdentity, BackendKind};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, time::SystemTime};
use storage_adapter::observed_scan::ObservedView;
use storage_adapter::{ObservedEntry, ObservedEntryExt, StorageEntryMessage};

/// 文件扫描记录结构体
/// 统一的数据结构，用于存储文件系统扫描结果
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub(crate) struct StorageEntryRecord {
    /// 文件或目录的名称
    pub name: String,
    /// 相对路径(主键)
    pub relative_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// 文件扩展名（可选）
    pub ext: Option<String>,
    /// 创建时间（时间戳）
    pub ctime: i64,
    /// 修改时间（时间戳）
    pub mtime: i64,
    /// 访问时间（时间戳）
    pub atime: i64,
    /// Unix权限模式（可选，如0o755）
    pub mode: Option<u32>,
    /// 存储类型 ("nas" 或 "s3")
    pub storage_type: String,
    /// 是否为符号链接
    pub is_symlink: bool,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否为普通文件
    pub is_regular_file: bool,
    /// 硬链接数量
    pub hard_links: u32,
    /// 当前扫描状态
    pub current_state: u8,
    /// 用户ID
    pub uid: Option<u32>,
    /// 组ID
    pub gid: Option<u32>,
    /// 索引节点号
    pub ino: Option<u64>,
    /// 文件句柄（NFS file handle / CIFS file ID 等）
    pub file_handle: Option<String>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: String,
    /// 文件标签
    pub tags: Option<String>,
    pub marker_state: Option<String>,
    pub backend_kind: String,
    pub backend_id: String,
    pub entry_snapshot: Vec<u8>,
}

impl StorageEntryRecord {
    #[cfg(test)]
    pub fn capture(entry: &ObservedEntry, generation: u8) -> Result<Self> {
        let fields = capture_summary(
            ObservedView {
                entry,
                version_index: entry.history().map(|_| 0),
            },
            generation,
        )?;
        Ok(Self {
            name: fields.name,
            relative_path: fields.relative_path,
            size: fields.size,
            ext: fields.ext,
            ctime: fields.ctime,
            mtime: fields.mtime,
            atime: fields.atime,
            mode: fields.mode,
            storage_type: fields.storage_type,
            is_symlink: fields.is_symlink,
            is_dir: fields.is_dir,
            is_regular_file: fields.is_regular_file,
            hard_links: fields.hard_links,
            current_state: fields.current_state,
            uid: fields.uid,
            gid: fields.gid,
            ino: fields.ino,
            file_handle: fields.file_handle,
            version_id: fields.version_id,
            tags: fields.tags,
            marker_state: fields.marker_state,
            backend_kind: entry.backend_kind().as_str().into(),
            backend_id: entry.source_identity().backend().stable_id().into(),
            entry_snapshot: entry.try_encode_snapshot()?.as_bytes().to_vec(),
        })
    }
    /// 目录持久化读取只接受完整原生 snapshot，不从查询列拼造观察。
    pub fn to_observation(&self) -> Result<ObservedEntry> {
        let backend = BackendIdentity::new(self.backend_kind.parse()?, self.backend_id.clone())?;
        decode_observation(&self.relative_path, &self.entry_snapshot, &backend)
    }
}

/// 流式观察回读只取 codec 和路径校验需要的字段。
#[derive(Deserialize, clickhouse::Row)]
pub(crate) struct SnapshotRow {
    relative_path: String,
    backend_kind: String,
    backend_id: String,
    entry_snapshot: Vec<u8>,
}

impl SnapshotRow {
    pub(crate) fn into_observation(self) -> Result<ObservedEntry> {
        let backend = BackendIdentity::new(self.backend_kind.parse()?, self.backend_id)?;
        decode_observation(&self.relative_path, &self.entry_snapshot, &backend)
    }
}

fn decode_observation(relative_path: &str, snapshot: &[u8], backend: &BackendIdentity) -> Result<ObservedEntry> {
    let entry = ObservedEntry::decode_snapshot(snapshot, backend)?;
    if entry.get_relative_path() != Path::new(relative_path)
        && entry
            .history_source_path()?
            .as_ref()
            .is_none_or(|path| path.as_str() != relative_path)
    {
        return Err(DatabaseError::ObservationProjectionMismatch { field: "relative_path" });
    }
    Ok(entry)
}

/// 文件增量扫描记录结构体
/// 对应 `FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION` 中的列定义
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub(crate) struct IncrementalStorageEntryRecord {
    /// 操作类型：new, changed, deleted, rename
    pub operation_type: String,
    /// 文件或目录的名称
    pub name: String,
    /// 相对路径
    pub relative_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// 文件扩展名（可选）
    pub ext: Option<String>,
    /// 创建时间（纳秒时间戳）
    pub ctime: i64,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,
    /// 访问时间（纳秒时间戳）
    pub atime: i64,
    /// Unix权限模式（可选，如0o755）
    pub mode: Option<u32>,
    /// 存储类型 ("nas" 或 "s3")
    pub storage_type: String,
    /// 是否为符号链接
    pub is_symlink: bool,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否为普通文件
    pub is_regular_file: bool,
    /// 硬链接数量
    pub hard_links: u32,
    /// 当前扫描状态
    pub current_state: u8,
    /// 用户ID
    pub uid: Option<u32>,
    /// 组ID
    pub gid: Option<u32>,
    /// 索引节点号
    pub ino: Option<u64>,
    /// 文件句柄（hex 字符串格式）
    pub file_handle: Option<String>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: String,
    /// 文件标签
    pub tags: Option<String>,
    pub marker_state: Option<String>,
    /// 记录创建时间
    pub create_at: i64,
    /// 备注信息（如重命名时的源路径）
    pub comment: Option<String>,
}

impl IncrementalStorageEntryRecord {
    pub fn from_message(message: &StorageEntryMessage) -> Result<Vec<Self>> {
        let now = i64::try_from(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos())?;
        let (entry, operation, comment) = match message {
            StorageEntryMessage::Scanned(entry) => (entry, "scanned", None),
            StorageEntryMessage::Packaged(entry) => (entry, "packaged", None),
            StorageEntryMessage::New(item) => {
                let row = capture_summary(item.view(), 0)?;
                return Ok(vec![incremental_record(row, "new", None, now)]);
            }
            StorageEntryMessage::Changed { entry, kind } => {
                let row = capture_summary(entry.view(), 0)?;
                return Ok(vec![incremental_record(row, kind.as_state_str(), None, now)]);
            }
            StorageEntryMessage::Deleted(entry) => (entry, "deleted", None),
            StorageEntryMessage::IntegrityChecked(entry) => (entry, "integrity_checked", None),
            StorageEntryMessage::Renamed((from, to)) => (to, "rename", Some(from.path().as_str().to_owned())),
            StorageEntryMessage::Error {
                event,
                path,
                entry,
                reason,
            } => {
                let comment = Some(format!("[{event}] {reason}"));
                if let Some(entry) = entry {
                    return Self::observed_rows(entry, "error", comment, now);
                }
                let row = EntrySummary {
                    name: path
                        .file_name()
                        .map_or_else(String::new, |value| value.to_string_lossy().into_owned()),
                    relative_path: path.to_string_lossy().into_owned(),
                    ..Default::default()
                };
                return Ok(vec![incremental_record(row, "error", comment, now)]);
            }
            StorageEntryMessage::TarManifest { .. } => {
                return Err(DatabaseError::ConversionError(
                    "tar manifest must use the manifest writer".into(),
                ));
            }
        };
        Self::observed_rows(entry, operation, comment, now)
    }

    fn observed_rows(entry: &ObservedEntry, operation: &str, comment: Option<String>, now: i64) -> Result<Vec<Self>> {
        ObservedView::all(entry)
            .map(|view| {
                let row = capture_summary(view, 0)?;
                Ok(incremental_record(row, operation, comment.clone(), now))
            })
            .collect()
    }
}

/// 仅写入的 manifest 行借用共享观察，只有 tags 的 SQL JSON 需要编码。
#[derive(Serialize, clickhouse::Row)]
pub(crate) struct TarManifestRecord<'a> {
    tar_path: &'a str,
    entry_path: &'a str,
    size: u64,
    ext: Option<&'a str>,
    mtime: i64,
    mode: Option<u32>,
    storage_type: &'static str,
    is_dir: bool,
    is_symlink: bool,
    uid: Option<u32>,
    gid: Option<u32>,
    version_id: &'a str,
    tags: Option<String>,
}

impl<'a> TarManifestRecord<'a> {
    pub(crate) fn from_entry(entry: &'a ObservedEntry, tar_path: &'a str) -> Self {
        let storage_type = match entry.backend_kind() {
            BackendKind::S3 => "s3",
            BackendKind::Hdfs => "hdfs",
            _ => "nas",
        };
        let tags = entry.get_tags().and_then(|tags| {
            serde_json::to_string(
                &tags
                    .iter()
                    .map(|tag| (tag.key(), tag.value()))
                    .collect::<BTreeMap<_, _>>(),
            )
            .ok()
        });
        Self {
            tar_path,
            entry_path: entry.path().as_str(),
            size: entry.get_size(),
            ext: entry.get_extension(),
            mtime: entry.get_mtime(),
            mode: entry.get_mode(),
            storage_type,
            is_dir: entry.get_is_dir(),
            is_symlink: entry.get_is_symlink(),
            uid: entry.get_uid(),
            gid: entry.get_gid(),
            version_id: entry.get_version_id().unwrap_or_default(),
            tags,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::observed_fixtures::observation;
    use data_mover::model::{EntryKind, HistoryOrder, ObjectHistory};
    use serde_json::json;
    use std::sync::Arc;
    use storage_adapter::observed_scan::ObservedItem;

    #[test]
    fn narrow_snapshot_projection_keeps_the_complete_native_observation() -> Result<()> {
        let entry = crate::observed_fixtures::directory_marker("folder", vec![("tag".into(), "kept".into())]);
        let record = StorageEntryRecord::capture(&entry, 1)?;
        let snapshot = SnapshotRow {
            relative_path: record.relative_path,
            backend_kind: record.backend_kind,
            backend_id: record.backend_id,
            entry_snapshot: record.entry_snapshot,
        };
        assert_eq!(snapshot.into_observation()?, entry);
        Ok(())
    }

    #[test]
    fn each_selected_completion_encodes_exactly_one_incremental_version() -> Result<()> {
        let mut observations: Vec<_> = [("old", 7, false), ("null", 11, false), ("latest", 19, true)]
            .into_iter()
            .map(|(id, size, latest)| {
                observation(
                    BackendKind::S3,
                    "object".into(),
                    EntryKind::File,
                    size,
                    [0, 1, 0],
                    None,
                    None,
                    None,
                    Some((if id == "null" { None } else { Some(id.into()) }, latest, false)),
                )
            })
            .collect();
        let versions = observations
            .iter()
            .flat_map(|entry| entry.history().unwrap().records().to_vec())
            .collect();
        let entry = Arc::new(
            observations
                .pop()
                .unwrap()
                .with_history(Some(Box::new(ObjectHistory::whole(versions, HistoryOrder::Server)?)))?,
        );
        for (index, (id, size)) in [("old", 7), ("null", 11), ("latest", 19)].into_iter().enumerate() {
            let message = StorageEntryMessage::New(ObservedItem {
                entry: Arc::clone(&entry),
                version_index: Some(index),
            });
            let rows = IncrementalStorageEntryRecord::from_message(&message)?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].version_id, id);
            assert_eq!(rows[0].size, size);
        }
        Ok(())
    }

    #[test]
    fn borrowed_tar_manifest_preserves_the_persisted_fields() -> Result<()> {
        let entry = observation(
            BackendKind::S3,
            "nested/目标.dat".into(),
            EntryKind::File,
            37,
            [11, 12, 13],
            None,
            None,
            Some(vec![("b".into(), "2".into()), ("a".into(), "1".into())]),
            Some((Some("v1".into()), true, false)),
        );
        let tar_path = String::from("bundle.tar");
        let row = TarManifestRecord::from_entry(&entry, &tar_path);
        assert_eq!(row.tar_path.as_ptr(), tar_path.as_ptr());
        assert_eq!(row.entry_path.as_ptr(), entry.path().as_str().as_ptr());
        assert_eq!(
            serde_json::to_value(row)?,
            json!({
                "tar_path":"bundle.tar", "entry_path":"nested/目标.dat", "size":37,
                "ext":"dat", "mtime":12, "mode":null, "storage_type":"s3",
                "is_dir":false, "is_symlink":false, "uid":null, "gid":null,
                "version_id":"v1", "tags":"{\"a\":\"1\",\"b\":\"2\"}",
            })
        );
        Ok(())
    }
}
