use crate::connected::path;
use crate::error::operation;
use crate::projection::nanos;
use crate::{EntryEnum, Result, StorageEnum, StorageError};
use data_mover::integrity::MtimePrecision as NativeMtimePrecision;
use data_mover::integrity::{
    self, IntegrityDifference, IntegrityMode, IntegrityOptions, IntegrityPlan, IntegrityRequest,
};
use data_mover::model::EntryKind;
use data_mover::storage::PrincipalIdentity;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
/// 结构化完整性差异中的条目类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityEntryKind {
    File,
    Directory,
    Symlink,
}

/// 完整性比较中返回非预期字节数的一侧。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegritySide {
    Source,
    Destination,
}

/// 独立完整性检查报告的结构化内容差异。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchDataField {
    EntryKind {
        src: IntegrityEntryKind,
        dest: IntegrityEntryKind,
    },
    Size {
        src: u64,
        dest: u64,
    },
    ReadLength {
        side: IntegritySide,
        expected: u64,
        actual: u64,
    },
    StreamOffset {
        side: IntegritySide,
        expected: u64,
        actual: u64,
    },
    Content {
        offset: u64,
    },
}

/// 完整性检查报告的结构化 POSIX 元数据差异。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchMetaField {
    Mtime { src: i64, dest: i64 },
    Uid { src: u32, dest: u32 },
    Gid { src: u32, dest: u32 },
    Mode { src: u32, dest: u32 },
    HdfsOwner { src: String, dest: String },
    HdfsGroup { src: String, dest: String },
}

/// [`IntegrityCheck`] 执行的数据验证范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityCheckMode {
    /// 比较条目类型、大小及支持的元数据，不读取文件内容。
    Quick,
    /// 执行全部快速检查，再按对齐的字节流比较文件内容。
    Full,
}

/// 比较前对修改时间戳的规范化方式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MtimePrecision {
    /// 比较原始纳秒时间戳。
    #[default]
    Exact,
    /// 推断每个时间戳的精度，并按较粗精度比较。
    Auto,
}

/// 控制独立完整性检查的选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrityCheckOptions {
    pub mode: IntegrityCheckMode,
    pub mtime_precision: MtimePrecision,
    pub mtime_tolerance: Duration,
}

impl IntegrityCheckOptions {
    #[must_use]
    pub const fn new(mode: IntegrityCheckMode) -> Self {
        Self {
            mode,
            mtime_precision: MtimePrecision::Exact,
            mtime_tolerance: Duration::ZERO,
        }
    }

    #[must_use]
    pub const fn with_mtime_precision(mut self, precision: MtimePrecision) -> Self {
        self.mtime_precision = precision;
        self
    }

    #[must_use]
    pub const fn with_mtime_tolerance(mut self, tolerance: Duration) -> Self {
        self.mtime_tolerance = tolerance;
        self
    }
}

/// 无状态的独立完整性检查器。
pub struct IntegrityCheck;

impl IntegrityCheck {
    pub async fn check_with_source_entry_and_options(
        src: &StorageEnum, dest: &StorageEnum, entry: &EntryEnum, options: IntegrityCheckOptions,
        cancel: Option<&CancellationToken>,
    ) -> Result<()> {
        let p = path(entry.get_relative_path())?;
        let report = integrity::compare(IntegrityRequest {
            source: src.storage().clone(),
            source_path: p.clone(),
            destination: dest.storage().clone(),
            destination_path: p,
            plan: IntegrityPlan::default(),
            options: IntegrityOptions {
                mode: match options.mode {
                    IntegrityCheckMode::Quick => IntegrityMode::Metadata,
                    IntegrityCheckMode::Full => IntegrityMode::Content,
                },
                mtime_precision: match options.mtime_precision {
                    MtimePrecision::Exact => NativeMtimePrecision::Exact,
                    MtimePrecision::Auto => NativeMtimePrecision::Coarsest,
                },
                mtime_tolerance: options.mtime_tolerance,
            },
            cancel: cancel.cloned().unwrap_or_default(),
        })
        .await
        .map_err(|failure| StorageError::from(failure.failure))?;
        let mut data = Vec::new();
        let mut metadata = Vec::new();
        for difference in report.differences() {
            match difference {
                IntegrityDifference::Missing { .. } => {
                    return Err(StorageError::FileNotFound(
                        entry.get_relative_path().display().to_string(),
                    ));
                }
                // 目录分配大小取决于文件系统，不代表内容长度。
                IntegrityDifference::Size { .. } if entry.get_is_dir() => {}
                IntegrityDifference::Size { source, destination } => data.push(MismatchDataField::Size {
                    src: source.unwrap_or(0),
                    dest: destination.unwrap_or(0),
                }),
                IntegrityDifference::Content { .. } => data.push(MismatchDataField::Content { offset: 0 }),
                IntegrityDifference::Kind { source, destination } => data.push(MismatchDataField::EntryKind {
                    src: kind(*source),
                    dest: kind(*destination),
                }),
                IntegrityDifference::Modified { source, destination } => metadata.push(MismatchMetaField::Mtime {
                    src: nanos(*source),
                    dest: nanos(*destination),
                }),
                IntegrityDifference::Permissions { source, destination } => {
                    if let (Some(src), Some(dest)) = (source.mode, destination.mode) {
                        if src != dest {
                            metadata.push(MismatchMetaField::Mode { src, dest });
                        }
                    }
                    if let (Some(PrincipalIdentity::Numeric(src)), Some(PrincipalIdentity::Numeric(dest))) =
                        (&source.owner, &destination.owner)
                    {
                        if src != dest {
                            metadata.push(MismatchMetaField::Uid { src: *src, dest: *dest });
                        }
                    }
                    if let (Some(PrincipalIdentity::Numeric(src)), Some(PrincipalIdentity::Numeric(dest))) =
                        (&source.group, &destination.group)
                    {
                        if src != dest {
                            metadata.push(MismatchMetaField::Gid { src: *src, dest: *dest });
                        }
                    }
                }
                _ => {
                    return Err(operation(format!("unsupported integrity difference: {difference:?}")));
                }
            }
        }
        if !data.is_empty() {
            return Err(StorageError::MismatchData(data));
        }
        if !metadata.is_empty() {
            return Err(StorageError::MismatchMeta(metadata));
        }
        Ok(())
    }
}
fn kind(value: EntryKind) -> IntegrityEntryKind {
    match value {
        EntryKind::Directory => IntegrityEntryKind::Directory,
        EntryKind::Symlink => IntegrityEntryKind::Symlink,
        _ => IntegrityEntryKind::File,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CreateStorageOptions, create_storage};
    use std::path::Path;

    #[tokio::test]
    #[ignore = "requires LAB_CIFS_TEST_URL pointing at an isolated writable root"]
    async fn cifs_directory_allocation_size_is_not_a_content_mismatch()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let source_root = tempfile::tempdir()?;
        std::fs::create_dir(source_root.path().join("nested"))?;
        let source = create_storage(
            source_root.path().to_str().ok_or("source path is not UTF-8")?,
            CreateStorageOptions::new(None, false),
        )
        .await?;
        let destination = create_storage(
            &std::env::var("LAB_CIFS_TEST_URL")?,
            CreateStorageOptions::new(None, true),
        )
        .await?;
        let entry = source.get_metadata(Path::new("nested")).await?;
        destination.create_dir_all(&entry).await?;
        destination.set_entry_metadata(&entry).await?;
        IntegrityCheck::check_with_source_entry_and_options(
            &source,
            &destination,
            &entry,
            IntegrityCheckOptions::new(IntegrityCheckMode::Full).with_mtime_precision(MtimePrecision::Auto),
            None,
        )
        .await?;
        destination.delete_file(&entry).await?;
        Ok(())
    }
}
