use crate::{EntryEnum, Result, StorageEnum, StorageError};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
/// Entry type used in structured integrity-check mismatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityEntryKind {
    File,
    Directory,
    Symlink,
}

/// Side of an integrity comparison that returned an unexpected byte count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegritySide {
    Source,
    Destination,
}

/// Structured content mismatches reported by an independent integrity check.
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

/// Structured POSIX metadata mismatches reported by an integrity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchMetaField {
    Mtime { src: i64, dest: i64 },
    Uid { src: u32, dest: u32 },
    Gid { src: u32, dest: u32 },
    Mode { src: u32, dest: u32 },
    HdfsOwner { src: String, dest: String },
    HdfsGroup { src: String, dest: String },
}

/// Amount of data validation performed by [`IntegrityCheck`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityCheckMode {
    /// Compare entry type, size, and supported metadata without reading file data.
    Quick,
    /// Perform all quick checks and compare file data as aligned byte streams.
    Full,
}

/// How modification timestamps are normalized before comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MtimePrecision {
    /// Compare the original nanosecond timestamps.
    #[default]
    Exact,
    /// Infer each timestamp's apparent precision and compare at the coarser one.
    Auto,
}

/// Options controlling an independent integrity check.
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

/// Stateless independent integrity checker.
pub struct IntegrityCheck;

impl IntegrityCheck {
    pub async fn check_with_source_entry_and_options(
        src: &StorageEnum, dest: &StorageEnum, entry: &EntryEnum, options: IntegrityCheckOptions,
        cancel: Option<&CancellationToken>,
    ) -> Result<()> {
        use data_mover::integrity::{
            self, IntegrityDifference, IntegrityMode, IntegrityOptions, IntegrityPlan, IntegrityRequest,
        };
        let p = crate::connected::path(entry.get_relative_path())?;
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
                    MtimePrecision::Exact => integrity::MtimePrecision::Exact,
                    MtimePrecision::Auto => integrity::MtimePrecision::Coarsest,
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
                // Directory allocation sizes are filesystem-specific, not payload lengths.
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
                    src: crate::projection::nanos(*source),
                    dest: crate::projection::nanos(*destination),
                }),
                IntegrityDifference::Permissions { source, destination } => {
                    if let (Some(src), Some(dest)) = (source.mode, destination.mode) {
                        if src != dest {
                            metadata.push(MismatchMetaField::Mode { src, dest });
                        }
                    }
                    use data_mover::storage::PrincipalIdentity;
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
                    return Err(crate::error::operation(format!(
                        "unsupported integrity difference: {difference:?}"
                    )));
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
fn kind(value: data_mover::model::EntryKind) -> IntegrityEntryKind {
    match value {
        data_mover::model::EntryKind::Directory => IntegrityEntryKind::Directory,
        data_mover::model::EntryKind::Symlink => IntegrityEntryKind::Symlink,
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
