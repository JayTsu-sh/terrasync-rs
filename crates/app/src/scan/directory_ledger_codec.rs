//! 目录贡献采用固定顺序的版本化记录；回调仍使用可读的具名字段。
use super::statistics::ScanCounts;
use crate::error::{AppError, Result};
use data_mover::traversal::EntryKindCounts;

fn kinds(value: EntryKindCounts) -> [u64; 8] {
    [
        value.directories,
        value.regular_files,
        value.symlinks,
        value.block_devices,
        value.character_devices,
        value.fifos,
        value.sockets,
        value.unknown,
    ]
}

fn from_kinds(value: [u64; 8]) -> EntryKindCounts {
    let [
        directories,
        regular_files,
        symlinks,
        block_devices,
        character_devices,
        fifos,
        sockets,
        unknown,
    ] = value;
    EntryKindCounts {
        directories,
        regular_files,
        symlinks,
        block_devices,
        character_devices,
        fifos,
        sockets,
        unknown,
    }
}

pub(super) fn encode(value: &ScanCounts) -> Result<String> {
    let kinds = [
        value.scanned,
        value.filtered,
        value.retained,
        value.failed,
        value.vanished,
    ]
    .map(kinds);
    let sizes = [
        value.pruned_directories,
        value.truncated_directories,
        value.regular_file_bytes,
        value.unknown_file_sizes,
        value.versions,
        value.delete_markers,
        value.all_versions_bytes,
        value.completed_listings,
        value.interrupted_listings,
        value.unreadable_listings,
        value.vanished_listings,
    ];
    Ok(serde_json::to_string(&(1u8, kinds, sizes))?)
}

pub(super) fn decode(value: &str) -> Result<ScanCounts> {
    let (version, kind_counts, sizes): (u8, [[u64; 8]; 5], [u64; 11]) = serde_json::from_str(value)?;
    if version != 1 {
        return Err(AppError::ScanError("Unsupported directory contribution format".into()));
    }
    let [scanned, filtered, retained, failed, vanished] = kind_counts.map(from_kinds);
    let [
        pruned_directories,
        truncated_directories,
        regular_file_bytes,
        unknown_file_sizes,
        versions,
        delete_markers,
        all_versions_bytes,
        completed_listings,
        interrupted_listings,
        unreadable_listings,
        vanished_listings,
    ] = sizes;
    let result = ScanCounts {
        scanned,
        filtered,
        retained,
        failed,
        vanished,
        pruned_directories,
        truncated_directories,
        regular_file_bytes,
        unknown_file_sizes,
        versions,
        delete_markers,
        all_versions_bytes,
        completed_listings,
        interrupted_listings,
        unreadable_listings,
        vanished_listings,
    };
    if result.scanned
        != result
            .filtered
            .saturating_add(result.retained)
            .saturating_add(result.failed)
            .saturating_add(result.vanished)
    {
        return Err(AppError::ScanError("Inconsistent directory contribution counts".into()));
    }
    Ok(result)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn compact_contribution_preserves_every_fact_and_rejects_wrong_formats() {
        let mut value = ScanCounts::default();
        value.retained = from_kinds([1, 2, 3, 4, 5, 6, 7, 8]);
        value.filtered = from_kinds([9, 10, 11, 12, 13, 14, 15, 16]);
        value.failed = from_kinds([17, 18, 19, 20, 21, 22, 23, 24]);
        value.vanished = from_kinds([25, 26, 27, 28, 29, 30, 31, 32]);
        value.scanned = value
            .retained
            .saturating_add(value.filtered)
            .saturating_add(value.failed)
            .saturating_add(value.vanished);
        value.regular_file_bytes = u64::MAX;
        value.versions = 200;
        value.delete_markers = 2;
        value.all_versions_bytes = u64::MAX;
        value.pruned_directories = 3;
        value.truncated_directories = 4;
        value.unknown_file_sizes = 5;
        value.completed_listings = 6;
        value.interrupted_listings = 7;
        value.unreadable_listings = 8;
        value.vanished_listings = 9;
        let encoded = encode(&value).unwrap();
        assert_eq!(decode(&encoded).unwrap(), value);
        assert!(decode(&encoded.replacen("[1,", "[2,", 1)).is_err());
        assert!(decode("[1,[],[]]").is_err());
    }
}
