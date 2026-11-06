//! Catalogue record validation and SQL classification invariants.
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::PathBuf;

use task_model::ChangeKind;
use task_model::ObservedEntryExt;

use super::comparison::JoinStrategy;
use super::deletion::classify_deletion_status;
use crate::DeletionStatus;
use crate::schema::records::StorageEntryRecord;
use crate::{DatabaseError, generate_scan_temp_table_name, get_scan_base_table_name, get_scan_state_table_name};

/// 构造 StorageEntryRecord，只需指定关键差异字段
fn make_record(relative_path: &str, file_handle: Option<&str>, ctime: i64) -> StorageEntryRecord {
    {
        let fixture_path: std::path::PathBuf = (relative_path.to_string()).into();
        let fixture_storage: String = "nas".to_string();
        let fixture_version: String = String::new();
        let fixture_handle: Option<String> = file_handle.map(String::from);
        let entry = crate::observed_fixtures::observation(
            if fixture_storage == "s3" {
                data_mover::model::BackendKind::S3
            } else {
                data_mover::model::BackendKind::Nfs
            },
            fixture_path,
            data_mover::model::EntryKind::File,
            1024,
            [ctime, ctime, ctime],
            Some(data_mover::model::OwnershipMode {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            }),
            fixture_handle.map(String::into_bytes),
            None,
            (!fixture_version.is_empty()).then_some((Some(fixture_version), true, false)),
        );
        StorageEntryRecord::capture(&entry, 0).unwrap()
    }
}

#[test]
fn persisted_snapshot_rejects_corruption_backend_and_path_mismatch() {
    let mut corrupted = make_record("file", None, 0);
    corrupted.entry_snapshot[0] ^= 0xff;
    assert!(matches!(
        corrupted.to_observation(),
        Err(DatabaseError::SnapshotDecode(_))
    ));
    let mut wrong_backend = make_record("file", None, 0);
    wrong_backend.backend_id = "another-backend".into();
    assert!(matches!(
        wrong_backend.to_observation(),
        Err(DatabaseError::SnapshotDecode(_))
    ));
    let mut wrong_path = make_record("file", None, 0);
    wrong_path.relative_path = "another-path".into();
    assert!(matches!(
        wrong_path.to_observation(),
        Err(DatabaseError::ObservationProjectionMismatch { .. })
    ));
}

// ═══ 1.1 StorageEntryRecord 转换测试 ═══

#[test]
fn test_nas_entry_roundtrip() {
    let nas = {
        let fixture_path: std::path::PathBuf = PathBuf::from("dir/test.txt");
        crate::observed_fixtures::observation(
            data_mover::model::BackendKind::Nfs,
            fixture_path,
            data_mover::model::EntryKind::File,
            2048,
            [200, 300, 100],
            Some(data_mover::model::OwnershipMode {
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            }),
            (Some(bytes::Bytes::from_static(&[0xDE, 0xAD]))).map(|handle| handle.to_vec()),
            None,
            None,
        )
    };

    let record = StorageEntryRecord::capture(&nas, 1).unwrap();
    assert_eq!(record.storage_type, "nas");
    assert_eq!(
        record.file_handle,
        Some(format!("54534934{}", hex::encode(nas.identity_key().as_bytes())))
    );
    assert_eq!(record.version_id, "");
    assert_eq!(record.current_state, 1);
    assert_eq!(record.size, 2048);

    let back = record.to_observation().unwrap();
    assert_eq!(back.get_name(), "test.txt");
    assert_eq!(back.get_size(), 2048);
    assert_eq!(back.get_mtime(), 300);
}

#[test]
fn test_s3_entry_roundtrip() {
    let s3 = {
        let fixture_path: std::path::PathBuf = ("bucket/obj.json".to_string()).into();
        crate::observed_fixtures::observation(
            data_mover::model::BackendKind::S3,
            fixture_path,
            data_mover::model::EntryKind::File,
            512,
            [0, 400, 0],
            None,
            None,
            None,
            Some((Some("v1".to_string()), true, false)),
        )
    };

    let record = StorageEntryRecord::capture(&s3, 0).unwrap();
    assert_eq!(record.storage_type, "s3");
    assert_eq!(record.version_id, "v1");
    assert_eq!(record.to_observation().unwrap(), s3);

    let back = record.to_observation().unwrap();
    assert_eq!(back.get_version_id(), Some("v1"));
}

#[test]
fn test_nas_entry_optional_fields_none() {
    let nas = {
        let fixture_path: std::path::PathBuf = PathBuf::from("a.txt");
        crate::observed_fixtures::observation(
            data_mover::model::BackendKind::Nfs,
            fixture_path,
            data_mover::model::EntryKind::File,
            0,
            [0, 0, 0],
            None,
            None,
            None,
            None,
        )
    };

    let record = StorageEntryRecord::capture(&nas, 0).unwrap();
    assert_eq!(record.file_handle, None);
    assert_eq!(record.mode, None);

    let back = record.to_observation().unwrap();
    assert!(back.source_identity().stable_bytes().is_none());
    assert_eq!(back.get_mode(), None);
}

#[test]
fn test_current_state_propagation() {
    let nas = {
        let fixture_path: std::path::PathBuf = PathBuf::from("a.txt");
        crate::observed_fixtures::observation(
            data_mover::model::BackendKind::Nfs,
            fixture_path,
            data_mover::model::EntryKind::File,
            100,
            [0, 0, 0],
            Some(data_mover::model::OwnershipMode {
                mode: 0o644,
                uid: 0u32,
                gid: 0u32,
            }),
            None,
            None,
            None,
        )
    };

    let r0 = StorageEntryRecord::capture(&nas, 0).unwrap();
    assert_eq!(r0.current_state, 0);

    let r1 = StorageEntryRecord::capture(&nas, 1).unwrap();
    assert_eq!(r1.current_state, 1);
}

// ═══ 1.2 file_handle 分组逻辑测试（优化 2 核心逻辑）═══

#[test]
fn test_classify_single_fh_is_deleted() {
    let fh_records = vec![make_record("a.txt", Some("abc"), 100)];
    let mut groups = HashMap::new();
    groups.insert(
        make_record("_", Some("abc"), 0).file_handle.unwrap(),
        vec![make_record("a.txt", Some("abc"), 100)],
    );

    let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
    assert_eq!(result.len(), 1);
    assert!(matches!(result[0], DeletionStatus::Deleted(_)));
}

#[test]
fn test_classify_two_fh_is_renamed() {
    let fh_records = vec![make_record("old.txt", Some("abc"), 100)];
    let mut groups = HashMap::new();
    groups.insert(
        make_record("_", Some("abc"), 0).file_handle.unwrap(),
        vec![
            make_record("old.txt", Some("abc"), 100),
            make_record("new.txt", Some("abc"), 200),
        ],
    );

    let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
    assert_eq!(result.len(), 1);
    match &result[0] {
        DeletionStatus::Renamed(from, to) => {
            assert_eq!(from.get_ctime(), 100);
            assert_eq!(to.get_ctime(), 200);
        }
        DeletionStatus::Deleted(_) => panic!("Expected Renamed"),
    }
}

#[test]
fn test_classify_renamed_reverses_ctime_order() {
    let fh_records = vec![make_record("old.txt", Some("abc"), 200)];
    let mut groups = HashMap::new();
    groups.insert(
        make_record("_", Some("abc"), 0).file_handle.unwrap(),
        vec![
            make_record("new.txt", Some("abc"), 200),
            make_record("old.txt", Some("abc"), 100),
        ],
    );

    let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
    assert_eq!(result.len(), 1);
    match &result[0] {
        DeletionStatus::Renamed(from, to) => {
            // ctime 小的是 from，大的是 to，与 groups 中顺序无关
            assert_eq!(from.get_ctime(), 100);
            assert_eq!(to.get_ctime(), 200);
        }
        DeletionStatus::Deleted(_) => panic!("Expected Renamed"),
    }
}

#[test]
fn test_classify_fh_not_in_groups() {
    let fh_records = vec![make_record("x.txt", Some("xyz"), 100)];
    let groups = HashMap::new(); // 空 groups

    let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
    assert_eq!(result.len(), 1);
    assert!(matches!(result[0], DeletionStatus::Deleted(_)));
}

#[test]
fn test_classify_mixed() {
    let no_fh = vec![make_record("a.txt", None, 100), make_record("b.txt", None, 200)];
    let fh_records = vec![
        make_record("c.txt", Some("fh_a"), 100),
        make_record("d.txt", Some("fh_b"), 100),
        make_record("e.txt", Some("fh_c"), 100),
    ];
    let mut groups = HashMap::new();
    // fh_a: 1 条 → Deleted
    groups.insert(
        make_record("_", Some("fh_a"), 0).file_handle.unwrap(),
        vec![make_record("c.txt", Some("fh_a"), 100)],
    );
    // fh_b: 2 条 → Renamed
    groups.insert(
        make_record("_", Some("fh_b"), 0).file_handle.unwrap(),
        vec![
            make_record("d.txt", Some("fh_b"), 100),
            make_record("d_new.txt", Some("fh_b"), 200),
        ],
    );
    // fh_c: 3 条 → 跳过 (error log)
    groups.insert(
        make_record("_", Some("fh_c"), 0).file_handle.unwrap(),
        vec![
            make_record("e1.txt", Some("fh_c"), 100),
            make_record("e2.txt", Some("fh_c"), 200),
            make_record("e3.txt", Some("fh_c"), 300),
        ],
    );

    let result = classify_deletion_status(no_fh, fh_records, &groups).unwrap();
    // 2 Deleted (no_fh) + 1 Deleted (fh_a) + 1 Renamed (fh_b) + 0 (fh_c skipped)
    let deleted_count = result
        .iter()
        .filter(|s| matches!(s, DeletionStatus::Deleted(_)))
        .count();
    let renamed_count = result
        .iter()
        .filter(|s| matches!(s, DeletionStatus::Renamed(_, _)))
        .count();
    assert_eq!(deleted_count, 3);
    assert_eq!(renamed_count, 1);
}

// ═══ 1.4 JoinStrategy 测试（优化 7 核心逻辑）═══

#[test]
fn test_join_strategy_from_file_handle_status() {
    // 两侧都无 fh（local/S3）→ Path
    assert!(matches!(
        JoinStrategy::from_file_handle_status(0, 0),
        JoinStrategy::Path
    ));
    // 任一侧有 fh（NFS/CIFS）→ FileHandle
    assert!(matches!(
        JoinStrategy::from_file_handle_status(10, 5),
        JoinStrategy::FileHandle
    ));
    assert!(matches!(
        JoinStrategy::from_file_handle_status(3, 0),
        JoinStrategy::FileHandle
    ));
    // 回归：两侧都无 fh 时，无论 base/temp 是否为空都必须用 Path。
    // 旧实现要求 base_total>0 才用 Path，导致全量中断后续传（base 空）误入
    // FileHandle，`NULL NOT IN (...)` 漏判 local 新增文件 → 静默不拷贝。
    assert!(matches!(
        JoinStrategy::from_file_handle_status(0, 0),
        JoinStrategy::Path
    ));
}

#[test]
fn test_detect_new_sql_path_mode() {
    let sql = JoinStrategy::Path.build_detect_new_sql("temp_x", "base_y");
    assert!(sql.contains("NOT IN (SELECT relative_path, version_id FROM base_y)"));
    assert!(sql.contains("FROM temp_x"));
    assert!(!sql.contains("FINAL"));
}

#[test]
fn test_detect_new_sql_fh_mode() {
    let sql = JoinStrategy::FileHandle.build_detect_new_sql("temp_x", "base_y");
    assert!(sql.contains("file_handle NOT IN"));
    assert!(sql.contains("FROM base_y WHERE file_handle IS NOT NULL"));
    assert!(!sql.contains("FINAL"));
}

/// 验证 NULL-safe 元数据等价比较的 SQL 形式：每个属性必须有 `IS NULL AND ... IS NULL` 兜底，
/// 既不依赖 sentinel（避免把真实 mode=0 与 NULL 混淆），也覆盖 `ClickHouse` 三值逻辑下的 `NULL=NULL` 漏判。
fn assert_null_safe_meta_clauses(sql: &str) {
    for col in ["mode", "uid", "gid"] {
        let clause = format!("(t.{col} IS NULL AND f.{col} IS NULL)");
        assert!(sql.contains(&clause), "missing NULL-safe clause for `{col}`:\n{sql}");
    }
    // 不应再出现历史 sentinel 形式
    assert!(
        !sql.contains("coalesce(t.mode, 0)"),
        "should no longer use coalesce sentinel:\n{sql}"
    );
}

#[test]
fn test_detect_changed_sql_path_mode_data_only() {
    let sql = JoinStrategy::Path.build_detect_changed_sql("temp_x", "base_y", ChangeKind::DataOnly);
    assert!(sql.contains("LIMIT 1 BY (relative_path, version_id)"));
    // DataOnly 条件：内容变了 + 属性未变
    assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
    assert_null_safe_meta_clauses(&sql);
    assert!(sql.contains("FROM base_y FINAL"));
}

#[test]
fn test_detect_changed_sql_path_mode_metadata_only() {
    let sql = JoinStrategy::Path.build_detect_changed_sql("temp_x", "base_y", ChangeKind::MetadataOnly);
    // MetadataOnly 条件：内容未变 + 属性变了
    assert!(sql.contains("t.size = f.size AND t.mtime = f.mtime"));
    assert_null_safe_meta_clauses(&sql);
    // 必须出现取反形式
    assert!(sql.contains("NOT ifNull(t.mode = f.mode"));
}

#[test]
fn test_detect_changed_sql_path_mode_both() {
    let sql = JoinStrategy::Path.build_detect_changed_sql("temp_x", "base_y", ChangeKind::Both);
    // Both 条件：内容和属性都变了
    assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
    assert_null_safe_meta_clauses(&sql);
    assert!(sql.contains("NOT ifNull(t.mode = f.mode"));
}

#[test]
fn test_detect_changed_sql_fh_mode_data_only() {
    let sql = JoinStrategy::FileHandle.build_detect_changed_sql("temp_x", "base_y", ChangeKind::DataOnly);
    // 路径等值条件：仅对路径未变的条目判 Changed，避免把 rename+changed 误判为纯 Changed
    assert!(sql.contains("LIMIT 1 BY (file_handle, relative_path)"));
    assert!(sql.contains("t.file_handle = f.file_handle"));
    assert!(sql.contains("t.relative_path = f.relative_path"));
    assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
    assert_null_safe_meta_clauses(&sql);
}

#[test]
fn test_detect_changed_sql_fh_mode_metadata_only() {
    let sql = JoinStrategy::FileHandle.build_detect_changed_sql("temp_x", "base_y", ChangeKind::MetadataOnly);
    assert!(sql.contains("t.size = f.size AND t.mtime = f.mtime"));
    assert_null_safe_meta_clauses(&sql);
    assert!(sql.contains("NOT ifNull(t.uid = f.uid"));
}

#[test]
fn test_detect_changed_sql_fh_mode_both() {
    let sql = JoinStrategy::FileHandle.build_detect_changed_sql("temp_x", "base_y", ChangeKind::Both);
    assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
    assert_null_safe_meta_clauses(&sql);
    assert!(sql.contains("NOT ifNull(t.gid = f.gid"));
}

#[test]
fn test_detect_deleted_sql() {
    let sql = JoinStrategy::build_detect_deleted_sql("base_y");
    assert!(sql.contains("FINAL"));
    assert!(sql.contains("current_state = ?"));
    assert!(!sql.contains("LIMIT 1 BY"));
}

#[test]
fn test_batch_fh_query_sql() {
    let sql = JoinStrategy::build_batch_fh_query_sql("base_y", 3);
    assert!(sql.contains("file_handle IN (?, ?, ?)"));
    assert!(sql.contains("LIMIT 1 BY (relative_path, version_id)"));
    assert!(!sql.contains("FINAL"));
}

// ═══ 1.5 表名 & 宏测试 ═══

#[test]
fn test_table_names() {
    assert_eq!(get_scan_base_table_name("job1"), "base_job1");
    assert_eq!(get_scan_state_table_name("job1"), "state_job1");

    let temp1 = generate_scan_temp_table_name();
    let temp2 = generate_scan_temp_table_name();
    assert!(temp1.starts_with("temp_"));
    assert!(temp2.starts_with("temp_"));
    assert_ne!(temp1, temp2);
}
