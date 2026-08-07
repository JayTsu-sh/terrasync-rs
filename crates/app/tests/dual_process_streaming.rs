#![allow(clippy::unwrap_used, clippy::expect_used)]
// 用 in-process transport 断言 Sender 流式发送序列 + 源 hash 正确。
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use app::disk_commit::disk_commit_task;
use app::receiver::ReceiverProgress;
use bytes::Bytes;
use data_mover::{DataChunk, EntryEnum, NASEntry, StorageEnum, create_storage};
use transport::message::{DcAck, DiskCommitMsg, FileOutcome, ReceiverMsg, SessionConfig};

// ============================================================
// disk_commit_task 测试辅助
// ============================================================

// 在 dir 上建一个 Local StorageEnum（不写任何数据文件）。
async fn local_storage(dir: &std::path::Path) -> Arc<StorageEnum> {
    let storage = create_storage(&dir.to_string_lossy(), None, false).await.unwrap();
    Arc::new(storage)
}

// 构造一个 NAS EntryEnum（测试用最小字段集，relative_path=name）。
fn make_nas_entry(name: &str, is_dir: bool, is_symlink: bool, size: u64, mode: u32) -> Arc<EntryEnum> {
    Arc::new(EntryEnum::NAS(NASEntry {
        name: name.rsplit('/').next().unwrap_or(name).to_string(),
        relative_path: PathBuf::from(name),
        extension: None,
        is_dir,
        size,
        atime: 0,
        ctime: 0,
        mtime: 0,
        mode,
        is_symlink,
        hard_links: Some(1),
        uid: None,
        gid: None,
        ino: None,
        file_handle: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    }))
}

// 构造大小为 size 的确定性字节序列 + 对应的文件 EntryEnum（is_dir=false，
// relative_path=name，size 匹配）。字节按 offset 变化，可验证乱序/错位写入。
// 不落盘——最终文件由 disk_commit_task 写入。
fn entry_for(_root: &std::path::Path, name: &str, size: usize) -> (Arc<EntryEnum>, Vec<u8>) {
    let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    (make_nas_entry(name, false, false, size as u64, 0o644), bytes)
}

// 构造一个目录 EntryEnum（is_dir=true）。
fn dir_entry(name: &str) -> Arc<EntryEnum> {
    make_nas_entry(name, true, false, 0, 0o755)
}

// 构造 SessionConfig，仅关心 enable_integrity_check，其余取默认。
fn session_cfg(integrity: bool) -> SessionConfig {
    SessionConfig {
        src_path: String::new(),
        qos: None,
        peak_qos_rate: 1.0,
        iops: None,
        enable_integrity_check: integrity,
        enable_acl: false,
        is_source_reserved: true,
        block_size: None,
        delete_target: false,
        delta_size_threshold: None,
    }
}

// 把 bytes 按 chunk_size 切成 (offset, Bytes) 列表（保序）。
fn chunkify(bytes: &[u8], chunk_size: usize) -> Vec<(u64, Bytes)> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        let end = (off + chunk_size).min(bytes.len());
        out.push((off as u64, Bytes::copy_from_slice(&bytes[off..end])));
        off = end;
    }
    out
}

// 起 disk_commit_task，喂消息，收集 ack DcAck（目录/符号链接透传 ReceiverMsg；
// 文件传输结果为 FileOutcome，redo 决策已上移到 Receiver 主 task，dc task 只上报 outcome）。
async fn run_dc(dest: Arc<StorageEnum>, session: SessionConfig, msgs: Vec<DiskCommitMsg>) -> Vec<DcAck> {
    run_dc_inner(dest, session, msgs, true).await.0
}

async fn run_dc_inner(
    dest: Arc<StorageEnum>, session: SessionConfig, msgs: Vec<DiskCommitMsg>, explicit_shutdown: bool,
) -> (Vec<DcAck>, Arc<ReceiverProgress>) {
    let (dc_tx, dc_rx) = tokio::sync::mpsc::channel(16);
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress = Arc::new(ReceiverProgress::new());
    let jh = tokio::spawn(disk_commit_task(dest, session, dc_rx, ack_tx, progress.clone()));
    for m in msgs {
        dc_tx.send(m).await.unwrap();
    }
    if explicit_shutdown {
        dc_tx.send(DiskCommitMsg::Shutdown).await.unwrap();
    }
    drop(dc_tx);
    jh.await.unwrap().unwrap();
    let mut acks = vec![];
    while let Ok(a) = ack_rx.try_recv() {
        acks.push(a);
    }
    (acks, progress)
}

#[tokio::test]
async fn dc_writes_full_file_and_acks_success() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "sub/f.bin", 5 * 1024 * 1024);
    let session = session_cfg(true);
    let src_hash = blake3::hash(&bytes).to_hex().to_string();

    let mut msgs = vec![
        DiskCommitMsg::CreateDir {
            entry: dir_entry("sub"),
        },
        DiskCommitMsg::FileBegin {
            ndx: 0,
            entry: entry.clone(),
        },
    ];
    for (off, c) in chunkify(&bytes, 1 << 20) {
        msgs.push(DiskCommitMsg::FileChunk {
            entry: entry.clone(),
            chunk: DataChunk { offset: off, data: c },
        });
    }
    msgs.push(DiskCommitMsg::FileCommit {
        ndx: 0,
        entry: entry.clone(),
        source_hash: Some(src_hash),
    });

    let acks = run_dc(dest, session, msgs).await;
    // CreateDir：无 redo 语义，直接透传 EntrySuccess
    assert!(
        acks.iter()
            .any(|a| matches!(a, DcAck::Entry(ReceiverMsg::EntrySuccess { .. })))
    );
    // FileCommit 校验通过：上报 FileOutcome::Success（不再是 dc task 自行发的 EntrySuccess）
    assert!(acks.iter().any(|a| matches!(
        a,
        DcAck::FileOutcome {
            ndx: 0,
            outcome: FileOutcome::Success
        }
    )));
    // 最终文件内容正确
    assert_eq!(std::fs::read(tmp.path().join("sub/f.bin")).unwrap(), bytes);
    // .part 已 rename 掉
    assert!(!tmp.path().join("sub/f.bin.terrasync-part").exists());
    // 元数据：set_entry_metadata 已应用（mode 低位）
    let meta = std::fs::metadata(tmp.path().join("sub/f.bin")).unwrap();
    assert_eq!(meta.permissions().mode() & 0o777, 0o644);
}

#[tokio::test]
async fn dc_hash_mismatch_rejects_and_cleans_part() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "f.bin", 2 * 1024 * 1024);
    let session = session_cfg(true);
    let mut msgs = vec![DiskCommitMsg::FileBegin {
        ndx: 0,
        entry: entry.clone(),
    }];
    for (off, c) in chunkify(&bytes, 1 << 20) {
        msgs.push(DiskCommitMsg::FileChunk {
            entry: entry.clone(),
            chunk: DataChunk { offset: off, data: c },
        });
    }
    // 注入错误 hash（64 位 hex，长度对但值错）
    msgs.push(DiskCommitMsg::FileCommit {
        ndx: 0,
        entry: entry.clone(),
        source_hash: Some("deadbeef".repeat(8)),
    });

    let acks = run_dc(dest, session, msgs).await;
    // dc task 不再自行决定 redo/error，只如实上报 HashMismatch；由 Receiver 主 task 决策
    assert!(acks.iter().any(|a| matches!(
        a,
        DcAck::FileOutcome {
            ndx: 0,
            outcome: FileOutcome::HashMismatch
        }
    )));
    // 不产生最终文件
    assert!(!tmp.path().join("f.bin").exists());
    // .part 已删除
    assert!(!tmp.path().join("f.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_size_mismatch_without_integrity_rejects_and_cleans_part() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let entry = make_nas_entry("truncated.bin", false, false, 4096, 0o644);
    let acks = run_dc(
        dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 3,
                entry: entry.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry: entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"short"),
                },
            },
            DiskCommitMsg::FileCommit {
                ndx: 3,
                entry,
                source_hash: None,
            },
        ],
    )
    .await;

    assert!(acks.iter().any(|ack| matches!(
        ack,
        DcAck::FileOutcome {
            ndx: 3,
            outcome: FileOutcome::SizeMismatch
        }
    )));
    assert!(!tmp.path().join("truncated.bin").exists());
    assert!(!tmp.path().join("truncated.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_zero_byte_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, _b) = entry_for(tmp.path(), "empty.bin", 0);
    // 完整性校验开启，但 0 字节文件源端读取返回 None hasher（data-mover 约定：
    // size==0 → 无 hash），真实 Sender 对空文件发 source_hash=None，故此处跟随契约传 None。
    let session = session_cfg(true);
    let acks = run_dc(
        dest,
        session,
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 0,
                entry: entry.clone(),
            },
            DiskCommitMsg::FileCommit {
                ndx: 0,
                entry: entry.clone(),
                source_hash: None,
            },
        ],
    )
    .await;
    assert!(acks.iter().any(|a| matches!(
        a,
        DcAck::FileOutcome {
            ndx: 0,
            outcome: FileOutcome::Success
        }
    )));
    assert_eq!(std::fs::metadata(tmp.path().join("empty.bin")).unwrap().len(), 0);
}

#[tokio::test]
async fn dc_creates_symlink() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let link = make_nas_entry("link.txt", false, true, 0, 0o777);
    let acks = run_dc(
        dest,
        session_cfg(false),
        vec![DiskCommitMsg::CreateSymlink {
            entry: link,
            target: PathBuf::from("target.txt"),
        }],
    )
    .await;
    assert!(
        acks.iter()
            .any(|a| matches!(a, DcAck::Entry(ReceiverMsg::EntrySuccess { .. })))
    );
    let lm = std::fs::symlink_metadata(tmp.path().join("link.txt")).unwrap();
    assert!(lm.file_type().is_symlink());
    assert_eq!(
        std::fs::read_link(tmp.path().join("link.txt")).unwrap(),
        PathBuf::from("target.txt")
    );
}

// 钉死回归：裸 FileCommit（无前置 FileBegin）不产生任何 ack，也不 panic。
// 防止 no-active 分支重新发出 EntryError（会与 FileBegin 失败路径重复 ack）。
#[tokio::test]
async fn dc_bare_commit_no_active_produces_no_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, _b) = entry_for(tmp.path(), "ghost.bin", 1024);
    let acks = run_dc(
        dest,
        session_cfg(true),
        vec![DiskCommitMsg::FileCommit {
            ndx: 0,
            entry,
            source_hash: Some("deadbeef".repeat(8)),
        }],
    )
    .await;
    assert!(
        acks.is_empty(),
        "无 active 的 FileCommit 不应产生任何 ack，实际: {acks:?}"
    );
}

// AbortFile 路径：FileBegin + 若干 FileChunk 后收到 AbortFile，应丢弃 .part、
// 不产生最终文件、不发任何 ack（Sender 已发 EntryError，避免重复信号）。
#[tokio::test]
async fn dc_abort_file_drops_part_no_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "aborted.bin", 2 * 1024 * 1024);
    let mut msgs = vec![DiskCommitMsg::FileBegin {
        ndx: 0,
        entry: entry.clone(),
    }];
    for (off, c) in chunkify(&bytes, 1 << 20) {
        msgs.push(DiskCommitMsg::FileChunk {
            entry: entry.clone(),
            chunk: DataChunk { offset: off, data: c },
        });
    }
    msgs.push(DiskCommitMsg::AbortFile);

    let acks = run_dc(dest, session_cfg(true), msgs).await;
    assert!(acks.is_empty(), "AbortFile 不应产生任何 ack，实际: {acks:?}");
    assert!(!tmp.path().join("aborted.bin").exists());
    assert!(!tmp.path().join("aborted.bin.terrasync-part").exists());
}

// Shutdown 与显式 abort 一样拥有 active writer：disk-commit task 返回前必须
// settle writer 并删除暂存数据。
#[tokio::test]
async fn dc_shutdown_active_file_cleans_part_no_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "shutdown.bin", 2 * 1024 * 1024);
    let mut msgs = vec![DiskCommitMsg::FileBegin {
        ndx: 0,
        entry: entry.clone(),
    }];
    for (off, data) in chunkify(&bytes, 1 << 20) {
        msgs.push(DiskCommitMsg::FileChunk {
            entry: entry.clone(),
            chunk: DataChunk { offset: off, data },
        });
    }

    let acks = run_dc(dest, session_cfg(true), msgs).await;
    assert!(acks.is_empty(), "shutdown must not invent a file outcome: {acks:?}");
    assert!(!tmp.path().join("shutdown.bin").exists());
    assert!(!tmp.path().join("shutdown.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_channel_close_settles_and_cleans_active_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "channel-close.bin", 1024);
    let (acks, _progress) = run_dc_inner(
        dest,
        session_cfg(true),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 0,
                entry: entry.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry,
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from(bytes),
                },
            },
        ],
        false,
    )
    .await;

    assert!(acks.is_empty());
    assert!(!tmp.path().join("channel-close.bin").exists());
    assert!(!tmp.path().join("channel-close.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_progress_counts_only_successful_commit() {
    let success_tmp = tempfile::tempdir().unwrap();
    let success_dest = local_storage(success_tmp.path()).await;
    let success_entry = make_nas_entry("success.bin", false, false, 4, 0o644);
    let (_acks, success_progress) = run_dc_inner(
        success_dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 1,
                entry: success_entry.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry: success_entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"done"),
                },
            },
            DiskCommitMsg::FileCommit {
                ndx: 1,
                entry: success_entry,
                source_hash: None,
            },
        ],
        true,
    )
    .await;
    assert_eq!(success_progress.files_transferred.load(Ordering::Relaxed), 1);
    assert_eq!(success_progress.bytes_transferred.load(Ordering::Relaxed), 4);

    let failed_tmp = tempfile::tempdir().unwrap();
    let failed_dest = local_storage(failed_tmp.path()).await;
    let failed_entry = make_nas_entry("failed.bin", false, false, 8, 0o644);
    let (_acks, failed_progress) = run_dc_inner(
        failed_dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 2,
                entry: failed_entry.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry: failed_entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"short"),
                },
            },
            DiskCommitMsg::FileCommit {
                ndx: 2,
                entry: failed_entry,
                source_hash: None,
            },
        ],
        true,
    )
    .await;
    assert_eq!(failed_progress.files_transferred.load(Ordering::Relaxed), 0);
    assert_eq!(failed_progress.bytes_transferred.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn dc_second_begin_does_not_replace_active_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (first, first_bytes) = entry_for(tmp.path(), "first.bin", 1024);
    let (second, _second_bytes) = entry_for(tmp.path(), "second.bin", 512);
    let source_hash = blake3::hash(&first_bytes).to_hex().to_string();

    let acks = run_dc(
        dest,
        session_cfg(true),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 1,
                entry: first.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry: first.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from(first_bytes.clone()),
                },
            },
            DiskCommitMsg::FileBegin { ndx: 2, entry: second },
            DiskCommitMsg::FileCommit {
                ndx: 1,
                entry: first,
                source_hash: Some(source_hash),
            },
        ],
    )
    .await;

    assert!(acks.iter().any(|ack| matches!(
        ack,
        DcAck::FileOutcome {
            ndx: 1,
            outcome: FileOutcome::Success
        }
    )));
    assert_eq!(std::fs::read(tmp.path().join("first.bin")).unwrap(), first_bytes);
    assert!(!tmp.path().join("first.bin.terrasync-part").exists());
    assert!(!tmp.path().join("second.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_delta_commit_does_not_commit_full_active_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "mode-mismatch.bin", 1024);
    let source_hash = blake3::hash(&bytes).to_hex().to_string();

    let acks = run_dc(
        dest,
        session_cfg(true),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 1,
                entry: entry.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry: entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from(bytes),
                },
            },
            DiskCommitMsg::DeltaCommit {
                ndx: 1,
                entry,
                source_hash: Some(source_hash),
            },
        ],
    )
    .await;

    assert!(
        acks.is_empty(),
        "mode-mismatched commit must not produce an outcome: {acks:?}"
    );
    assert!(!tmp.path().join("mode-mismatch.bin").exists());
    assert!(!tmp.path().join("mode-mismatch.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_delta_token_for_another_entry_does_not_mutate_active_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let active = make_nas_entry("active.bin", false, false, 5, 0o644);
    let other = make_nas_entry("other.bin", false, false, 5, 0o644);

    let acks = run_dc(
        dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::DeltaBegin {
                ndx: 7,
                entry: active.clone(),
                block_size: 5,
            },
            DiskCommitMsg::DeltaData {
                entry: other,
                data: Bytes::from_static(b"wrong"),
            },
            DiskCommitMsg::DeltaCommit {
                ndx: 7,
                entry: active,
                source_hash: None,
            },
        ],
    )
    .await;

    assert!(acks.iter().any(|ack| matches!(
        ack,
        DcAck::FileOutcome {
            ndx: 7,
            outcome: FileOutcome::SizeMismatch
        }
    )));
    assert!(!tmp.path().join("active.bin").exists());
    assert!(!tmp.path().join("active.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_mode_mismatched_data_does_not_mutate_active_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let full = make_nas_entry("full.bin", false, false, 4, 0o644);
    let delta = make_nas_entry("delta.bin", false, false, 5, 0o644);

    let (acks, progress) = run_dc_inner(
        dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 1,
                entry: full.clone(),
            },
            DiskCommitMsg::DeltaData {
                entry: full.clone(),
                data: Bytes::from_static(b"wrong"),
            },
            DiskCommitMsg::FileChunk {
                entry: full.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"full"),
                },
            },
            DiskCommitMsg::FileCommit {
                ndx: 1,
                entry: full,
                source_hash: None,
            },
            DiskCommitMsg::DeltaBegin {
                ndx: 2,
                entry: delta.clone(),
                block_size: 5,
            },
            DiskCommitMsg::FileChunk {
                entry: delta.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"wrong"),
                },
            },
            DiskCommitMsg::DeltaData {
                entry: delta.clone(),
                data: Bytes::from_static(b"delta"),
            },
            DiskCommitMsg::DeltaCommit {
                ndx: 2,
                entry: delta,
                source_hash: None,
            },
        ],
        true,
    )
    .await;

    assert_eq!(
        acks.iter()
            .filter(|ack| matches!(
                ack,
                DcAck::FileOutcome {
                    outcome: FileOutcome::Success,
                    ..
                }
            ))
            .count(),
        2
    );
    assert_eq!(std::fs::read(tmp.path().join("full.bin")).unwrap(), b"full");
    assert_eq!(std::fs::read(tmp.path().join("delta.bin")).unwrap(), b"delta");
    assert_eq!(progress.files_transferred.load(Ordering::Relaxed), 2);
    assert_eq!(progress.bytes_transferred.load(Ordering::Relaxed), 9);
    for path in ["full.bin", "delta.bin"] {
        let metadata = std::fs::metadata(tmp.path().join(path)).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o644);
        assert!(!tmp.path().join(format!("{path}.terrasync-part")).exists());
    }
}

#[tokio::test]
async fn dc_abort_during_delta_cleans_part_without_outcome() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let entry = make_nas_entry("delta-abort.bin", false, false, 5, 0o644);
    let acks = run_dc(
        dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::DeltaBegin {
                ndx: 5,
                entry: entry.clone(),
                block_size: 5,
            },
            DiskCommitMsg::DeltaData {
                entry,
                data: Bytes::from_static(b"delta"),
            },
            DiskCommitMsg::AbortFile,
        ],
    )
    .await;
    assert!(acks.is_empty());
    assert!(!tmp.path().join("delta-abort.bin").exists());
    assert!(!tmp.path().join("delta-abort.bin.terrasync-part").exists());
}

#[tokio::test]
async fn dc_commands_without_begin_and_duplicate_commit_do_not_duplicate_outcomes() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let entry = make_nas_entry("once.bin", false, false, 4, 0o644);
    let acks = run_dc(
        dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::FileChunk {
                entry: entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"drop"),
                },
            },
            DiskCommitMsg::FileCommit {
                ndx: 4,
                entry: entry.clone(),
                source_hash: None,
            },
            DiskCommitMsg::FileBegin {
                ndx: 4,
                entry: entry.clone(),
            },
            DiskCommitMsg::FileChunk {
                entry: entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"once"),
                },
            },
            DiskCommitMsg::FileCommit {
                ndx: 4,
                entry: entry.clone(),
                source_hash: None,
            },
            DiskCommitMsg::FileCommit {
                ndx: 4,
                entry,
                source_hash: None,
            },
        ],
    )
    .await;

    assert_eq!(acks.len(), 1);
    assert!(matches!(
        &acks[0],
        DcAck::FileOutcome {
            ndx: 4,
            outcome: FileOutcome::Success
        }
    ));
}

#[tokio::test]
async fn dc_failed_begin_then_commit_reports_one_outcome() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let entry = make_nas_entry("../escape.bin", false, false, 4, 0o644);
    let acks = run_dc(
        dest,
        session_cfg(false),
        vec![
            DiskCommitMsg::FileBegin {
                ndx: 9,
                entry: entry.clone(),
            },
            DiskCommitMsg::FileCommit {
                ndx: 9,
                entry,
                source_hash: None,
            },
        ],
    )
    .await;

    assert_eq!(acks.len(), 1);
    assert!(matches!(
        &acks[0],
        DcAck::FileOutcome {
            ndx: 9,
            outcome: FileOutcome::HardError(_)
        }
    ));
    assert!(!tmp.path().parent().unwrap().join("escape.bin").exists());
}

// ============================================================
// delta 重建三段式（issue #54 阶段 3）：DeltaBegin/DeltaMatch/DeltaData/DeltaCommit
// 直接驱动 disk_commit_task，basis block 经真实 read_chunk_stream 读取（不再是内存
// basis_data 切片），验证与 dc 全量路径共用的三段式落盘/hash 校验/清理语义一致。
// ============================================================

// Match token（basis block）+ Data token（字面量）交错重建：basis.bin 预先落盘为旧内容，
// DeltaCommit 后应原子替换为新内容，且 .part 被清理。
#[tokio::test]
async fn dc_delta_reconstructs_from_basis_and_literal_tokens() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let basis_content = b"AAAAABBBBBCCCCCDDDDD"; // 4 个 5 字节 block
    std::fs::write(tmp.path().join("basis.bin"), basis_content).unwrap();

    let new_content = b"AAAAAXXXXXCCCCCDDDDD"; // block0 命中 + 字面量替换 block1 + block2/3 命中
    let entry = make_nas_entry("basis.bin", false, false, new_content.len() as u64, 0o644);
    let source_hash = blake3::hash(new_content).to_hex().to_string();

    let msgs = vec![
        DiskCommitMsg::DeltaBegin {
            ndx: 0,
            entry: entry.clone(),
            block_size: 5,
        },
        DiskCommitMsg::DeltaMatch {
            entry: entry.clone(),
            block_index: 0,
        },
        DiskCommitMsg::DeltaData {
            entry: entry.clone(),
            data: Bytes::from_static(b"XXXXX"),
        },
        DiskCommitMsg::DeltaMatch {
            entry: entry.clone(),
            block_index: 2,
        },
        DiskCommitMsg::DeltaMatch {
            entry: entry.clone(),
            block_index: 3,
        },
        DiskCommitMsg::DeltaCommit {
            ndx: 0,
            entry: entry.clone(),
            source_hash: Some(source_hash),
        },
    ];

    let acks = run_dc(dest, session_cfg(true), msgs).await;
    assert!(acks.iter().any(|a| matches!(
        a,
        DcAck::FileOutcome {
            ndx: 0,
            outcome: FileOutcome::Success
        }
    )));
    assert_eq!(std::fs::read(tmp.path().join("basis.bin")).unwrap(), new_content);
    assert!(!tmp.path().join("basis.bin.terrasync-part").exists());
}

// 空 tokens 的 delta（源文件为空）：DeltaBegin 后直接 DeltaCommit，应产出空文件，
// 与全量路径 dc_zero_byte_file 的空文件语义一致。
#[tokio::test]
async fn dc_delta_zero_tokens_produces_empty_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    std::fs::write(tmp.path().join("shrink.bin"), b"old content here").unwrap();
    let entry = make_nas_entry("shrink.bin", false, false, 0, 0o644);

    let acks = run_dc(
        dest,
        session_cfg(true),
        vec![
            DiskCommitMsg::DeltaBegin {
                ndx: 0,
                entry: entry.clone(),
                block_size: 5,
            },
            DiskCommitMsg::DeltaCommit {
                ndx: 0,
                entry: entry.clone(),
                source_hash: None,
            },
        ],
    )
    .await;

    assert!(acks.iter().any(|a| matches!(
        a,
        DcAck::FileOutcome {
            ndx: 0,
            outcome: FileOutcome::Success
        }
    )));
    assert_eq!(std::fs::metadata(tmp.path().join("shrink.bin")).unwrap().len(), 0);
}

// basis 读失败（引用的 basis file 在磁盘上根本不存在）：应上报 HardError 并中止该
// ActiveFile（丢弃 .part），不产生最终文件——钉死 push_delta_token 的错误路径。
#[tokio::test]
async fn dc_delta_basis_read_failure_reports_hard_error_and_cleans_part() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    // 故意不落盘 "missing.bin"：resume_prepare(resume=false) 不依赖目标文件预先存在，
    // 但 DeltaMatch 触发的 basis 区间读会因文件不存在而失败。
    let entry = make_nas_entry("missing.bin", false, false, 10, 0o644);

    let acks = run_dc(
        dest,
        session_cfg(true),
        vec![
            DiskCommitMsg::DeltaBegin {
                ndx: 0,
                entry: entry.clone(),
                block_size: 5,
            },
            DiskCommitMsg::DeltaMatch {
                entry: entry.clone(),
                block_index: 0,
            },
            DiskCommitMsg::DeltaData {
                entry: entry.clone(),
                data: Bytes::from_static(b"ignored"),
            },
            DiskCommitMsg::DeltaCommit {
                ndx: 0,
                entry: entry.clone(),
                source_hash: None,
            },
        ],
    )
    .await;

    assert!(
        acks.iter().any(|a| matches!(
            a,
            DcAck::FileOutcome {
                ndx: 0,
                outcome: FileOutcome::HardError(_)
            }
        )),
        "basis 读失败应上报 HardError，实际: {acks:?}"
    );
    assert_eq!(acks.len(), 1, "失败后的残余命令不得产生重复 outcome");
    assert!(!tmp.path().join("missing.bin").exists());
    assert!(!tmp.path().join("missing.bin.terrasync-part").exists());
}
