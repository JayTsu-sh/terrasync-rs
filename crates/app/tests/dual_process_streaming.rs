#![allow(clippy::unwrap_used, clippy::expect_used)]
// 用 in-process transport 断言 Sender 流式发送序列 + 源 hash 正确。
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

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
    let (dc_tx, dc_rx) = tokio::sync::mpsc::channel(16);
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress = Arc::new(ReceiverProgress::new());
    let jh = tokio::spawn(disk_commit_task(dest, session, dc_rx, ack_tx, progress));
    for m in msgs {
        dc_tx.send(m).await.unwrap();
    }
    dc_tx.send(DiskCommitMsg::Shutdown).await.unwrap();
    drop(dc_tx);
    jh.await.unwrap().unwrap();
    let mut acks = vec![];
    while let Ok(a) = ack_rx.try_recv() {
        acks.push(a);
    }
    acks
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
            ndx: 0,
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
            ndx: 0,
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
            ndx: 0,
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
