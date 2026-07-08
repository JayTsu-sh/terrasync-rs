# Dual-process 3-part streaming (full-transfer) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the dual-process full-transfer path so neither Sender nor Receiver buffers a whole file in RAM — Sender streams via `read_chunk_stream`, Receiver drives data-mover's 3-part API (`resume_prepare`/`write_chunk_stream`/`commit_chunk_stream`) from a dedicated disk-commit task, with destination `.part` read-back integrity verification.

**Architecture:** The transport is a single ordered message channel; files are processed one `ndx` at a time. On the Sender, `handle_full_transfer` reads the source with `read_chunk_stream` and forwards `DataChunk`s as `FileData`, ending with `EndOfFile{source_hash}`. On the Receiver, `recv_file_data_phase` becomes a thin router that forwards to a new `disk_commit_task`, which per file does `resume_prepare` → feed a `write_chunk_stream` channel → at `EndOfFile` read back the `.part` hash, compare to the Sender's `source_hash`, then `commit_chunk_stream` (atomic rename). Delta/tar/command-mode paths are untouched.

**Tech Stack:** Rust (nightly), tokio, `data-mover` (git dep, `main` `cd6d6710`), `transport` crate (`SenderMsg`/`ReceiverMsg`/`DiskCommitMsg`), `blake3`, BLAKE3 hex hashes.

**Spec:** `docs/superpowers/specs/2026-07-08-dual-process-3part-streaming-design.md`

## Global Constraints

- No `.unwrap()` / `.expect()` in non-test code (workspace clippy `deny`). Tests use `#[allow(clippy::unwrap_used)]`.
- All `use` at file top; in-function paths ≤ 2 segments (import longer paths).
- Each crate uses its `thiserror` error enum; propagate with `?`/`#[from]`, never `.to_string()` into a `String` variant. App errors: `crate::error::{AppError, Result}`.
- Comments in Chinese; identifiers in English.
- Scope: **full-transfer path only.** Do not modify delta (`DeltaData`/`DeltaMatch`), tar/packaged, or single-process command mode (`CopyEntry`/`receiver_task`).
- Data-mover is already at `cd6d6710` in `Cargo.lock` (uncommitted working-tree change — commit it in Task 1).
- Confirmed data-mover APIs (do not redefine):
  - `StorageEnum::read_chunk_stream(from: &StorageEnum, entry: &EntryEnum, intervals: Option<Vec<(u64,u64)>>, qos: Option<QosManager>, enable_integrity_check: bool, capacity: usize) -> (mpsc::Receiver<DataChunk>, JoinHandle<Result<Option<HashCalculator>>>)`
  - `StorageEnum::resume_prepare(dest: &StorageEnum, entry: &EntryEnum, part_path: &Path, resume: bool) -> Result<(Vec<(u64,u64)>, StreamHandle)>`
  - `StorageEnum::write_chunk_stream(dest: &StorageEnum, entry: &EntryEnum, rx: mpsc::Receiver<DataChunk>, handle: &StreamHandle, bytes_counter: Option<Arc<AtomicU64>>, on_committed: CommitCallback) -> Result<()>`
  - `StorageEnum::commit_chunk_stream(dest: &StorageEnum, entry: &EntryEnum, size: u64, handle: StreamHandle) -> Result<()>`
  - `StorageEnum::compute_hash(&self, relative_path: &Path, size: u64) -> Result<String>` (BLAKE3 hex)
  - `DataChunk { offset: u64, data: bytes::Bytes }`; `HashCalculator::finalize(self) -> String`; `type CommitCallback = Arc<dyn Fn(u64,u64)+Send+Sync>`.
  - `.part` path helper: `crate::byte_resume` (`is_part_file`, and the `.terrasync-part` suffix convention used by data-mover). Derive `part_path` the same way `copy_file_with_resume`/`should_resume` already do — reuse that helper, do not hand-roll the suffix.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/transport/src/message.rs` | `DiskCommitMsg` enum shape | Add `FileBegin{entry}`; change `FileChunk` to carry `DataChunk` |
| `crates/app/src/remote_sync.rs` | Sender full-transfer | Rewrite `handle_full_transfer` to stream via `read_chunk_stream`; thread `enable_integrity_check` |
| `crates/app/src/disk_commit.rs` (new) | Receiver disk-commit task (owns dest writes, drives 3-part API) | Create |
| `crates/app/src/receiver.rs` | Receiver router | `recv_file_data_phase` forwards to dc task; delete full-transfer `BytesMut`; drain on shutdown |
| `crates/app/src/lib.rs` | module list | Add `mod disk_commit;` |
| `crates/app/tests/dual_process_streaming.rs` (new) | Integration tests (in-process transport) | Create |

**Decision:** the disk-commit task lives in its own module (`disk_commit.rs`) — it's a well-bounded unit and keeps `receiver.rs` from ballooning past its current ~640 lines.

---

## Task 1: Finalize `DiskCommitMsg` protocol shape + commit the lock bump

**Files:**
- Modify: `crates/transport/src/message.rs:200-243` (the `DiskCommitMsg` enum)
- Verify: `git grep "DiskCommitMsg::"` shows no existing consumers (it's scaffolding)

**Interfaces:**
- Produces: `DiskCommitMsg::FileBegin { entry: Arc<EntryEnum> }`, `DiskCommitMsg::FileChunk { entry: Arc<EntryEnum>, chunk: DataChunk }`, unchanged `FileCommit`/`CreateDir`/`CreateSymlink`/`Shutdown`.

- [ ] **Step 1: Confirm `DiskCommitMsg` is unused scaffolding**

Run: `cd /home/keyee/Projects/terrasync-rs && git grep -n "DiskCommitMsg::"`
Expected: no matches (only the enum definition in `message.rs`). If there ARE consumers, stop and reconcile their field access before changing shapes.

- [ ] **Step 2: Edit the enum** — in `crates/transport/src/message.rs`, add `FileBegin` and change `FileChunk` to carry a `DataChunk` (import already present: `use data_mover::{DataChunk, EntryEnum};`).

```rust
    // ── 全量文件传输（3 段流式驱动） ──
    /// 文件开始：Receiver 据此 resume_prepare + 起 write_chunk_stream
    FileBegin { entry: Arc<EntryEnum> },
    /// 文件数据块（直接转发 DataChunk 给 write_chunk_stream 的 channel）
    FileChunk { entry: Arc<EntryEnum>, chunk: DataChunk },
    /// 文件传输结束，提交：读回 .part hash 校验 + set_metadata + ACL + 原子 rename
    FileCommit {
        entry: Arc<EntryEnum>,
        source_hash: Option<String>,
    },
```
(Delete the old `FileChunk { entry, data, offset }` variant; keep `CreateDir`/`CreateSymlink`/delta variants/`TarPacked`/`Shutdown` as they are.)

- [ ] **Step 3: Build the transport crate**

Run: `cargo build -p transport`
Expected: compiles (only the enum changed; no consumers yet).

- [ ] **Step 4: Commit (lock bump + protocol)**

```bash
git add Cargo.lock crates/transport/src/message.rs
git commit -m "chore(deps+proto): bump data-mover cd6d6710; DiskCommitMsg FileBegin + FileChunk(DataChunk)

PR #1 已合并到 data-mover main；补 DiskCommitMsg 全量流式变体，供 Receiver disk-commit task 使用。"
```

---

## Task 2: Sender — stream `handle_full_transfer` via `read_chunk_stream`

**Files:**
- Modify: `crates/app/src/remote_sync.rs:220-278` (`handle_full_transfer`) and its call site `:170`
- Test: `crates/app/tests/dual_process_streaming.rs` (new)

**Interfaces:**
- Consumes: `StorageEnum::read_chunk_stream` (see Global Constraints).
- Produces: `handle_full_transfer(transport, src_storage, entry, qos: Option<&QosManager>, enable_integrity_check: bool, enable_acl: bool) -> Result<()>` — same `SenderMsg` sequence (`CreateDir` | `CreateSymlink` | `FileBegin`,`FileData`*,`EndOfFile{source_hash}`), but streamed.

- [ ] **Step 1: Write the failing test** — create `crates/app/tests/dual_process_streaming.rs`:

```rust
#![allow(clippy::unwrap_used, clippy::expect_used)]
// 用 in-process transport 断言 Sender 流式发送序列 + 源 hash 正确。
use std::sync::Arc;
use bytes::BytesMut;
use data_mover::{EntryEnum, StorageEnum};
use transport::message::SenderMsg;
use transport::in_process::create_in_process_pair;

// 帮助函数：在临时目录建一个 Local StorageEnum 和一个 size 字节的随机文件，返回 (storage, entry, bytes)
async fn local_file(dir: &std::path::Path, name: &str, size: usize) -> (Arc<StorageEnum>, Arc<EntryEnum>, Vec<u8>) {
    // 见实现说明：用 data_mover 的 Local 构造 + NASEntry::from_path/scan 拿到 EntryEnum。
    // （实现时参考 crates/app 现有测试如何构造 Local StorageEnum 与 EntryEnum。）
    unimplemented!("construct Local storage + entry for {name} size {size} under {dir:?}")
}

#[tokio::test]
async fn sender_streams_file_and_sends_correct_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, entry, bytes) = local_file(tmp.path(), "big.bin", 9 * 1024 * 1024).await;
    let (sender_t, receiver_t) = create_in_process_pair();

    let entry2 = entry.clone();
    let jh = tokio::spawn(async move {
        app::remote_sync::handle_full_transfer(&sender_t, &src, &entry2, None, true, false).await
    });

    // 收集 Sender 消息，重组文件字节
    let mut got = BytesMut::new();
    let mut source_hash = None;
    loop {
        match receiver_t.recv().await {
            Some(SenderMsg::FileBegin { .. }) => {}
            Some(SenderMsg::FileData { chunk, .. }) => got.extend_from_slice(&chunk.data),
            Some(SenderMsg::EndOfFile { source_hash: h, .. }) => { source_hash = h; break; }
            other => panic!("unexpected {other:?}"),
        }
    }
    jh.await.unwrap().unwrap();
    assert_eq!(&got[..], &bytes[..], "重组字节应等于源文件");
    assert_eq!(source_hash.unwrap(), blake3::hash(&bytes).to_hex().to_string());
}
```
(`handle_full_transfer` must be reachable from the test: make it `pub` in `remote_sync.rs`, and ensure `pub mod remote_sync;` in `lib.rs`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p app --test dual_process_streaming sender_streams_file_and_sends_correct_hash`
Expected: FAIL to compile (`handle_full_transfer` arity/visibility) or `unimplemented!`.

- [ ] **Step 3: Rewrite `handle_full_transfer`** — replace the file branch (`remote_sync.rs:240-274`) and update the signature. Keep dir/symlink branches and the trailing `send_acl_if_enabled` call.

```rust
pub async fn handle_full_transfer(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>,
    qos: Option<&QosManager>, enable_integrity_check: bool, enable_acl: bool,
) -> Result<()> {
    if entry.get_is_dir() {
        transport.send(SenderMsg::CreateDir { entry: entry.clone() }).await?;
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(entry).await {
            Ok(target) => transport.send(SenderMsg::CreateSymlink { entry: entry.clone(), target }).await?,
            Err(e) => error!("[Sender Remote] read_symlink {:?}: {}", entry.get_relative_path(), e),
        }
    } else {
        // 流式读源文件：read_chunk_stream 内部按块读 + per-chunk QoS + hash
        let (mut rx, hash_handle) = StorageEnum::read_chunk_stream(
            src_storage, entry, None, qos.cloned(), enable_integrity_check, /*capacity*/ 8,
        );
        transport.send(SenderMsg::FileBegin { entry: entry.clone() }).await?;
        while let Some(chunk) = rx.recv().await {
            transport.send(SenderMsg::FileData { entry: entry.clone(), chunk }).await?;
        }
        // 读任务收尾：拿到源 hash（enable_integrity_check 时为 Some）
        let source_hash = hash_handle.await
            .map_err(|e| AppError::CopyError(format!("read_chunk_stream join: {e}")))??
            .map(|h| h.finalize());
        transport.send(SenderMsg::EndOfFile { entry: entry.clone(), source_hash }).await?;
    }
    send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
    Ok(())
}
```
Notes: `read_chunk_stream` takes `qos: Option<QosManager>` by value → use `qos.cloned()`. It applies per-chunk QoS internally, so the old manual `q.acquire(...)` loop is gone. `FILE_CHUNK_SIZE` and `blake3::hash` become unused in this fn — remove `FILE_CHUNK_SIZE` only if no other user remains (`git grep FILE_CHUNK_SIZE`); leave `blake3` import if the delta path still uses it.

- [ ] **Step 4: Update the call site** at `remote_sync.rs:170` to pass `enable_integrity_check`. The value is available where `SessionConfig` is built (`remote_sync.rs:51 enable_integrity_check: config.enable_integrity_check`); thread that same `app_config` value into `process_requests` and down to the call:

```rust
handle_full_transfer(transport, src_storage, entry, qos, enable_integrity_check, enable_acl).await?;
```
Add an `enable_integrity_check: bool` parameter to `process_requests` (and wherever it's called in `run`), sourced from `app_config.sync`/`SessionConfig` — mirror how `enable_acl` is already threaded.

- [ ] **Step 5: Implement the test helper** `local_file` (replace `unimplemented!`) using the same Local-storage/entry construction other `crates/app` tests use (`git grep -l "StorageEnum::Local" crates/app` for a pattern). Write `size` bytes of `vec![0xAB; size]` (deterministic, no RNG).

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p app --test dual_process_streaming sender_streams_file_and_sends_correct_hash`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/app/src/remote_sync.rs crates/app/src/lib.rs crates/app/tests/dual_process_streaming.rs
git commit -m "feat(remote-sync): stream full-transfer via read_chunk_stream (no whole-file buffer)"
```

---

## Task 3: Receiver — the `disk_commit_task`

**Files:**
- Create: `crates/app/src/disk_commit.rs`
- Modify: `crates/app/src/lib.rs` (add `pub(crate) mod disk_commit;`)
- Test: `crates/app/tests/dual_process_streaming.rs` (extend)

**Interfaces:**
- Consumes: `DiskCommitMsg` (Task 1); `resume_prepare`/`write_chunk_stream`/`commit_chunk_stream`/`compute_hash` (Global Constraints); `ReceiverProgress` (`receiver.rs:30`).
- Produces:
  `pub(crate) async fn disk_commit_task(dest: Arc<StorageEnum>, session: SessionConfig, mut rx: mpsc::Receiver<DiskCommitMsg>, ack: DiskCommitAck, progress: Arc<ReceiverProgress>) -> Result<()>`
  where `pub(crate) enum DiskCommitAck` wraps sending `ReceiverMsg::EntrySuccess/EntryError` — pass the `&dyn ReceiverTransport` via an `Arc`-wrapped handle. Simplest: `disk_commit_task` takes `ack_tx: mpsc::Sender<ReceiverMsg>` and the router forwards those to the transport. Define that channel here.

- [ ] **Step 1: Write the failing tests** (append to `crates/app/tests/dual_process_streaming.rs`):

```rust
use app::disk_commit::{disk_commit_task};
use transport::message::DiskCommitMsg;
use data_mover::DataChunk;

// 帮助：起 dc task，喂消息，收集 ack ReceiverMsg
async fn run_dc(dest: Arc<StorageEnum>, session: transport::message::SessionConfig,
                msgs: Vec<DiskCommitMsg>) -> Vec<transport::message::ReceiverMsg> {
    let (dc_tx, dc_rx) = tokio::sync::mpsc::channel(16);
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(64);
    let progress = Arc::new(app::receiver::ReceiverProgress::new());
    let jh = tokio::spawn(disk_commit_task(dest, session, dc_rx, ack_tx, progress));
    for m in msgs { dc_tx.send(m).await.unwrap(); }
    dc_tx.send(DiskCommitMsg::Shutdown).await.unwrap();
    drop(dc_tx);
    jh.await.unwrap().unwrap();
    let mut acks = vec![];
    while let Ok(a) = ack_rx.try_recv() { acks.push(a); }
    acks
}

#[tokio::test]
async fn dc_writes_full_file_and_acks_success() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = /* Local StorageEnum rooted at tmp */ local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "sub/f.bin", 5 * 1024 * 1024); // entry with is_dir=false
    let session = session_cfg(true /*integrity*/);
    let src_hash = blake3::hash(&bytes).to_hex().to_string();

    let mut msgs = vec![DiskCommitMsg::CreateDir { entry: dir_entry("sub") },
                        DiskCommitMsg::FileBegin { entry: entry.clone() }];
    for (off, c) in chunkify(&bytes, 1<<20) { msgs.push(DiskCommitMsg::FileChunk { entry: entry.clone(), chunk: DataChunk { offset: off, data: c } }); }
    msgs.push(DiskCommitMsg::FileCommit { entry: entry.clone(), source_hash: Some(src_hash) });

    let acks = run_dc(dest, session, msgs).await;
    assert!(acks.iter().any(|a| matches!(a, transport::message::ReceiverMsg::EntrySuccess { .. })));
    assert_eq!(std::fs::read(tmp.path().join("sub/f.bin")).unwrap(), bytes);      // 最终文件正确
    assert!(!tmp.path().join("sub/f.bin.terrasync-part").exists());              // .part 已 rename 掉
}

#[tokio::test]
async fn dc_hash_mismatch_rejects_and_cleans_part() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, bytes) = entry_for(tmp.path(), "f.bin", 2 * 1024 * 1024);
    let session = session_cfg(true);
    let mut msgs = vec![DiskCommitMsg::FileBegin { entry: entry.clone() }];
    for (off, c) in chunkify(&bytes, 1<<20) { msgs.push(DiskCommitMsg::FileChunk { entry: entry.clone(), chunk: DataChunk { offset: off, data: c } }); }
    msgs.push(DiskCommitMsg::FileCommit { entry: entry.clone(), source_hash: Some("deadbeef".repeat(8)) }); // 错的 hash

    let acks = run_dc(dest, session, msgs).await;
    assert!(acks.iter().any(|a| matches!(a, transport::message::ReceiverMsg::EntryError { .. })));
    assert!(!tmp.path().join("f.bin").exists());                    // 不产生最终文件
    assert!(!tmp.path().join("f.bin.terrasync-part").exists());     // .part 已删除
}

#[tokio::test]
async fn dc_zero_byte_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let (entry, _b) = entry_for(tmp.path(), "empty.bin", 0);
    let session = session_cfg(true);
    let acks = run_dc(dest, session, vec![
        DiskCommitMsg::FileBegin { entry: entry.clone() },
        DiskCommitMsg::FileCommit { entry: entry.clone(), source_hash: Some(blake3::hash(b"").to_hex().to_string()) },
    ]).await;
    assert!(acks.iter().any(|a| matches!(a, transport::message::ReceiverMsg::EntrySuccess { .. })));
    assert_eq!(std::fs::metadata(tmp.path().join("empty.bin")).unwrap().len(), 0);
}
```
(Helpers `local_storage`, `entry_for`, `dir_entry`, `session_cfg`, `chunkify` go in a `mod helpers` in the test file; implement in Step 4. `entry_for` returns an `Arc<EntryEnum>` whose `relative_path` is the given name and `size` matches.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p app --test dual_process_streaming dc_`
Expected: FAIL to compile (`app::disk_commit` missing).

- [ ] **Step 3: Implement `crates/app/src/disk_commit.rs`**

```rust
//! 双进程模式 Receiver 的落盘任务：串行消费 DiskCommitMsg，
//! 对全量文件用 data-mover 3 段 API（resume_prepare/write_chunk_stream/commit_chunk_stream）
//! 流式写入 .part，EndOfFile 时读回 .part hash 校验后原子 rename。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use data_mover::{DataChunk, EntryEnum, StorageEnum};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use transport::message::{DiskCommitMsg, ReceiverMsg, SessionConfig};

use crate::byte_resume::part_path_for;          // 见下：复用现有 .part 路径推导
use crate::error::{AppError, Result};
use crate::receiver::ReceiverProgress;

struct ActiveFile {
    entry: Arc<EntryEnum>,
    tx_inner: mpsc::Sender<DataChunk>,
    write_join: JoinHandle<data_mover::error::Result<()>>,
    handle: data_mover::StreamHandle,
    part_path: PathBuf,
    size: u64,
}

pub(crate) async fn disk_commit_task(
    dest: Arc<StorageEnum>, session: SessionConfig, mut rx: mpsc::Receiver<DiskCommitMsg>,
    ack_tx: mpsc::Sender<ReceiverMsg>, progress: Arc<ReceiverProgress>,
) -> Result<()> {
    let mut active: Option<ActiveFile> = None;

    while let Some(msg) = rx.recv().await {
        match msg {
            DiskCommitMsg::CreateDir { entry } => {
                if let Err(e) = dest.create_dir_all(&entry).await {
                    warn!("[dc] create_dir {:?}: {}", entry.get_relative_path(), e);
                }
                let _ = dest.set_entry_metadata(&entry).await;
                progress.dirs_created.fetch_add(1, Ordering::Relaxed);
                let _ = ack_tx.send(ReceiverMsg::EntrySuccess { entry }).await;
            }
            DiskCommitMsg::CreateSymlink { entry, target } => {
                match dest.create_symlink(&entry, &target).await {
                    Ok(()) => { let _ = ack_tx.send(ReceiverMsg::EntrySuccess { entry }).await; }
                    Err(e) => { let _ = ack_tx.send(ReceiverMsg::EntryError { entry, reason: format!("{e}") }).await; }
                }
            }
            DiskCommitMsg::FileBegin { entry } => {
                let part_path = part_path_for(&entry);
                match StorageEnum::resume_prepare(&dest, &entry, &part_path, false).await {
                    Ok((_missing, handle)) => {
                        let (tx_inner, rx_inner) = mpsc::channel::<DataChunk>(8);
                        let dest2 = dest.clone();
                        let entry2 = entry.clone();
                        let handle2 = handle.clone();
                        let write_join = tokio::spawn(async move {
                            StorageEnum::write_chunk_stream(&dest2, &entry2, rx_inner, &handle2, None, noop_commit()).await
                        });
                        active = Some(ActiveFile { entry: entry.clone(), tx_inner, write_join, handle, part_path, size: entry.get_size() });
                    }
                    Err(e) => {
                        error!("[dc] resume_prepare {:?}: {}", entry.get_relative_path(), e);
                        let _ = ack_tx.send(ReceiverMsg::EntryError { entry, reason: format!("{e}") }).await;
                    }
                }
            }
            DiskCommitMsg::FileChunk { entry, chunk } => {
                if let Some(a) = active.as_ref() {
                    if a.tx_inner.send(chunk).await.is_err() {
                        error!("[dc] write channel closed early for {:?}", entry.get_relative_path());
                    }
                }
            }
            DiskCommitMsg::FileCommit { entry, source_hash } => {
                if let Some(a) = active.take() {
                    finalize_file(&dest, &session, a, source_hash, &ack_tx, &progress).await;
                } else {
                    warn!("[dc] FileCommit without active stream: {:?}", entry.get_relative_path());
                }
            }
            DiskCommitMsg::Shutdown => break,
            _ => { /* delta/tar 变体不走 dc task（见 receiver 路由） */ }
        }
    }
    Ok(())
}

async fn finalize_file(
    dest: &Arc<StorageEnum>, session: &SessionConfig, a: ActiveFile,
    source_hash: Option<String>, ack_tx: &mpsc::Sender<ReceiverMsg>, progress: &Arc<ReceiverProgress>,
) {
    let ActiveFile { entry, tx_inner, write_join, handle, part_path, size } = a;
    drop(tx_inner); // 关闭 channel → write_chunk_stream 收尾
    if let Err(e) = write_join.await.map_err(|e| AppError::CopyError(format!("write join: {e}"))).and_then(|r| r.map_err(AppError::from)) {
        error!("[dc] write {:?}: {}", entry.get_relative_path(), e);
        let _ = dest.remove_file(&part_path).await;
        let _ = ack_tx.send(ReceiverMsg::EntryError { entry, reason: format!("{e}") }).await;
        return;
    }
    // 读回 .part hash 校验（verify-before-rename）
    if session.enable_integrity_check {
        if let Some(expected) = source_hash.as_ref() {
            match dest.compute_hash(&part_path, size).await {
                Ok(actual) if &actual == expected => {}
                Ok(actual) => {
                    error!("[dc] hash mismatch {:?}: {} != {}", entry.get_relative_path(), actual, expected);
                    let _ = dest.remove_file(&part_path).await;
                    let _ = ack_tx.send(ReceiverMsg::EntryError { entry, reason: "hash mismatch".into() }).await;
                    return;
                }
                Err(e) => {
                    let _ = dest.remove_file(&part_path).await;
                    let _ = ack_tx.send(ReceiverMsg::EntryError { entry, reason: format!("hash read-back: {e}") }).await;
                    return;
                }
            }
        }
    }
    if let Err(e) = StorageEnum::commit_chunk_stream(dest, &entry, size, handle).await {
        error!("[dc] commit {:?}: {}", entry.get_relative_path(), e);
        let _ = dest.remove_file(&part_path).await;
        let _ = ack_tx.send(ReceiverMsg::EntryError { entry, reason: format!("{e}") }).await;
        return;
    }
    let _ = dest.set_entry_metadata(&entry).await;
    progress.files_transferred.fetch_add(1, Ordering::Relaxed);
    progress.bytes_transferred.fetch_add(size, Ordering::Relaxed);
    let _ = ack_tx.send(ReceiverMsg::EntrySuccess { entry }).await;
}

fn noop_commit() -> data_mover::CommitCallback { std::sync::Arc::new(|_off, _len| {}) }
```
Reconcile against real signatures during compile: exact `dest.remove_file`/`create_dir_all`/`set_entry_metadata`/`create_symlink`/`read_symlink` names (`git grep "fn create_dir_all\|fn set_entry_metadata\|fn remove_file" ~/.cargo/git/.../cd6d671/src`), the `StreamHandle: Clone` bound, and `part_path_for` — if `crate::byte_resume` has no such public helper, add a small `pub(crate) fn part_path_for(entry: &EntryEnum) -> PathBuf` there that appends the same `.terrasync-part` suffix `resume_prepare` expects (find the suffix in data-mover `should_resume`/`copy_file_with_resume`).

- [ ] **Step 4: Add `mod disk_commit;` + implement test helpers** — in `crates/app/src/lib.rs` add `pub(crate) mod disk_commit;` (or `pub mod` if tests need it directly; tests use `app::disk_commit::disk_commit_task`, so `pub mod disk_commit;` and `pub(crate)` on internals as needed — expose only `disk_commit_task`). Implement the `mod helpers` (`local_storage`, `entry_for`, `dir_entry`, `session_cfg`, `chunkify`) in the test file.

- [ ] **Step 5: Run the dc tests to verify they pass**

Run: `cargo test -p app --test dual_process_streaming dc_`
Expected: PASS (3 tests: success, hash-mismatch, zero-byte).

- [ ] **Step 6: Commit**

```bash
git add crates/app/src/disk_commit.rs crates/app/src/lib.rs crates/app/tests/dual_process_streaming.rs
git commit -m "feat(receiver): disk-commit task drives 3-part streaming write + read-back verify"
```

---

## Task 4: Receiver — route `recv_file_data_phase` through the dc task

**Files:**
- Modify: `crates/app/src/receiver.rs:465-555` (`recv_file_data_phase`) + imports (`:12` remove `BufMut, BytesMut` if now unused by the full path; delta keeps its own buffer, so check)
- Test: `crates/app/tests/dual_process_streaming.rs` (end-to-end recv test)

**Interfaces:**
- Consumes: `disk_commit_task` (Task 3), `DiskCommitMsg` (Task 1).
- Produces: unchanged public signature of `recv_file_data_phase`.

- [ ] **Step 1: Write the failing end-to-end test** (append):

```rust
#[tokio::test]
async fn recv_phase_routes_full_files_to_disk() {
    // 用 in-process pair：手工扮演 Sender 发 CreateDir + FileBegin/FileData/EndOfFile + TransferDone，
    // 调 recv_file_data_phase，断言目标端文件落地 + 收到 EntrySuccess。
    let tmp = tempfile::tempdir().unwrap();
    let dest = local_storage(tmp.path()).await;
    let session = session_cfg(true);
    let (entry, bytes) = entry_for(tmp.path(), "d/f.bin", 3 * 1024 * 1024);
    let (sender_t, receiver_t) = create_in_process_pair();

    // Sender 侧脚本
    let src_hash = blake3::hash(&bytes).to_hex().to_string();
    let e = entry.clone();
    tokio::spawn(async move {
        sender_t.send(SenderMsg::CreateDir { entry: dir_entry("d") }).await.unwrap();
        sender_t.send(SenderMsg::FileBegin { entry: e.clone() }).await.unwrap();
        for (off, c) in chunkify(&bytes, 1<<20) {
            sender_t.send(SenderMsg::FileData { entry: e.clone(), chunk: DataChunk { offset: off, data: c } }).await.unwrap();
        }
        sender_t.send(SenderMsg::EndOfFile { entry: e.clone(), source_hash: Some(src_hash) }).await.unwrap();
        sender_t.send(SenderMsg::TransferDone).await.unwrap();
    });

    let progress = Arc::new(app::receiver::ReceiverProgress::new());
    let (_ptx, prx) = tokio::sync::mpsc::channel(4);
    app::receiver::recv_file_data_phase(&receiver_t, &dest, &session, &progress, prx).await.unwrap();
    assert_eq!(std::fs::read(tmp.path().join("d/f.bin")).unwrap(), bytes);
}
```
(Make `recv_file_data_phase` and `ReceiverProgress` reachable: `pub` on the fn, `pub mod receiver;` in lib.rs.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p app --test dual_process_streaming recv_phase_routes_full_files_to_disk`
Expected: FAIL (still the BytesMut path; or content mismatch because full path isn't routed yet).

- [ ] **Step 3: Rewrite `recv_file_data_phase`** — spawn the dc task + an ack-forwarding loop; route messages; keep delta inline; drain on `TransferDone`.

```rust
async fn recv_file_data_phase(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, session_config: &SessionConfig,
    progress: &Arc<ReceiverProgress>, mut progress_rx: MpscReceiver<ProgressSnapshot>,
) -> Result<()> {
    info!("[Receiver Remote] Phase 2: Receiving file data (streaming)");

    let (dc_tx, dc_rx) = tokio::sync::mpsc::channel::<DiskCommitMsg>(16);
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel::<ReceiverMsg>(64);
    let dc_join = tokio::spawn(crate::disk_commit::disk_commit_task(
        dest_storage.clone(), session_config.clone(), dc_rx, ack_tx, progress.clone(),
    ));

    // delta 缓冲（保持原样，仅 delta 路径使用）
    let mut delta_tokens: Vec<sync_delta::DeltaToken> = Vec::new();

    loop {
        tokio::select! {
            Some(snapshot) = progress_rx.recv() => { let _ = transport.send(ReceiverMsg::Progress(snapshot)).await; }
            Some(ack) = ack_rx.recv() => { let _ = transport.send(ack).await; }
            msg = transport.recv() => { match msg {
                Some(SenderMsg::CreateDir { entry })       => { let _ = dc_tx.send(DiskCommitMsg::CreateDir { entry }).await; }
                Some(SenderMsg::CreateSymlink { entry, target }) => { let _ = dc_tx.send(DiskCommitMsg::CreateSymlink { entry, target }).await; }
                Some(SenderMsg::FileBegin { entry })       => { delta_tokens.clear(); let _ = dc_tx.send(DiskCommitMsg::FileBegin { entry }).await; }
                Some(SenderMsg::FileData { entry, chunk }) => { let _ = dc_tx.send(DiskCommitMsg::FileChunk { entry, chunk }).await; }
                Some(SenderMsg::EndOfFile { entry, source_hash }) if delta_tokens.is_empty() =>
                    { let _ = dc_tx.send(DiskCommitMsg::FileCommit { entry, source_hash }).await; }
                // ── delta 路径：保持原有 inline 逻辑不变 ──
                Some(SenderMsg::DeltaMatch { block_index, .. }) => delta_tokens.push(sync_delta::DeltaToken::Match { block_index }),
                Some(SenderMsg::DeltaData { data, .. })         => delta_tokens.push(sync_delta::DeltaToken::Data(data)),
                Some(SenderMsg::EndOfFile { entry, source_hash }) => {
                    let tokens = std::mem::take(&mut delta_tokens);
                    handle_end_of_file(transport, dest_storage, entry, source_hash, tokens, bytes::Bytes::new(), progress).await;
                }
                Some(SenderMsg::SetAcl { entry, acl_data }) => {
                    if session_config.enable_acl { let _ = dest_storage.set_acl_bytes(entry.get_relative_path(), &acl_data).await; }
                }
                Some(SenderMsg::TransferDone) => { break; }
                Some(_) => {}
                None => return Err(AppError::CopyError("Transport closed during data phase".into())),
            }}
        }
    }

    // 收尾：关闭 dc_tx → dc task 处理完积压后退出；drain 剩余 ack
    let _ = dc_tx.send(DiskCommitMsg::Shutdown).await;
    drop(dc_tx);
    dc_join.await.map_err(|e| AppError::CopyError(format!("dc task join: {e}")))??;
    while let Ok(ack) = ack_rx.try_recv() { let _ = transport.send(ack).await; }
    Ok(())
}
```
Then **delete** `handle_end_of_file`'s **full-transfer** responsibilities that are now unused? No — `handle_end_of_file` is still used by the delta branch (with `tokens` non-empty). Keep it, but its `if tokens.is_empty() { file_data }` branch now only receives `Bytes::new()` (delta always has tokens). Leave `handle_end_of_file` for delta; the full path no longer calls it. Remove the now-unused `file_data_buf` and the `FileData`-append logic (done above — no `file_data_buf`). Remove `use bytes::{BufMut, BytesMut}` if unused now (`cargo build` will warn); keep `bytes::Bytes`.

Add imports at top of `receiver.rs`: `use transport::message::DiskCommitMsg;`.

- [ ] **Step 4: Run the e2e recv test**

Run: `cargo test -p app --test dual_process_streaming recv_phase_routes_full_files_to_disk`
Expected: PASS.

- [ ] **Step 5: Run the whole test file**

Run: `cargo test -p app --test dual_process_streaming`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/app/src/receiver.rs
git commit -m "feat(receiver): route full-transfer through disk-commit task; drop whole-file BytesMut"
```

---

## Task 5: Regression + lint gate

**Files:** none (verification only).

- [ ] **Step 1: Full workspace tests**

Run: `cargo test --workspace --no-fail-fast`
Expected: PASS. Pay attention to any existing dual-process / command-mode / delta / tar tests — none should regress.

- [ ] **Step 2: Format + clippy (deny gate)**

Run: `cargo fmt && cargo clippy --workspace --all-targets`
Expected: no `deny`-level findings (no new `unwrap`/`expect` in non-test code).

- [ ] **Step 3: Commit any fmt-only changes**

```bash
git add -A && git commit -m "style: cargo fmt" || echo "nothing to format"
```

---

## Task 6: Dual-process NFS e2e (real environment, RSS-bounded)

**Files:** none (runtime validation; run on migration node `10.128.133.213`). This is the acceptance test the spec requires; capture evidence for the PR.

**Environment (from the PR #1 e2e session):** build on `/root/jay/github/terrasync-rs`; source `nfs://192.168.131.214:/m1-source:/pr1-e2e`, dest export `/m1-target3` on `192.168.131.215`; NFS URL format `nfs://HOST:/EXPORT:/SUBPATH`; local ClickHouse `10.128.133.213:8123`; SSH root/`xuanyuan=1` via askpass.

- [ ] **Step 1: Build the branch on the node** (feat/byte-level-resume with these changes, data-mover `cd6d6710`), reusing the warm `target/`.

Run (on node): `cd /root/jay/github/terrasync-rs && git fetch && git checkout feat/byte-level-resume && cargo build --release --bin terrasync`
Expected: `BUILD_RC=0`, binary at `target/release/terrasync`.

- [ ] **Step 2: Stage the controlled dataset** under `m1-source/pr1-e2e` (11 dirs / 8 files / 3 symlinks incl. a 3 GB file) — reuse `make-dataset.sh` from the PR #1 session.

- [ ] **Step 3: Start the Receiver daemon** on the dest side, then run the Sender with `--remote`, monitoring Receiver RSS.

```bash
# Receiver (serve) — writes to m1-target3, prints its TLS cert path
terrasync -c /root/pr1-config.toml serve --listen 0.0.0.0:9876 \
  "nfs://192.168.131.215:/m1-target3:/pr1-e2e" --tls-cert-out /root/serve.crt &
SERVE_PID=$!
# sample RSS during transfer
( while kill -0 $SERVE_PID 2>/dev/null; do ps -o rss= -p $SERVE_PID; sleep 1; done ) > /root/serve_rss.log &
# Sender
terrasync -c /root/pr1-config.toml sync -i pr1e2e_dp \
  --remote 127.0.0.1:9876 --tls-server-cert /root/serve.crt \
  "nfs://192.168.131.214:/m1-source:/pr1-e2e" "nfs://192.168.131.215:/m1-target3:/pr1-e2e"
```
(Confirm exact `serve` flag names against `terrasync serve --help` / `commands_enum.rs:151`; `--listen`, dest path arg, and `--tls-cert-out` per `serve_cmd`.)

- [ ] **Step 4: Assert results**
  - `terrasync -c /root/pr1-config.toml integrity-check "…m1-source:/pr1-e2e" "…m1-target3:/pr1-e2e"` → `All Passed`.
  - `sort -n /root/serve_rss.log | tail -1` → **peak RSS ≪ 3 GB** (target: < ~500 MB). This is the proof the whole-file buffer is gone.

- [ ] **Step 5: Clean up** the node exactly as the PR #1 session did (remove `pr1-e2e` from source + dest, drop CH tables, restore jay's tree to `main` + stash pop). Do **not** leave test data or a modified tree.

- [ ] **Step 6: Post evidence** to issue #21 (integrity result + RSS peak vs. baseline) and open the terrasync PR for `feat/byte-level-resume`.

---

## Self-Review

**Spec coverage:**
- §3.1 Sender streaming → Task 2 ✓
- §3.2 disk-commit task (resume_prepare/write_chunk_stream/commit + read-back) → Task 3 ✓
- §3.3 router + delete BytesMut + shutdown drain → Task 4 ✓
- §4 hash read-back on Receiver, source_hash via EndOfFile, gated by enable_integrity_check → Tasks 2 (send) + 3 (verify) ✓
- §5 ordering/backpressure (FIFO dc_tx, bounded channels) → Task 3/4 ✓
- §6 error/edge (mismatch, 0-byte, write error, shutdown drain) → Task 3 tests + Task 4 drain ✓
- §7 tests (unit + regression + e2e RSS-bounded) → Tasks 3,4 (unit), 5 (regression), 6 (e2e) ✓
- §8 files touched → File Structure + tasks ✓
- Stale `.part` truncation (§6) → covered by `resume=false` in Task 3 Step 3; note added to reconcile NFS truncate at compile.

**Placeholder scan:** the `unimplemented!` in Task 2 Step 1 is a *deliberate failing-test stub* filled in Task 2 Step 5 (TDD). Test helper bodies (`local_storage`/`entry_for`/…) are specified by contract and implemented in Task 3 Step 4 — not left vague. No "TODO/handle edge cases" placeholders in production code.

**Type consistency:** `handle_full_transfer(…, enable_integrity_check, enable_acl)` used identically in Task 2 Step 3/4. `disk_commit_task(dest, session, rx, ack_tx, progress)` signature identical in Task 3 (def) and Task 4 (call). `DiskCommitMsg::FileChunk{entry, chunk: DataChunk}` and `FileBegin{entry}` consistent across Tasks 1/3/4. `ReceiverMsg::EntrySuccess/EntryError` used as acks throughout (matches current behavior).
