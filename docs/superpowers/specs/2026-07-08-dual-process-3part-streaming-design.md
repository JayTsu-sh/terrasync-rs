# PR #B — Dual-process 3-part streaming (full-transfer path)

- **Date:** 2026-07-08
- **Status:** Design approved (pending spec review)
- **Related:** data-mover PR #1 (merged → `main` `cd6d6710`), terrasync issue #21
- **Branch:** `feat/byte-level-resume`

---

## 1. Background & goal

The dual-process sync mode (`terrasync serve` on the destination + `terrasync sync --remote HOST:PORT` on the source) currently buffers **each whole file in RAM on both sides**:

- **Sender** — `crates/app/src/remote_sync.rs::handle_full_transfer` calls `read_file_from`, reading the entire file into a `Vec<u8>` before emitting `FileData`.
- **Receiver** — `crates/app/src/receiver.rs::recv_file_data_phase` accumulates all `FileData` chunks into one global `BytesMut` (`file_data_buf`), then writes the whole thing at `EndOfFile` via `write_file_from_bytes`.

This caps scalability (a file of size *N* pins ~*N* bytes of RAM in **both** processes) and does not use data-mover PR #1's 3-part streaming write API, which was added specifically so the Receiver can drive writes chunk-by-chunk.

**Goal:** convert the **full-transfer** dual-process path to memory-bounded streaming on both sides — Sender via `read_chunk_stream`, Receiver via `resume_prepare → write_chunk_stream → commit_chunk_stream` driven by a dedicated disk-commit task — with destination read-back integrity verification and recv‖commit overlap.

## 2. Scope

**In scope**
- Sender full-transfer read: `read_file_from` → `read_chunk_stream`.
- Receiver full-transfer write: whole-file `BytesMut` → per-file 3-part streaming via a disk-commit task.
- Integrity verification by destination `.part` read-back (parity with `copy_file_resumable`).

**Out of scope (unchanged behavior)**
- **Over-the-wire (connection-drop) resume** — the Sender always reads the full file (`intervals = None`). Local-restart resume is a data-mover property already covered in-process; wire-resume is a future PR.
- **Delta path** (`SenderMsg::DeltaData` / `DeltaMatch`, "Phase 5") — untouched.
- **Tar / packaged path** — untouched.
- **Single-process command mode** (`SenderMsg::CopyEntry`, `receiver_task`) — untouched.

## 3. Architecture

### 3.1 Sender — `crates/app/src/remote_sync.rs::handle_full_transfer`

Replace the whole-file read with a streaming read:

```rust
let (mut rx, hash_handle) = StorageEnum::read_chunk_stream(
    src_storage, entry, None /* full */, qos, enable_integrity_check, capacity,
);
transport.send(SenderMsg::FileBegin { entry: entry.clone() }).await?;
while let Some(chunk) = rx.recv().await {
    transport.send(SenderMsg::FileData { entry: entry.clone(), chunk }).await?;
}
let source_hash: Option<String> = hash_handle.await??.map(|h| h.finalize());
transport.send(SenderMsg::EndOfFile { entry: entry.clone(), source_hash }).await?;
```

- Memory bounded by `capacity` chunks (was the whole file).
- `source_hash` is a free byproduct of the streaming read (only when `enable_integrity_check`; otherwise `None`).
- `intervals = None` always (full read; no wire-resume).
- `DataChunk { offset, data }` already carries the offset, so it forwards unchanged to the Receiver.

### 3.2 Receiver — disk-commit task (`crates/app/src/receiver.rs`)

A new long-lived task owns `dest_storage` and is fed via the **existing** `DiskCommitMsg` SPSC channel (`crates/transport/src/message.rs`). `recv_file_data_phase` becomes a thin router. Because the transport is a single ordered stream, at most **one file is active at a time**.

dc-task state: `Option<ActiveFile { entry, tx_inner: mpsc::Sender<DataChunk>, write_join: JoinHandle<Result<()>>, handle: StreamHandle, part_path, size }>`.

Message handling:

- **`FileBegin { entry }`** *(new `DiskCommitMsg::FileBegin`)*: `(missing, handle) = resume_prepare(dest, entry, part_path, resume = false)`; create a bounded inner channel; spawn `write_chunk_stream(dest, entry, rx_inner, &handle, bytes_counter, on_committed)`; store `ActiveFile`. Starting the stream on `FileBegin` guarantees even a 0-byte file (no `FileChunk`) gets a `.part` + commit.
- **`FileChunk { entry, chunk }`**: `tx_inner.send(chunk).await` — bounded send provides backpressure and bounds memory. (Forward the `DataChunk` as-is; `write_chunk_stream` consumes `DataChunk` directly.)
- **`FileCommit { entry, source_hash }`**: drop `tx_inner` → `write_join.await??` (all bytes durable in `.part`). Then:
  - If `enable_integrity_check` && `source_hash.is_some()`: `dst_hash = dest.compute_hash(&part_path, size).await?`; if `dst_hash != source_hash` → delete `.part`, send `EntryError`, clear state, continue.
  - Else / on match: `commit_chunk_stream(dest, entry, size, handle)` (set_file_len + atomic rename `.part`→final); `dest.set_entry_metadata(entry)`; ACL if enabled; send `EntrySuccess`.
  - On any write/commit error: delete `.part`, send `EntryError`.
- **`CreateDir` / `CreateSymlink`**: unchanged (create + `set_metadata` + ACL + `EntrySuccess`).
- **`Shutdown`**: finish the active file if present, then exit.

### 3.3 Router — `recv_file_data_phase`

- `FileBegin` → `DiskCommitMsg::FileBegin`
- `FileData { entry, chunk }` → `DiskCommitMsg::FileChunk { entry, chunk }`
- `EndOfFile { entry, source_hash }` → `DiskCommitMsg::FileCommit { entry, source_hash }`
- `CreateDir` / `CreateSymlink` → forward to dc task
- **Delta** (`DeltaData` / `DeltaMatch` + their terminating `EndOfFile`) → **handled inline exactly as today** (unchanged `handle_end_of_file` delta branch). See §5 for the ordering argument that this remains safe.
- `TransferDone` → send `Shutdown`, **`await` the dc-task join**, then return.
- **Delete the full-transfer `file_data_buf: BytesMut`.** (The delta path keeps its own `delta_tokens` buffer — out of scope.)

Progress reporting (`ProgressSnapshot` via `progress_rx`) is preserved; `on_committed(offset, len)` updates `bytes_transferred`.

## 4. Integrity verification (hash)

| Concern | Decision |
|---|---|
| Source hash | Computed **on the Sender**, as a byproduct of `read_chunk_stream` (no extra source read). |
| Dest hash | Computed **on the Receiver**, by reading back the persisted `.part` via `compute_hash(part_path, size)`. |
| Comparison | **On the Receiver**, at `FileCommit`, **before** `commit_chunk_stream` (verify-before-rename → a mismatch never pollutes the final path; parity with PR #1 T5). |
| Transport | Source hash travels Sender→Receiver in `SenderMsg::EndOfFile.source_hash: Option<String>` (BLAKE3 hex). **No new protocol field.** |
| Gating | Only when `SessionConfig.enable_integrity_check`. Otherwise `source_hash = None` → skip read-back + compare. |

**Rationale:** hashing in-flight chunks is *incorrect* for streamed/resumable writes (it misses an already-persisted prefix and never checks what actually landed on disk). Read-back mirrors `copy_file_resumable` exactly. `compute_hash` is `pub` (storage_enum.rs:508) and already used by `integrity_check.rs`, so **no data-mover change is required**.

## 5. Ordering, concurrency, backpressure

- **Single ordered transport** → one file's data at a time; the dc task processes `DiskCommitMsg` **FIFO**.
- **Parent-before-child:** directories precede their children in walk order and are processed FIFO, so a child file's `FileBegin` never runs before its parent `CreateDir` — no missing-parent races. Delta files (handled inline) are likewise preceded by their parent `CreateDir`, which the router forwards to the dc task first; because the router awaits nothing between forwarding the dir and the inline delta write **except** messages that are themselves ordered after the dir, parent existence holds. *(Implementation note: if any inline-delta parent-ordering edge case surfaces, route the delta branch through the dc task's existing `DeltaBegin/DeltaData/DeltaMatch/DeltaCommit` variants — they exist for this — without changing delta logic.)*
- **Overlap:** while the dc task commits file *N* (which, with integrity on, includes a full read-back hash of *N*'s `.part` — significant I/O), the router keeps draining the transport into the bounded `dc_tx` buffer, so the network link is not idle during fsync/rename/read-back. This is **recv(network) ‖ commit(disk)** overlap, bounded by `dc_tx` capacity. The dc task still serializes *write(N+1)* after *commit(N)* (single active-file slot). Full *write(N+1) ‖ commit(N)* overlap would need double-buffering (two active files) — deferred as a future perf option.
- **Backpressure / memory bound:** bounded `dc_tx` channel + bounded `write_chunk_stream` inner channel → end-to-end memory is O(a few chunks + dc_tx capacity), independent of file size.

## 6. Error handling & edge cases

- **Write error** (write task / `write_chunk_stream`): delete `.part`, `EntryError`, dc task continues to next file.
- **Hash mismatch:** delete `.part`, `EntryError`, no rename (parity: current code sends `EntryError` on mismatch — no auto-redo).
- **0-byte file:** `FileBegin` starts the stream, zero `FileChunk`, `FileCommit` → empty `.part` → `set_file_len(0)` + rename.
- **Stale `.part`** from a prior crashed run: `resume = false` → `resume_prepare` returns `missing = [(0, size)]`; the write path must truncate/overwrite from offset 0 (relies on PR #1's create-with-truncate semantics; verify NFS `.part` is opened truncating).
- **Sender source read error:** Sender emits `EntryError` / skips (as today).
- **Shutdown ordering:** `recv_file_data_phase` awaits the dc-task join before returning `Ok(())`, so every commit is durable before `AllDone` is sent.

## 7. Testing

**Unit** (`crates/app`, `in_process` transport, `#[cfg(test)]`):
- Full file: stream → commit → destination content + metadata correct, `EntrySuccess`.
- Injected hash mismatch → `EntryError`, no final file, `.part` deleted.
- 0-byte file → empty final file created.
- Directory + symlink creation → `EntrySuccess`.
- Simulated write error → `EntryError`, `.part` deleted.

**Regression:** `cargo test --workspace --no-fail-fast` green (command mode, delta, tar, single-process untouched).

**Dual-process NFS e2e** (real environment — this is the deferred acceptance test, now due):
- `serve` on the destination (bind on the 192.168.131.0/24 business net, dest = `nfs://192.168.131.215:/m1-target3:/…`) ↔ `sync --remote HOST:PORT` on the source (`nfs://192.168.131.214:/m1-source:/pr1-e2e`, the controlled dataset incl. the 3 GB file).
- Assert: `integrity-check` passes; counts + metadata match source.
- **Receiver RSS bounded:** sample `ps -o rss=` of the `serve` process during the 3 GB transfer; assert peak stays far below file size (target: a few hundred MB, not ~3 GB). Baseline: current code peaks ≈ file size. This is the concrete proof the whole-file buffer is gone.

## 8. Files touched

| File | Change |
|---|---|
| `crates/app/src/remote_sync.rs` | Sender full-transfer → `read_chunk_stream` streaming |
| `crates/app/src/receiver.rs` | New disk-commit task + router; remove full-transfer `BytesMut` |
| `crates/transport/src/message.rs` | Add `DiskCommitMsg::FileBegin { entry }` (and `FileChunk` carrying `DataChunk`) |
| `crates/app/src/…` (tests) | Unit tests above |
| `Cargo.lock` | Already bumped to `cd6d6710` (PR #B item 1 — done) |

## 9. Non-goals recap / follow-ups

- Over-the-wire (connection-drop) resume — future PR.
- Delta path streaming — future PR.
- Double-buffering (write *N+1* ‖ commit *N*) — future perf enhancement.
