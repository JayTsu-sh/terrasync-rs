//! 双进程 Receiver 的落盘命令分派器。
//!
//! 目录与符号链接在此即时处理；full/delta 单文件生命周期全部委托给
//! `FileCommitter` 深模块。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use data_mover::StorageEnum;
use sync_delta::DeltaToken;
use tokio::sync::mpsc;
use tracing::warn;
use transport::message::{DcAck, DiskCommitMsg, ReceiverMsg, SessionConfig};

use crate::error::Result;
use crate::file_commit::{FileCommitter, FileTransferMode};
use crate::receiver::ReceiverProgress;

/// 串行分派 Receiver 落盘命令，并将文件命令交给单一生命周期所有者。
pub async fn disk_commit_task(
    dest: Arc<StorageEnum>, session: SessionConfig, mut rx: mpsc::Receiver<DiskCommitMsg>,
    ack_tx: mpsc::UnboundedSender<DcAck>, progress: Arc<ReceiverProgress>,
) -> Result<()> {
    let mut files = FileCommitter::new(dest.clone(), session, ack_tx.clone(), progress.clone());

    while let Some(msg) = rx.recv().await {
        match msg {
            DiskCommitMsg::CreateDir { entry } => {
                if let Err(error) = dest.create_dir_all(&entry).await {
                    warn!("[dc] create_dir {:?}: {error}", entry.get_relative_path());
                }
                let _ = dest.set_entry_metadata(&entry).await;
                progress.dirs_created.fetch_add(1, Ordering::Relaxed);
                let _ = ack_tx.send(DcAck::Entry(ReceiverMsg::EntrySuccess { entry }));
            }
            DiskCommitMsg::CreateSymlink { entry, target } => match dest.create_symlink(&entry, &target).await {
                Ok(()) => {
                    progress.files_transferred.fetch_add(1, Ordering::Relaxed);
                    let _ = ack_tx.send(DcAck::Entry(ReceiverMsg::EntrySuccess { entry }));
                }
                Err(error) => {
                    let _ = ack_tx.send(DcAck::Entry(ReceiverMsg::EntryError {
                        entry,
                        reason: error.to_string(),
                    }));
                }
            },
            DiskCommitMsg::FileBegin { ndx, entry } => files.begin_full(ndx, entry).await,
            DiskCommitMsg::FileChunk { entry, chunk } => files.push_full(&entry, chunk).await,
            DiskCommitMsg::FileCommit {
                ndx,
                entry,
                source_hash,
            } => files.commit(ndx, &entry, source_hash, FileTransferMode::Full).await,
            DiskCommitMsg::DeltaBegin { ndx, entry, block_size } => {
                files.begin_delta(ndx, entry, block_size).await;
            }
            DiskCommitMsg::DeltaMatch { entry, block_index } => {
                files.push_delta(&entry, DeltaToken::Match { block_index }).await;
            }
            DiskCommitMsg::DeltaData { entry, data } => {
                files.push_delta(&entry, DeltaToken::Data(data)).await;
            }
            DiskCommitMsg::DeltaCommit {
                ndx,
                entry,
                source_hash,
            } => files.commit(ndx, &entry, source_hash, FileTransferMode::Delta).await,
            DiskCommitMsg::AbortFile => files.abort().await,
            DiskCommitMsg::Shutdown => {
                files.shutdown().await;
                break;
            }
            DiskCommitMsg::TarPacked { .. } => {}
        }
    }
    files.shutdown().await;
    Ok(())
}
