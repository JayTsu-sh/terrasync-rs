#![allow(clippy::expect_used)]
#![cfg(unix)]
use crate::error::StorageError;
use crate::file_commit::stream as commit_stream;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};
use bytes::Bytes;
use data_mover::model::ObservedEntry;
use data_mover::storage::PositionedChunk;
use data_mover::transfer::DirectPatchError;
use std::{error::Error, os::unix::fs::MetadataExt, path::Path};
use task_model::TransferMode;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

async fn planned(
    target: &StorageContext, payload: &[u8],
) -> Result<(ObservedEntry, crate::file_commit::stream::StreamHandle), Box<dyn Error>> {
    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), payload)?;
    let source = create_storage(
        source_root.path().to_str().ok_or("bad source path")?,
        CreateStorageOptions::new(None, false),
    )
    .await?;
    let entry = observe_path(source.storage(), Path::new("file")).await?;
    let handle =
        commit_stream::prepare_stream_with_mode(target, &entry, TransferMode::Direct, CancellationToken::new()).await?;
    let (tx, rx) = mpsc::channel(4);
    let writer_handle = handle.clone();
    let writer = tokio::spawn(async move {
        commit_stream::write_chunk_stream(rx, &writer_handle, std::sync::Arc::new(|_, _| {})).await
    });
    for (index, chunk) in payload.chunks(4).enumerate() {
        tx.send(PositionedChunk {
            offset: (index * 4) as u64,
            data: Bytes::copy_from_slice(chunk),
        })
        .await?;
    }
    drop(tx);
    writer.await??;
    Ok((entry, handle))
}

#[tokio::test]
async fn direct_only_writes_changed_ranges_and_handles_moves_growth_and_shrinkage() -> Result<(), Box<dyn Error>> {
    for (old, new, changed) in [
        (b"AAAABBBBCCCCDDDD".as_slice(), b"AAAAXXXXCCCCDDDD".as_slice(), 4),
        (b"AAAABBBBCCCCDDDD".as_slice(), b"BBBBAAAACCCCDDDD".as_slice(), 8),
        (b"AAAABBBB".as_slice(), b"AAAABBBBCCCC".as_slice(), 4),
        (b"AAAABBBBCCCC".as_slice(), b"AAAA".as_slice(), 0),
    ] {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), old)?;
        let inode = std::fs::metadata(root.path().join("file"))?.ino();
        let target = create_storage(
            root.path().to_str().ok_or("bad path")?,
            CreateStorageOptions::new(None, false),
        )
        .await?;
        let (_entry, handle) = planned(&target, new).await?;
        assert_eq!(
            std::fs::read(root.path().join("file"))?,
            old,
            "planning must keep every basis byte intact"
        );
        commit_stream::commit_chunk_stream(handle.clone()).await?;
        assert_eq!(std::fs::read(root.path().join("file"))?, new);
        assert_eq!(std::fs::metadata(root.path().join("file"))?.ino(), inode);
        assert_eq!(handle.written_bytes().await, changed);
    }
    Ok(())
}

#[tokio::test]
async fn interrupted_direct_and_concurrent_basis_change_do_not_apply_planned_patches() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"AAAABBBB")?;
    let target = create_storage(
        root.path().to_str().ok_or("bad path")?,
        CreateStorageOptions::new(None, false),
    )
    .await?;
    let (_, handle) = planned(&target, b"AAAAXXXX").await?;
    handle.discard().await?;
    assert_eq!(std::fs::read(root.path().join("file"))?, b"AAAABBBB");
    let (_entry, handle) = planned(&target, b"AAAAXXXX").await?;
    std::fs::write(root.path().join("file"), b"EXTERNAL")?;
    let error = commit_stream::commit_chunk_stream(handle)
        .await
        .expect_err("changed basis must be rejected");
    assert!(matches!(
        error,
        StorageError::DirectPatch(DirectPatchError::BasisChanged(_))
    ));
    assert_eq!(std::fs::read(root.path().join("file"))?, b"EXTERNAL");
    Ok(())
}

#[tokio::test]
async fn cancelled_direct_commit_preserves_basis_and_parent_is_not_cancelled_by_discard() -> Result<(), Box<dyn Error>>
{
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"AAAABBBB")?;
    let target = create_storage(
        root.path().to_str().ok_or("bad path")?,
        CreateStorageOptions::new(None, false),
    )
    .await?;
    let (entry, handle) = planned(&target, b"AAAAXXXX").await?;
    handle.cancel();
    let error = commit_stream::commit_chunk_stream(handle)
        .await
        .expect_err("取消后不得应用已经接收的补丁");
    assert!(error.is_cancelled());
    assert!(matches!(error, StorageError::DirectPatch(DirectPatchError::Cancelled)));
    assert_eq!(std::fs::read(root.path().join("file"))?, b"AAAABBBB");

    let parent = CancellationToken::new();
    let handle = commit_stream::prepare_stream_with_mode(&target, &entry, TransferMode::Direct, parent.clone()).await?;
    handle.discard().await?;
    assert!(!parent.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn caller_cancellation_interrupts_direct_writer_waiting_for_input() -> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"AAAABBBB")?;
    let target = create_storage(
        root.path().to_str().ok_or("bad path")?,
        CreateStorageOptions::new(None, false),
    )
    .await?;
    let entry = observe_path(target.storage(), Path::new("file")).await?;
    let cancel = CancellationToken::new();
    let handle = commit_stream::prepare_stream_with_mode(&target, &entry, TransferMode::Direct, cancel.clone()).await?;
    let (_tx, rx) = mpsc::channel(1);
    let writing = commit_stream::write_planned_stream(rx, &handle);
    let (result, ()) = tokio::join!(timeout(Duration::from_secs(1), writing), async {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    assert!(matches!(
        result?,
        Err(StorageError::DirectPatch(DirectPatchError::Cancelled))
    ));
    handle.discard().await?;
    assert_eq!(std::fs::read(root.path().join("file"))?, b"AAAABBBB");
    Ok(())
}
