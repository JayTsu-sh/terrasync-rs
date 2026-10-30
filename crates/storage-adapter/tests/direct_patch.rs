#![allow(clippy::expect_used)]
#![cfg(unix)]
use bytes::Bytes;
use data_mover::transfer::DirectPatchError;
use std::{error::Error, os::unix::fs::MetadataExt, path::Path};
use storage_adapter::{
    CreateStorageOptions, DataChunk, EntryEnum, StorageEnum, StorageError, TransferMode, create_storage,
};
use tokio::sync::mpsc;

async fn planned(
    target: &StorageEnum, payload: &[u8],
) -> Result<(EntryEnum, storage_adapter::StreamHandle), Box<dyn Error>> {
    let mut entry = target.get_metadata(Path::new("file")).await?;
    if let EntryEnum::NAS(file) = &mut entry {
        file.size = payload.len() as u64;
    }
    let handle = StorageEnum::prepare_stream_with_mode(target, &entry, TransferMode::Direct).await?;
    let (tx, rx) = mpsc::channel(4);
    let writer_target = target.clone();
    let writer_entry = entry.clone();
    let writer_handle = handle.clone();
    let writer = tokio::spawn(async move {
        StorageEnum::write_chunk_stream(
            &writer_target,
            &writer_entry,
            rx,
            &writer_handle,
            None,
            std::sync::Arc::new(|_, _| {}),
        )
        .await
    });
    for (index, chunk) in payload.chunks(4).enumerate() {
        tx.send(DataChunk {
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
        let (entry, handle) = planned(&target, new).await?;
        assert_eq!(
            std::fs::read(root.path().join("file"))?,
            old,
            "planning must keep every basis byte intact"
        );
        StorageEnum::commit_chunk_stream(&target, &entry, new.len() as u64, handle.clone()).await?;
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
    let (entry, handle) = planned(&target, b"AAAAXXXX").await?;
    std::fs::write(root.path().join("file"), b"EXTERNAL")?;
    let error = StorageEnum::commit_chunk_stream(&target, &entry, 8, handle)
        .await
        .expect_err("changed basis must be rejected");
    assert!(matches!(
        error,
        StorageError::DirectPatch(DirectPatchError::BasisChanged(_))
    ));
    assert_eq!(std::fs::read(root.path().join("file"))?, b"EXTERNAL");
    Ok(())
}
