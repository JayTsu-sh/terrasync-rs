use bytes::Bytes;
use std::{path::Path, sync::Arc};
use storage_adapter::{
    CopyOptions, CreateStorageOptions, DataChunk, EntryEnum, StorageEntryMessage, StorageEnum, TarPackOptions,
    WalkOptions, create_storage,
};
use tempfile::TempDir;

async fn local(root: &Path) -> storage_adapter::Result<StorageEnum> {
    create_storage(
        root.to_str()
            .ok_or_else(|| storage_adapter::StorageError::InvalidPath("test root".into()))?,
        CreateStorageOptions::new(Some(64 * 1024), false),
    )
    .await
}
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn opaque_scan_checkpoint_survives_reconnection_without_skipping_entries() -> TestResult {
    use std::{collections::BTreeSet, time::Duration};
    use storage_adapter::ResumableScanItem;
    use tokio_util::sync::CancellationToken;
    let root = TempDir::new()?;
    let mut expected = BTreeSet::new();
    for index in 0..30 {
        let dir = format!("dir{index:03}");
        std::fs::create_dir(root.path().join(&dir))?;
        expected.insert(dir.clone());
        for name in ["a", "b"] {
            let path = format!("{dir}/{name}");
            std::fs::write(root.path().join(&path), b"value")?;
            expected.insert(path);
        }
    }
    let source = local(root.path()).await?;
    let options = WalkOptions {
        concurrency: 1,
        ..Default::default()
    };
    let mut scan = source.resumable_scan(&options, None, CancellationToken::new(), Duration::ZERO)?;
    let mut seen = BTreeSet::new();
    let mut saved = None;
    while let Some(item) = scan.next().await {
        match item? {
            ResumableScanItem::Entries(entries) => seen.extend(
                entries
                    .iter()
                    .map(|entry| entry.get_relative_path().to_string_lossy().into_owned()),
            ),
            ResumableScanItem::Checkpoint(bytes) if !seen.is_empty() => {
                saved = Some(bytes);
                break;
            }
            _ => {}
        }
    }
    let checkpoint = saved.ok_or("no checkpoint emitted")?;
    drop(scan);
    drop(source);
    let source = local(root.path()).await?;
    let mut resumed = source.resumable_scan(
        &options,
        Some(checkpoint.clone()),
        CancellationToken::new(),
        Duration::ZERO,
    )?;
    while let Some(item) = resumed.next().await {
        if let ResumableScanItem::Entries(entries) = item? {
            seen.extend(
                entries
                    .iter()
                    .map(|entry| entry.get_relative_path().to_string_lossy().into_owned()),
            );
        }
    }
    assert!(matches!(
        resumed.finish().await?,
        data_mover::traversal::TraversalOutcome::Completed(_)
    ));
    assert_eq!(seen, expected);
    let changed = WalkOptions {
        depth: Some(1),
        ..options
    };
    let mut rejected = source.resumable_scan(&changed, Some(checkpoint), CancellationToken::new(), Duration::ZERO)?;
    assert!(rejected.next().await.is_none());
    assert!(rejected.finish().await.is_err());
    Ok(())
}

#[tokio::test]
async fn scan_filter_and_depth_are_applied_by_role_traversal() -> TestResult {
    let root = TempDir::new()?;
    tokio::fs::create_dir_all(root.path().join("nested/deeper")).await?;
    tokio::fs::write(root.path().join("a.txt"), b"one").await?;
    tokio::fs::write(root.path().join("nested/b.txt"), b"two").await?;
    tokio::fs::write(root.path().join("nested/deeper/c.txt"), b"three").await?;
    let storage = local(root.path()).await?;
    let iter = storage
        .walkdir(
            None,
            WalkOptions {
                depth: Some(2),
                match_expressions: Some(storage_adapter::filter::parse_filter_expression(
                    "extension == \"txt\"",
                )?),
                ..Default::default()
            },
        )
        .await?;
    let mut paths = Vec::new();
    while let Some(message) = iter.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => paths.push(entry.get_relative_path().to_path_buf()),
            other => return Err(format!("unexpected event: {other:?}").into()),
        }
    }
    paths.sort();
    assert_eq!(paths, vec![Path::new("a.txt"), Path::new("nested/b.txt")]);
    Ok(())
}
#[tokio::test]
async fn role_copy_replaces_shorter_file_and_move_removes_only_verified_source() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    tokio::fs::write(source.path().join("payload"), b"new").await?;
    tokio::fs::write(destination.path().join("payload"), b"old-long-data").await?;
    let src = local(source.path()).await?;
    let dst = local(destination.path()).await?;
    let entry = src.get_metadata(Path::new("payload")).await?;
    StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        CopyOptions {
            is_source_reserved: true,
            enable_integrity_check: true,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(tokio::fs::read(destination.path().join("payload")).await?, b"new");
    assert!(source.path().join("payload").exists());
    StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        CopyOptions {
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await?;
    assert!(!source.path().join("payload").exists());
    Ok(())
}
#[tokio::test]
async fn streamed_destination_is_published_only_after_complete() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let payload = vec![0x59; 192 * 1024 + 7];
    tokio::fs::write(source.path().join("payload"), &payload).await?;
    tokio::fs::write(destination.path().join("payload"), b"old").await?;
    let src = local(source.path()).await?;
    let dst = local(destination.path()).await?;
    let entry = src.get_metadata(Path::new("payload")).await?;
    let handle = StorageEnum::prepare_stream(&dst, &entry).await?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let writer_dst = dst.clone();
    let writer_entry = entry.clone();
    let writer_handle = handle.clone();
    let writer = tokio::spawn(async move {
        StorageEnum::write_chunk_stream(
            &writer_dst,
            &writer_entry,
            rx,
            &writer_handle,
            None,
            Arc::new(|_, _| {}),
        )
        .await
    });
    for (index, chunk) in payload.chunks(64 * 1024).enumerate() {
        tx.send(DataChunk {
            offset: (index * 64 * 1024) as u64,
            data: Bytes::copy_from_slice(chunk),
        })
        .await?;
    }
    drop(tx);
    writer.await??;
    assert_eq!(tokio::fs::read(destination.path().join("payload")).await?, b"old");
    assert_eq!(handle.received_size().await, payload.len() as u64);
    StorageEnum::commit_chunk_stream(&dst, &entry, payload.len() as u64, handle).await?;
    assert_eq!(tokio::fs::read(destination.path().join("payload")).await?, payload);
    Ok(())
}
#[tokio::test]
async fn short_stream_cannot_replace_an_existing_destination() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    tokio::fs::write(source.path().join("payload"), b"complete").await?;
    tokio::fs::write(destination.path().join("payload"), b"old").await?;
    let src = local(source.path()).await?;
    let dst = local(destination.path()).await?;
    let entry = src.get_metadata(Path::new("payload")).await?;
    let handle = StorageEnum::prepare_stream(&dst, &entry).await?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::from_static(b"short"),
    })
    .await?;
    drop(tx);
    assert!(
        StorageEnum::write_chunk_stream(&dst, &entry, rx, &handle, None, Arc::new(|_, _| {}))
            .await
            .is_err()
    );
    assert_eq!(tokio::fs::read(destination.path().join("payload")).await?, b"old");
    handle.discard().await?;
    Ok(())
}
#[tokio::test]
async fn tar_uses_checked_pax_size_and_bounded_source_reads() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let name = format!("{}.bin", "long".repeat(35));
    tokio::fs::write(source.path().join(&name), b"archive").await?;
    let src = local(source.path()).await?;
    let dst = local(destination.path()).await?;
    let entry = Arc::new(src.get_metadata(Path::new(&name)).await?);
    let size = StorageEnum::pack_files_to_tar(
        &src,
        &dst,
        &[entry],
        Path::new("packed.tar"),
        0,
        TarPackOptions::default(),
    )
    .await?;
    let archive = tokio::fs::read(destination.path().join("packed.tar")).await?;
    assert_eq!(size, archive.len() as u64);
    assert!(size > 2048);
    assert!(archive.windows(name.len()).any(|window| window == name.as_bytes()));
    assert!(archive.windows(7).any(|window| window == b"archive"));
    Ok(())
}
#[tokio::test]
async fn namespace_symlinks_and_recursive_delete_use_storage_roles() -> TestResult {
    let root = TempDir::new()?;
    tokio::fs::create_dir_all(root.path().join("a/b")).await?;
    tokio::fs::write(root.path().join("a/b/data"), b"keep target").await?;
    let storage = local(root.path()).await?;
    let mut link = storage.get_metadata(Path::new("a/b/data")).await?;
    if let EntryEnum::NAS(ref mut entry) = link {
        entry.relative_path = "a/link".into();
        entry.name = "link".into();
        entry.is_symlink = true;
    }
    storage.create_symlink(&link, Path::new("b/data")).await?;
    assert_eq!(storage.read_symlink(&link).await?, Path::new("b/data"));
    let directory = storage.get_metadata(Path::new("a")).await?;
    storage.delete_dir_all(&directory).await?;
    assert!(!root.path().join("a").exists());
    Ok(())
}

#[tokio::test]
async fn packaged_date_directory_stops_traversal_at_requested_package_depth() -> TestResult {
    let root = TempDir::new()?;
    tokio::fs::create_dir_all(root.path().join("archive/20250101/child")).await?;
    tokio::fs::write(root.path().join("archive/20250101/child/data"), b"payload").await?;
    let storage = local(root.path()).await?;
    for (depth, expected) in [(0, "archive/20250101"), (1, "archive/20250101/child")] {
        let iter = storage
            .walkdir(
                None,
                WalkOptions {
                    packaged: true,
                    package_depth: depth,
                    match_expressions: Some(storage_adapter::filter::parse_filter_expression(
                        "dir_date <= 20260101",
                    )?),
                    ..Default::default()
                },
            )
            .await?;
        let mut packaged = Vec::new();
        while let Some(message) = iter.next().await {
            match message {
                StorageEntryMessage::Packaged(entry) => packaged.push(entry.get_relative_path().to_path_buf()),
                StorageEntryMessage::Scanned(entry) => assert!(!entry.get_is_regular_file()),
                other => return Err(format!("unexpected package event: {other:?}").into()),
            }
        }
        assert_eq!(packaged, vec![Path::new(expected)]);
    }
    Ok(())
}

#[tokio::test]
async fn caller_offset_records_cannot_authorize_destination_resume() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let payload = vec![0x39; 256 * 1024 + 13];
    tokio::fs::write(source.path().join("payload"), &payload).await?;
    let src = local(source.path()).await?;
    let dst = local(destination.path()).await?;
    let entry = src.get_metadata(Path::new("payload")).await?;
    assert!(
        entry.get_file_handle().is_none(),
        "local runtime identity must not become a cross-scan rename key"
    );
    let resume = storage_adapter::ResumeContext {
        part_relative_path: "untrusted.part".into(),
        missing_intervals: Vec::new(),
        on_committed: Arc::new(|_, _| {}),
    };
    StorageEnum::copy_file_resumable(
        &src,
        &dst,
        &entry,
        CopyOptions {
            is_source_reserved: true,
            ..Default::default()
        },
        resume,
    )
    .await?;
    assert_eq!(tokio::fs::read(destination.path().join("payload")).await?, payload);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn observed_zero_mode_is_preserved_instead_of_becoming_unknown() -> TestResult {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir()?;
    let file = root.path().join("zero-mode");
    std::fs::write(&file, b"payload")?;
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000))?;
    let storage = local(root.path()).await?;
    let entry = storage.get_metadata(Path::new("zero-mode")).await?;
    assert_eq!(entry.get_mode().map(|mode| mode & 0o7777), Some(0));
    Ok(())
}
