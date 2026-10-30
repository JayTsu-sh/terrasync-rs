#![cfg(unix)]
//! 通过旧目标的硬链接观察实际写入语义，防止显式模式被静默替换。
use std::{error::Error, os::unix::fs::MetadataExt, path::Path};
use storage_adapter::{CopyOptions, CreateStorageOptions, StorageEnum, TransferMode, create_storage};

#[tokio::test]
async fn direct_updates_existing_inode_while_atomic_and_checkpoint_replace_it() -> Result<(), Box<dyn Error>> {
    for mode in [TransferMode::Direct, TransferMode::Atomic, TransferMode::Checkpoint] {
        for size in [17, 256 * 1024 + 13] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            let payload = vec![0x5a; size];
            std::fs::write(source_root.path().join("file"), &payload)?;
            std::fs::write(target_root.path().join("file"), b"old")?;
            std::fs::hard_link(target_root.path().join("file"), target_root.path().join("basis"))?;
            let inode = std::fs::metadata(target_root.path().join("file"))?.ino();
            let source = create_storage(
                source_root.path().to_str().ok_or("invalid path")?,
                CreateStorageOptions::new(Some(64 * 1024), false),
            )
            .await?;
            let target = create_storage(
                target_root.path().to_str().ok_or("invalid path")?,
                CreateStorageOptions::new(Some(64 * 1024), false),
            )
            .await?;
            let entry = source.get_metadata(Path::new("file")).await?;
            StorageEnum::copy_file(
                &source,
                &target,
                &entry,
                CopyOptions {
                    transfer_mode: mode,
                    is_source_reserved: true,
                    enable_integrity_check: true,
                    ..Default::default()
                },
            )
            .await?;
            assert_eq!(std::fs::read(target_root.path().join("file"))?, payload);
            let new_inode = std::fs::metadata(target_root.path().join("file"))?.ino();
            if mode == TransferMode::Direct {
                assert_eq!(new_inode, inode);
                assert_eq!(std::fs::read(target_root.path().join("basis"))?, payload);
            } else {
                assert_ne!(new_inode, inode);
                assert_eq!(std::fs::read(target_root.path().join("basis"))?, b"old");
            }
        }
    }
    Ok(())
}
