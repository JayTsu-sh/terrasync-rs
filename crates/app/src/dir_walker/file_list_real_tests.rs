//! 复用独占 NFS relay，验证文件列表点查询的恢复预算与在途取消。
use std::{env, time::Duration};

use data_mover::storage::{NamespaceReadFailure, NamespaceReadPolicy};
use task_model::StorageEntryMessage;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::walkdir_from_file_list;
use crate::{
    error::{AppError, Result, StorageError},
    storage_factory::{CreateStorageOptions, create_storage},
};

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn control(mode: &str) -> Result<()> {
    let address = environment("LAB_SOURCE_GUARD_CONTROL")?;
    let mut stream = TcpStream::connect(&address).await?;
    stream
        .write_all(format!("GET /{mode} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
        .await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    assert!(reply.starts_with(b"HTTP/1.0 204"));
    Ok(())
}

async fn case(mode: &str) -> Result<()> {
    control("normal").await?;
    let source = create_storage(
        &environment("LAB_SOURCE_GUARD_NFS_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let root = tempfile::tempdir()?;
    let paths = root.path().join("selection.txt");
    std::fs::write(&paths, format!("file-list-{mode}\n"))?;
    let cancel = CancellationToken::new();
    let stopping = cancel.clone();
    let cancel_during_request = mode == "cancel";
    control(mode).await?;
    let started = Instant::now();
    let watcher = tokio::spawn(async move {
        if cancel_during_request {
            sleep(Duration::from_millis(250)).await;
            stopping.cancel();
        }
    });
    let work = async {
        let iterator = walkdir_from_file_list(
            source,
            &paths.to_string_lossy(),
            None,
            NamespaceReadPolicy {
                operation_timeout: Duration::from_secs(1),
                recovery_window: Duration::from_secs(3),
                retry_interval: Duration::from_millis(100),
            },
            cancel,
        )?;
        let mut scanned = 0;
        while let Some(message) = iterator.next().await {
            if matches!(message, StorageEntryMessage::Scanned(_)) {
                scanned += 1;
            }
        }
        iterator.wait_for_producer().await;
        Ok::<_, AppError>((scanned, iterator.take_error()))
    };
    let result = timeout(Duration::from_secs(8), work).await;
    let elapsed = started.elapsed();
    control("normal").await?;
    watcher.await?;
    let (scanned, failure) =
        result.map_err(|_| AppError::ConfigError("NFS file-list observation exceeded owned deadline".into()))??;
    eprintln!(
        "real NFS file-list mode={mode} elapsed_ms={} scanned={scanned} failure={failure:?}",
        elapsed.as_millis()
    );
    if mode == "flash" {
        assert_eq!(scanned, 1);
        assert!(failure.is_none());
    } else {
        assert_eq!(scanned, 0);
        assert!(matches!(failure, Some(StorageError::NamespaceRead(_))), "{failure:?}");
        if mode == "cancel" {
            assert!(matches!(
                failure,
                Some(StorageError::NamespaceRead(NamespaceReadFailure::Cancelled { .. }))
            ));
            assert!(elapsed < Duration::from_secs(1));
        } else {
            assert!(elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(5));
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-source-metadata-guard.py --file-list and owned prepared NFS prefix"]
async fn file_list_nfs_read_recovers_stops_and_cancels() -> Result<()> {
    for mode in ["flash", "disconnect", "stall", "cancel"] {
        case(mode).await?;
    }
    Ok(())
}
