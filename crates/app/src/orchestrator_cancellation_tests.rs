//! 初始化取消覆盖配额等待、连接和退避；连接异常由原有客户端和共享作用域分类。

use std::{env, error::Error, fs, path::Path, result::Result as StdResult, time::Duration};

use tokio::{
    sync::Semaphore,
    task::yield_now,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::create_storage_pair_with_retry;
use crate::error::{AppError, StorageError};

type TestResult = StdResult<(), Box<dyn Error>>;

#[tokio::test]
async fn initialization_waiting_for_a_mount_slot_is_cancellable() -> TestResult {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("file"), b"retained source")?;
    let destination = root.path().join("never-created");
    let source_name = root.path().to_string_lossy();
    let destination_name = destination.to_string_lossy();
    let semaphore = Semaphore::new(0);
    let token = CancellationToken::new();
    let operation = create_storage_pair_with_retry(
        &semaphore,
        &source_name,
        &destination_name,
        None,
        "cancelled slot",
        Some(&token),
    );
    let trigger = async {
        yield_now().await;
        token.cancel();
    };
    let (result, ()) = timeout(Duration::from_millis(500), async { tokio::join!(operation, trigger) }).await?;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    assert!(!destination.exists());
    assert_eq!(fs::read(root.path().join("file"))?, b"retained source");
    Ok(())
}

#[tokio::test]
async fn unsuccessful_initialization_can_stop_before_exhausting_its_retry_waits() -> TestResult {
    let root = tempfile::tempdir()?;
    let source = root.path().join("missing-source");
    let destination = root.path().join("never-created");
    let semaphore = Semaphore::new(1);
    let token = CancellationToken::new();
    let source_name = source.to_string_lossy();
    let destination_name = destination.to_string_lossy();
    let operation = create_storage_pair_with_retry(
        &semaphore,
        &source_name,
        &destination_name,
        None,
        "cancelled initialization",
        Some(&token),
    );
    let trigger = async {
        sleep(Duration::from_millis(25)).await;
        token.cancel();
    };
    let (result, ()) = timeout(Duration::from_millis(500), async { tokio::join!(operation, trigger) }).await?;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    assert!(!source.exists());
    assert!(!destination.exists());
    assert_eq!(semaphore.available_permits(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-copy-worker-cancellation-local.py owned RustFS fixture"]
async fn real_storage_initialization_observes_cancellation_without_source_or_object_mutation() -> TestResult {
    let source = env::var("LAB_WORKER_SOURCE_ROOT")?;
    let destination = env::var("LAB_WORKER_TARGET_URL")?;
    let seen = env::var("LAB_WORKER_REQUEST_SEEN")?;
    let fault = env::var("LAB_WORKER_FAULT")?;
    let semaphore = Semaphore::new(1);
    let token = CancellationToken::new();
    let operation = create_storage_pair_with_retry(
        &semaphore,
        &source,
        &destination,
        None,
        "real S3 initialization",
        Some(&token),
    );
    let trigger = async {
        if fault == "cancel" {
            while !Path::new(&seen).exists() {
                sleep(Duration::from_millis(5)).await;
            }
            sleep(Duration::from_millis(250)).await;
            let sent = Instant::now();
            token.cancel();
            Some(sent)
        } else {
            None
        }
    };
    let started = Instant::now();
    let (result, sent) = timeout(Duration::from_secs(10), async { tokio::join!(operation, trigger) }).await?;
    println!(
        "real storage initialization fault={fault} elapsed_ms={} cancelled={}",
        started.elapsed().as_millis(),
        token.is_cancelled()
    );
    match fault.as_str() {
        "normal" => {
            result?;
        }
        "cancel" => {
            assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
            assert!(sent.ok_or("cancellation not sent")?.elapsed() < Duration::from_secs(1));
        }
        _ => return Err("unknown initialization fault".into()),
    }
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(fs::read(Path::new(&source).join("file"))?, b"retained source");
    Ok(())
}
