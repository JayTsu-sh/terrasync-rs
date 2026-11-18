//! 真实 Local 遍历的正常缓冲、取消背压及生产任务收束。
use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use data_mover::storage::NamespaceReadPolicy;
use tokio::{sync::Notify, task::yield_now, time::timeout};
use tokio_util::sync::CancellationToken;

use super::AsyncReceiver;
use crate::{
    config::initialize_scan_config,
    dir_walker::DirectoryWalker,
    error::{Result as AppResult, StorageError},
    scan::ScanType,
    scan::source::{WalkDirAsyncIterator, WalkOptions},
    storage_factory::{StorageRole, create_storage_for_role},
};

type TestResult = Result<(), Box<dyn Error>>;

async fn file_list_walk(
    count: usize, cancel: CancellationToken,
) -> AppResult<(tempfile::TempDir, WalkDirAsyncIterator)> {
    let root = tempfile::tempdir()?;
    let selection = root.path().join("selection.txt");
    let mut names = String::new();
    for index in 0..count {
        let name = format!("entry-{index}");
        std::fs::write(root.path().join(&name), b"data")?;
        names.push_str(&name);
        names.push('\n');
    }
    std::fs::write(&selection, names)?;
    let config = initialize_scan_config(
        "file-list-lifecycle",
        &root.path().to_string_lossy(),
        0,
        ScanType::Full,
        &None,
        &None,
        2,
        false,
        &Some(selection.to_string_lossy().into_owned()),
        false,
        0,
    )?;
    let iterator = DirectoryWalker::new(config)
        .with_cancellation(Some(cancel))
        .with_file_list_read_policy(NamespaceReadPolicy {
            operation_timeout: Duration::from_secs(1),
            recovery_window: Duration::from_secs(3),
            retry_interval: Duration::from_millis(10),
        })
        .walk()
        .await?;
    Ok((root, iterator))
}

async fn local_walk(count: usize, cancel: CancellationToken) -> AppResult<(tempfile::TempDir, WalkDirAsyncIterator)> {
    let root = tempfile::tempdir()?;
    for index in 0..count {
        std::fs::write(root.path().join(format!("entry-{index}")), b"data")?;
    }
    let storage = create_storage_for_role(&root.path().to_string_lossy(), None, false, StorageRole::Source).await?;
    let iterator = storage
        .walkdir(
            None,
            WalkOptions {
                cancel: Some(cancel),
                concurrency: 2,
                ..Default::default()
            },
        )
        .await?;
    Ok((root, iterator))
}

#[tokio::test]
async fn normal_producer_completion_preserves_all_buffered_entries() -> TestResult {
    let parent = CancellationToken::new();
    let (_root, iterator) = local_walk(17, parent.clone()).await?;
    timeout(Duration::from_secs(2), iterator.wait_for_producer()).await?;
    let mut count = 0;
    while iterator.next().await.is_some() {
        count += 1;
    }
    assert_eq!(count, 17);
    assert!(iterator.take_error().is_none());
    assert!(!parent.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn closing_backpressured_walk_drains_native_producer_without_cancelling_parent() -> TestResult {
    let parent = CancellationToken::new();
    let (_root, iterator) = local_walk(1024, parent.clone()).await?;
    timeout(Duration::from_secs(2), async {
        while !iterator.rx.is_full() {
            yield_now().await;
        }
    })
    .await?;
    iterator.close();
    assert!(iterator.next().await.is_none());
    timeout(Duration::from_secs(2), iterator.wait_for_producer()).await?;
    let error = iterator.take_error();
    assert!(error.as_ref().is_some_and(StorageError::is_cancelled), "{error:?}");
    assert!(!parent.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn parent_cancellation_stops_delivery_and_waits_for_local_producer() -> TestResult {
    let parent = CancellationToken::new();
    let (_root, iterator) = local_walk(1024, parent.clone()).await?;
    timeout(Duration::from_secs(2), async {
        while !iterator.rx.is_full() {
            yield_now().await;
        }
    })
    .await?;
    parent.cancel();
    assert!(iterator.next().await.is_none());
    timeout(Duration::from_secs(2), iterator.wait_for_producer()).await?;
    let error = iterator.take_error();
    assert!(error.as_ref().is_some_and(StorageError::is_cancelled), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn cancelling_completed_producer_does_not_acknowledge_discarded_buffer() -> TestResult {
    let parent = CancellationToken::new();
    let (_root, iterator) = local_walk(17, parent.clone()).await?;
    timeout(Duration::from_secs(2), iterator.wait_for_producer()).await?;
    parent.cancel();
    assert!(iterator.next().await.is_none());
    let error = iterator.take_error();
    assert!(error.as_ref().is_some_and(StorageError::is_cancelled), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn concurrent_waiters_both_wait_for_actual_producer_exit() -> TestResult {
    let parent = CancellationToken::new();
    let cancel = parent.child_token();
    let release = Arc::new(Notify::new());
    let task_release = release.clone();
    let task_cancel = cancel.clone();
    let (sender, receiver) = async_channel::bounded::<()>(1);
    let task = tokio::spawn(async move {
        task_cancel.cancelled().await;
        task_release.notified().await;
        drop(sender);
    });
    let iterator = AsyncReceiver::new(receiver).with_producer(task, cancel);
    iterator.close();
    let clone = iterator.clone();
    let mut first = Box::pin(iterator.wait_for_producer());
    let mut second = Box::pin(clone.wait_for_producer());
    assert!(timeout(Duration::from_millis(10), first.as_mut()).await.is_err());
    assert!(timeout(Duration::from_millis(10), second.as_mut()).await.is_err());
    release.notify_one();
    timeout(Duration::from_secs(1), first).await?;
    timeout(Duration::from_secs(1), second).await?;
    assert!(!parent.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn producer_panic_preserves_typed_join_failure() -> TestResult {
    let (sender, receiver) = async_channel::bounded::<()>(1);
    let task = tokio::spawn(async move {
        assert!(sender.send(()).await.is_ok());
        drop(sender);
        panic!("owned producer panic fixture");
    });
    let iterator = AsyncReceiver::new(receiver).with_producer(task, CancellationToken::new());
    timeout(Duration::from_secs(1), async {
        while !iterator.rx.is_closed() {
            yield_now().await;
        }
    })
    .await?;
    // 复现协调者实际顺序：关闭未交付缓冲，再 join 并读取原始原因。
    iterator.close();
    iterator.wait_for_producer().await;
    assert!(matches!(iterator.take_error(), Some(StorageError::TaskJoinError(_))));
    Ok(())
}

#[tokio::test]
async fn producer_panic_preserves_preceding_real_stream_failure() -> TestResult {
    let (sender, receiver) = async_channel::bounded::<()>(1);
    let task = tokio::spawn(async move {
        drop(sender);
        panic!("owned producer panic after observation failure");
    });
    let iterator = AsyncReceiver::new(receiver)
        .with_error(Arc::new(Mutex::new(Some(StorageError::FileNotFound(
            "selected".into(),
        )))))
        .with_producer(task, CancellationToken::new());
    iterator.close();
    iterator.wait_for_producer().await;
    match iterator.take_error() {
        Some(StorageError::StreamProducerFailed { source, preceding }) => {
            assert!(source.is_panic());
            assert!(matches!(*preceding, StorageError::FileNotFound(ref path) if path == "selected"));
        }
        error => panic!("expected both typed causes: {error:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn file_list_normal_eof_preserves_all_buffered_entries() -> TestResult {
    let parent = CancellationToken::new();
    let (_root, iterator) = file_list_walk(17, parent.clone()).await?;
    timeout(Duration::from_secs(2), iterator.wait_for_producer()).await?;
    let mut count = 0;
    while iterator.next().await.is_some() {
        count += 1;
    }
    assert_eq!(count, 17);
    assert!(iterator.take_error().is_none());
    assert!(!parent.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn file_list_backpressure_is_cancelled_and_joined_without_source_mutation() -> TestResult {
    let parent = CancellationToken::new();
    let (root, iterator) = file_list_walk(1100, parent.clone()).await?;
    timeout(Duration::from_secs(5), async {
        while !iterator.rx.is_full() {
            yield_now().await;
        }
    })
    .await?;
    parent.cancel();
    assert!(iterator.next().await.is_none());
    timeout(Duration::from_secs(1), iterator.wait_for_producer()).await?;
    let error = iterator.take_error();
    assert!(error.as_ref().is_some_and(StorageError::is_cancelled), "{error:?}");
    for index in 0..1100 {
        assert_eq!(std::fs::read(root.path().join(format!("entry-{index}")))?, b"data");
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_empty_file_list_is_not_successful_enumeration() -> TestResult {
    let parent = CancellationToken::new();
    parent.cancel();
    let (_root, iterator) = file_list_walk(0, parent).await?;
    assert!(iterator.next().await.is_none());
    timeout(Duration::from_secs(1), iterator.wait_for_producer()).await?;
    let error = iterator.take_error();
    assert!(error.as_ref().is_some_and(StorageError::is_cancelled), "{error:?}");
    Ok(())
}
