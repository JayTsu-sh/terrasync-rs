//! 仅显式运行：生产Checkpoint模式、真实中断及持久化前缀复用，不提供扫描续扫。
use std::{
    error::Error,
    fs::{File, FileTimes, Permissions},
    io::{Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::Path,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use data_mover::storage::artifacts::{ArtifactKind, artifact_name, is_artifact_name};
use task_model::StorageEntryMessage;

use super::*;
use crate::consumer::stats::delivery::tests::{Reply, server};

#[path = "checkpoint_destination.rs"]
mod checkpoint_destination;

const FILE_SIZE: u64 = 512 * 1024 * 1024 + 17;
const HOLD_AT: u64 = 384 * 1024 * 1024;
const OLD_FINAL: &[u8] = b"existing final survives interrupted checkpoint";

fn source_metadata(root: &Path) -> TestResult {
    let future = SystemTime::now() + Duration::from_secs(3600);
    for (name, mode, modified) in [
        ("dir/payload", 0o640, 1_700_000_000_123_456_789),
        ("dir", 0o751, 1_700_000_000_223_456_789),
        ("", 0o750, 1_700_000_000_323_456_789),
    ] {
        let path = root.join(name);
        std::fs::set_permissions(&path, Permissions::from_mode(mode))?;
        File::open(path)?.set_times(
            FileTimes::new()
                .set_accessed(future)
                .set_modified(UNIX_EPOCH + Duration::from_nanos(modified)),
        )?;
    }
    Ok(())
}

fn fixture(source: &Path, target: &Path, large: bool) -> TestResult {
    std::fs::create_dir(source.join("dir"))?;
    if large {
        let mut payload = File::create(source.join("dir/payload"))?;
        payload.set_len(FILE_SIZE)?;
        for index in 0..8 {
            payload.seek(SeekFrom::Start(index * 64 * 1024 * 1024))?;
            payload.write_all(format!("checkpoint-pattern-{index}").as_bytes())?;
        }
        payload.seek(SeekFrom::End(-17))?;
        payload.write_all(b"checkpoint-tail!!")?;
        payload.sync_all()?;
    } else {
        std::fs::write(source.join("dir/payload"), PAYLOAD)?;
        symlink(LINK_TARGET, source.join("link"))?;
    }
    source_metadata(source)?;
    std::fs::create_dir(target.join("dir"))?;
    std::fs::set_permissions(target.join("dir"), Permissions::from_mode(0o777))?;
    if large {
        std::fs::write(target.join("dir/payload"), OLD_FINAL)?;
    }
    Ok(())
}

fn inputs(source: &Path, target: &Path, callback: CallbackEndpoint) -> Inputs {
    Inputs {
        case: Case::Normal,
        run: format!("nightly-main-nfs-{}", Uuid::new_v4()),
        source_url: source.to_string_lossy().into_owned(),
        target_url: target.to_string_lossy().into_owned(),
        callback,
        control: None,
    }
}

fn sha256(path: &Path) -> TestResult<String> {
    let output = Command::new("sha256sum").arg("--").arg(path).output()?;
    assert!(output.status.success(), "real sha256sum failed");
    let text = std::str::from_utf8(&output.stdout)?;
    let digest = text.split_whitespace().next().ok_or("missing SHA256 output")?;
    assert_eq!(digest.len(), 64);
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    Ok(digest.to_owned())
}

#[derive(Debug, PartialEq, Eq)]
struct BaselineMetadata {
    mode: u32,
    uid: u32,
    gid: u32,
    accessed: SystemTime,
    modified: SystemTime,
}
fn metadata(path: &Path) -> TestResult<BaselineMetadata> {
    let value = std::fs::metadata(path)?;
    Ok(BaselineMetadata {
        mode: value.permissions().mode() & 0o7777,
        uid: value.uid(),
        gid: value.gid(),
        accessed: value.accessed()?,
        modified: value.modified()?,
    })
}

fn typed_cancellation(error: &AppError) -> bool {
    let mut cause: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(error) = cause {
        if error
            .downcast_ref::<StorageError>()
            .is_some_and(StorageError::is_cancelled)
        {
            return true;
        }
        if error
            .downcast_ref::<StorageRoleFailure>()
            .is_some_and(|failure| match failure {
                StorageRoleFailure::Entry(failure) => failure.class() == FailureClass::Cancelled,
                StorageRoleFailure::Session(failure) => failure.class() == FailureClass::Cancelled,
            })
        {
            return true;
        }
        cause = error.source();
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit controlled Checkpoint-mode directory/Unicode-link/HTTP204 component acceptance"]
async fn local_controlled_checkpoint_mode_finishes_directories_and_unicode_link() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    fixture(source.path(), target.path(), false)?;
    let callback = server(vec![Reply::NoContent; 8]).await;
    let input = inputs(source.path(), target.path(), callback.endpoint.clone());
    let src = create_storage(&input.source_url, CreateStorageOptions::default()).await?;
    let expected = src
        .storage()
        .namespace()?
        .stat(&StoragePath::new("dir")?, false)
        .await?;
    let dst = create_storage(&input.target_url, CreateStorageOptions::default()).await?;
    let mut outcome =
        exercise_with_transfer(&input, src, dst, TransferMode::Checkpoint, CancellationToken::new()).await?;
    check_outcome(&input, &mut outcome);
    inspect_normal(&input, &expected).await?;
    for name in ["dir/payload", "dir", ""] {
        assert_eq!(
            metadata(&source.path().join(name))?,
            metadata(&target.path().join(name))?
        );
    }
    println!(
        "MAIN_CHECKPOINT_NORMAL {}",
        json!({"configured_mode":"Checkpoint", "scan_checkpoint":false, "small_file_does_not_prove_physical_checkpoint":true, "admitted":outcome.admitted})
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 512MiB actual Local durable checkpoint interruption and resumed destination write proof"]
async fn local_controlled_checkpoint_interruption_reuses_persisted_prefix() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    fixture(source.path(), target.path(), true)?;
    let before = metadata(&source.path().join("dir/payload"))?;
    let expected_sha = sha256(&source.path().join("dir/payload"))?;
    assert_eq!(
        metadata(&source.path().join("dir/payload"))?,
        before,
        "this fixture requires source atime to remain stable on its actual filesystem"
    );
    let callback = server(vec![Reply::NoContent; 16]).await;
    let input = inputs(source.path(), target.path(), callback.endpoint.clone());
    let first_cancel = CancellationToken::new();
    let src = create_storage(&input.source_url, CreateStorageOptions::default()).await?;
    let mut dst = create_storage(&input.target_url, CreateStorageOptions::default()).await?;
    let first_facts = checkpoint_destination::install(&mut dst, first_cancel.clone(), true)?;
    let first = exercise_with_transfer(&input, src, dst, TransferMode::Checkpoint, first_cancel).await?;
    assert!(
        first.result.as_ref().is_err_and(typed_cancellation),
        "{:?}",
        first.result
    );
    assert!(!first.root_reopened);
    assert!(
        !first
            .events
            .iter()
            .any(|event| matches!(event, StorageEntryMessage::New(_)))
    );
    assert_eq!(full(&first.after).error_stats.total(), 1);
    assert_eq!(std::fs::read(target.path().join("dir/payload"))?, OLD_FINAL);
    assert_eq!(metadata(&source.path().join("dir/payload"))?, before);
    let confirmed = first_facts.durable_prefix.load(Ordering::SeqCst);
    assert!(confirmed > 0 && confirmed < FILE_SIZE);
    assert!(first_facts.input_held.load(Ordering::SeqCst));
    assert_eq!(first_facts.checkpointed_writes.load(Ordering::SeqCst), 1);
    assert!(first_facts.actual_forwarded_bytes.load(Ordering::SeqCst) < FILE_SIZE);
    let pointer = target
        .path()
        .join("dir")
        .join(artifact_name("payload", ArtifactKind::Pointer));
    assert!(std::fs::metadata(&pointer)?.len() > 0);
    let pointer_sha = sha256(&pointer)?;
    let first_error = first.result.as_ref().err().unwrap().to_string();
    let first_report = first.after.report.clone();
    // 旧执行持有的真实stage/claim随错误一起释放；不带着旧lease启动下一执行。
    drop(first);
    let second_cancel = CancellationToken::new();
    let src = create_storage(&input.source_url, CreateStorageOptions::default()).await?;
    let mut dst = create_storage(&input.target_url, CreateStorageOptions::default()).await?;
    let second_facts = checkpoint_destination::install(&mut dst, second_cancel.clone(), false)?;
    let mut second = exercise_with_transfer(&input, src, dst, TransferMode::Checkpoint, second_cancel).await?;
    assert!(second.result.is_ok(), "{:?}", second.result);
    assert_eq!(second.admitted, 2);
    assert!(second.root_reopened);
    assert_eq!(full(&second.after).error_stats.total(), 0);
    assert_eq!(full(&second.after).regular_file_count, 1);
    assert_eq!(full(&second.after).dir_count, 1);
    assert!(second.after.callback.outbox.prepare_batch().is_none());
    let first_prepare = first_facts.prepares.lock().unwrap()[0].clone();
    let second_prepare = second_facts.prepares.lock().unwrap()[0].clone();
    assert_eq!(first_prepare.transfer_identity, second_prepare.transfer_identity);
    assert_eq!(first_prepare.recovery_binding, second_prepare.recovery_binding);
    assert!(second_prepare.resumed && second_prepare.restored, "{second_prepare:?}");
    assert!(
        second_prepare.offset >= confirmed && second_prepare.offset < FILE_SIZE,
        "{second_prepare:?}"
    );
    assert!(second_prepare.reused_bytes > 0);
    let actual_new_bytes = second_facts.actual_forwarded_bytes.load(Ordering::SeqCst);
    assert!(actual_new_bytes > 0 && actual_new_bytes < FILE_SIZE);
    assert!(actual_new_bytes <= FILE_SIZE - second_prepare.offset);
    assert_eq!(
        actual_new_bytes + second_prepare.reused_bytes,
        FILE_SIZE,
        "real forwarded suffix plus observed reused ranges must cover the file exactly"
    );
    assert_eq!(std::fs::metadata(target.path().join("dir/payload"))?.len(), FILE_SIZE);
    assert_eq!(sha256(&target.path().join("dir/payload"))?, expected_sha);
    assert_eq!(sha256(&source.path().join("dir/payload"))?, expected_sha);
    for name in ["dir/payload", "dir", ""] {
        assert_eq!(
            metadata(&source.path().join(name))?,
            metadata(&target.path().join(name))?
        );
    }
    for name in ["", "dir"] {
        assert!(
            std::fs::read_dir(target.path().join(name))?.all(|entry| !is_artifact_name(&entry.unwrap().file_name()))
        );
    }
    let fs = Command::new("stat")
        .args(["-f", "-c", "%T", "--"])
        .arg(source.path())
        .output()?;
    assert!(fs.status.success());
    println!(
        "MAIN_CHECKPOINT_RESUME {}",
        json!({
            "source_size":FILE_SIZE, "held_input_from":HOLD_AT, "advertised_automatic_interval":first_facts.advertised_interval,
            "real_pointer_prefix_before_cancel":confirmed, "pointer_sha256_before_retry":pointer_sha,
            "cancel_after_write_start_ms":first_facts.cancellation_millis.load(Ordering::SeqCst),
            "first_prepare":first_prepare, "resumed_prepare":second_prepare,
            "first_forwarded_destination_bytes":first_facts.actual_forwarded_bytes.load(Ordering::SeqCst),
            "second_forwarded_destination_bytes":actual_new_bytes, "final_source_and_target_sha256":expected_sha,
            "first_error":first_error, "first_report":first_report, "second_report":second.after.report,
            "filesystem_type":String::from_utf8_lossy(&fs.stdout).trim(), "source_future_atime_verified_stable":true,
            "scan_checkpoint":false, "component_not_cli":true, "retained_source_not_delete_proof":true,
            "no_checkpoint_artifacts_after_success":true
        })
    );
    Ok(())
}
