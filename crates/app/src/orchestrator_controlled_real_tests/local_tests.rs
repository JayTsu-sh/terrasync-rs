//! 本地真实文件离线验证阶段屏障、生产guard和实际HTTP回调；不冒充NFS协议验收。
use std::{
    fs::{File, FileTimes, Metadata, Permissions},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use super::*;
use crate::consumer::stats::{
    ProgressReport,
    delivery::tests::{Reply, server},
};

#[derive(Debug, PartialEq, Eq)]
struct Attributes {
    inode: u64,
    mode: u32,
    modified: SystemTime,
}
impl From<Metadata> for Attributes {
    fn from(metadata: Metadata) -> Self {
        Self {
            inode: metadata.ino(),
            mode: metadata.permissions().mode() & 0o7777,
            modified: metadata.modified().unwrap(),
        }
    }
}
struct Replacement {
    old: Attributes,
    new: Attributes,
}

async fn request_body(stream: &mut TcpStream) -> TestResult<(String, serde_json::Value)> {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).await?;
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
        assert!(bytes.len() <= 65_536);
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            let header = std::str::from_utf8(&bytes[..end])?;
            let length: usize = header
                .lines()
                .find_map(|line| line.strip_prefix("Content-Length: "))
                .ok_or("missing phase length")?
                .parse()?;
            if bytes.len() >= end + 4 + length {
                return Ok((
                    header.to_owned(),
                    serde_json::from_slice(&bytes[end + 4..end + 4 + length])?,
                ));
            }
        }
    }
}

async fn replacement_control(
    case: Case, run: &str, target: PathBuf,
) -> TestResult<(phase::Control, JoinHandle<TestResult<Replacement>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let control = phase::Control {
        address: listener.local_addr()?,
        run: run.to_owned(),
        token: Uuid::new_v4().simple().to_string(),
        case,
    };
    let expected = control.clone();
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let (header, body) = request_body(&mut stream).await?;
        assert!(header.starts_with("POST /phase HTTP/1.1\r\n"));
        assert!(header.contains(&format!("X-Run-Token: {}", expected.token)));
        assert_eq!(body["run"], expected.run);
        assert_eq!(body["case"], case.name());
        assert_eq!(body["phase"], case.phase());
        assert_eq!(body["fresh_call"], case.trigger().unwrap());
        assert_eq!(body["path"], "dir");
        assert_eq!(body["fresh"], true);
        let old = Attributes::from(std::fs::metadata(target.join("dir"))?);
        std::fs::rename(target.join("dir"), target.join("held-dir"))?;
        std::fs::create_dir(target.join("dir"))?;
        std::fs::set_permissions(target.join("dir"), Permissions::from_mode(0o711))?;
        File::open(target.join("dir"))?
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1_600_000_000)))?;
        let new = Attributes::from(std::fs::metadata(target.join("dir"))?);
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        stream.shutdown().await?;
        Ok(Replacement { old, new })
    });
    Ok((control, handle))
}

fn fixture(source: &Path, target: &Path, case: Case) -> TestResult {
    std::fs::create_dir(source.join("dir"))?;
    std::fs::write(source.join("dir/payload"), PAYLOAD)?;
    std::fs::set_permissions(source.join("dir"), Permissions::from_mode(0o751))?;
    File::open(source.join("dir"))?
        .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789)))?;
    if case == Case::Normal {
        symlink(LINK_TARGET, source.join("link"))?;
    }
    std::fs::create_dir(target.join("dir"))?;
    std::fs::set_permissions(target.join("dir"), Permissions::from_mode(0o777))?;
    Ok(())
}

async fn local_case(case: Case) -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    fixture(source.path(), target.path(), case)?;
    let run = format!("nightly-main-nfs-{}", Uuid::new_v4());
    let callback = server(vec![Reply::NoContent; 8]).await;
    let (control, replaced) = if case.trigger().is_some() {
        let (control, handle) = replacement_control(case, &run, target.path().to_owned()).await?;
        (Some(control), Some(handle))
    } else {
        (None, None)
    };
    let inputs = Inputs {
        case,
        run,
        source_url: source.path().to_string_lossy().into_owned(),
        target_url: target.path().to_string_lossy().into_owned(),
        callback: callback.endpoint.clone(),
        control,
    };
    let source_storage = create_storage(&inputs.source_url, CreateStorageOptions::default()).await?;
    let expected = source_storage
        .storage()
        .namespace()?
        .stat(&StoragePath::new("dir")?, false)
        .await?;
    let target_storage = create_storage(&inputs.target_url, CreateStorageOptions::default()).await?;
    let mut outcome = exercise(&inputs, source_storage, target_storage).await?;
    check_outcome(&inputs, &mut outcome);
    if case == Case::Normal {
        inspect_normal(&inputs, &expected).await?;
    }
    if let Some(replaced) = replaced {
        let replaced = timeout(Duration::from_secs(5), replaced).await???;
        assert_eq!(
            Attributes::from(std::fs::metadata(target.path().join("held-dir"))?),
            replaced.old
        );
        assert_eq!(
            Attributes::from(std::fs::metadata(target.path().join("dir"))?),
            replaced.new
        );
        assert_ne!(replaced.old.inode, replaced.new.inode);
        assert_eq!(std::fs::read(target.path().join("held-dir/payload"))?, PAYLOAD);
        assert_eq!(std::fs::read_dir(target.path().join("dir"))?.count(), 0);
        assert_eq!(outcome.evidence.bindings.load(Ordering::SeqCst), 1);
        let trace = outcome.evidence.calls.lock().unwrap();
        let fresh_phases: Vec<_> = trace.iter().filter(|call| call.fresh).collect();
        let prepared = fresh_phases[0];
        assert_eq!(prepared.fresh_call, 1);
        assert!(prepared.failure.is_none());
        assert!(prepared.identity.is_some());
        assert_eq!(prepared.mode, Some(0o777));
        if case == Case::TargetPost {
            assert_eq!(replaced.old.mode, directory_values(&expected)?.0);
            assert_eq!(
                replaced.old.modified.duration_since(UNIX_EPOCH)?.as_nanos() as i128,
                directory_values(&expected)?.1,
                "替换发生在实际元数据 apply 完成后、最后联合 fresh 核验读取之前"
            );
            assert_eq!(fresh_phases.len(), 2);
            let prepare_index = trace.iter().position(|call| call.fresh_call == 1).unwrap();
            assert_eq!(
                trace.len() - prepare_index,
                2,
                "Local 内联快路不需要额外 namespace stat"
            );
            let post = fresh_phases[1];
            assert_eq!(post.fresh_call, case.trigger().unwrap());
            assert!(post.failure.is_none());
            assert_ne!(prepared.identity, post.identity);
            assert_eq!(post.mode, Some(replaced.new.mode));
            assert_eq!(
                post.modified_unix_nanos,
                Some(replaced.new.modified.duration_since(UNIX_EPOCH)?.as_nanos() as i128)
            );
        } else {
            assert_eq!(replaced.old.mode, 0o777, "对象替换发生在写入前");
            assert_eq!(fresh_phases.len(), 1);
        }
    }
    assert_eq!(std::fs::read(source.path().join("dir/payload"))?, PAYLOAD);
    let bodies = callback.bodies.lock().await;
    let reports: Vec<ProgressReport> = bodies.iter().map(|body| serde_json::from_str(body).unwrap()).collect();
    let events: Vec<_> = reports
        .iter()
        .filter_map(|report| report.diagnostics.as_ref())
        .flat_map(|batch| &batch.events)
        .collect();
    assert_eq!(events.len(), usize::from(case != Case::Normal));
    if let Some(event) = events.first() {
        assert!(event.message.contains("目标发布状态"));
        assert_eq!(event.path.as_deref(), Some("dir"));
    }
    assert!(
        reports
            .iter()
            .any(|report| report.is_final && report.diagnostics.is_none() && report.diagnostics_complete)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_component_normal_unicode_link_and_http204() -> TestResult {
    local_case(Case::Normal).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_component_pre_barrier_delegates_after_replacement() -> TestResult {
    local_case(Case::TargetPre).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_component_replacement_after_metadata_apply_before_final_fresh() -> TestResult {
    local_case(Case::TargetPost).await
}
