//! 用真实的单进程复制入口、Local 目标和周期 HTTP 报告核验中断前后字节口径。

use std::{
    env, fs,
    io::{self, Seek, SeekFrom, Write},
    path::Path,
    process::Stdio,
    time::{Duration, UNIX_EPOCH},
};

use data_mover::storage::{
    artifacts::{ArtifactKind, artifact_name},
    checkpoint::CheckpointData,
    pointer::DestinationPointer,
};
use task_model::{SourceCleanupPolicy, TransferMode};
use tokio::{
    process::{Child, Command},
    time::{sleep, timeout},
};
use utils::app_config::AppConfig;

use crate::callback::CallbackEndpoint;
use crate::config::SyncJobConfig;
use crate::consumer::stats::delivery::tests::{Reply, Server, server};
use crate::report::{ProgressDetail, ProgressReport};
use crate::sync::sync;

const MIB: u64 = 1024 * 1024;
const FILE: &str = "payload";
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "spawned by the copy progress test with a private fixture"]
async fn copy_progress_child() -> TestResult {
    let config = env::var_os("TERRASYNC_COPY_PROGRESS_CONFIG");
    AppConfig::init(
        Some(include_str!("../../cli/src/resources/default_config.toml")),
        config.as_deref().map(Path::new),
    )?;
    AppConfig::override_with(|config| config.database.enabled = false)?;
    let root = env::var("TERRASYNC_COPY_PROGRESS_ROOT")?;
    let transfer_mode = match env::var("TERRASYNC_COPY_PROGRESS_POLICY")?.as_str() {
        "direct" => TransferMode::Direct,
        "checkpoint" => TransferMode::Checkpoint,
        _ => return Err("unexpected copy progress policy".into()),
    };
    sync(SyncJobConfig {
        job_id: "copy-progress".into(),
        job_dir: format!("{root}/job"),
        job_dir_pre_existing: Path::new(&format!("{root}/job")).exists(),
        src_path: format!("{root}/source"),
        dest_path: env::var("TERRASYNC_COPY_PROGRESS_TARGET").unwrap_or_else(|_| format!("{root}/target")),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: env::var_os("TERRASYNC_COPY_PROGRESS_READ_BACK").is_some(),
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: Some(env::var("TERRASYNC_COPY_PROGRESS_QOS")?),
        peak_qos_rate: 1.0,
        block_size: Some("4MiB".into()),
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: String::new(),
        progress_callback_url: Some(CallbackEndpoint::parse(&env::var("TERRASYNC_COPY_PROGRESS_CALLBACK")?)?),
        transfer_mode,
        delete_target: false,
        concurrency: Some(1),
    })
    .await?;
    Ok(())
}

fn start(root: &Path, endpoint: &CallbackEndpoint, policy: &str, qos: &str, audit: bool) -> TestResult<Child> {
    let executable = env::current_exe()?;
    let mut command = if audit {
        let mut trace = Command::new("strace");
        trace
            .args(["-ff", "-ttt", "-T", "-y", "-s", "0", "-e", "trace=pwrite64", "-o"])
            .arg(root.join("audit"))
            .arg(executable);
        trace
    } else {
        Command::new(executable)
    };
    Ok(command
        .args([
            "--ignored",
            "--exact",
            "copy_progress_real_tests::copy_progress_child",
            "--nocapture",
        ])
        .env("TERRASYNC_COPY_PROGRESS_ROOT", root)
        .env("TERRASYNC_COPY_PROGRESS_CALLBACK", endpoint.request_url().as_str())
        .env("TERRASYNC_COPY_PROGRESS_POLICY", policy)
        .env("TERRASYNC_COPY_PROGRESS_QOS", qos)
        .env("DATA_MOVER_LOCAL_READ_INFLIGHT", "1")
        .env("DATA_MOVER_LOCAL_WRITE_INFLIGHT", "1")
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()?)
}

fn fixture(root: &Path, total: u64) -> TestResult {
    fs::create_dir(root.join("source"))?;
    fs::create_dir(root.join("target"))?;
    let path = root.join("source").join(FILE);
    let mut file = fs::File::create(path)?;
    file.set_len(total)?;
    file.seek(SeekFrom::End(-7))?;
    file.write_all(b"payload")?;
    file.sync_all()?;
    Ok(())
}

async fn reports(server: &Server) -> TestResult<Vec<ProgressReport>> {
    server
        .bodies
        .lock()
        .await
        .iter()
        .map(|raw| Ok(serde_json::from_str(raw)?))
        .collect()
}

fn published(report: &ProgressReport) -> u64 {
    match report.detail {
        ProgressDetail::Full { transferred_bytes, .. } | ProgressDetail::Incremental { transferred_bytes, .. } => {
            transferred_bytes
        }
    }
}

fn durable_bytes(root: &Path) -> TestResult<u64> {
    let pointer = root.join("target").join(artifact_name(FILE, ArtifactKind::Pointer));
    let bytes = match fs::read(pointer) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let pointer = DestinationPointer::decode(&bytes).map_err(|error| io::Error::other(format!("{error:?}")))?;
    match pointer.checkpoint {
        Some(checkpoint) => Ok(CheckpointData::decode(&checkpoint)?
            .ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum()),
        None => Ok(pointer.durable_prefix.unwrap_or_default()),
    }
}

#[derive(Debug)]
struct PayloadWrite {
    completed: f64,
    offset: u64,
    length: u64,
}

fn payload_writes(root: &Path) -> TestResult<Vec<PayloadWrite>> {
    let direct = format!("/target/{FILE}>");
    let staged = format!("/target/{}>", artifact_name(FILE, ArtifactKind::Stage));
    let mut writes = Vec::new();
    for trace in fs::read_dir(root)? {
        let trace = trace?;
        if !trace.file_name().to_string_lossy().starts_with("audit.") {
            continue;
        }
        for line in fs::read_to_string(trace.path())?.lines() {
            if !line.contains(&direct) && !line.contains(&staged) {
                continue;
            }
            let (timestamp, operation) = line.split_once(' ').ok_or("missing syscall timestamp")?;
            let (call, result) = operation.rsplit_once(" = ").ok_or("missing syscall result")?;
            let (_, offset) = call.rsplit_once(", ").ok_or("missing write offset")?;
            let (length, elapsed) = result.split_once(" <").ok_or("missing syscall duration")?;
            let length = length.trim().parse::<u64>()?;
            writes.push(PayloadWrite {
                completed: timestamp.parse::<f64>()? + elapsed.trim_end_matches('>').parse::<f64>()?,
                offset: offset.trim_end_matches(')').parse()?,
                length,
            });
        }
    }
    writes.sort_unstable_by_key(|write| write.offset);
    Ok(writes)
}

async fn check_finished(child: &mut Child, receiver: &Server, root: &Path, reused: u64, total: u64) -> TestResult {
    assert!(timeout(Duration::from_secs(35), child.wait()).await??.success());
    let snapshots = reports(receiver).await?;
    let bytes = total - reused;
    let final_report = snapshots
        .iter()
        .rev()
        .find(|report| report.is_final)
        .ok_or("missing final report")?;
    assert_eq!((published(final_report), final_report.in_flight_bytes), (bytes, 0));
    let prefix = snapshots
        .iter()
        .filter(|report| !report.is_final && report.in_flight_bytes > 0);
    let observed: Vec<u64> = prefix
        .map(|report| {
            assert_eq!(published(report), 0);
            assert!(report.in_flight_bytes <= bytes);
            report.in_flight_bytes
        })
        .collect();
    assert!(
        observed.windows(2).any(|pair| pair[1] > pair[0]),
        "expected two growing samples: {observed:?}"
    );
    let writes = payload_writes(root)?;
    let mut next_offset = reused;
    for write in &writes {
        assert_eq!(write.offset, next_offset, "unexpected Local payload write {write:?}");
        next_offset += write.length;
    }
    assert_eq!(next_offset, total);
    let receipt_times = receiver.received_at.lock().await;
    assert_eq!(receipt_times.len(), snapshots.len());
    for (report, received_at) in snapshots.iter().zip(receipt_times.iter()) {
        if report.is_final || report.in_flight_bytes == 0 {
            continue;
        }
        let timestamp = received_at.duration_since(UNIX_EPOCH)?.as_secs_f64();
        let written_at = |cutoff| {
            writes
                .iter()
                .filter(|write| write.completed <= cutoff)
                .map(|write| write.length)
                .sum::<u64>()
        };
        // The callback snapshot is produced before the loopback server receives it.
        let lower = written_at(timestamp - 0.25);
        let upper = written_at(timestamp);
        assert!(
            (lower..=upper).contains(&report.in_flight_bytes),
            "reported {} bytes, syscalls confirmed {lower}..={upper}",
            report.in_flight_bytes
        );
    }
    assert_eq!(
        fs::read(root.join("source").join(FILE))?,
        fs::read(root.join("target").join(FILE))?
    );
    Ok(())
}

async fn restart_case(keep_checkpoint: bool) -> TestResult {
    let root = tempfile::tempdir()?;
    let total = 160 * MIB + 17;
    fixture(root.path(), total)?;
    let first = server(vec![Reply::NoContent; 20]).await;
    let mut child = start(root.path(), &first.endpoint, "checkpoint", "16MiB/s", false)?;
    let observed = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(report) = reports(&first).await?.iter().find(|report| {
                let bytes = report.in_flight_bytes;
                !report.is_final
                    && if keep_checkpoint {
                        (72 * MIB..128 * MIB).contains(&bytes)
                    } else {
                        (1..64 * MIB).contains(&bytes)
                    }
            }) {
                assert_eq!(published(report), 0);
                break Ok::<u64, Box<dyn std::error::Error>>(report.in_flight_bytes);
            }
            if child.try_wait()?.is_some() {
                return Err("child finished before the planned interruption".into());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    child.kill().await?;
    assert!(!root.path().join("target").join(FILE).exists());
    let reused = durable_bytes(root.path())?;
    assert_eq!(reused, if keep_checkpoint { 64 * MIB } else { 0 });
    assert!(
        observed > reused,
        "the killed round observed more write bytes than could be reused"
    );
    let second = server(vec![Reply::NoContent; 20]).await;
    let mut child = start(root.path(), &second.endpoint, "checkpoint", "16MiB/s", true)?;
    check_finished(&mut child, &second, root.path(), reused, total).await?;
    eprintln!(
        "checkpoint interruption observed={observed} reused={reused} published_after_restart={}",
        total - reused
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Linux strace to audit Local writes during callback and SIGKILL recovery validation"]
async fn real_copy_progress_reports_confirmed_direct_and_restarted_writes() -> TestResult {
    let root = tempfile::tempdir()?;
    let total = 64 * MIB + 17;
    fixture(root.path(), total)?;
    let receiver = server(vec![Reply::NoContent; 20]).await;
    let mut child = start(root.path(), &receiver.endpoint, "direct", "8MiB/s", true)?;
    check_finished(&mut child, &receiver, root.path(), 0, total).await?;
    restart_case(true).await?;
    restart_case(false).await
}
