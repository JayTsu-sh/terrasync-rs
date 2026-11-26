//! 用真实任务、限速文件流与真实 CH 故障验证在途取消和检查点恢复。
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    time::{Duration, Instant},
};

use task_model::{SourceCleanupPolicy, TransferMode};
use tokio::{
    join,
    time::{sleep, timeout},
};
use utils::app_config::AppConfig;

use super::{Fixture, has_database_cause};
use crate::{
    callback::CallbackEndpoint,
    config::SyncJobConfig,
    error::{AppError, Result},
    orchestrator::SyncOrchestrator,
};

const PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;

fn create_payload(path: &Path) -> Result<()> {
    let mut file = File::create(path)?;
    let chunk = vec![0x5a; CHUNK_BYTES];
    for _ in 0..PAYLOAD_BYTES / CHUNK_BYTES {
        file.write_all(&chunk)?;
    }
    Ok(())
}

fn partial_payload_exists(root: &Path, minimum_written: u64) -> Result<bool> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let mut file = File::open(entry.path())?;
        let size = file.metadata()?.len();
        if size < minimum_written {
            continue;
        }
        let mut byte = [0];
        file.read_exact(&mut byte)?;
        if byte[0] != 0x5a {
            continue;
        }
        file.seek(SeekFrom::Start(minimum_written - 1))?;
        file.read_exact(&mut byte)?;
        if byte[0] != 0x5a {
            continue;
        }
        if size < PAYLOAD_BYTES as u64 {
            return Ok(true);
        }
        file.seek(SeekFrom::End(-1))?;
        file.read_exact(&mut byte)?;
        if byte[0] != 0x5a {
            return Ok(true);
        }
    }
    Ok(false)
}

fn verify_payload(path: &Path) -> Result<()> {
    let mut file = File::open(path)?;
    assert_eq!(file.metadata()?.len(), PAYLOAD_BYTES as u64);
    let mut chunk = vec![0; CHUNK_BYTES];
    for _ in 0..PAYLOAD_BYTES / CHUNK_BYTES {
        file.read_exact(&mut chunk)?;
        assert!(chunk.iter().all(|byte| *byte == 0x5a));
    }
    Ok(())
}

async fn case(fixture: &Fixture, incremental: bool, mode: TransferMode, fault: &str, file_list: bool) -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let directory = tempfile::tempdir()?;
    let callback = std::env::var("LAB_COPY_DATA_CANCEL_CALLBACK")
        .map_err(|_| AppError::ConfigError("owned data cancel callback missing".into()))?;
    let mut command = SyncJobConfig {
        job_id: format!("data_cancel_{incremental}_{}_{fault}", mode == TransferMode::Checkpoint),
        job_dir: directory.path().to_string_lossy().into_owned(),
        job_dir_pre_existing: false,
        src_path: source.path().to_string_lossy().into_owned(),
        dest_path: target.path().to_string_lossy().into_owned(),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: false,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: 1.0,
        block_size: Some("64KiB".into()),
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: "owned database and active data cancellation".into(),
        progress_callback_url: Some(CallbackEndpoint::parse(&callback)?),
        transfer_mode: mode,
        delete_target: false,
        concurrency: Some(2),
    };
    AppConfig::override_with(|global| {
        global.database.enabled = true;
        global.database.batch_size = 1;
        global.database.clickhouse = fixture.config.clone();
    })?;
    std::fs::write(source.path().join("ledger"), b"confirmed ledger")?;
    if incremental {
        SyncOrchestrator::new_local(command.clone()).run().await?;
    }
    create_payload(&source.path().join("payload"))?;
    if file_list {
        let selection = directory.path().join("selection.txt");
        std::fs::write(&selection, b"ledger\npayload\n")?;
        command.file_list = Some(selection.to_string_lossy().into_owned());
    }
    command.qos = Some("16MiB/s".into());
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    let orchestrator = SyncOrchestrator::new_local(command.clone());
    let run = timeout(Duration::from_secs(25), orchestrator.run());
    // 原生 Local 自动检查点阈值为 64 MiB；先跨过屏障，才能验证真实复用。
    // Direct 也要跨过一次周期回调，确保取消前观察到正的在途字节。
    let minimum_written = if mode == TransferMode::Checkpoint { 80 } else { 56 } * 1024 * 1024;
    let inject = async {
        for _ in 0..240 {
            if partial_payload_exists(target.path(), minimum_written)? {
                let started = Instant::now();
                fixture.control(fault).await?;
                return Ok(started);
            }
            sleep(Duration::from_millis(50)).await;
        }
        Err(AppError::ConfigError(
            "no active partial payload under source QoS".into(),
        ))
    };
    let (result, injected): (_, Result<Instant>) = join!(run, inject);
    fixture.control("normal").await?;
    let injected = injected?;
    let elapsed = injected.elapsed();
    let result = result.map_err(|_| AppError::ConfigError("active data did not stop within owned deadline".into()))?;
    let error = result
        .err()
        .ok_or_else(|| AppError::ConfigError("database outage incorrectly completed migration".into()))?;
    assert!(has_database_cause(&error), "{error}");
    assert!(elapsed < Duration::from_secs(8), "{elapsed:?}: {error}");
    verify_payload(&source.path().join("payload"))?;
    assert_eq!(std::fs::read(source.path().join("ledger"))?, b"confirmed ledger");
    assert!(
        !target.path().join("payload").exists(),
        "unpublished or bad data cannot be acknowledged as a final file"
    );
    if mode == TransferMode::Direct {
        assert!(
            !partial_payload_exists(target.path(), CHUNK_BYTES as u64)?,
            "owned Direct bad output must be removed after writers settle"
        );
    } else {
        assert!(
            partial_payload_exists(target.path(), CHUNK_BYTES as u64)?,
            "valid interrupted checkpoint data must remain"
        );
        command.qos = None;
        command.source_cleanup = SourceCleanupPolicy::Retain;
        SyncOrchestrator::new_local(command).run().await?;
        verify_payload(&target.path().join("payload"))?;
        verify_payload(&source.path().join("payload"))?;
    }
    eprintln!(
        "copy-data-cancel cluster={} incremental={incremental} checkpoint={} file_list={file_list} fault={fault} fault_to_stop_ms={} source_retained=true direct_cleanup_or_checkpoint_resume=true",
        fixture.config.cluster.is_some(),
        mode == TransferMode::Checkpoint,
        elapsed.as_millis()
    );
    Ok(())
}

async fn run_cases(file_list: bool) -> Result<()> {
    AppConfig::init(
        Some(include_str!("../../../../cli/src/resources/default_config.toml")),
        None,
    )?;
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            for incremental in [false, true] {
                for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
                    for fault in ["running_disconnect", "running_stall"] {
                        case(&fixture, incremental, mode, fault, file_list).await?;
                    }
                }
            }
            Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires --copy-data-cancel owned real ClickHouse pair, fault proxy and callback"]
async fn real_database_fault_cancels_active_direct_and_checkpoint_data() -> Result<()> {
    run_cases(false).await
}

#[tokio::test]
#[ignore = "requires --copy-file-list-cancel owned real ClickHouse pair, fault proxy and callback"]
async fn real_database_fault_cancels_file_list_direct_and_checkpoint_data() -> Result<()> {
    run_cases(true).await
}
