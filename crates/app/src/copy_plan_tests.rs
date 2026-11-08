//! 真实 Local／ClickHouse／HTTP 下执行时间窗口和源清理代次切换。
use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use std::{
    fs::{File, FileTimes},
    time::{Duration, UNIX_EPOCH},
};
use task_model::migration::{CopyGeneration, CopyGenerationPhase, MigrationMode, MigrationReason, TargetCleanupPolicy};
use task_model::{SourceCleanupPolicy, TaskModelError};

use super::request_tests::startup_fault;
use super::tests::job;
use crate::consumer::stats::delivery::FinalCallbackReport;
use crate::consumer::stats::{ProgressDetail, ProgressReport};
use crate::error::{AppError, Result};
use crate::{callback::CallbackEndpoint, config::SyncJobConfig, orchestrator::SyncOrchestrator};

const DAY: i64 = 86_400;

async fn execute(
    config: &ClickHouseConfig, command: &SyncJobConfig, day: i64, mode: MigrationMode,
) -> Result<(CopyGeneration, u64)> {
    SyncOrchestrator::new_local(command.clone())
        .with_filter_time(day * DAY)
        .run()
        .await?;
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("accepted migration required".into()))?;
    assert_eq!(generation.phase, CopyGenerationPhase::Completed);
    assert_eq!(generation.plan.mode, mode);
    assert_eq!(generation.request.filter_evaluated_at, day * DAY);
    assert_eq!(generation.plan.target_cleanup, TargetCleanupPolicy::Preserve);
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(report.task_error_count, 0);
    assert_eq!(report.migration, Some(generation.report()));
    if !command.source_cleanup.retains_source() {
        assert_eq!(database.get_count("base").await?, 0, "删源迁移本轮结束即清理 base");
    }
    let bytes = match report.detail {
        ProgressDetail::Full {
            error_count,
            transferred_bytes,
            ..
        } => {
            assert_eq!(error_count, 0);
            transferred_bytes
        }
        ProgressDetail::Incremental {
            error_count,
            transferred_bytes,
            deleted_count,
            renamed_count,
            ..
        } => {
            assert_eq!((error_count, deleted_count, renamed_count), (0, 0, 0));
            transferred_bytes
        }
    };
    Ok((generation, bytes))
}

pub(super) async fn transitions(config: &ClickHouseConfig) -> Result<()> {
    for selection in ["include", "exclude", "combined"] {
        let directory = tempfile::tempdir()?;
        let source = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        let other = tempfile::tempdir()?;
        for (name, day) in [("a", 10), ("b", 6)] {
            std::fs::write(source.path().join(name), name.as_bytes())?;
            File::options()
                .write(true)
                .open(source.path().join(name))?
                .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs((day * DAY) as u64)))?;
        }
        let mut command = job(directory.path());
        command.job_id = format!("plan_window_{selection}");
        command.src_path = source.path().to_string_lossy().into_owned();
        command.dest_path = target.path().to_string_lossy().into_owned();
        command.concurrency = Some(2);
        command.progress_callback_url = Some(CallbackEndpoint::parse(
            &std::env::var("LAB_REQUEST_POLICY_CALLBACK_URL")
                .map_err(|_| AppError::ConfigError("owned callback required".into()))?,
        )?);
        match selection {
            "include" => command.r#match = Some("modified < 7d".into()),
            "exclude" => command.exclude = Some("modified > 7d".into()),
            _ => {
                command.r#match = Some("type == file".into());
                command.exclude = Some("modified > 7d".into());
            }
        }
        let (initial, bytes) = execute(config, &command, 16, MigrationMode::Full).await?;
        assert_eq!(bytes, 1);
        assert!(target.path().join("a").is_file() && !target.path().join("b").exists());
        let original = std::fs::metadata(target.path().join("a"))?;
        let (outside, bytes) = execute(config, &command, 18, MigrationMode::Incremental).await?;
        assert_eq!(bytes, 0);
        assert_eq!(
            outside.request.selection.definition_key,
            initial.request.selection.definition_key
        );
        assert!(outside.plan.reasons.contains(&MigrationReason::RelativeTimeWindow));
        let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
        assert_eq!(database.get_count("base").await?, 1);
        assert_eq!(
            std::fs::metadata(target.path().join("a"))?.modified()?,
            original.modified()?
        );
        let (_, bytes) = execute(config, &command, 12, MigrationMode::Incremental).await?;
        assert_eq!(bytes, 1, "only newly selected b transfers data");
        let (_, bytes) = execute(config, &command, 16, MigrationMode::Incremental).await?;
        assert_eq!(bytes, 0, "returning confirmed a must not retransmit");
        assert_eq!(database.get_count("base").await?, 2);
        command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
        let (_, bytes) = execute(config, &command, 16, MigrationMode::Incremental).await?;
        assert_eq!(bytes, 0);
        assert!(!source.path().join("a").exists() && source.path().join("b").is_file());
        let (next, bytes) = execute(config, &command, 12, MigrationMode::Full).await?;
        assert_eq!(bytes, 1);
        assert!(next.plan.reasons.contains(&MigrationReason::PreviousSourceDeleted));
        assert!(!source.path().join("b").exists());
        command.source_cleanup = SourceCleanupPolicy::Retain;
        execute(config, &command, 12, MigrationMode::Full).await?;
        command.r#match = None;
        command.exclude = None;
        let (changed, bytes) = execute(config, &command, 12, MigrationMode::Incremental).await?;
        assert_eq!(bytes, 0);
        assert!(changed.plan.reasons.contains(&MigrationReason::SelectionChanged));
        for name in ["a", "b"] {
            assert_eq!(std::fs::read(target.path().join(name))?, name.as_bytes());
        }
        let before = database
            .latest_copy_generation()
            .await?
            .ok_or_else(|| AppError::ConfigError("accepted migration required".into()))?;
        command.dest_path = other.path().to_string_lossy().into_owned();
        let result = SyncOrchestrator::new_local(command.clone())
            .with_filter_time(12 * DAY)
            .run()
            .await;
        assert!(matches!(
            result,
            Err(AppError::DatabaseError(DatabaseError::MigrationPolicy(
                TaskModelError::CopyBindingChanged
            )))
        ));
        assert_eq!(database.latest_copy_generation().await?, Some(before));
        assert_eq!(std::fs::read_dir(other.path())?.count(), 0);
        eprintln!(
            "plan-window cluster={} selection={selection} real_runs=8 out_and_back=true preserved_facts=true transfer_bytes=1,0,1,0,0,1,0,0 delete_selected_only=true previous_delete_forces_full=true binding_rejected_without_rebind=true callbacks_and_archives_match_plan=true",
            config.cluster.is_some()
        );
    }
    Ok(())
}

pub(super) async fn lifecycle_edges(config: &ClickHouseConfig) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let other = tempfile::tempdir()?;
    std::fs::write(source.path().join("old"), b"x")?;
    let mut command = job(directory.path());
    command.job_id = "plan_edge_prepare".into();
    command.src_path = source.path().to_string_lossy().into_owned();
    command.dest_path = target.path().to_string_lossy().into_owned();
    command.r#match = Some("type == file".into());
    command.concurrency = Some(2);
    command.progress_callback_url = Some(CallbackEndpoint::parse(
        &std::env::var("LAB_REQUEST_POLICY_CALLBACK_URL")
            .map_err(|_| AppError::ConfigError("owned callback required".into()))?,
    )?);
    execute(config, &command, 12, MigrationMode::Full).await?;
    startup_fault("reject_prepare").await?;
    let failed = SyncOrchestrator::new_local(command.clone())
        .with_filter_time(12 * DAY)
        .run()
        .await;
    startup_fault("normal").await?;
    assert!(failed.is_err());
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let failed = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("accepted failure required".into()))?;
    assert_eq!(failed.phase, CopyGenerationPhase::Failed);
    assert_eq!(failed.plan.mode, MigrationMode::Incremental);
    assert!(!failed.baseline_prepared);
    assert_eq!(database.get_count("base").await?, 1);
    let (recovered, _) = execute(config, &command, 12, MigrationMode::Full).await?;
    assert!(recovered.plan.reasons.contains(&MigrationReason::BaselineUnavailable));

    // 过滤范围不能授权 mirror；拒绝请求不得覆盖上一已接受代次。
    command.delete_target = true;
    let rejected = SyncOrchestrator::new_local(command.clone()).run().await;
    assert!(matches!(
        rejected,
        Err(AppError::DatabaseError(DatabaseError::MigrationPolicy(
            TaskModelError::TargetCleanupConflict
        )))
    ));
    assert_eq!(database.latest_copy_generation().await?, Some(recovered.clone()));
    assert!(source.path().join("old").exists() && target.path().join("old").exists());
    command.delete_target = false;

    // base 不再存在也不能重新绑定同一个 job；请求历史独立于事实表。
    database.drop_table_by_name(&format!("base_{}", command.job_id)).await?;
    command.dest_path = other.path().to_string_lossy().into_owned();
    let rejected = SyncOrchestrator::new_local(command.clone()).run().await;
    assert!(matches!(
        rejected,
        Err(AppError::DatabaseError(DatabaseError::MigrationPolicy(
            TaskModelError::CopyBindingChanged
        )))
    ));
    assert_eq!(database.latest_copy_generation().await?, Some(recovered));
    assert_eq!(std::fs::read_dir(other.path())?.count(), 0);
    command.dest_path = target.path().to_string_lossy().into_owned();
    execute(config, &command, 12, MigrationMode::Full).await?;

    // 新 job 无过滤：删源增量将 rename 当新路径复制，旧目标明确保留。
    command.job_id = "plan_edge_rename".into();
    command.r#match = None;
    execute(config, &command, 12, MigrationMode::Full).await?;
    std::fs::rename(source.path().join("old"), source.path().join("new"))?;
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    let (_, bytes) = execute(config, &command, 12, MigrationMode::Incremental).await?;
    assert_eq!(bytes, 1);
    assert!(!source.path().join("new").exists());
    for name in ["old", "new"] {
        assert_eq!(std::fs::read(target.path().join(name))?, b"x");
    }
    eprintln!(
        "plan-lifecycle cluster={} accepted_prepare_failure=true next_full=true mirror_rejected=true binding_survives_base_drop=true delete_rename_copies_new_and_preserves_old=true",
        config.cluster.is_some()
    );
    Ok(())
}

async fn archived_report(
    config: &ClickHouseConfig, job_id: &str, generation: &CopyGeneration,
) -> Result<ProgressReport> {
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{job_id} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1"))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    Ok(serde_json::from_str(&saved.payload_json)?)
}

/// 复制和删源已经完成后，base 清理故障必须使任务失败且准确保留最终报告。
pub(super) async fn retirement_failures(config: &ClickHouseConfig) -> Result<()> {
    for mode in ["disconnect", "stall", "lost"] {
        let directory = tempfile::tempdir()?;
        let source = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        std::fs::write(source.path().join("payload"), b"ready target")?;
        let mut command = job(directory.path());
        command.job_id = format!("retirement_callback_{mode}");
        command.src_path = source.path().to_string_lossy().into_owned();
        command.dest_path = target.path().to_string_lossy().into_owned();
        command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
        command.progress_callback_url = Some(CallbackEndpoint::parse(
            &std::env::var("LAB_REQUEST_POLICY_CALLBACK_URL")
                .map_err(|_| AppError::ConfigError("owned callback required".into()))?,
        )?);
        startup_fault(&format!("retirement_{mode}")).await?;
        let result = SyncOrchestrator::new_local(command.clone()).run().await;
        startup_fault("normal").await?;
        assert!(
            matches!(
                result,
                Err(AppError::DatabaseError(DatabaseError::CopyBaseRetirementFailed { .. }))
            ),
            "{result:?}"
        );
        assert!(!source.path().join("payload").exists());
        assert_eq!(std::fs::read(target.path().join("payload"))?, b"ready target");
        let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
        let generation = database
            .latest_copy_generation()
            .await?
            .ok_or_else(|| AppError::ConfigError("accepted generation required".into()))?;
        assert_eq!(generation.phase, CopyGenerationPhase::Failed);
        let report = archived_report(config, &command.job_id, &generation).await?;
        assert!(report.is_final && report.diagnostics_complete);
        assert_eq!(report.task_error_count, 1);
        assert_eq!(report.migration, Some(generation.report()));
        assert!(matches!(report.detail, ProgressDetail::Full { error_count: 0, .. }));
        if mode == "lost" {
            assert_eq!(database.get_count("base").await?, 0);
        } else {
            assert!(database.get_count("base").await? > 0);
        }
        // 失败删源代次仍记录策略，下一代全量；目标中已迁移数据不会被反向删除。
        std::fs::write(source.path().join("next"), b"next payload")?;
        SyncOrchestrator::new_local(command.clone()).run().await?;
        assert_eq!(database.get_count("base").await?, 0);
        let next = database
            .latest_copy_generation()
            .await?
            .ok_or_else(|| AppError::ConfigError("next generation required".into()))?;
        assert_eq!(next.plan.mode, MigrationMode::Full);
        assert_eq!(next.phase, CopyGenerationPhase::Completed);
        assert_eq!(next.sequence, generation.sequence + 1);
        assert_eq!(
            archived_report(config, &command.job_id, &next).await?.task_error_count,
            0
        );
        assert_eq!(std::fs::read(target.path().join("payload"))?, b"ready target");
        assert_eq!(std::fs::read(target.path().join("next"))?, b"next payload");
        assert!(!source.path().join("next").exists());
        eprintln!(
            "retirement-callback cluster={} fault={mode} source_already_deleted=true target_retained=true failed_report_saved=true next_full=true next_base_empty=true",
            config.cluster.is_some()
        );
    }
    Ok(())
}
