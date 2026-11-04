use std::net::SocketAddr;
use std::time::Duration;

use app::consumer::stats::diagnostics::{CallbackAck, CallbackIdentity, DiagnosticBatch, DiagnosticOutbox};
use axum::Router;
use axum::http::StatusCode;
use axum::routing::post;
use chrono::Utc;
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::net::TcpListener;
use tokio_util::task::AbortOnDropHandle;

use super::progress_repo::SqliteProgressRepo;
use crate::api::state::AppState;
use crate::api::task_handler;
use crate::domain::progress::{ProgressSubmission, TaskProgress};
use crate::domain::repository::ProgressRepository;
use crate::error::Result;

async fn fixture() -> Result<(SqlitePool, SqliteProgressRepo)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
        .execute(&pool)
        .await?;
    sqlx::raw_sql("INSERT INTO endpoints(id,name,endpoint_type,config_json) VALUES ('ep','ep','local','{}');
        INSERT INTO migration_tasks(id,name,task_type,source_endpoint_id,config_json,status) VALUES ('task','task','scan','ep','{}','running');
        INSERT INTO task_executions(id,task_id,execution_type,status) VALUES ('execution','task','full','running');")
        .execute(&pool).await?;
    Ok((pool.clone(), SqliteProgressRepo::new(pool)))
}

fn identity(round: &str, attempt: &str, round_time: u64, attempt_time: u64, sequence: u64) -> CallbackIdentity {
    CallbackIdentity {
        round_id: round.into(),
        attempt_id: attempt.into(),
        round_started_at_millis: round_time,
        attempt_started_at_millis: attempt_time,
        sequence,
    }
}

fn progress(json: &str, is_final: bool) -> TaskProgress {
    TaskProgress {
        task_id: "task".into(),
        report_json: json.into(),
        is_final,
        updated_at: Utc::now(),
    }
}

fn batch(identity: &CallbackIdentity) -> DiagnosticBatch {
    let mut outbox = DiagnosticOutbox::new(identity.round_id.clone(), identity.attempt_id.clone(), 4, 2);
    outbox.enqueue(
        "scan",
        "connectivity",
        "failed s3://user:secret@server/bucket?token=hidden",
        None,
    );
    outbox.prepare_batch().unwrap()
}

#[tokio::test]
async fn duplicate_delivery_is_durable_and_cannot_replace_newer_progress() -> Result<()> {
    let (pool, repo) = fixture().await?;
    let first = identity("round", "attempt", 1, 2, 1);
    let batch = batch(&first);
    let old = progress("first", false);
    let receipt = repo
        .receive(ProgressSubmission {
            progress: &old,
            identity: Some(&first),
            diagnostics: Some(&batch),
            execution_id: None,
            final_stats: None,
        })
        .await?;
    assert!(receipt.accepted);
    assert_eq!(receipt.ack.batch_id.as_deref(), Some(batch.batch_id.as_str()));
    let newer = CallbackIdentity {
        sequence: 2,
        ..first.clone()
    };
    let latest = progress("recovering", false);
    repo.receive(ProgressSubmission {
        progress: &latest,
        identity: Some(&newer),
        diagnostics: None,
        execution_id: None,
        final_stats: None,
    })
    .await?;
    let restarted_receiver = SqliteProgressRepo::new(pool.clone());
    let duplicate = restarted_receiver
        .receive(ProgressSubmission {
            progress: &old,
            identity: Some(&first),
            diagnostics: Some(&batch),
            execution_id: None,
            final_stats: None,
        })
        .await?;
    assert!(!duplicate.accepted);
    assert_eq!(duplicate.ack, receipt.ack);
    assert_eq!(repo.find_by_task_id("task").await?.unwrap().report_json, "recovering");
    let events: Vec<String> = sqlx::query_scalar("SELECT event_json FROM task_diagnostic_events")
        .fetch_all(&pool)
        .await?;
    assert_eq!(events.len(), 1);
    assert!(!events[0].contains("secret"));
    assert!(!events[0].contains("hidden"));
    Ok(())
}

#[tokio::test]
async fn failed_transaction_returns_no_ack_and_retry_keeps_every_event() -> Result<()> {
    let (pool, repo) = fixture().await?;
    sqlx::raw_sql("CREATE TRIGGER reject_progress BEFORE INSERT ON task_progress BEGIN SELECT RAISE(ABORT,'injected failure'); END;")
        .execute(&pool).await?;
    let identity = identity("round", "attempt", 1, 2, 1);
    let batch = batch(&identity);
    let report = progress("first", false);
    let failed = repo
        .receive(ProgressSubmission {
            progress: &report,
            identity: Some(&identity),
            diagnostics: Some(&batch),
            execution_id: None,
            final_stats: None,
        })
        .await;
    assert!(failed.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER reject_progress").execute(&pool).await?;
    let retried = repo
        .receive(ProgressSubmission {
            progress: &report,
            identity: Some(&identity),
            diagnostics: Some(&batch),
            execution_id: None,
            final_stats: None,
        })
        .await?;
    assert_eq!(retried.ack.batch_id.as_deref(), Some(batch.batch_id.as_str()));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn rounds_attempts_and_final_reports_cannot_move_progress_backwards() -> Result<()> {
    let (_pool, repo) = fixture().await?;
    for (id, text, accepted, is_final) in [
        (identity("r1", "a1", 1, 2, 1), "first", true, false),
        (identity("r1", "a2", 1, 3, 1), "resumed", true, false),
        (identity("r1", "a1", 1, 2, 99), "old final", false, true),
        (identity("r2", "a3", 4, 5, 1), "new round", true, false),
        (identity("r1", "a2", 1, 3, 100), "old round final", false, true),
        (
            identity("conflict", "a3", 4, 5, 100),
            "same millisecond round conflict",
            false,
            true,
        ),
        (
            identity("r2", "conflict", 4, 5, 100),
            "same millisecond attempt conflict",
            false,
            true,
        ),
        (identity("r2", "a3", 4, 5, 2), "finished", true, true),
        (identity("r2", "a3", 4, 5, 3), "late periodic", false, false),
    ] {
        let report = progress(text, is_final);
        let receipt = repo
            .receive(ProgressSubmission {
                progress: &report,
                identity: Some(&id),
                diagnostics: None,
                execution_id: None,
                final_stats: None,
            })
            .await?;
        assert_eq!(receipt.accepted, accepted, "{text}");
    }
    assert_eq!(repo.find_by_task_id("task").await?.unwrap().report_json, "finished");
    Ok(())
}

#[tokio::test]
async fn old_execution_is_acknowledged_without_updating_current_execution() -> Result<()> {
    let (pool, repo) = fixture().await?;
    sqlx::raw_sql("UPDATE task_executions SET status='completed' WHERE id='execution';
        INSERT INTO task_executions(id,task_id,execution_type,status) VALUES ('new-execution','task','full','running');")
        .execute(&pool).await?;
    let old_identity = identity("round", "attempt", 1, 2, 1);
    let batch = batch(&old_identity);
    let report = progress("old final", true);
    let receipt = repo
        .receive(ProgressSubmission {
            progress: &report,
            identity: Some(&old_identity),
            diagnostics: Some(&batch),
            execution_id: Some("execution"),
            final_stats: None,
        })
        .await?;
    assert!(!receipt.accepted);
    assert!(receipt.ack.batch_id.is_some());
    assert!(repo.find_by_task_id("task").await?.is_none());
    let stats: Option<String> = sqlx::query_scalar("SELECT stats_json FROM task_executions WHERE id='new-execution'")
        .fetch_one(&pool)
        .await?;
    assert!(stats.is_none());
    Ok(())
}

#[tokio::test]
async fn unknown_or_other_task_execution_cannot_store_details_or_receive_an_ack() -> Result<()> {
    let (pool, repo) = fixture().await?;
    sqlx::raw_sql("INSERT INTO migration_tasks(id,name,task_type,source_endpoint_id,config_json,status) VALUES ('other','other','scan','ep','{}','running');
        INSERT INTO task_executions(id,task_id,execution_type,status) VALUES ('other-execution','other','full','running');")
        .execute(&pool).await?;
    let identity = identity("round", "attempt", 1, 2, 1);
    let batch = batch(&identity);
    let report = progress("wrong execution", false);
    for execution in ["unknown", "other-execution"] {
        assert!(
            repo.receive(ProgressSubmission {
                progress: &report,
                identity: Some(&identity),
                diagnostics: Some(&batch),
                execution_id: Some(execution),
                final_stats: None,
            })
            .await
            .is_err()
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    assert!(repo.find_by_task_id("task").await?.is_none());
    Ok(())
}

#[tokio::test]
async fn conflicting_replayed_batch_is_rejected_without_overwriting_durable_event() -> Result<()> {
    let (pool, repo) = fixture().await?;
    let identity = identity("round", "attempt", 1, 2, 1);
    let mut batch = batch(&identity);
    let report = progress("first", false);
    repo.receive(ProgressSubmission {
        progress: &report,
        identity: Some(&identity),
        diagnostics: Some(&batch),
        execution_id: None,
        final_stats: None,
    })
    .await?;
    batch.events[0].message = "different payload".into();
    assert!(
        repo.receive(ProgressSubmission {
            progress: &report,
            identity: Some(&identity),
            diagnostics: Some(&batch),
            execution_id: None,
            final_stats: None,
        })
        .await
        .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn legacy_reports_still_work_but_cannot_overwrite_an_identified_attempt() -> Result<()> {
    let (_pool, repo) = fixture().await?;
    let report = progress("legacy", false);
    assert!(
        repo.receive(ProgressSubmission {
            progress: &report,
            identity: None,
            diagnostics: None,
            execution_id: None,
            final_stats: None,
        })
        .await?
        .accepted
    );
    let identity = identity("round", "attempt", 1, 2, 1);
    let current = progress("current", false);
    repo.receive(ProgressSubmission {
        progress: &current,
        identity: Some(&identity),
        diagnostics: None,
        execution_id: None,
        final_stats: None,
    })
    .await?;
    assert!(
        !repo
            .receive(ProgressSubmission {
                progress: &report,
                identity: None,
                diagnostics: None,
                execution_id: None,
                final_stats: None,
            })
            .await?
            .accepted
    );
    assert_eq!(repo.find_by_task_id("task").await?.unwrap().report_json, "current");
    Ok(())
}

#[tokio::test]
async fn diagnostics_are_paged_and_omissions_count_only_unique_batches() -> Result<()> {
    let (_pool, repo) = fixture().await?;
    let identity = identity("round", "attempt", 1, 2, 1);
    let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 2, 2);
    outbox.enqueue("scan", "reset", "first", None);
    outbox.enqueue("scan", "reset", "second", None);
    assert!(!outbox.enqueue("scan", "reset", "omitted", None));
    let batch = outbox.prepare_batch().unwrap();
    let report = progress("first", false);
    for _ in 0..2 {
        repo.receive(ProgressSubmission {
            progress: &report,
            identity: Some(&identity),
            diagnostics: Some(&batch),
            execution_id: None,
            final_stats: None,
        })
        .await?;
    }
    let first = repo.diagnostics("task", 0, 1).await?;
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.omitted_count, 1);
    assert_eq!(first.next_offset, Some(1));
    let second = repo.diagnostics("task", 1, 1).await?;
    assert_eq!(second.events.len(), 1);
    assert_ne!(first.events[0].event_id, second.events[0].event_id);
    assert_eq!(second.next_offset, None);
    assert_eq!(second.omitted_count, 1);
    Ok(())
}

#[tokio::test]
async fn callback_http_failure_retries_same_batch_and_only_acknowledges_committed_details() {
    let (pool, _repo) = fixture().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/tasks/{id}/progress", post(task_handler::update_progress))
        .with_state(AppState::new(pool.clone()));
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await
    }));
    let identity = identity("round", "attempt", 1, 2, 1);
    let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 4, 2);
    outbox.enqueue("scan", "reset", "first", None);
    let batch = outbox.prepare_batch().unwrap();
    let report = serde_json::json!({
        "job_id":"task", "job_type":"FullScan", "elapsed_secs":1.0, "speed_bytes_per_sec":0.0,
        "is_final":false, "callback_identity":identity, "diagnostics":batch,
        "detail":{"kind":"Full","scanned_files":0,"scanned_dirs":0,"total_bytes":0,"transferred_bytes":0,"error_count":1}
    });
    let url = format!("http://{address}/tasks/task/progress?execution_id=execution");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_progress BEFORE INSERT ON task_progress BEGIN SELECT RAISE(ABORT,'injected failure'); END;")
        .execute(&pool).await.unwrap();
    assert_eq!(
        client.post(&url).json(&report).send().await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(outbox.prepare_batch(), Some(batch.clone()));
    outbox.enqueue("scan", "reset", "arrived during retry", None);
    sqlx::query("DROP TRIGGER reject_progress")
        .execute(&pool)
        .await
        .unwrap();
    for _ in 0..2 {
        let response = client.post(&url).json(&report).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ack: CallbackAck = response.json().await.unwrap();
        assert_eq!(ack.batch_id.as_deref(), Some(batch.batch_id.as_str()));
    }
    assert!(outbox.ack(&batch.batch_id));
    assert_eq!(
        outbox.prepare_batch().unwrap().events[0].message,
        "arrived during retry"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn a_conflicting_event_id_inside_one_batch_rolls_back_all_details() -> Result<()> {
    let (pool, repo) = fixture().await?;
    let identity = identity("round", "attempt", 1, 2, 1);
    let mut batch = batch(&identity);
    let mut conflict = batch.events[0].clone();
    conflict.message = "a different actual event".into();
    batch.events.push(conflict);
    assert!(
        repo.receive(ProgressSubmission {
            progress: &progress("conflict", false),
            identity: Some(&identity),
            diagnostics: Some(&batch),
            execution_id: Some("execution"),
            final_stats: None,
        })
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count() FROM task_diagnostic_events")
            .fetch_one(&pool)
            .await?,
        0
    );
    Ok(())
}
