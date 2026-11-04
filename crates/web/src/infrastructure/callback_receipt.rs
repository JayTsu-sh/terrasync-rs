//! 回调接收事务：明细去重独立于进度排序，提交后才能返回批次 ACK。

use std::cmp::Ordering;

use app::consumer::stats::diagnostics::{CallbackAck, CallbackIdentity, DiagnosticBatch, MAX_DIAGNOSTIC_BATCH_EVENTS};
use sqlx::{Sqlite, Transaction};

use crate::domain::progress::{ProgressReceipt, ProgressSubmission};
use crate::error::{Result, WebError};

pub async fn receive(
    transaction: &mut Transaction<'_, Sqlite>, submission: ProgressSubmission<'_>,
) -> Result<ProgressReceipt> {
    let task_id = &submission.progress.task_id;
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM migration_tasks WHERE id=?")
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await?;
    let status = status.ok_or_else(|| WebError::TaskNotFound(task_id.clone()))?;
    validate_submission(&submission)?;
    // 结束的本任务执行仍可补投明细；未知或其他任务的执行不得向本任务写入。
    let execution_running = if let Some(execution_id) = submission.execution_id {
        let execution_status: Option<String> =
            sqlx::query_scalar("SELECT status FROM task_executions WHERE id=? AND task_id=?")
                .bind(execution_id)
                .bind(task_id)
                .fetch_optional(&mut **transaction)
                .await?;
        Some(execution_status.ok_or_else(invalid_callback)? == "running")
    } else {
        None
    };
    if let Some(batch) = submission.diagnostics {
        store_batch(transaction, task_id, batch).await?;
    }
    let current: Option<(String, bool)> =
        sqlx::query_as("SELECT identity_json,is_final FROM task_callback_cursor WHERE task_id=?")
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?;
    let mut accepted = match (submission.identity, current) {
        (Some(identity), Some((json, final_report))) => follows(identity, &serde_json::from_str(&json)?, final_report),
        (Some(_), None) | (None, None) => true,
        (None, Some(_)) => false,
    };
    if let Some(running) = execution_running {
        accepted &= running;
    }
    if submission.progress.is_final && status != "running" {
        accepted = false;
    }
    if accepted {
        store_progress(transaction, &submission).await?;
    }
    Ok(ProgressReceipt {
        accepted,
        ack: CallbackAck {
            batch_id: submission.diagnostics.map(|batch| batch.batch_id.clone()),
        },
    })
}

fn validate_submission(submission: &ProgressSubmission<'_>) -> Result<()> {
    if let Some(identity) = submission.identity {
        if !valid_id(&identity.round_id)
            || !valid_id(&identity.attempt_id)
            || identity.round_started_at_millis > identity.attempt_started_at_millis
        {
            return Err(invalid_callback());
        }
    }
    if let Some(batch) = submission.diagnostics {
        if !valid_id(&batch.batch_id) || batch.events.len() > MAX_DIAGNOSTIC_BATCH_EVENTS {
            return Err(invalid_callback());
        }
        for event in &batch.events {
            if !valid_id(&event.event_id)
                || !valid_id(&event.round_id)
                || !valid_id(&event.attempt_id)
                || submission
                    .identity
                    .is_none_or(|identity| identity.round_id != event.round_id)
            {
                return Err(invalid_callback());
            }
        }
        if submission.identity.is_none() {
            return Err(invalid_callback());
        }
    }
    Ok(())
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && value.chars().all(|c| c.is_ascii_alphanumeric() || "-_:./".contains(c))
}

fn invalid_callback() -> WebError {
    WebError::ValidationError("Invalid callback diagnostic identity or batch".into())
}

fn follows(next: &CallbackIdentity, current: &CallbackIdentity, final_report: bool) -> bool {
    match next.round_started_at_millis.cmp(&current.round_started_at_millis) {
        Ordering::Greater => return next.round_id != current.round_id,
        Ordering::Less => return false,
        Ordering::Equal if next.round_id != current.round_id => return false,
        Ordering::Equal => {}
    }
    match next.attempt_started_at_millis.cmp(&current.attempt_started_at_millis) {
        Ordering::Greater => next.attempt_id != current.attempt_id,
        Ordering::Less => false,
        Ordering::Equal => next.attempt_id == current.attempt_id && next.sequence > current.sequence && !final_report,
    }
}

async fn store_batch(transaction: &mut Transaction<'_, Sqlite>, task_id: &str, batch: &DiagnosticBatch) -> Result<()> {
    let mut sanitized = batch.clone();
    for event in &mut sanitized.events {
        event.sanitize();
    }
    let json = serde_json::to_string(&sanitized)?;
    let existing: Option<String> =
        sqlx::query_scalar("SELECT batch_json FROM task_diagnostic_batches WHERE task_id=? AND batch_id=?")
            .bind(task_id)
            .bind(&batch.batch_id)
            .fetch_optional(&mut **transaction)
            .await?;
    if let Some(existing) = existing {
        return if existing == json {
            Ok(())
        } else {
            Err(invalid_callback())
        };
    }
    for event in &sanitized.events {
        let event_json = serde_json::to_string(event)?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT event_json FROM task_diagnostic_events WHERE task_id=? AND event_id=?")
                .bind(task_id)
                .bind(&event.event_id)
                .fetch_optional(&mut **transaction)
                .await?;
        if existing.as_ref().is_some_and(|existing| existing != &event_json) {
            return Err(invalid_callback());
        }
        sqlx::query("INSERT OR IGNORE INTO task_diagnostic_events(task_id,event_id,event_json) VALUES (?,?,?)")
            .bind(task_id)
            .bind(&event.event_id)
            .bind(event_json)
            .execute(&mut **transaction)
            .await?;
    }
    sqlx::query("INSERT INTO task_diagnostic_batches(task_id,batch_id,batch_json) VALUES (?,?,?)")
        .bind(task_id)
        .bind(&batch.batch_id)
        .bind(json)
        .execute(&mut **transaction)
        .await?;
    let omitted: Option<String> =
        sqlx::query_scalar("SELECT omitted_count FROM task_diagnostic_summary WHERE task_id=?")
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?;
    let total = parse_omitted(omitted.as_deref())?.saturating_add(batch.omitted_count);
    sqlx::query("INSERT INTO task_diagnostic_summary(task_id,omitted_count) VALUES (?,?) ON CONFLICT(task_id) DO UPDATE SET omitted_count=excluded.omitted_count")
        .bind(task_id).bind(total.to_string()).execute(&mut **transaction).await?;
    Ok(())
}

pub(super) fn parse_omitted(value: Option<&str>) -> Result<u64> {
    Ok(value.map(serde_json::from_str).transpose()?.unwrap_or(0))
}

async fn store_progress(transaction: &mut Transaction<'_, Sqlite>, submission: &ProgressSubmission<'_>) -> Result<()> {
    let progress = submission.progress;
    sqlx::query("INSERT INTO task_progress(task_id,report_json,is_final,updated_at) VALUES (?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET report_json=excluded.report_json,is_final=excluded.is_final,updated_at=excluded.updated_at")
        .bind(&progress.task_id).bind(&progress.report_json).bind(progress.is_final).bind(progress.updated_at.to_rfc3339())
        .execute(&mut **transaction).await?;
    if let Some(identity) = submission.identity {
        sqlx::query("INSERT INTO task_callback_cursor(task_id,identity_json,is_final) VALUES (?,?,?) ON CONFLICT(task_id) DO UPDATE SET identity_json=excluded.identity_json,is_final=excluded.is_final")
            .bind(&progress.task_id).bind(serde_json::to_string(identity)?).bind(progress.is_final)
            .execute(&mut **transaction).await?;
    }
    if let Some(stats) = submission.final_stats {
        // 绑定执行 ID 后，旧进程的最终报告不能给新执行记录写统计。
        sqlx::query("UPDATE task_executions SET stats_json=? WHERE id=COALESCE(?,(SELECT id FROM task_executions WHERE task_id=? AND status='running' ORDER BY started_at DESC LIMIT 1)) AND task_id=? AND status='running'")
            .bind(serde_json::to_string(stats)?).bind(submission.execution_id).bind(&progress.task_id).bind(&progress.task_id)
            .execute(&mut **transaction).await?;
    }
    Ok(())
}
