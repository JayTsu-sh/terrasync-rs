//! 本地迁移的准入、代次选择和持久化收尾。

use std::{sync::Arc, time::Duration};

use data_mover::storage::NamespaceReadPolicy;
use db::ClickHouseDatabase;
use futures::FutureExt;
use task_model::migration::{
    CopyGenerationPhase, CopyGenerationPlan, CopyRequestDescriptor, MigrationMode, MigrationReason, MigrationReport,
    TargetCleanupPolicy,
};
use task_model::{TargetRetentionReason, TransferMode};
use tokio_util::sync::CancellationToken;
use tracing::info;
use utils::app_config::AppConfig;

use super::SyncOrchestrator;
use crate::consumer::ConsumerManager;
use crate::copy_completion;
use crate::copy_execution::{CopyDatabaseExecution, CopyLease, begin_copy_catalogue, compile_copy_request};
use crate::copy_root::CopyRootMetadata;
use crate::error::{AppError, Result};
use crate::file_copy::transfer as copy_transfer;
use crate::migration_plan::MigrationPlan;
use crate::source_cleanup::{SourceCleanupCheck, remove_confirmed_sources};
use crate::storage_factory::{StorageContext, StorageRole, create_storage_for_role};

pub(super) struct RemainingSourceCleanup {
    pub(super) source: Option<Arc<StorageContext>>,
    pub(super) block_size: Option<usize>,
    pub(super) check: SourceCleanupCheck,
}

impl SyncOrchestrator {
    pub(super) async fn run_local_prepared(
        &self, app_config: &AppConfig, consumers: &mut ConsumerManager,
    ) -> Result<()> {
        let c = &self.config;
        info!(transfer_mode = ?c.transfer_mode, "Sync transfer mode selected");
        if c.packaged && c.transfer_mode != TransferMode::Direct {
            return Err(AppError::ConfigError(
                "Packaged sync supports Direct only; generated archives do not support --resume".into(),
            ));
        }
        let (observation_plan, request, root_source, root_destination) = {
            let destination =
                Arc::new(create_storage_for_role(&c.dest_path, None, true, StorageRole::Destination).await?);
            let source = Arc::new(create_storage_for_role(&c.src_path, None, false, StorageRole::Source).await?);
            if !c.source_cleanup.retains_source() && source.storage().identity() == destination.storage().identity() {
                return Err(AppError::ConfigError("移动任务的源端和目标端不能是同一存储根".into()));
            }
            copy_transfer::validate_transfer_mode(&destination, c.transfer_mode)?;
            let plan = MigrationPlan::compile(&source, &destination, c.enable_acl).await?;
            #[cfg(test)]
            let plan = {
                let mut plan = plan;
                if let Some(epoch) = self.filter_time {
                    plan.filter_time_anchor = epoch;
                }
                plan
            };
            plan.log();
            let request = compile_copy_request(
                c,
                &source,
                &destination,
                &plan,
                app_config,
                consumers.copy_attempt_id().await?,
            )
            .await?;
            (Some(plan), request, source, destination)
        };

        // 模式选择与代次准备共用独占权，避免读取旧策略后被另一执行切换代次。
        let _lease = CopyLease::acquire(&app_config.database.clickhouse, c)?;
        if !c.source_cleanup.retains_source() && !app_config.database.enabled {
            return Err(AppError::ConfigError(
                "当前移动任务要求启用 ClickHouse 目录和代次记录".into(),
            ));
        }
        if c.packaged && !c.source_cleanup.retains_source() {
            return Err(AppError::ConfigError(
                "打包移动需要逐项清单确认与校验，当前拒绝提前删除源目录".into(),
            ));
        }
        // database.enabled=true 时必须成功构造；false 时忽略错误（未启用），走文件系统兜底
        let db_instance = if app_config.database.enabled {
            Some(ClickHouseDatabase::connect(&app_config.database.clickhouse, &c.job_id).await?)
        } else {
            None
        };
        let mut pipeline_database = db_instance.clone();
        let execution = pipeline_database
            .as_mut()
            .map(|database| CopyDatabaseExecution::prepare(database, CancellationToken::new()))
            .transpose()?;
        consumers.set_copy_database_execution(execution.clone())?;
        let root_metadata = CopyRootMetadata::new(
            root_source,
            root_destination,
            observation_plan.as_ref().is_some_and(|plan| plan.root_metadata),
            observation_plan
                .as_ref()
                .map(|plan| plan.observation)
                .unwrap_or_default(),
            NamespaceReadPolicy {
                operation_timeout: Duration::from_secs(app_config.scan.operation_timeout_secs),
                recovery_window: Duration::from_secs(app_config.scan.recovery_window_secs),
                retry_interval: Duration::from_millis(100),
            },
            execution
                .as_ref()
                .map(|execution| execution.token().clone())
                .unwrap_or_default(),
            !c.source_cleanup.retains_source(),
        );
        // 任务边界堆存 future，避免编排状态机与数据库恢复层的 poll 栈叠加。
        let work = self
            .run_planned_local(
                observation_plan,
                &request,
                app_config,
                consumers,
                pipeline_database,
                &root_metadata,
            )
            .boxed_local();
        let mut result = match execution {
            Some(execution) => execution.run(work).await,
            None => work.await,
        };
        if let Some(database) = db_instance {
            let phase = if result.is_ok() {
                CopyGenerationPhase::Completed
            } else {
                CopyGenerationPhase::Failed
            };
            let finalization = match root_metadata.snapshot() {
                Ok(snapshot) => database.finish_copy_generation(phase, snapshot).await,
                Err(error) => {
                    // 原值编码错误也必须交付；仍尝试保存失败终态，不伪造根事实。
                    result = match result {
                        Ok(()) => Err(error),
                        Err(source) => Err(AppError::MigrationAndRootMetadataFailed {
                            source: Box::new(source),
                            metadata: Box::new(error),
                        }),
                    };
                    database.finish_copy_generation(CopyGenerationPhase::Failed, None).await
                }
            };
            if let Ok(generation) = database.current_copy_generation().await {
                consumers.set_migration_report(generation.report()).await;
                if let Err(source) = result {
                    result = match consumers
                        .record_target_retention_range(
                            TargetRetentionReason::IncompleteEnumeration,
                            "本轮迁移未完整收尾，未展开的旧目标范围全部保留，未逐项确认源缺失".into(),
                        )
                        .await
                    {
                        Ok(()) => Err(source),
                        Err(reporting) => Err(AppError::MigrationReportingFailed {
                            source: Box::new(source),
                            reporting: Box::new(reporting),
                        }),
                    };
                }
            }
            return match (result, finalization) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(source), Ok(())) => Err(source),
                (Ok(()), Err(source)) => Err(source.into()),
                (Err(source), Err(finalization)) => Err(AppError::MigrationFinalizationFailed {
                    source: Box::new(source),
                    finalization,
                }),
            };
        }
        result
    }

    pub(super) async fn run_planned_local(
        &self, observation_plan: Option<MigrationPlan>, request: &CopyRequestDescriptor, app_config: &AppConfig,
        consumers: &mut ConsumerManager, database: Option<ClickHouseDatabase>, root_metadata: &CopyRootMetadata,
    ) -> Result<()> {
        let report = if let Some(database) = &database {
            begin_copy_catalogue(database, request).await?;
            database.current_copy_generation().await?.report()
        } else {
            MigrationReport {
                generation: None,
                execution_id: request.execution_id.clone(),
                filter_evaluated_at: request.filter_evaluated_at,
                source_cleanup: request.source_cleanup,
                target_history: request.binding.target_history,
                plan: CopyGenerationPlan::decide(request, None, false)?,
                phase: CopyGenerationPhase::Prepared,
                baseline_prepared: false,
            }
        };
        info!(mode=?report.plan.mode, target_cleanup=?report.plan.target_cleanup, reasons=?report.plan.reasons,
            filter_evaluated_at=report.filter_evaluated_at, generation=?report.generation, execution_id=%report.execution_id, "Migration execution plan");
        consumers.set_migration_report(report.clone()).await;
        root_metadata.capture().await?;
        let result = async {
            if report.plan.target_cleanup == TargetCleanupPolicy::Preserve {
                let reason = if !request.source_cleanup.retains_source()
                    || report.plan.reasons.contains(&MigrationReason::PreviousSourceDeleted)
                {
                    TargetRetentionReason::SourceCleanupPolicy
                } else if request.selection.is_filtered()
                    || request.selection.relative_time
                    || report.plan.reasons.contains(&MigrationReason::SelectionChanged)
                {
                    TargetRetentionReason::FilteredSelection
                } else {
                    TargetRetentionReason::BaselineUnavailable
                };
                consumers
                    .record_target_retention_range(
                        reason,
                        format!("本轮采用 Preserve，原因 {:?}；未展开的旧目标范围全部保留，不逐项回源查询，也不推断缺失删除或破坏性 rename", report.plan.reasons),
                    )
                    .await?;
            }
            // 任务边界隔离管线状态机的布局，避免继续嵌套到 CLI／Web 入口。
            match report.plan.mode {
                MigrationMode::Full => {
                    self.run_sync(observation_plan, app_config, consumers, database, root_metadata)
                        .boxed_local()
                        .await
                }
                MigrationMode::Incremental => {
                    self.run_incremental_sync(
                        observation_plan,
                        app_config,
                        consumers,
                        database.ok_or_else(|| AppError::ConfigError("增量迁移需要已准备的复制目录".into()))?,
                        root_metadata,
                    )
                    .boxed_local()
                    .await
                }
            }
        }
        .await;
        root_metadata.retain_capture_failure(result)
    }

    /// 先完成目标元数据和目录提交，再按本轮证明清理剩余源项。
    pub(super) async fn complete_persisted(
        &self, database: &ClickHouseDatabase, destination: Arc<StorageContext>, consumers: &ConsumerManager,
        root_metadata: &CopyRootMetadata, cleanup: Option<RemainingSourceCleanup>,
    ) -> Result<()> {
        copy_completion::complete(
            database.clone(),
            destination.clone(),
            Some(consumers.task_statistics()),
            consumers.copy_database_execution(),
            Some(root_metadata),
        )
        .await?;
        if let Some(cleanup) = cleanup {
            let source = match cleanup.source {
                Some(source) => source,
                None => Arc::new(
                    create_storage_for_role(
                        &self.config.src_path,
                        cleanup.block_size.map(|size| size as u64),
                        false,
                        StorageRole::Source,
                    )
                    .await?,
                ),
            };
            remove_confirmed_sources(database.clone(), source, destination, cleanup.check).await?;
        }
        Ok(())
    }
}
