#![allow(clippy::unwrap_used)]
use super::*;

fn request() -> CopyRequestDescriptor {
    CopyRequestDescriptor {
        execution_id: "execution".into(),
        binding: CopyBinding {
            source: NamespaceBinding {
                backend_kind: "local".into(),
                namespace: "source".into(),
            },
            target: NamespaceBinding {
                backend_kind: "local".into(),
                namespace: "target".into(),
            },
            target_history: false,
            mapping: PathMapping::Relative,
        },
        source_cleanup: SourceCleanupPolicy::Retain,
        selection: CopySelection {
            include: None,
            exclude: None,
            file_list: None,
            definition_key: "unfiltered-v1".into(),
            relative_time: false,
        },
        contract: CopyContract {
            acl: false,
            verification: VerificationLevel::Quick,
        },
        runtime: CopyRuntimeSettings {
            transfer_mode: TransferMode::Direct,
            scan_concurrency: 2,
            copy_concurrency: 2,
            block_size: None,
            delta_size_threshold: None,
            qos: None,
            peak_qos_rate: 2.0,
            iops: None,
        },
        filter_evaluated_at: 1,
        delete_target: false,
    }
}

fn previous(request: CopyRequestDescriptor, phase: CopyGenerationPhase, prepared: bool) -> CopyGeneration {
    CopyGeneration {
        root_snapshot: None,
        sequence: 1,
        revision: 1,
        plan: CopyGenerationPlan::decide(&request, None, false).unwrap(),
        request,
        phase,
        baseline_prepared: prepared,
        target_reconciliation_completed: None,
    }
}

#[test]
fn policy_matrix_uses_accepted_cleanup_and_actual_baseline_readiness() {
    for prior_cleanup in [SourceCleanupPolicy::Retain, SourceCleanupPolicy::DeleteSelectedReady] {
        for current_cleanup in [SourceCleanupPolicy::Retain, SourceCleanupPolicy::DeleteSelectedReady] {
            for phase in [
                CopyGenerationPhase::Accepted,
                CopyGenerationPhase::Prepared,
                CopyGenerationPhase::Completed,
                CopyGenerationPhase::Failed,
                CopyGenerationPhase::Cancelled,
            ] {
                for prepared in [false, true] {
                    for available in [false, true] {
                        let mut prior = request();
                        prior.source_cleanup = prior_cleanup;
                        let prior = previous(prior, phase, prepared);
                        let mut current = request();
                        current.source_cleanup = current_cleanup;
                        let plan = CopyGenerationPlan::decide(&current, Some(&prior), available).unwrap();
                        assert_eq!(
                            plan.mode,
                            if prior_cleanup.retains_source()
                                && prepared
                                && available
                                && phase != CopyGenerationPhase::Accepted
                            {
                                MigrationMode::Incremental
                            } else {
                                MigrationMode::Full
                            }
                        );
                        if !current_cleanup.retains_source() {
                            assert_eq!(plan.target_cleanup, TargetCleanupPolicy::Preserve);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn changed_clock_or_selection_reuses_confirmed_facts_but_preserves_target() {
    let mut first = request();
    first.selection.include = Some("modified < 7d".into());
    first.selection.relative_time = true;
    first.selection.definition_key = "ordered-window-v1".into();
    let prior = previous(first.clone(), CopyGenerationPhase::Failed, true);
    first.filter_evaluated_at += 86_400;
    let plan = CopyGenerationPlan::decide(&first, Some(&prior), true).unwrap();
    assert_eq!(plan.mode, MigrationMode::Incremental);
    assert_eq!(plan.target_cleanup, TargetCleanupPolicy::Preserve);
    assert!(!plan.reasons.contains(&MigrationReason::SelectionChanged));
    let unfiltered = request();
    let plan = CopyGenerationPlan::decide(&unfiltered, Some(&prior), true).unwrap();
    assert_eq!(plan.mode, MigrationMode::Incremental);
    assert_eq!(plan.target_cleanup, TargetCleanupPolicy::Preserve);
    assert!(plan.reasons.contains(&MigrationReason::SelectionChanged));
}

#[test]
fn bindings_are_immutable_even_after_source_deleting_or_unprepared_attempts() {
    let mut first = request();
    first.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    let prior = previous(first, CopyGenerationPhase::Failed, false);
    for change in 0..5 {
        let mut current = request();
        match change {
            0 => current.binding.source.namespace.push_str("-other"),
            1 => current.binding.target.namespace.push_str("-other"),
            2 => current.binding.source.backend_kind = "nfs".into(),
            3 => current.binding.target_history = true,
            _ => current.binding.mapping = PathMapping::Packaged { depth: 1 },
        }
        assert!(matches!(
            CopyGenerationPlan::decide(&current, Some(&prior), false),
            Err(TaskModelError::CopyBindingChanged)
        ));
    }
}

#[test]
fn contract_changes_revalidate_selected_entries_without_changing_binding() {
    let prior = previous(request(), CopyGenerationPhase::Completed, true);
    let mut current = request();
    current.contract.verification = VerificationLevel::Full;
    let plan = CopyGenerationPlan::decide(&current, Some(&prior), true).unwrap();
    assert_eq!(plan.mode, MigrationMode::Incremental);
    assert!(plan.force_copy && plan.reasons.contains(&MigrationReason::ContractChanged));
}

#[test]
fn resume_and_scheduling_changes_do_not_invalidate_confirmed_copy_facts() {
    let prior = previous(request(), CopyGenerationPhase::Completed, true);
    let mut current = request();
    current.runtime.transfer_mode = TransferMode::Checkpoint;
    current.runtime.scan_concurrency = 8;
    current.runtime.peak_qos_rate = 1.0;
    let plan = CopyGenerationPlan::decide(&current, Some(&prior), true).unwrap();
    assert_eq!(plan.mode, MigrationMode::Incremental);
    assert!(!plan.force_copy);
}

#[test]
fn mirror_conflicts_fail_before_acceptance() {
    let prior = previous(request(), CopyGenerationPhase::Completed, true);
    for case in 0..3 {
        let mut current = request();
        current.delete_target = true;
        match case {
            0 => current.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady,
            1 => current.selection.exclude = Some("name == '*.tmp'".into()),
            _ => {
                current.selection.file_list = Some(FileSelection {
                    path: "paths".into(),
                    digest: "digest".into(),
                });
            }
        }
        assert!(matches!(
            CopyGenerationPlan::decide(&current, Some(&prior), true),
            Err(TaskModelError::TargetCleanupConflict)
        ));
    }
}
