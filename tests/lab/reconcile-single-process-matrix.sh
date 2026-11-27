#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname "$0")/single-process-matrix-common.sh"
matrix_paths "${1:?run id required}"
mode="${2:---direct}"
producer_outcome="${3:-}"
lifecycle_outcome="${4:-}"
finalizer_outcome="${5:-}"
case "$mode" in --direct|--report-only) ;; *) exit 2 ;; esac
case "$producer_outcome" in ''|success|failure|cancelled) ;; *) exit 2 ;; esac
case "$lifecycle_outcome" in ''|success|failure|cancelled) ;; *) exit 2 ;; esac
case "$finalizer_outcome" in ''|success|failure|cancelled) ;; *) exit 2 ;; esac
watch_matrix_cancellation
# Matrix may have been cancelled before creating any state.
[[ -d "$state_root" ]] || exit 0
exec 9>"$state_root/run.lock"
lock_wait=130
if [[ "$mode" == --report-only ]]; then lock_wait=20; fi
flock --exclusive --wait "$lock_wait" 9
[[ -f "$state_root/run.json" ]] || exit 0

cleanup_runtime=""
finish() {
  local status=$?
  trap - EXIT
  if [[ "$cancelled" == true ]]; then
    evidence mark workflow_outcome cancelled || true
    evidence publish || true
    status=143
  fi
  if [[ -n "$cleanup_runtime" ]]; then rm -rf -- "$cleanup_runtime"; fi
  exit "$status"
}
trap finish EXIT

evidence record-outcomes "$producer_outcome" "$lifecycle_outcome" "$finalizer_outcome"
if [[ "${TS_SINGLE_WORKFLOW_CANCELLED:-false}" == true ||
      "$producer_outcome" == cancelled || "$lifecycle_outcome" == cancelled ]]; then
  evidence mark workflow_outcome cancelled
fi
evidence interrupt
evidence publish
[[ "$mode" == --direct ]] || exit 0

check_cancelled() {
  [[ "$cancelled" == false ]] || exit 143
}

check_cancelled
cleanup_runtime="$(mktemp -d "${RUNNER_TEMP:-/tmp}/terrasync-matrix-cleanup.XXXXXXXX")"
raw_log="$cleanup_runtime/operation.log"

status=0
if [[ -f "$TS_SINGLE_CIFS_RECEIPT" ]]; then
  cifs_failed=()
  cifs_confirmed=false
  for role in source destination; do
    if [[ "$(evidence status "cifs.delete.$role")" == passed ]] || cifs_action delete "$role" 30s; then
      cifs_confirmed=true
    else
      cifs_failed+=("$role")
    fi
    check_cancelled
  done
  for role in "${cifs_failed[@]}"; do
    # 对端确认目录消失后，用新连接重查此前响应不明的端点。
    if [[ "$cifs_confirmed" == true ]] && evidence retryable "cifs.delete.$role"; then
      cifs_action confirm "$role" 30s || status=1
    else
      status=1
    fi
    check_cancelled
  done
elif [[ "$(evidence status cifs.claim.source)" != not_attempted ]]; then
  echo "CIFS reconciliation requires the private receipt for $run_id" >&2
  status=1
fi
if evidence touched dxn && [[ "$(evidence status dxn.abort):$(evidence status dxn.delete):$(evidence status dxn.verify)" != passed:passed:passed ]]; then
  if [[ "$LAB_DXN_S3_ENDPOINT/$LAB_DXN_S3_BUCKET" != "$(evidence field dxn_target)" ]]; then
    echo "DXN reconciliation target differs from the recorded run" >&2
    status=1
  else
    for pair in abort:abort-multipart-prefix delete:delete-prefix verify:verify-clean-prefix; do
      operation="${pair%%:*}"
      mutates=true
      if [[ "$operation" == verify ]]; then mutates=false; fi
      LAB_S3_ACCESS_KEY="$LAB_DXN_S3_ACCESS_KEY" LAB_S3_SECRET_KEY="$LAB_DXN_S3_SECRET_KEY" \
        record_operation "dxn.$operation" "$mutates" 30s s3_dxn -- \
        python3 "$repo_root/tests/lab/s3_helper.py" "${pair#*:}" \
          --endpoint "$LAB_DXN_S3_ENDPOINT" --bucket "$LAB_DXN_S3_BUCKET" --prefix "$matrix_prefix/" || status=1
      check_cancelled
    done
  fi
fi
check_cancelled
evidence publish
(( status == 0 )) || exit "$status"
if ! evidence touched hdfs || [[ "$(evidence status hdfs.delete)" == passed ]]; then
  rm -f -- "$binary"
fi
# 失败或取消继续保留证据，验收必须满足矩阵验证器。
validation="$(evidence validation)"
if [[ "$validation" == required ]]; then
  python3 "$repo_root/tests/lab/validate-single-process-matrix.py" "$aggregate"
fi
