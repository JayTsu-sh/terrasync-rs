#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname "$0")/single-process-matrix-common.sh"
matrix_paths "${1:?run id required}"
mode="${2:---direct}"
workflow_outcome="${3:-}"
case "$mode" in --direct|--hdfs-tunnel|--hdfs-only|--report-only) ;; *) exit 2 ;; esac
# Matrix may have been cancelled before creating any state.
[[ -d "$state_root" ]] || exit 0
if [[ "$mode" == --hdfs-only ]]; then
  # 子命令沿用父进程的运行锁，在 HDFS 隧道内使用相同回执。
  flock --exclusive --nonblock 9
else
  exec 9>"$state_root/run.lock"
  lock_wait=130
  if [[ "$mode" == --report-only ]]; then lock_wait=20; fi
  flock --exclusive --wait "$lock_wait" 9
fi
[[ -f "$state_root/run.json" ]] || exit 0

if [[ "$mode" != --hdfs-only ]]; then
  if [[ -n "$workflow_outcome" ]]; then
    evidence mark workflow_outcome "$workflow_outcome"
  fi
  evidence interrupt
  if [[ "$mode" == --report-only ]]; then
    evidence publish "$workflow_outcome"
    exit
  fi
fi

cleanup_runtime="$(mktemp -d "${RUNNER_TEMP:-/tmp}/terrasync-matrix-cleanup.XXXXXXXX")"
trap 'rm -rf -- "$cleanup_runtime"' EXIT
raw_log="$cleanup_runtime/operation.log"
run_bounded() {
  local deadline="$1"
  shift
  timeout --foreground --kill-after=4s "$deadline" "$@"
}

delete_hdfs() {
  local root
  root="$(hdfs_run_root "$hdfs_scope")" || return
  if [[ "$root" != "$(evidence field hdfs_target)" ]]; then
    echo "HDFS_CLEANUP_FAILURE=InvalidInput" >&2
    return 2
  fi
  export LAB_HDFS_DESTINATION_CCACHE="FILE:$cleanup_runtime/destination.ccache"
  run_bounded 45s "$binary" --delete-hdfs-run "${root%/$hdfs_scope/hdfs}" "$hdfs_scope"
}

if [[ "$mode" == --hdfs-only ]]; then
  delete_hdfs
  exit
fi

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
  done
  for role in "${cifs_failed[@]}"; do
    # 对端确认目录消失后，用新连接重查此前响应不明的端点。
    if [[ "$cifs_confirmed" == true ]] && evidence retryable "cifs.delete.$role"; then
      cifs_action confirm "$role" 30s || status=1
    else
      status=1
    fi
  done
elif [[ "$(evidence status cifs.claim.source)" != not_attempted ]]; then
  echo "CIFS reconciliation requires the private receipt for $run_id" >&2
  status=1
fi
if evidence touched hdfs && [[ "$(evidence status hdfs.delete)" != passed ]]; then
  hdfs_command=("$0" "$run_id" --hdfs-only)
  if [[ "$mode" == --hdfs-tunnel ]]; then
    hdfs_command=("$repo_root/tests/lab/with-hdfs-tunnel.sh" "${hdfs_command[@]}")
  fi
  record_operation hdfs.delete true 70s hdfs:destination -- "${hdfs_command[@]}" || status=1
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
    done
  fi
fi
evidence publish "$workflow_outcome"
(( status == 0 )) || exit "$status"
rm -f -- "$binary"
# 失败或取消继续保留证据，验收必须满足矩阵验证器。
if [[ "$(evidence field producer)" == finished && "$(evidence field workflow_outcome)" != cancelled ]]; then
  python3 "$repo_root/tests/lab/validate-single-process-matrix.py" "$aggregate"
fi
