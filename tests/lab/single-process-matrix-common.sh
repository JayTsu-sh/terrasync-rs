#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

matrix_paths() {
  run_id="$1"
  validate_run_id "$run_id"
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  lab_root="/tmp/terrasync-lab/$run_id/single-process-matrix"
  state_home="${TS_SINGLE_STATE_ROOT:-$HOME/.local/state/terrasync-single-matrix}"
  state_root="$state_home/$run_id"
  results_root="$state_root/evidence"
  aggregate="$results_root/matrix.json"
  binary="$state_root/driver"
  export TS_SINGLE_MATRIX_RUN_ID="$run_id" TS_SINGLE_CIFS_RECEIPT="$state_root/cifs-receipt.json"
  export LAB_CIFS_NIGHTLY_USERNAME LAB_CIFS_NIGHTLY_PASSWORD
  export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB
  export LAB_HDFS_BASE_PATH="${LAB_HDFS_BASE_PATH:-/tmp/terrasync-nightly}"
  hdfs_scope="${run_id%%-*}-$(printf %s "$run_id" | sha256sum | cut -d' ' -f1)-matrix"
  matrix_prefix="ci/$run_id/single-process-matrix"
}

evidence() {
  local command="$1"
  shift
  python3 "$repo_root/tests/lab/single-matrix-evidence.py" "$command" "$results_root" "$@"
}

record_operation() {
  local name="$1" mutates="$2" deadline="$3" attempt status=0
  shift 3
  local endpoints=()
  while [[ "$1" != -- ]]; do endpoints+=("$1"); shift; done
  shift
  attempt="$(evidence begin "$name" "$mutates" "${endpoints[@]}")" || return
  # 下游原始输出只进入私有临时文件。公开证据仅保留允许列表中的诊断字段。
  run_bounded "$deadline" "$@" >"$raw_log" 2>&1 || status=$?
  evidence finish "$attempt" "$status" "$raw_log" || return
  return "$status"
}

cifs_action() {
  local action="$1" role="$2" deadline="$3" mutates=false operation="$1"
  case "$action" in claim|delete) mutates=true ;; esac
  if [[ "$action" == confirm ]]; then operation=delete; fi
  record_operation "cifs.$operation.$role" "$mutates" "$deadline" "cifs_fas2750:$role" -- \
    "$binary" "--$action-cifs-run" "$run_id" "$role"
}
