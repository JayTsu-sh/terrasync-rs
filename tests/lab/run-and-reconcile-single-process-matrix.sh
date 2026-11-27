#!/usr/bin/env bash
# TS-SINGLE lifecycle contract v4
# The caller holds the shared lab lock until this command exits.
set -euo pipefail
run_id="${1:?run id required}"
mode="${2:---direct}"
script_dir="$(dirname "$0")"
producer=("$script_dir/run-single-process-matrix.sh" "$run_id")
case "$mode" in
  --direct) ;;
  --hdfs-tunnel) producer=("$script_dir/with-hdfs-tunnel.sh" "${producer[@]}") ;;
  *) exit 2 ;;
esac

cancelled=false
trap 'cancelled=true' INT TERM HUP
run_phase() {
  local pid status=0
  [[ "$cancelled" == false ]] || return 143
  "$@" &
  pid=$!
  if [[ "$cancelled" == false ]]; then
    wait "$pid" || status=$?
  fi
  if [[ "$cancelled" == true ]]; then
    kill -TERM "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do
      wait "$pid" || true
    done
    return 143
  fi
  return "$status"
}

producer_status=0
run_phase "${producer[@]}" || producer_status=$?
case "$producer_status" in
  0) outcome=success ;;
  130|143) outcome=cancelled ;;
  *) outcome=failure ;;
esac
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'producer_outcome=%s\n' "$outcome" >> "$GITHUB_OUTPUT"
fi
[[ "$cancelled" == false ]] || exit 143

reconcile_status=0
run_phase "$script_dir/reconcile-single-process-matrix.sh" "$run_id" --direct "$outcome" || reconcile_status=$?
case "$reconcile_status" in
  0) finalizer_outcome=success ;;
  130|143) finalizer_outcome=cancelled ;;
  *) finalizer_outcome=failure ;;
esac
# 初次收尾退出后，在释放实验室锁前记录实际结果。
publish_status=0
"$script_dir/reconcile-single-process-matrix.sh" "$run_id" --report-only "$outcome" "" "$finalizer_outcome" || publish_status=$?
[[ "$cancelled" == false ]] || exit 143
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'finalizer_outcome=%s\n' "$finalizer_outcome" >> "$GITHUB_OUTPUT"
  if (( reconcile_status == 0 && publish_status == 0 )); then
    printf 'reconciled=true\n' >> "$GITHUB_OUTPUT"
  fi
fi
(( producer_status == 0 )) || exit "$producer_status"
(( reconcile_status == 0 )) || exit "$reconcile_status"
exit "$publish_status"
