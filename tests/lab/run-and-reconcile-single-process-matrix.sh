#!/usr/bin/env bash
# TS-SINGLE lifecycle contract v3
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
[[ "$cancelled" == false ]] || exit 143
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  if (( reconcile_status == 0 )); then finalizer_outcome=success; else finalizer_outcome=failure; fi
  printf 'finalizer_outcome=%s\n' "$finalizer_outcome" >> "$GITHUB_OUTPUT"
fi
if (( reconcile_status == 0 )) && [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'reconciled=true\n' >> "$GITHUB_OUTPUT"
fi
(( producer_status == 0 )) || exit "$producer_status"
exit "$reconcile_status"
