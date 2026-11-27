#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB
export LAB_HDFS_BASE_PATH="${LAB_HDFS_BASE_PATH:-/tmp/terrasync-nightly}"
profiles=(local nfs3 nfs40 nfs41 cifs_fas2750 s3_standard s3_dxn hdfs)
deferred_profiles=()
cifs_enabled=true
if [[ "${TS_SINGLE_DEFER_CIFS:-false}" == true ]]; then
  # Temporary lab policy: exercise every non-CIFS direction while FAS2750
  # root provisioning is deferred. The evidence validator records this as a
  # 49-cell degraded run, never as the full 64-cell acceptance gate.
  profiles=(local nfs3 nfs40 nfs41 s3_standard s3_dxn hdfs)
  deferred_profiles=(cifs_fas2750)
  cifs_enabled=false
fi
require_single_matrix_credentials "$cifs_enabled"
if [[ ! "$LAB_DXN_S3_ENDPOINT" =~ ^http://[a-zA-Z0-9.-]+(:[0-9]+)?$ ]]; then
  echo "LAB_DXN_S3_ENDPOINT must be an HTTP origin without credentials or a path" >&2
  exit 2
fi

# The matrix driver is a separate process.  Export explicit, already-validated lab endpoints
# and credentials rather than reconstructing them from a legacy storage URL in its argv.
export LAB_SOURCE_DATA LAB_DEST_DATA LAB_CIFS_SOURCE_DATA LAB_CIFS_DEST_DATA LAB_CIFS_SHARE LAB_CIFS_WRITABLE_ROOT
export LAB_CIFS_USERNAME LAB_CIFS_PASSWORD LAB_S3_BUCKET LAB_S3_ACCESS_KEY LAB_S3_SECRET_KEY
export LAB_DXN_S3_BUCKET LAB_DXN_S3_ENDPOINT LAB_DXN_S3_ACCESS_KEY LAB_DXN_S3_SECRET_KEY
export LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
target_root="$(cargo metadata --format-version 1 --no-deps --manifest-path "$repo_root/Cargo.toml" | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="${TERRASYNC_SINGLE_MATRIX_BINARY:-$target_root/release/examples/single_process_matrix}"
lab_root="/tmp/terrasync-lab/$run_id/single-process-matrix"
runtime_root="$lab_root/runtime"
results_root="${RUNNER_TEMP:-/tmp}/terrasync-harness-results/$run_id-single-process"
aggregate="$results_root/matrix.json"
hdfs_scope="${run_id%%-*}-$(printf %s "$run_id" | sha256sum | cut -d' ' -f1)-matrix"
cifs_scope="$hdfs_scope"
hdfs_run_root="$(hdfs_run_root "$hdfs_scope")"
hdfs_base="${hdfs_run_root%/$hdfs_scope/hdfs}"
hdfs_root="$hdfs_run_root/single-process-matrix"
matrix_prefix="ci/$run_id/single-process-matrix"
dxn_touched=false
hdfs_touched=false
cifs_touched=false

hdfs_cleanup=not_attempted
dxn_cleanup=not_attempted
cifs_cleanup=not_attempted
shielded_cleanup() (
  trap '' INT TERM HUP
  "$@"
)
cleanup_resources() {
  local status=0 cleanup_cancelled=false cleanup_exit
  trap 'cleanup_cancelled=true' INT TERM HUP
  if [[ "$cifs_touched" == true ]]; then
    if shielded_cleanup timeout --foreground --kill-after=5s 45s "$binary" --delete-cifs-run "$cifs_scope" >"$runtime_root/cifs-cleanup.log" 2>&1; then
      cifs_cleanup=passed
    else
      echo 'CIFS scoped cleanup failed' >&2
      cifs_cleanup=failed
      status=1
    fi
    cifs_touched=false
  fi
  if [[ "$hdfs_touched" == true ]]; then
    if shielded_cleanup timeout --foreground --kill-after=5s 45s "$binary" --delete-hdfs-run "$hdfs_base" "$hdfs_scope" >"$runtime_root/hdfs-cleanup.log" 2>&1; then
      hdfs_cleanup=passed
    else
      cleanup_exit=$?
      report_hdfs_cleanup_failure "$cleanup_exit" "$runtime_root/hdfs-cleanup.log"
      hdfs_cleanup=failed
      status=1
    fi
    hdfs_touched=false
  fi
  if [[ "$dxn_touched" == true ]]; then
    dxn_cleanup=passed
    for operation in abort-multipart-prefix delete-prefix verify-clean-prefix; do
      LAB_S3_ACCESS_KEY="$LAB_DXN_S3_ACCESS_KEY" LAB_S3_SECRET_KEY="$LAB_DXN_S3_SECRET_KEY" \
        shielded_cleanup timeout --foreground --kill-after=5s 45s python3 "$repo_root/tests/lab/s3_helper.py" "$operation" \
          --endpoint "$LAB_DXN_S3_ENDPOINT" --bucket "$LAB_DXN_S3_BUCKET" --prefix "$matrix_prefix/" || {
          echo "DXN $operation cleanup failed for $matrix_prefix/" >&2
          dxn_cleanup=failed
          status=1
        }
    done
    dxn_touched=false
  fi
  trap - INT TERM HUP
  [[ "$cleanup_cancelled" == false ]] || return 143
  return "$status"
}
cleanup() {
  local status="$?"
  trap - EXIT
  local cleanup_status=0
  cleanup_resources || cleanup_status=$?
  if (( cleanup_status == 143 )); then status=143
  elif (( cleanup_status != 0 )); then status=1
  fi
  rm -rf -- "$lab_root"
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$runtime_root" "$results_root"
LAB_S3_ACCESS_KEY="$LAB_DXN_S3_ACCESS_KEY" LAB_S3_SECRET_KEY="$LAB_DXN_S3_SECRET_KEY" \
  timeout --foreground --kill-after=5s 45s python3 "$repo_root/tests/lab/s3_helper.py" assert-unversioned \
    --endpoint "$LAB_DXN_S3_ENDPOINT" --bucket "$LAB_DXN_S3_BUCKET"
export LAB_HDFS_SOURCE_CCACHE="FILE:$runtime_root/source.ccache"
export LAB_HDFS_DESTINATION_CCACHE="FILE:$runtime_root/destination.ccache"
# Evidence is attributed to the current checkout and locked dependency graph. Rebuilding is
# mandatory: accepting an executable left by another checkout would forge those commit identities.
env -u LAB_DXN_S3_ACCESS_KEY -u LAB_DXN_S3_SECRET_KEY -u LAB_S3_ACCESS_KEY -u LAB_S3_SECRET_KEY \
  -u LAB_CIFS_USERNAME -u LAB_CIFS_PASSWORD -u LAB_HDFS_KEYTAB \
  cargo build --release --locked --manifest-path "$repo_root/Cargo.toml" -p app --example single_process_matrix

profile_root_url() {
  local role="$1" profile="$2" host
  [[ "$role" == source ]] && host="$LAB_SOURCE_DATA" || host="$LAB_DEST_DATA"
  case "$profile" in
    local) printf '%s' "$lab_root" ;;
    nfs3) printf 'nfs://%s%s:/%s?version=3&noresvport=true' "$host" "$LAB_NFS3_EXPORT" "$matrix_prefix" ;;
    nfs40) printf 'nfs://%s%s:/%s?version=4.0&noresvport=true&uid=0&gid=0' "$LAB_NFS40_DATA" "$LAB_NFS40_EXPORT" "$matrix_prefix" ;;
    nfs41) printf 'nfs://%s%s:/%s?version=4.1&noresvport=true' "$host" "$LAB_NFS41_EXPORT" "$matrix_prefix" ;;
    cifs_fas2750)
      printf '%s/%s/single-process-matrix' "$LAB_CIFS_WRITABLE_ROOT" "$cifs_scope"
      ;;
    s3_standard|s3_dxn) printf '%s' "$matrix_prefix" ;;
    hdfs) printf '%s' "$hdfs_root" ;;
    *) echo "unknown matrix profile: $profile" >&2; return 2 ;;
  esac
}

storage_url() {
  local role="$1" profile="$2" cell="$3" root path query
  root="$(profile_root_url "$role" "$profile")"
  case "$profile" in
    local) printf '%s/%s/%s' "$lab_root" "$cell" "$role" ;;
    # NFS options are encoded in a query string.  The configured namespace
    # must remain in the path portion; appending it after `?` corrupts the
    # final option value (for example, noresvport=true/local__nfs3/source).
    nfs3|nfs40|nfs41)
      path="${root%%\?*}"
      query="${root#*\?}"
      printf '%s/%s/%s?%s' "$path" "$cell" "$role" "$query"
      ;;
    *) printf '%s/%s/%s' "$root" "$cell" "$role" ;;
  esac
}

run_matrix_transfer() {
  "$binary" "$@"
}

write_cell_result() {
  local source="$1" destination="$2" outcome="$3" started="$4" completed="$5" artifact="$6"
  python3 - "$artifact" "$source" "$destination" "$outcome" "$started" "$completed" \
    "$(profile_fingerprint "$source") -> $(profile_fingerprint "$destination")" <<'PY'
import json, os, sys
path, source, destination, outcome, started, completed, fingerprint = sys.argv[1:]
with open(path, "w", encoding="utf-8") as handle:
    json.dump({
        "gate_id": f"TS-SINGLE/{source}__{destination}",
        "source_profile": source,
        "destination_profile": destination,
        "outcome": outcome,
        "fixture_set": "single-process-functional-v1",
        "started_at": started,
        "completed_at": completed,
        "environment_fingerprint": fingerprint,
        "artifact_links": [f"artifact:{os.path.basename(path)}"],
    }, handle, sort_keys=True)
    handle.write("\n")
PY
}

profile_fingerprint() {
  case "$1" in
    local) printf '%s' 'vm102-data-mover-rs/local-filesystem' ;;
    nfs3) printf '%s' 'shared-nfs3/10.10.1.12-to-10.10.1.13' ;;
    nfs40) printf '%s' 'dxn-nfs40/10.131.7.201/jay_nfs' ;;
    nfs41) printf '%s' 'shared-nfs41/10.10.1.12-to-10.10.1.13' ;;
    cifs_fas2750) printf '%s' 'fas2750/10.128.61.200+10.128.61.201/ontap_lisaauto_cifs' ;;
    s3_standard) printf '%s' 'shared-standard-s3/10.10.1.12-to-10.10.1.13' ;;
    s3_dxn) printf 'shared-real-dxn/%s' "$LAB_DXN_S3_ENDPOINT" ;;
    hdfs) printf '%s' "${LAB_HDFS_FINGERPRINT:-hdfs-ha-kerberos/10.131.9.30+10.131.9.33}" ;;
    *) echo "unknown matrix profile: $1" >&2; return 2 ;;
  esac
}

run_cell() {
  local source="$1" destination="$2"
  local cell="${source}__${destination}"
  local fixture="$lab_root/$cell-fixture" readback="$lab_root/$cell-readback"
  local source_root destination_root expected actual started completed artifact
  fixture="$fixture/payload.bin"
  readback="$readback/payload.bin"
  mkdir -p "$(dirname "$fixture")" "$(dirname "$readback")"
  python3 - "$fixture" "$cell" <<'PY'
import pathlib, sys
path, cell = sys.argv[1:]
payload = (f"terrasync-single-process:{cell}\n".encode() * 8193) + b"tail"
pathlib.Path(path).write_bytes(payload)
PY
  expected="$(sha256sum "$fixture" | cut -d' ' -f1)"
  source_root="$(storage_url source "$source" "$cell")"
  destination_root="$(storage_url destination "$destination" "$cell")"
  if [[ "$source" == hdfs || "$destination" == hdfs ]]; then
    hdfs_touched=true
  fi
  if [[ "$source" == cifs_fas2750 || "$destination" == cifs_fas2750 ]]; then
    cifs_touched=true
  fi
  if [[ "$source" == s3_dxn || "$destination" == s3_dxn ]]; then
    dxn_touched=true
  fi

  # The local backend treats its configured root as an existing storage
  # directory.  Remote backends establish their own namespace during
  # connection, but the local endpoints used by cells containing `local`
  # must exist before the matrix driver connects them.
  if [[ "$source" == "local" ]]; then
    mkdir -p "$source_root"
  fi
  if [[ "$destination" == "local" ]]; then
    mkdir -p "$destination_root"
  fi

  artifact="$results_root/$cell.json"
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if run_matrix_transfer local source "$(dirname "$fixture")" "$source" source "$source_root" "matrix-preload-$cell" && \
    run_matrix_transfer "$source" source "$source_root" "$destination" destination "$destination_root" "matrix-copy-$cell" && \
    run_matrix_transfer "$destination" destination "$destination_root" local destination "$(dirname "$readback")" "matrix-readback-$cell"; then
    if ! actual="$(sha256sum "$readback" | cut -d' ' -f1)" || [[ "$actual" != "$expected" ]]; then
      completed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      write_cell_result "$source" "$destination" failed "$started" "$completed" "$artifact"
      echo "$cell readback missing, unreadable, or checksum mismatch" >&2
      return 0
    fi
    completed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    write_cell_result "$source" "$destination" passed "$started" "$completed" "$artifact"
    echo "TS-SINGLE/$cell passed"
  else
    completed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    write_cell_result "$source" "$destination" failed "$started" "$completed" "$artifact"
    echo "TS-SINGLE/$cell failed" >&2
  fi
}

for source in "${profiles[@]}"; do
  for destination in "${profiles[@]}"; do
    run_cell "$source" "$destination"
  done
done

# Finish cleanup before reporting acceptance. The exit trap handles early failures.
cleanup_status=0
cleanup_resources || cleanup_status=$?
(( cleanup_status != 143 )) || exit 143

# 先保留各存储路径的结果，再由汇总校验判定整个验收。
terrasync_commit="$(git -C "$repo_root" rev-parse HEAD)"
data_mover_commit="$(sed -n 's/.*data-mover-rs\.git?rev=\([0-9a-f]*\)#.*/\1/p' "$repo_root/Cargo.lock" | head -1)"
profiles_csv="$(IFS=,; printf '%s' "${profiles[*]}")"
deferred_profiles_csv="$(IFS=,; printf '%s' "${deferred_profiles[*]}")"
python3 - "$aggregate" "$results_root" "$run_id" "$terrasync_commit" "$data_mover_commit" "$profiles_csv" "$deferred_profiles_csv" "$hdfs_cleanup" "$dxn_cleanup" "$cifs_cleanup" <<'PY'
import glob, json, os, sys
output, root, run_id, terrasync, data_mover, profiles, deferred_profiles, hdfs_cleanup, dxn_cleanup, cifs_cleanup = sys.argv[1:]
cells = []
for path in sorted(glob.glob(os.path.join(root, "*__*.json"))):
    with open(path, encoding="utf-8") as handle:
        cells.append(json.load(handle))
report = {
    "schema_version": 1,
    "repository": "JayTsu-sh/terrasync-rs",
    "exact_commit": terrasync,
    "dependency_commits": {"data-mover-rs": data_mover},
    "run_id": run_id,
    "mode": "terrasync_single_process",
    "profiles": profiles.split(",") if profiles else [],
    "deferred_profiles": deferred_profiles.split(",") if deferred_profiles else [],
    "cleanup": {"hdfs": hdfs_cleanup, "s3_dxn": dxn_cleanup, "cifs": cifs_cleanup},
    "cells": cells,
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump(report, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
python3 "$(dirname "$0")/validate-single-process-matrix.py" "$aggregate"
echo "TS-SINGLE ${#profiles[@]}-profile matrix passed: $aggregate"
