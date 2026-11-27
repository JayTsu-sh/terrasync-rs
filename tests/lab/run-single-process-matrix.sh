#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname "$0")/single-process-matrix-common.sh"
matrix_paths "${1:?run id required}"
cd "$repo_root"

profiles=(local nfs3 nfs40 nfs41 cifs_fas2750 s3_standard s3_dxn hdfs)
deferred_profiles=()
cifs_enabled=true
if [[ "${TS_SINGLE_DEFER_CIFS:-false}" == true ]]; then
  # 显式降级诊断覆盖其余七种存储配置。
  profiles=(local nfs3 nfs40 nfs41 s3_standard s3_dxn hdfs)
  deferred_profiles=(cifs_fas2750)
  cifs_enabled=false
fi
export LAB_SOURCE_DATA LAB_DEST_DATA LAB_S3_BUCKET LAB_S3_ACCESS_KEY LAB_S3_SECRET_KEY
export LAB_DXN_S3_BUCKET LAB_DXN_S3_ENDPOINT LAB_DXN_S3_ACCESS_KEY LAB_DXN_S3_SECRET_KEY

runtime_root="$lab_root/runtime"
raw_log="$runtime_root/operation.log"
hdfs_touched=false
watch_matrix_cancellation

cleanup_hdfs() (
  # HDFS 只在生产进程退出时清理一次，沿用当前隧道和凭据。
  trap '' INT TERM HUP
  cancelled=false
  matrix_kill_grace=5
  record_operation hdfs.delete true 45s hdfs:destination -- \
    "$binary" --delete-hdfs-run "$hdfs_base" "$hdfs_scope"
)

check_cancelled() {
  [[ "$cancelled" == false ]] || exit 143
}

cleanup() {
  local status="$?"
  trap - EXIT
  if [[ -f "$state_root/run.json" ]]; then
    if [[ "$hdfs_touched" == true ]]; then
      cleanup_hdfs || status=1
    fi
    if [[ "$cancelled" == true ]] || (( status == 130 || status == 143 )); then
      status=143
      evidence mark producer cancelled || status=143
    elif (( status != 0 )); then
      evidence mark producer failed || true
    fi
    evidence publish || { if (( status == 0 )); then status=1; fi; }
  fi
  rm -rf -- "$lab_root"
  exit "$status"
}

clean_source() {
  local changes head
  changes="$(git --no-optional-locks status --porcelain --untracked-files=all)" || return
  head="$(git rev-parse HEAD)" || return
  [[ -z "$changes" && "$head" == "$source_commit" ]]
}

mkdir -p "$state_home" "${lab_root%/*}"
# 同一 run ID 不覆盖证据或回执。锁跨越所有可能改变存储的子进程。
mkdir "$state_root"
exec 9>"$state_root/run.lock"
flock --exclusive 9
mkdir "$results_root"
mkdir "$lab_root" "$runtime_root"
trap cleanup EXIT
source_commit="$(git rev-parse HEAD)"
source_state=clean
if ! clean_source; then source_state=dirty; fi
data_mover_commit="$(sed -n 's/.*data-mover-rs\.git?rev=[0-9a-f]*#\([0-9a-f]*\)".*/\1/p' Cargo.lock | head -1)"
profiles_csv="$(IFS=,; printf '%s' "${profiles[*]}")"
deferred_profiles_csv="$(IFS=,; printf '%s' "${deferred_profiles[*]}")"
reported_commit=""
if [[ "$source_state" == clean ]]; then reported_commit="$source_commit"; fi
evidence init "$run_id" "$reported_commit" "$data_mover_commit" "$source_state" "$profiles_csv" "$deferred_profiles_csv"
if [[ "$source_state" != clean ]]; then
  echo "TS-SINGLE exact-commit evidence requires a clean source checkout" >&2
  exit 2
fi

require_single_matrix_credentials "$cifs_enabled"
if [[ ! "$LAB_DXN_S3_ENDPOINT" =~ ^http://[a-zA-Z0-9.-]+(:[0-9]+)?$ ]]; then
  echo "LAB_DXN_S3_ENDPOINT must be an HTTP origin without credentials or a path" >&2
  exit 2
fi
target_root="$(cargo metadata --format-version 1 --no-deps --manifest-path "$repo_root/Cargo.toml" | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
check_cancelled
LAB_S3_ACCESS_KEY="$LAB_DXN_S3_ACCESS_KEY" LAB_S3_SECRET_KEY="$LAB_DXN_S3_SECRET_KEY" \
  record_operation dxn.preflight false 45s s3_dxn -- \
    python3 "$repo_root/tests/lab/s3_helper.py" assert-unversioned \
    --endpoint "$LAB_DXN_S3_ENDPOINT" --bucket "$LAB_DXN_S3_BUCKET"
export LAB_HDFS_SOURCE_CCACHE="FILE:$runtime_root/source.ccache"
export LAB_HDFS_DESTINATION_CCACHE="FILE:$runtime_root/destination.ccache"
# 用当前锁定源码构建，并为下一步保留同一个可执行文件。
env -u LAB_DXN_S3_ACCESS_KEY -u LAB_DXN_S3_SECRET_KEY -u LAB_S3_ACCESS_KEY -u LAB_S3_SECRET_KEY \
  -u LAB_CIFS_USERNAME -u LAB_CIFS_PASSWORD -u LAB_HDFS_KEYTAB \
  -u LAB_CIFS_NIGHTLY_USERNAME -u LAB_CIFS_NIGHTLY_PASSWORD \
  cargo build --release --locked --manifest-path "$repo_root/Cargo.toml" -p app --example single_process_matrix
if ! clean_source; then
  evidence mark source_state dirty
  echo "TS-SINGLE source changed during the build" >&2
  exit 2
fi
cp --reflink=auto "$target_root/release/examples/single_process_matrix" "$binary"
sync "$binary"
check_cancelled
hdfs_run_root="$(hdfs_run_root "$hdfs_scope")"
hdfs_base="${hdfs_run_root%/$hdfs_scope/hdfs}"
hdfs_root="$hdfs_run_root/single-process-matrix"
evidence mark dxn_target "$LAB_DXN_S3_ENDPOINT/$LAB_DXN_S3_BUCKET"

profile_root_url() {
  local role="$1" profile="$2" host
  [[ "$role" == source ]] && host="$LAB_SOURCE_DATA" || host="$LAB_DEST_DATA"
  case "$profile" in
    local) printf '%s' "$lab_root" ;;
    nfs3) printf 'nfs://%s%s:/%s?version=3&noresvport=true' "$host" "$LAB_NFS3_EXPORT" "$matrix_prefix" ;;
    nfs40) printf 'nfs://%s%s:/%s?version=4.0&noresvport=true&uid=0&gid=0' "$LAB_NFS40_DATA" "$LAB_NFS40_EXPORT" "$matrix_prefix" ;;
    nfs41) printf 'nfs://%s%s:/%s?version=4.1&noresvport=true' "$host" "$LAB_NFS41_EXPORT" "$matrix_prefix" ;;
    s3_standard|s3_dxn) printf '%s' "$matrix_prefix" ;;
    hdfs) printf '%s' "$hdfs_root" ;;
    *) echo "unknown matrix profile: $profile" >&2; return 2 ;;
  esac
}

storage_url() {
  local role="$1" profile="$2" cell="$3" root path query
  if [[ "$profile" == cifs_fas2750 ]]; then
    printf '%s' "$cell"
    return
  fi
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

cell_transfer() {
  local operation="$1"
  shift
  record_operation "$operation" true 120s "$1:$2" "$4:$5" -- "$binary" "$@"
}

profile_fingerprint() {
  case "$1" in
    local) printf '%s' 'vm102-data-mover-rs/local-filesystem' ;;
    nfs3) printf '%s' 'shared-nfs3/10.10.1.12-to-10.10.1.13' ;;
    nfs40) printf '%s' 'dxn-nfs40/10.131.7.201/jay_nfs' ;;
    nfs41) printf '%s' 'shared-nfs41/10.10.1.12-to-10.10.1.13' ;;
    cifs_fas2750) printf '%s' 'fas2750/10.128.61.200+10.128.61.201/terrasync_nightly' ;;
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
  local source_root destination_root expected actual="" started completed phase_outcome=failed
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
  if [[ "$hdfs_touched" == false && ( "$source" == hdfs || "$destination" == hdfs ) ]]; then
    evidence mark touched hdfs
    hdfs_touched=true
  fi
  if [[ "$source" == s3_dxn || "$destination" == s3_dxn ]]; then
    evidence mark touched dxn
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

  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if cell_transfer "$cell.preload" local source "$(dirname "$fixture")" "$source" source "$source_root" "matrix-preload-$cell" &&
    cell_transfer "$cell.copy" "$source" source "$source_root" "$destination" destination "$destination_root" "matrix-copy-$cell" &&
    cell_transfer "$cell.readback" "$destination" destination "$destination_root" local destination "$(dirname "$readback")" "matrix-readback-$cell"; then
    if actual="$(sha256sum "$readback" | cut -d' ' -f1)" && [[ "$actual" == "$expected" ]]; then
      phase_outcome=passed
    else
      echo "$cell readback missing, unreadable, or checksum mismatch" >&2
    fi
  fi
  completed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  evidence cell "$source" "$destination" "$phase_outcome" "$started" "$completed" \
    "$(profile_fingerprint "$source") -> $(profile_fingerprint "$destination")" "$expected" "$actual"
  echo "TS-SINGLE/$cell $phase_outcome"
}

if [[ "$cifs_enabled" == true ]]; then
  cifs_action prepare source 5s
  cifs_action claim source 20s
  cifs_action verify destination 20s
  evidence publish
fi

for source in "${profiles[@]}"; do
  for destination in "${profiles[@]}"; do
    check_cancelled
    run_cell "$source" "$destination"
  done
done

check_cancelled
if ! clean_source; then
  evidence mark source_state dirty
  echo "TS-SINGLE source changed during the run" >&2
  exit 2
fi
evidence mark producer finished
