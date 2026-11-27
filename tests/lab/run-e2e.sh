#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
require_s3_credentials

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
target_root="$(cargo metadata --format-version 1 --no-deps --manifest-path "$repo_root/Cargo.toml" | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="${TERRASYNC_BINARY:-$target_root/release/terrasync}"
lab_root="/tmp/terrasync-lab/$run_id"
runtime_root="$lab_root/runtime"
fixture_root="$lab_root/fixtures"
mkdir -p "$runtime_root" "$fixture_root"
database_name="$(clickhouse_database_name "$run_id")"
clickhouse_query "CREATE DATABASE IF NOT EXISTS $database_name"
cat > "$lab_root/config.toml" <<EOF
[database]
enabled = true
batch_size = 800000

[database.clickhouse]
dsn = "$LAB_CLICKHOUSE_DSN"
dial_timeout = 5
read_timeout = 30
database = "$database_name"
username = "$LAB_CLICKHOUSE_USER"
password = "$LAB_CLICKHOUSE_PASSWORD"
EOF

storage_url() {
  local role="$1" backend="$2" case_id="$3" host export_path
  [[ "$role" == source ]] && host="$LAB_SOURCE_DATA" || host="$LAB_DEST_DATA"
  case "$backend" in
    local) printf '%s/%s/%s' "$lab_root" "$case_id" "$role" ;;
    nfs3)
      export_path="$LAB_NFS3_EXPORT"
      printf 'nfs://%s%s:/ci/%s/%s/%s?version=3&noresvport=true' "$host" "$export_path" "$run_id" "$case_id" "$role"
      ;;
    nfs41)
      export_path="$LAB_NFS41_EXPORT"
      printf 'nfs://%s%s:/ci/%s/%s/%s?version=4.1&noresvport=true' "$host" "$export_path" "$run_id" "$case_id" "$role"
      ;;
    s3)
      printf 's3://%s:%s@%s.%s:9000/ci/%s/%s/%s' \
        "$LAB_S3_ACCESS_KEY" "$LAB_S3_SECRET_KEY" "$LAB_S3_BUCKET" "$host" "$run_id" "$case_id" "$role"
      ;;
    *) echo "unsupported backend: $backend" >&2; return 2 ;;
  esac
}

prepare_storage_root() {
  local role="$1" backend="$2" case_id="$3" host export_path
  case "$backend" in
    local) mkdir -p "$lab_root/$case_id/$role" ;;
    nfs3|nfs41)
      [[ "$role" == source ]] && host="$LAB_SOURCE_MGMT" || host="$LAB_DEST_MGMT"
      [[ "$backend" == nfs3 ]] && export_path="$LAB_NFS3_EXPORT" || export_path="$LAB_NFS41_EXPORT"
      ssh_lab_root "$host" \
        "mkdir -p '$export_path/ci/$run_id/$case_id/$role' && chown '$LAB_FIXTURE_UID:$LAB_FIXTURE_GID' '$export_path/ci/$run_id/$case_id/$role' && chmod 0777 '$export_path/ci/$run_id/$case_id/$role'"
      ;;
    s3) ;;
  esac
}

put_fixture() {
  local role="$1" backend="$2" case_id="$3" key="$4" file="$5" host export_path
  prepare_storage_root "$role" "$backend" "$case_id"
  case "$backend" in
    local) install -m 0640 "$file" "$lab_root/$case_id/$role/$key" ;;
    nfs3|nfs41)
      [[ "$role" == source ]] && host="$LAB_SOURCE_MGMT" || host="$LAB_DEST_MGMT"
      [[ "$backend" == nfs3 ]] && export_path="$LAB_NFS3_EXPORT" || export_path="$LAB_NFS41_EXPORT"
      ssh_lab_root "$host" \
        "cat > '$export_path/ci/$run_id/$case_id/$role/$key' && chown '$LAB_FIXTURE_UID:$LAB_FIXTURE_GID' '$export_path/ci/$run_id/$case_id/$role/$key'" \
        < "$file"
      ;;
    s3)
      [[ "$role" == source ]] && host="$LAB_SOURCE_DATA" || host="$LAB_DEST_DATA"
      python3 "$(dirname "$0")/s3_helper.py" put --endpoint "$host" --bucket "$LAB_S3_BUCKET" \
        --key "ci/$run_id/$case_id/$role/$key" --file "$file"
      ;;
  esac
}

object_hash() {
  local role="$1" backend="$2" case_id="$3" key="$4" host export_path
  case "$backend" in
    local) sha256sum "$lab_root/$case_id/$role/$key" | cut -d' ' -f1 ;;
    nfs3|nfs41)
      [[ "$role" == source ]] && host="$LAB_SOURCE_MGMT" || host="$LAB_DEST_MGMT"
      [[ "$backend" == nfs3 ]] && export_path="$LAB_NFS3_EXPORT" || export_path="$LAB_NFS41_EXPORT"
      ssh_lab_root "$host" "sha256sum '$export_path/ci/$run_id/$case_id/$role/$key' | cut -d' ' -f1"
      ;;
    s3)
      [[ "$role" == source ]] && host="$LAB_SOURCE_DATA" || host="$LAB_DEST_DATA"
      python3 "$(dirname "$0")/s3_helper.py" sha256 --endpoint "$host" --bucket "$LAB_S3_BUCKET" \
        --key "ci/$run_id/$case_id/$role/$key"
      ;;
  esac
}

run_terrasync() {
  (cd "$runtime_root" && "$binary" -c "$lab_root/config.toml" "$@")
}

committed_scan() {
  local job="$1" row generation epoch
  row="$(clickhouse_query "SELECT scan_state,scan_epoch FROM $database_name.state_$job FINAL WHERE id=1 AND baseline_committed FORMAT TSVRaw")"
  read -r generation epoch <<<"$row"
  if [[ ! "$generation" =~ ^[01]$ || ! "$epoch" =~ ^[1-9][0-9]*$ ]]; then
    echo "$job has no valid committed scan generation: $row" >&2
    return 1
  fi
  printf '%s\n' "$row"
}

next_scan_commit() {
  local job="$1" previous="$2" current previous_generation previous_epoch generation epoch
  current="$(committed_scan "$job")" || return
  read -r previous_generation previous_epoch <<<"$previous"
  read -r generation epoch <<<"$current"
  if [[ "$generation" == "$previous_generation" || "$epoch" -le "$previous_epoch" ]]; then
    echo "$job did not advance from committed scan [$previous] to a new generation: [$current]" >&2
    return 1
  fi
  printf '%s\n' "$current"
}

backends=(local nfs3 nfs41 s3)
for source_backend in "${backends[@]}"; do
  for destination_backend in "${backends[@]}"; do
    case_id="${source_backend}-to-${destination_backend}"
    key="payload.txt"
    fixture="$fixture_root/$case_id.txt"
    printf 'terrasync-%s-%s\n' "$run_id" "$case_id" > "$fixture"
    expected_hash="$(sha256sum "$fixture" | cut -d' ' -f1)"
    put_fixture source "$source_backend" "$case_id" "$key" "$fixture"
    prepare_storage_root destination "$destination_backend" "$case_id"

    run_terrasync sync --resume --id "lab-$case_id" \
      "$(storage_url source "$source_backend" "$case_id")" \
      "$(storage_url destination "$destination_backend" "$case_id")"
    actual_hash="$(object_hash destination "$destination_backend" "$case_id" "$key")"
    [[ "$actual_hash" == "$expected_hash" ]] || {
      echo "$case_id checksum mismatch: expected=$expected_hash actual=$actual_hash" >&2
      exit 1
    }
    run_terrasync integrity-check --id "lab-$case_id-quick" --quick \
      "$(storage_url source "$source_backend" "$case_id")" \
      "$(storage_url destination "$destination_backend" "$case_id")"
    run_terrasync integrity-check --id "lab-$case_id-full" \
      "$(storage_url source "$source_backend" "$case_id")" \
      "$(storage_url destination "$destination_backend" "$case_id")"
    echo "$case_id full sync and integrity verified"
  done
done

# Reuse the same job state after changing source data. This is the essential
# incremental-sync scenario from the former protocol-specific E2E skills.
for backend in "${backends[@]}"; do
  case_id="incremental-$backend"
  original="$fixture_root/$case_id-original.txt"
  changed="$fixture_root/$case_id-changed.txt"
  added="$fixture_root/$case_id-added.txt"
  printf 'original-%s\n' "$backend" > "$original"
  printf 'changed-%s\n' "$backend" > "$changed"
  printf 'added-%s\n' "$backend" > "$added"
  put_fixture source "$backend" "$case_id" existing.txt "$original"
  prepare_storage_root destination "$backend" "$case_id"
  run_terrasync sync --resume --id "lab-$case_id" \
    "$(storage_url source "$backend" "$case_id")" "$(storage_url destination "$backend" "$case_id")"
  put_fixture source "$backend" "$case_id" existing.txt "$changed"
  put_fixture source "$backend" "$case_id" added.txt "$added"
  run_terrasync sync --resume --id "lab-$case_id" \
    "$(storage_url source "$backend" "$case_id")" "$(storage_url destination "$backend" "$case_id")"
  [[ "$(object_hash destination "$backend" "$case_id" existing.txt)" == "$(sha256sum "$changed" | cut -d' ' -f1)" ]]
  [[ "$(object_hash destination "$backend" "$case_id" added.txt)" == "$(sha256sum "$added" | cut -d' ' -f1)" ]]
  echo "$backend incremental sync verified"
done

# 纯增量扫描 generation 闸门：不完整快照不得推进 generation，
# 也不得把不可读但仍存活的条目判为删除。
case_id="incremental-scan-generation"
scan_job_id="lab_incremental_scan_generation"
scan_source="$lab_root/$case_id/source"
protected_dir="$scan_source/protected"
mkdir -p "$protected_dir"
printf 'stable\n' > "$protected_dir/still-live.txt"
printf 'baseline\n' > "$scan_source/changed.txt"
printf 'will-be-deleted\n' > "$scan_source/deleted.txt"
printf 'will-be-renamed\n' > "$scan_source/renamed-from.txt"

run_terrasync scan --id "$scan_job_id" "$scan_source"
baseline_commit="$(committed_scan "$scan_job_id")"

printf 'changed\n' > "$scan_source/changed.txt"
fault_log="$lab_root/$case_id/fault.log"
if TERRASYNC_LAB_SCAN_FAIL_AFTER_EVENTS=1 \
  run_terrasync scan --id "$scan_job_id" "$scan_source" >"$fault_log" 2>&1; then
  echo "incomplete incremental scan unexpectedly succeeded" >&2
  exit 1
fi
if ! grep -Fq 'Lab failure after durable events, before base update' "$fault_log"; then
  cat "$fault_log" >&2
  echo "incremental scan failed before reaching the intended injection point" >&2
  exit 1
fi

after_fault="$(committed_scan "$scan_job_id")"
[[ "$after_fault" == "$baseline_commit" ]] || {
  echo "failed incremental scan changed the committed baseline from [$baseline_commit] to [$after_fault]" >&2
  exit 1
}
live_rows="$(clickhouse_query "SELECT count() FROM $database_name.base_$scan_job_id FINAL WHERE relative_path = 'protected/still-live.txt' FORMAT TSVRaw")"
[[ "$live_rows" == 1 ]] || {
  echo "failed incremental scan deleted a still-live entry" >&2
  exit 1
}

# 在失败轮之后准备四类可观察变化；重试必须在一个 generation 内全部识别。
printf 'new\n' > "$scan_source/new.txt"
rm "$scan_source/deleted.txt"
mv "$scan_source/renamed-from.txt" "$scan_source/renamed-to.txt"
run_terrasync scan --id "$scan_job_id" "$scan_source"
retry_commit="$(next_scan_commit "$scan_job_id" "$baseline_commit")"
read -r retry_generation retry_epoch <<<"$retry_commit"
wrong_generation_rows="$(clickhouse_query "SELECT count() FROM $database_name.base_$scan_job_id FINAL WHERE current_state != $retry_generation FORMAT TSVRaw")"
[[ "$wrong_generation_rows" == 0 ]] || {
  echo "incremental scan retry left $wrong_generation_rows base rows outside committed generation $retry_generation" >&2
  exit 1
}
retry_events="FROM $database_name.incremental_$scan_job_id FINAL WHERE toUnixTimestamp64Nano(create_at) >= $retry_epoch"
new_events="$(clickhouse_query "SELECT count() $retry_events AND operation_type = 'new' AND relative_path = 'new.txt' FORMAT TSVRaw")"
changed_events="$(clickhouse_query "SELECT count() $retry_events AND operation_type IN ('data_changed', 'both_changed') AND relative_path = 'changed.txt' FORMAT TSVRaw")"
deleted_events="$(clickhouse_query "SELECT count() $retry_events AND operation_type = 'deleted' AND relative_path = 'deleted.txt' FORMAT TSVRaw")"
renamed_new_events="$(clickhouse_query "SELECT count() $retry_events AND operation_type = 'new' AND relative_path = 'renamed-to.txt' FORMAT TSVRaw")"
renamed_deleted_events="$(clickhouse_query "SELECT count() $retry_events AND operation_type = 'deleted' AND relative_path = 'renamed-from.txt' FORMAT TSVRaw")"
[[ "$new_events" == 1 ]] || { echo "incremental retry expected one New event, got $new_events" >&2; exit 1; }
[[ "$changed_events" == 1 ]] || { echo "incremental retry expected one Changed event, got $changed_events" >&2; exit 1; }
[[ "$deleted_events" == 1 ]] || { echo "incremental retry expected one Deleted event, got $deleted_events" >&2; exit 1; }
# local 扫描没有稳定 file_handle，重命名按 Deleted+New 表达；NFS 场景在下方验证 Renamed。
[[ "$renamed_new_events" == 1 && "$renamed_deleted_events" == 1 ]] || {
  echo "local incremental retry expected one Deleted and one New for rename, got $renamed_deleted_events and $renamed_new_events" >&2
  exit 1
}
deleted_base_rows="$(clickhouse_query "SELECT count() FROM $database_name.base_$scan_job_id FINAL WHERE relative_path = 'deleted.txt' FORMAT TSVRaw")"
[[ "$deleted_base_rows" == 0 ]] || { echo "incremental retry kept deleted base row" >&2; exit 1; }

run_terrasync scan --id "$scan_job_id" "$scan_source"
next_scan_commit "$scan_job_id" "$retry_commit" >/dev/null
echo "incremental scan generation failure/retry barrier verified"

# NFS 4.1 提供稳定 file_handle，单独验证 Renamed 检测输出。
case_id="incremental-scan-rename-nfs41"
rename_job_id="lab_incremental_scan_rename_nfs41"
rename_fixture="$fixture_root/$case_id.txt"
printf 'rename-on-nfs41\n' > "$rename_fixture"
put_fixture source nfs41 "$case_id" renamed-from.txt "$rename_fixture"
rename_source="$(storage_url source nfs41 "$case_id")"
run_terrasync scan --id "$rename_job_id" "$rename_source"
rename_baseline="$(committed_scan "$rename_job_id")"
ssh_lab_root "$LAB_SOURCE_MGMT" \
  "mv '$LAB_NFS41_EXPORT/ci/$run_id/$case_id/source/renamed-from.txt' '$LAB_NFS41_EXPORT/ci/$run_id/$case_id/source/renamed-to.txt'"
run_terrasync scan --id "$rename_job_id" "$rename_source"
rename_commit="$(next_scan_commit "$rename_job_id" "$rename_baseline")"
read -r _ rename_epoch <<<"$rename_commit"
rename_events="$(clickhouse_query "SELECT count() FROM $database_name.incremental_$rename_job_id FINAL WHERE toUnixTimestamp64Nano(create_at) >= $rename_epoch AND operation_type = 'rename' AND relative_path = 'renamed-to.txt' AND comment = 'renamed-from.txt' FORMAT TSVRaw")"
[[ "$rename_events" == 1 ]] || { echo "NFS 4.1 incremental scan expected one Renamed event, got $rename_events" >&2; exit 1; }
echo "NFS 4.1 incremental scan rename detection verified"

# Preserve the former local-filter scenario.
case_id="local-filter"
mkdir -p "$lab_root/$case_id/source" "$lab_root/$case_id/destination"
printf 'included\n' > "$lab_root/$case_id/source/included.txt"
printf 'excluded\n' > "$lab_root/$case_id/source/excluded.txt"
run_terrasync sync --resume --id lab-local-filter --exclude 'name=="excluded.txt"' \
  "$lab_root/$case_id/source" "$lab_root/$case_id/destination"
test -f "$lab_root/$case_id/destination/included.txt"
test ! -e "$lab_root/$case_id/destination/excluded.txt"
echo "local filter verified"
