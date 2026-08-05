#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
require_s3_credentials

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
binary="$repo_root/target/release/terrasync"
lab_root="/tmp/terrasync-lab/$run_id"
runtime_root="$lab_root/runtime"
fixture_root="$lab_root/fixtures"
mkdir -p "$runtime_root" "$fixture_root"
database_name="$(clickhouse_database_name "$run_id")"
clickhouse_query "CREATE DATABASE IF NOT EXISTS $database_name"
cat > "$lab_root/config.toml" <<EOF
[database]
enabled = true
type = "clickhouse"
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
      ssh_lab_root "$host" "mkdir -p '$export_path/ci/$run_id/$case_id/$role' && chmod 0777 '$export_path/ci/$run_id/$case_id/$role'"
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
      ssh_lab_root "$host" "cat > '$export_path/ci/$run_id/$case_id/$role/$key'" < "$file"
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

    run_terrasync sync --id "lab-$case_id" \
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
  run_terrasync sync --id "lab-$case_id" \
    "$(storage_url source "$backend" "$case_id")" "$(storage_url destination "$backend" "$case_id")"
  put_fixture source "$backend" "$case_id" existing.txt "$changed"
  put_fixture source "$backend" "$case_id" added.txt "$added"
  run_terrasync sync --id "lab-$case_id" \
    "$(storage_url source "$backend" "$case_id")" "$(storage_url destination "$backend" "$case_id")"
  [[ "$(object_hash destination "$backend" "$case_id" existing.txt)" == "$(sha256sum "$changed" | cut -d' ' -f1)" ]]
  [[ "$(object_hash destination "$backend" "$case_id" added.txt)" == "$(sha256sum "$added" | cut -d' ' -f1)" ]]
  echo "$backend incremental sync verified"
done

# Preserve the former local-filter scenario.
case_id="local-filter"
mkdir -p "$lab_root/$case_id/source" "$lab_root/$case_id/destination"
printf 'included\n' > "$lab_root/$case_id/source/included.txt"
printf 'excluded\n' > "$lab_root/$case_id/source/excluded.txt"
run_terrasync sync --id lab-local-filter --exclude 'name=="excluded.txt"' \
  "$lab_root/$case_id/source" "$lab_root/$case_id/destination"
test -f "$lab_root/$case_id/destination/included.txt"
test ! -e "$lab_root/$case_id/destination/excluded.txt"
echo "local filter verified"
