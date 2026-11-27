#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
require_hdfs_credentials

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
target_root="$(cargo metadata --format-version 1 --no-deps --manifest-path "$repo_root/Cargo.toml" | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="${TERRASYNC_BINARY:-$target_root/release/terrasync}"
matrix_binary="$target_root/release/examples/single_process_matrix"
lab_root="/tmp/terrasync-lab/$run_id/hdfs"
runtime_root="$lab_root/runtime"
fixture_root="$lab_root/fixture"
results_root="${RUNNER_TEMP:-/tmp}/terrasync-harness-results"
single_config="$runtime_root/single.toml"
sender_config="$runtime_root/sender.toml"
receiver_config="$runtime_root/receiver.toml"
missing_config="$runtime_root/missing.toml"
invalid_config="$runtime_root/invalid.toml"
source_keytab="$runtime_root/source.keytab"
destination_keytab="$runtime_root/destination.keytab"
source_cache="FILE:$runtime_root/source.ccache"
destination_cache="FILE:$runtime_root/destination.ccache"
invalid_keytab="$runtime_root/does-not-exist.keytab"
hdfs_scope="${run_id%%-*}-$(printf %s "$run_id" | sha256sum | cut -d' ' -f1)-e2e"
hdfs_root="$(hdfs_run_root "$hdfs_scope")"
hdfs_base="${hdfs_root%/$hdfs_scope/hdfs}"
hdfs_source="$hdfs_root/source"
hdfs_single_destination="$hdfs_root/single-destination"
hdfs_remote_destination="$hdfs_root/remote-destination"
database_nonce="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
database_name="$(clickhouse_database_name "${run_id}_hdfs_${database_nonce}")"
receiver_pid=""
hdfs_touched=false
database_created=false
database_create_confirmed=false
database_cleanup=not_attempted
cleanup_evidence="$results_root/$run_id-hdfs-cleanup.json"

write_cleanup_evidence() {
  python3 - "$cleanup_evidence" "$database_name" "$database_create_confirmed" "$database_cleanup" <<'PYJSON'
import json, os, sys, tempfile
from pathlib import Path
path, database, confirmed, cleanup = sys.argv[1:]
temporary = None
try:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=Path(path).parent, delete=False) as out:
        temporary = out.name
        json.dump({"clickhouse_database": database, "create_confirmed": confirmed == "true", "cleanup": cleanup}, out)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
finally:
    if temporary and os.path.exists(temporary):
        os.unlink(temporary)
PYJSON
}

run_with_config() {
  local config="$1"
  shift
  (cd "$runtime_root" && "$binary" -c "$config" "$@")
}

shielded_cleanup() (
  trap '' INT TERM HUP
  "$@"
)
write_initial_evidence() {
  local cancelled=false status=0
  trap 'cancelled=true' INT TERM HUP
  shielded_cleanup write_cleanup_evidence || status=$?
  trap - INT TERM HUP
  [[ "$cancelled" == false ]] || return 143
  return "$status"
}
cleanup_resources() {
  local status=0 cleanup_cancelled=false attempts attempt
  trap 'cleanup_cancelled=true' INT TERM HUP
  if [[ -n "$receiver_pid" ]]; then
    kill "$receiver_pid" 2>/dev/null || true
    wait "$receiver_pid" 2>/dev/null || true
    receiver_pid=""
  fi
  if [[ "$hdfs_touched" == true ]]; then
    # Delete only this gate's directory relative to the dedicated HDFS base.
    if ! LAB_HDFS_ADMIN_USER="$LAB_HDFS_ADMIN_USER" LAB_HDFS_CONFIG_DIR="$LAB_HDFS_CONFIG_DIR" \
      LAB_HDFS_DESTINATION_CCACHE="$destination_cache" LAB_HDFS_KEYTAB="$destination_keytab" \
      shielded_cleanup timeout --foreground --kill-after=5s 45s "$matrix_binary" --delete-hdfs-run "$hdfs_base" "$hdfs_scope" >/dev/null 2>&1; then
      echo 'HDFS certification cleanup failed' >&2
      status=1
    fi
    hdfs_touched=false
  fi
  if [[ "$database_created" == true ]]; then
    # A timed-out CREATE may still be executing. For an uncertain response,
    # retry the idempotent drop over a short reconciliation window. The random
    # database name is unique to this run and is never shared with other jobs.
    attempts=1
    database_cleanup=passed
    [[ "$database_create_confirmed" == true ]] || { attempts=3; database_cleanup=uncertain; }
    for ((attempt=1; attempt<=attempts; attempt++)); do
      if ! shielded_cleanup clickhouse_query "DROP DATABASE IF EXISTS $database_name" >/dev/null 2>&1; then
        echo 'HDFS certification database cleanup failed' >&2
        database_cleanup=failed
        status=1
      fi
      (( attempt == attempts )) || shielded_cleanup sleep 1
    done
    database_created=false
    shielded_cleanup write_cleanup_evidence || status=1
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
  rm -f "$single_config" "$sender_config" "$receiver_config" "$missing_config" "$invalid_config" \
    "$source_keytab" "$destination_keytab" "$runtime_root/source.ccache" "$runtime_root/destination.ccache"
  rm -rf -- "$lab_root"
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$runtime_root" "$fixture_root" "$results_root"
install -m 0600 "$LAB_HDFS_KEYTAB" "$source_keytab"
install -m 0600 "$LAB_HDFS_KEYTAB" "$destination_keytab"
database_created=true
write_initial_evidence
clickhouse_query "CREATE DATABASE $database_name"
database_create_confirmed=true
write_initial_evidence

write_base_config() {
  local output="$1"
  cat > "$output" <<EOF
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
}

write_base_config "$single_config"
cat >> "$single_config" <<EOF
[storage.source.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.source.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$source_keytab"
cache = "$source_cache"
[storage.destination.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.destination.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$destination_keytab"
cache = "$destination_cache"
EOF

write_base_config "$sender_config"
cat >> "$sender_config" <<EOF
[storage.source.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.source.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$source_keytab"
cache = "$source_cache"
EOF

write_base_config "$receiver_config"
cat >> "$receiver_config" <<EOF
[storage.destination.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.destination.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$destination_keytab"
cache = "$destination_cache"
EOF

write_base_config "$missing_config"
write_base_config "$invalid_config"
cat >> "$invalid_config" <<EOF
[storage.source.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.source.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$invalid_keytab"
cache = "FILE:$runtime_root/invalid.ccache"
EOF

missing_log="$results_root/$run_id-hdfs-missing-config.log"
invalid_log="$results_root/$run_id-hdfs-invalid-credential.log"
if run_with_config "$missing_config" scan "$hdfs_source" >"$missing_log" 2>&1; then
  echo "HDFS scan unexpectedly accepted missing source configuration" >&2
  exit 1
fi
if run_with_config "$invalid_config" scan "$hdfs_source" >"$invalid_log" 2>&1; then
  echo "HDFS scan unexpectedly accepted an invalid keytab" >&2
  exit 1
fi
for sensitive_value in \
  "$invalid_keytab" "FILE:$runtime_root/invalid.ccache" "$LAB_HDFS_ADMIN_USER" \
  "$source_keytab" "$destination_keytab" "$source_cache" "$destination_cache"; do
  if grep -Fq "$sensitive_value" "$invalid_log" || grep -Fq "$sensitive_value" "$missing_log"; then
    echo "HDFS negative-path error leaked credential details" >&2
    exit 1
  fi
done

# Readback uses the protocol-neutral data path; HDFS names do not map to local numeric ownership.
env -u LAB_HDFS_KEYTAB -u LAB_CIFS_USERNAME -u LAB_CIFS_PASSWORD \
  cargo build --release --locked --manifest-path "$repo_root/Cargo.toml" -p app --example single_process_matrix
hdfs_touched=true
printf 'terrasync-hdfs-%s\n' "$run_id" > "$fixture_root/payload.bin"
expected_hash="$(sha256sum "$fixture_root/payload.bin" | cut -d' ' -f1)"

verify_destination() {
  local mode="$1" destination="$2" readback="$lab_root/readback-$1" actual_hash
  verify_integrity_report run_with_config "$single_config" integrity-check --id "hdfs-$mode-integrity-$run_id" \
    "$hdfs_source" "$destination"
  mkdir -p "$readback"
  LAB_HDFS_ADMIN_USER="$LAB_HDFS_ADMIN_USER" LAB_HDFS_CONFIG_DIR="$LAB_HDFS_CONFIG_DIR" \
    LAB_HDFS_KEYTAB="$source_keytab" LAB_HDFS_SOURCE_CCACHE="$source_cache" \
    "$matrix_binary" hdfs source "$destination" local destination "$readback" "hdfs-$mode-readback-$run_id"
  actual_hash="$(sha256sum "$readback/payload.bin" | cut -d' ' -f1)"
  [[ "$actual_hash" == "$expected_hash" ]] || {
    echo "HDFS $mode destination checksum mismatch" >&2
    return 1
  }
}

run_with_config "$single_config" sync --resume --id "hdfs-preload-$run_id" "$fixture_root" "$hdfs_source"
run_with_config "$single_config" sync --resume --id "hdfs-single-$run_id" --enable-integrity-check \
  "$hdfs_source" "$hdfs_single_destination"
verify_destination single "$hdfs_single_destination"

receiver_cert="$runtime_root/receiver.der"
receiver_log="$results_root/$run_id-hdfs-receiver.log"
run_with_config "$receiver_config" serve --listen 127.0.0.1:19876 --tls-cert-out "$receiver_cert" \
  --token "$run_id" "$hdfs_remote_destination" >"$receiver_log" 2>&1 &
receiver_pid="$!"
for _ in $(seq 1 100); do
  [[ -s "$receiver_cert" ]] && break
  kill -0 "$receiver_pid" 2>/dev/null || {
    sed -E 's#hdfs://[^@]+@#hdfs://<redacted>@#g' "$receiver_log" >&2
    exit 1
  }
  sleep 0.1
done
[[ -s "$receiver_cert" ]]

run_with_config "$sender_config" sync --resume --id "hdfs-remote-$run_id" --enable-integrity-check \
  --remote 127.0.0.1:19876 --tls-server-cert "$receiver_cert" --token "$run_id" \
  "$hdfs_source" "$hdfs_remote_destination"
wait "$receiver_pid"
receiver_pid=""
verify_destination remote "$hdfs_remote_destination"

cleanup_resources

report="$results_root/$run_id-hdfs.json"
terrasync_candidate="$(git -C "$repo_root" rev-parse HEAD)"
data_mover_candidate="$(sed -n 's/.*data-mover-rs\.git?rev=\([0-9a-f]*\)#.*/\1/p' "$repo_root/Cargo.lock" | head -1)"
[[ "$data_mover_candidate" =~ ^[0-9a-f]{40}$ ]]
python3 - "$report" "$run_id" "$terrasync_candidate" "$data_mover_candidate" "$expected_hash" <<'PY'
import json, sys
output, run_id, terrasync_sha, data_mover_sha, payload_hash = sys.argv[1:]
with open(output, "w", encoding="utf-8") as handle:
    json.dump({
        "schema_version": 1,
        "run_id": run_id,
        "terrasync_candidate": terrasync_sha,
        "data_mover_candidate": data_mover_sha,
        "gates": {
            "negative_missing_source_config": "pass",
            "negative_invalid_credentials_secret_safe": "pass",
            "single_process_hdfs_to_hdfs": "pass",
            "remote_process_hdfs_to_hdfs": "pass"
        },
        "remote_role_isolation": {
            "sender_config_roles": ["source"],
            "receiver_config_roles": ["destination"]
        },
        "cleanup": {"hdfs": "passed", "clickhouse": "passed"},
        "payload_sha256": payload_hash
    }, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
echo "HDFS single-process and remote-process gates passed: $report"
