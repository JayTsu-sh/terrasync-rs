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
hdfs_root="$(hdfs_run_root "$run_id")"
hdfs_source="$hdfs_root/source"
hdfs_single_destination="$hdfs_root/single-destination"
hdfs_remote_destination="$hdfs_root/remote-destination"
database_name="$(clickhouse_database_name "${run_id}_hdfs")"
receiver_pid=""

run_with_config() {
  local config="$1"
  shift
  (cd "$runtime_root" && "$binary" -c "$config" "$@")
}

cleanup() {
  local status="$?"
  trap - EXIT
  if [[ -n "$receiver_pid" ]]; then
    kill "$receiver_pid" 2>/dev/null || true
    wait "$receiver_pid" 2>/dev/null || true
  fi
  if [[ -f "$single_config" ]]; then
    run_with_config "$single_config" rm "$hdfs_root" >/dev/null 2>&1 || true
  fi
  clickhouse_query "DROP DATABASE IF EXISTS $database_name" >/dev/null 2>&1 || true
  rm -f "$single_config" "$sender_config" "$receiver_config" "$missing_config" "$invalid_config" \
    "$source_keytab" "$destination_keytab" "$runtime_root/source.ccache" "$runtime_root/destination.ccache"
  rm -rf -- "$lab_root"
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$runtime_root" "$fixture_root" "$results_root"
install -m 0600 "$LAB_HDFS_KEYTAB" "$source_keytab"
install -m 0600 "$LAB_HDFS_KEYTAB" "$destination_keytab"
clickhouse_query "CREATE DATABASE IF NOT EXISTS $database_name"

write_base_config() {
  local output="$1"
  cat > "$output" <<EOF
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

printf 'terrasync-hdfs-%s\n' "$run_id" > "$fixture_root/payload.txt"
expected_hash="$(sha256sum "$fixture_root/payload.txt" | cut -d' ' -f1)"

run_with_config "$single_config" sync --id "hdfs-preload-$run_id" "$fixture_root" "$hdfs_source"
run_with_config "$single_config" sync --id "hdfs-single-$run_id" --enable-integrity-check \
  "$hdfs_source" "$hdfs_single_destination"
run_with_config "$single_config" integrity-check --id "hdfs-single-integrity-$run_id" \
  "$hdfs_source" "$hdfs_single_destination"

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

run_with_config "$sender_config" sync --id "hdfs-remote-$run_id" --enable-integrity-check \
  --remote 127.0.0.1:19876 --tls-server-cert "$receiver_cert" --token "$run_id" \
  "$hdfs_source" "$hdfs_remote_destination"
wait "$receiver_pid"
receiver_pid=""
run_with_config "$single_config" integrity-check --id "hdfs-remote-integrity-$run_id" \
  "$hdfs_source" "$hdfs_remote_destination"

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
        "payload_sha256": payload_hash
    }, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
echo "HDFS single-process and remote-process gates passed: $report"
