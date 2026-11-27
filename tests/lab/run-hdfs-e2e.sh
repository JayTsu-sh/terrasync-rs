#!/usr/bin/env bash
set -euo pipefail
umask 077

attempt_root="${LAB_HDFS_MANAGED_DIR:?LAB_HDFS_MANAGED_DIR is required}"
[[ "$attempt_root" == /* && -d "$attempt_root" && ! -L "$attempt_root" && -O "$attempt_root" &&
  "$(stat -c %a -- "$attempt_root")" == 700 ]] || {
  echo 'LAB_HDFS_MANAGED_DIR must be an absolute, private attempt directory owned by the runner' >&2
  exit 2
}
# The manager can keep evidence on disk while the gate uses bounded scratch space.
state_root="${LAB_HDFS_STATE_DIR-$attempt_root}"
[[ "$state_root" == /* && -d "$state_root" && ! -L "$state_root" && -O "$state_root" &&
  "$(stat -c %a -- "$state_root")" == 700 ]] || {
  echo 'LAB_HDFS_STATE_DIR must be an absolute, private attempt directory owned by the runner' >&2
  exit 2
}
runtime_root="$attempt_root/runtime"
mkdir -- "$runtime_root"
if [[ -e "$state_root/hdfs-cleanup.json" || -e "$state_root/gate-report.json" ]]; then
  rmdir -- "$runtime_root"
  echo 'HDFS certification requires a fresh attempt directory' >&2
  exit 2
fi

run_id=""
hdfs_scope=""
hdfs_authority=""
hdfs_base_path=""
routing_fingerprint=""
receiver_pid=""
cancelled=false
hdfs_cleanup=not_attempted
hdfs_cleanup_reason_record=""

write_evidence() (
  # The intent must survive interruption between publication and the first write.
  trap '' INT TERM HUP
  python3 - "$state_root" "$run_id" "$@" <<'PY'
import json
import os
from pathlib import Path
import sys
import tempfile

root, run_id, name, *values = sys.argv[1:]
root = Path(root)
record = {"schema_version": 1, "run_id": run_id or None}
if name == "hdfs-cleanup":
    status, scope, base_path, authority, fingerprint, reason = values
    record.update(
        status=status,
        reason=reason or None,
        scope=scope or None,
        target={"base_path": base_path, "authority": authority, "routing_fingerprint": fingerprint}
        if authority else None,
    )
elif name == "gate-phase":
    record["phase"], = values
else:
    terrasync_sha, data_mover_sha, payload_hash = values
    record.update(
        terrasync_candidate=terrasync_sha,
        data_mover_candidate=data_mover_sha,
        gates={
            "negative_missing_source_config": "pass",
            "negative_invalid_credentials_secret_safe": "pass",
            "single_process_hdfs_to_hdfs": "pass",
            "remote_process_hdfs_to_hdfs": "pass",
        },
        remote_role_isolation={
            "sender_config_roles": ["source"],
            "receiver_config_roles": ["destination"],
        },
        cleanup={"hdfs": "passed"},
        payload_sha256=payload_hash,
    )

with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=root, delete=False) as out:
    temporary = Path(out.name)
    try:
        json.dump(record, out, indent=2, sort_keys=True)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
        temporary.replace(root / f"{name}.json")
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
PY
)

write_cleanup_evidence() {
  write_evidence hdfs-cleanup "$1" "$hdfs_scope" "$hdfs_base_path" "$hdfs_authority" \
    "$routing_fingerprint" "$hdfs_cleanup_reason_record"
}

cleanup() {
  local status=$? cleanup_exit cleanup_log="$attempt_root/hdfs-cleanup.log"
  trap - EXIT
  trap 'cancelled=true' INT TERM HUP
  if [[ -n "$receiver_pid" ]]; then
    kill -TERM "$receiver_pid" 2>/dev/null || true
    if ! await_receiver 5; then
      kill -KILL "$receiver_pid" 2>/dev/null || true
      if ! await_receiver 2; then
        cancelled=true
      fi
    fi
    if [[ "$cancelled" == false ]]; then
      wait "$receiver_pid" 2>/dev/null || true
    fi
  fi

  case "$status" in
    124|129|130|137|143) cancelled=true ;;
  esac
  # Only delete this attempt's scope. After cancellation or timeout, in-flight HDFS writes may still land.
  if [[ "$hdfs_cleanup" == unknown && "$cancelled" == false ]]; then
    write_evidence gate-phase hdfs_cleanup || exit 1
    if LAB_HDFS_DESTINATION_CCACHE="$destination_cache" LAB_HDFS_KEYTAB="$destination_keytab" \
      timeout --foreground --kill-after=5s 45s "$matrix_binary" --delete-hdfs-run "$hdfs_base" "$hdfs_scope" >"$cleanup_log" 2>&1; then
      hdfs_cleanup=passed
    else
      cleanup_exit=$?
      hdfs_cleanup_reason_record="$(hdfs_cleanup_reason "$cleanup_exit" "$cleanup_log")"
      case "$hdfs_cleanup_reason_record" in
        Authentication|PermissionDenied|InvalidInput|Unsupported)
          [[ "$cancelled" == true ]] || hdfs_cleanup=failed ;;
      esac
      report_hdfs_cleanup_failure "$cleanup_exit" "$cleanup_log"
      status=1
    fi
    write_cleanup_evidence "$hdfs_cleanup" || status=1
  fi

  rm -rf -- "$runtime_root" || status=1
  [[ "$cancelled" == false ]] || exit 143
  if (( status == 0 )) && [[ "$hdfs_cleanup" == passed ]]; then
    write_evidence gate-report "$TERRASYNC_CANDIDATE" "$DATA_MOVER_CANDIDATE" "$expected_hash" || exit 1
    echo "HDFS single-process and remote-process gates passed: $state_root/gate-report.json"
  fi
  [[ "$cancelled" == false ]] || exit 143
  exit "$status"
}

trap cleanup EXIT
trap 'cancelled=true; exit 143' INT TERM HUP
write_cleanup_evidence not_attempted

# Check the controller's values before common.sh can supply shared lab defaults.
: "${LAB_CLICKHOUSE_DSN:?LAB_CLICKHOUSE_DSN is required}"
: "${LAB_CLICKHOUSE_USER:?LAB_CLICKHOUSE_USER is required}"
: "${LAB_CLICKHOUSE_PASSWORD:?LAB_CLICKHOUSE_PASSWORD is required}"
if [[ ! "$LAB_CLICKHOUSE_DSN" =~ ^http://(127\.0\.0\.1|\[::1\]):([1-9][0-9]{0,4})/?$ ]] ||
  (( BASH_REMATCH[2] > 65535 )); then
  echo 'LAB_CLICKHOUSE_DSN must be a loopback HTTP origin with a valid port' >&2
  exit 2
fi
database_name="${LAB_CLICKHOUSE_DATABASE-terrasync_lab}"
[[ "$database_name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
  echo 'LAB_CLICKHOUSE_DATABASE must be a SQL identifier' >&2
  exit 2
}
[[ "${TERRASYNC_CANDIDATE:-}" =~ ^[a-f0-9]{40}$ && "${DATA_MOVER_CANDIDATE:-}" =~ ^[a-f0-9]{40}$ ]] || {
  echo 'TERRASYNC_CANDIDATE and DATA_MOVER_CANDIDATE must be full commit SHAs' >&2
  exit 2
}
binary="$(realpath -e -- "${TERRASYNC_BINARY:?TERRASYNC_BINARY is required}")"
matrix_binary="$(realpath -e -- "${TERRASYNC_SINGLE_MATRIX_BINARY:?TERRASYNC_SINGLE_MATRIX_BINARY is required}")"
[[ -f "$binary" && -x "$binary" && -f "$matrix_binary" && -x "$matrix_binary" ]] || {
  echo 'Both prebuilt HDFS gate binaries must be executable files' >&2
  exit 2
}

source "$(dirname "$0")/common.sh"
run_id="${1:?run id required}"
validate_run_id "$run_id"
export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB
export LAB_HDFS_BASE_PATH="${LAB_HDFS_BASE_PATH:-/tmp/terrasync-nightly}"
require_hdfs_credentials

hdfs_scope="${run_id%%-*}-$(printf %s "$run_id" | sha256sum | cut -d' ' -f1)-e2e"
hdfs_root="$(hdfs_run_root "$hdfs_scope")"
hdfs_base="${hdfs_root%/$hdfs_scope/hdfs}"
hdfs_base_path="$LAB_HDFS_BASE_PATH"
hdfs_authority="${hdfs_base#*@}"
hdfs_authority="${hdfs_authority%%/*}"
routing_fingerprint="${LAB_HDFS_FINGERPRINT:?LAB_HDFS_FINGERPRINT is required}"
[[ "$hdfs_authority" =~ ^([A-Za-z0-9_.-]+|\[[[:xdigit:]:.]+\])(:[0-9]+)?$ &&
  "$routing_fingerprint" =~ ^[A-Za-z0-9._+:/-]+$ ]] || {
  echo 'Invalid HDFS authority or routing fingerprint' >&2
  exit 2
}
write_cleanup_evidence not_attempted

hdfs_source="$hdfs_root/source"
hdfs_single_destination="$hdfs_root/single-destination"
hdfs_remote_destination="$hdfs_root/remote-destination"
fixture_root="$runtime_root/fixture"
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

await_receiver() {
  local deadline=$(( SECONDS + $1 ))
  while kill -0 "$receiver_pid" 2>/dev/null; do
    (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
}

run_with_config() (
  cd "$runtime_root"
  exec timeout --foreground --kill-after=5s 120s "$binary" -c "$@"
)

mkdir -- "$fixture_root"
install -m 0600 "$LAB_HDFS_KEYTAB" "$source_keytab"
install -m 0600 "$LAB_HDFS_KEYTAB" "$destination_keytab"

python3 - "$database_name" > "$single_config" <<'PY'
import json
import os
import sys

print('''[database]
enabled = true
batch_size = 800000

[database.clickhouse]
dial_timeout = 5
read_timeout = 30''')
for key, value in {
    "dsn": os.environ["LAB_CLICKHOUSE_DSN"],
    "database": sys.argv[1],
    "username": os.environ["LAB_CLICKHOUSE_USER"],
    "password": os.environ["LAB_CLICKHOUSE_PASSWORD"],
}.items():
    quoted = json.dumps(value, ensure_ascii=False).replace("\x7f", r"\u007f")
    print(f"{key} = {quoted}")
PY
for config in "$sender_config" "$receiver_config" "$missing_config" "$invalid_config"; do
  cp -- "$single_config" "$config"
done

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

cat >> "$sender_config" <<EOF
[storage.source.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.source.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$source_keytab"
cache = "$source_cache"
EOF

cat >> "$receiver_config" <<EOF
[storage.destination.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.destination.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$destination_keytab"
cache = "$destination_cache"
EOF

cat >> "$invalid_config" <<EOF
[storage.source.hdfs]
config_dir = "$LAB_HDFS_CONFIG_DIR"
[storage.source.hdfs.kerberos]
principal = "$LAB_HDFS_ADMIN_USER"
keytab = "$invalid_keytab"
cache = "FILE:$runtime_root/invalid.ccache"
EOF

negative_gate() {
  local config="$1" expected="$2" log="$attempt_root/$3.log" status=0 sensitive_value
  run_with_config "$config" scan "$hdfs_source" >"$log" 2>&1 || status=$?
  for sensitive_value in "$invalid_keytab" "FILE:$runtime_root/invalid.ccache" \
    "$LAB_HDFS_ADMIN_USER" "$LAB_HDFS_KEYTAB" "$source_keytab" "$destination_keytab" \
    "$source_cache" "$destination_cache" "$LAB_CLICKHOUSE_PASSWORD"; do
    if grep -Fq -- "$sensitive_value" "$log"; then
      echo 'HDFS negative-path error leaked credential details' >&2
      return 1
    fi
  done
  if [[ "$status" != 1 ]] || ! grep -Fxq -- "$expected" "$log"; then
    echo "HDFS $3 did not produce the expected rejection (exit $status)" >&2
    return 1
  fi
}

write_evidence gate-phase negative_configuration
negative_gate "$missing_config" 'Error: Configuration error: HDFS Source role configuration is required' \
  negative_missing_source_config
write_evidence gate-phase negative_credentials
negative_gate "$invalid_config" \
  'Error: failed to connect Hdfs backend: HDFS get storage root failed (Authentication): authentication failed' \
  negative_invalid_credentials_secret_safe

printf 'terrasync-hdfs-%s\n' "$run_id" > "$fixture_root/payload.bin"
expected_hash="$(sha256sum "$fixture_root/payload.bin" | cut -d' ' -f1)"

# Readback uses the protocol-neutral data path without mapping HDFS names to local ownership.
verify_destination() {
  local mode="$1" destination="$2" readback="$runtime_root/readback-$1" actual_hash
  write_evidence gate-phase "${mode}_integrity"
  verify_integrity_report run_with_config "$single_config" integrity-check --id "hdfs-$mode-integrity-$run_id" \
    "$hdfs_source" "$destination"
  write_evidence gate-phase "${mode}_readback"
  mkdir -p "$readback"
  LAB_HDFS_KEYTAB="$source_keytab" LAB_HDFS_SOURCE_CCACHE="$source_cache" \
    timeout --foreground --kill-after=5s 120s "$matrix_binary" hdfs source "$destination" local destination "$readback" "hdfs-$mode-readback-$run_id"
  actual_hash="$(sha256sum "$readback/payload.bin" | cut -d' ' -f1)"
  [[ "$actual_hash" == "$expected_hash" ]] || {
    echo "HDFS $mode destination checksum mismatch" >&2
    return 1
  }
}

# From this point a failed or interrupted gate may have left HDFS data.
write_cleanup_evidence unknown
hdfs_cleanup=unknown
write_evidence gate-phase preload
run_with_config "$single_config" sync --resume --id "hdfs-preload-$run_id" "$fixture_root" "$hdfs_source"
write_evidence gate-phase single_copy
run_with_config "$single_config" sync --resume --id "hdfs-single-$run_id" --enable-integrity-check \
  "$hdfs_source" "$hdfs_single_destination"
verify_destination single "$hdfs_single_destination"

write_evidence gate-phase receiver_start
receiver_cert="$runtime_root/receiver.der"
receiver_log="$attempt_root/hdfs-receiver.log"
# Track the actual writer PID before acting on a pending cancellation.
trap 'cancelled=true' INT TERM HUP
(
  cd "$runtime_root"
  exec "$binary" -c "$receiver_config" serve --listen 127.0.0.1:19876 --tls-cert-out "$receiver_cert" \
    --token "$run_id" "$hdfs_remote_destination"
) >"$receiver_log" 2>&1 &
receiver_pid=$!
trap 'cancelled=true; exit 143' INT TERM HUP
[[ "$cancelled" == false ]] || exit 143
for _ in $(seq 1 100); do
  [[ -s "$receiver_cert" ]] && break
  kill -0 "$receiver_pid" 2>/dev/null || { echo 'HDFS receiver exited before becoming ready' >&2; exit 1; }
  sleep 0.1
done
[[ -s "$receiver_cert" ]]

write_evidence gate-phase remote_copy
run_with_config "$sender_config" sync --resume --id "hdfs-remote-$run_id" --enable-integrity-check \
  --remote 127.0.0.1:19876 --tls-server-cert "$receiver_cert" --token "$run_id" \
  "$hdfs_source" "$hdfs_remote_destination"
receiver_status=0
write_evidence gate-phase receiver_exit
await_receiver 30 || { echo 'HDFS receiver exit timed out' >&2; exit 124; }
wait "$receiver_pid" || receiver_status=$?
receiver_pid=""
(( receiver_status == 0 ))
verify_destination remote "$hdfs_remote_destination"
