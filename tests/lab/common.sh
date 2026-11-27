#!/usr/bin/env bash
set -euo pipefail

LAB_SSH_USER="${LAB_SSH_USER:-ci-runner}"
LAB_ROOT_SSH_USER="${LAB_ROOT_SSH_USER:-root}"
LAB_SSH_KEY="${LAB_SSH_KEY:-/home/github-runner/.ssh/terrasync_lab}"
LAB_SOURCE_MGMT="${LAB_SOURCE_MGMT:-10.131.9.12}"
LAB_DEST_MGMT="${LAB_DEST_MGMT:-10.131.9.13}"
LAB_WORKER_MGMT="${LAB_WORKER_MGMT:-10.131.9.14}"
LAB_SOURCE_DATA="${LAB_SOURCE_DATA:-10.10.1.12}"
LAB_DEST_DATA="${LAB_DEST_DATA:-10.10.1.13}"
LAB_WORKER_DATA="${LAB_WORKER_DATA:-10.10.1.14}"
LAB_NFS3_EXPORT="${LAB_NFS3_EXPORT:-/srv/nfs/v3}"
LAB_NFS40_DATA="${LAB_NFS40_DATA:-10.131.7.201}"
LAB_NFS40_EXPORT="${LAB_NFS40_EXPORT:-/jay_nfs}"
LAB_NFS41_EXPORT="${LAB_NFS41_EXPORT:-/srv/nfs/v4}"
LAB_S3_BUCKET="${LAB_S3_BUCKET:-terrasync-ci}"
LAB_DXN_S3_ENDPOINT="${LAB_DXN_S3_ENDPOINT:-http://10.131.7.201:8184}"
LAB_DXN_S3_BUCKET="${LAB_DXN_S3_BUCKET:-test-agent-s3-bucket-202608301331}"
LAB_HDFS_LOCATION="${LAB_HDFS_LOCATION:-hdfs://hdfs%2Fterrasync-runner%40HDFS.LOCAL@hdfs-ha/}"
LAB_HDFS_ADMIN_USER="${LAB_HDFS_ADMIN_USER:-hdfs/terrasync-runner@HDFS.LOCAL}"
LAB_HDFS_CONFIG_DIR="${LAB_HDFS_CONFIG_DIR:-}"
LAB_HDFS_KEYTAB="${LAB_HDFS_KEYTAB:-}"
LAB_CLICKHOUSE_DSN="${LAB_CLICKHOUSE_DSN:-http://10.131.9.11:8123}"
LAB_CLICKHOUSE_USER="${LAB_CLICKHOUSE_USER:-default}"
LAB_CLICKHOUSE_PASSWORD="${LAB_CLICKHOUSE_PASSWORD:-}"
LAB_FIXTURE_UID="${LAB_FIXTURE_UID:-1000}"
LAB_FIXTURE_GID="${LAB_FIXTURE_GID:-1000}"

# AWS SDK/urllib3 proxy bypass matching is host based and does not consistently
# honor the runner's CIDR entries. Keep all lab control/data traffic direct,
# including the bucket-style hostnames constructed by run-e2e.sh.
LAB_DXN_S3_AUTHORITY="${LAB_DXN_S3_ENDPOINT#*://}"
LAB_DXN_S3_HOST="${LAB_DXN_S3_AUTHORITY%%:*}"
LAB_NO_PROXY_HOSTS="${LAB_SOURCE_MGMT},${LAB_DEST_MGMT},${LAB_WORKER_MGMT},${LAB_SOURCE_DATA},${LAB_DEST_DATA},${LAB_WORKER_DATA},${LAB_NFS40_DATA},10.128.61.200,10.128.61.201,${LAB_S3_BUCKET}.${LAB_SOURCE_DATA},${LAB_S3_BUCKET}.${LAB_DEST_DATA},${LAB_S3_BUCKET}.${LAB_WORKER_DATA},${LAB_DXN_S3_HOST},${LAB_DXN_S3_BUCKET}.${LAB_DXN_S3_HOST}"
export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${LAB_NO_PROXY_HOSTS}"
export no_proxy="$NO_PROXY"

clickhouse_query() {
  curl --noproxy '*' --fail --silent --show-error --connect-timeout 5 --max-time 30 \
    --user "$LAB_CLICKHOUSE_USER:$LAB_CLICKHOUSE_PASSWORD" \
    --data-binary "$1" "$LAB_CLICKHOUSE_DSN/"
}

clickhouse_database_name() {
  local run_id="$1"
  printf 'terrasync_ci_%s' "${run_id//[^A-Za-z0-9_]/_}"
}

verify_integrity_report() {
  local report status=0
  report="$("$@")" || status=$?
  printf '%s\n' "$report"
  (( status == 0 )) || return "$status"
  # 两套验收的目录下都只有一个 payload 文件。
  if ! grep -Eq 'Checked:[[:space:]]+1[[:space:]]*$' <<<"$report" ||
    ! grep -Fq 'All Passed ✓' <<<"$report"; then
    echo "integrity check must examine the single payload and report no issues" >&2
    return 1
  fi
}

require_s3_credentials() {
  if [[ -z "${LAB_S3_ACCESS_KEY:-}" || -z "${LAB_S3_SECRET_KEY:-}" ]]; then
    local -a source_credentials destination_credentials
    mapfile -t source_credentials < <(
      ssh_lab_root "$LAB_SOURCE_MGMT" \
        ". /etc/default/rustfs; printf '%s\\n%s\\n' \"\$RUSTFS_ACCESS_KEY\" \"\$RUSTFS_SECRET_KEY\""
    )
    mapfile -t destination_credentials < <(
      ssh_lab_root "$LAB_DEST_MGMT" \
        ". /etc/default/rustfs; printf '%s\\n%s\\n' \"\$RUSTFS_ACCESS_KEY\" \"\$RUSTFS_SECRET_KEY\""
    )
    [[ "${#source_credentials[@]}" == 2 && "${#destination_credentials[@]}" == 2 ]] || {
      echo "failed to load complete RustFS credentials" >&2
      return 2
    }
    [[ "${source_credentials[0]}" == "${destination_credentials[0]}" &&
      "${source_credentials[1]}" == "${destination_credentials[1]}" ]] || {
      echo "source and destination RustFS credentials differ" >&2
      return 2
    }
    export LAB_S3_ACCESS_KEY="${source_credentials[0]}"
    export LAB_S3_SECRET_KEY="${source_credentials[1]}"
  fi
  : "${LAB_S3_ACCESS_KEY:?LAB_S3_ACCESS_KEY is required}"
  : "${LAB_S3_SECRET_KEY:?LAB_S3_SECRET_KEY is required}"
}

report_hdfs_cleanup_failure() {
  local status="$1" log="$2" reason
  case "$status" in
    124|137) reason=timeout ;;
    130|143) reason=cancelled ;;
    *)
      reason="$(sed -nE 's/^HDFS_CLEANUP_FAILURE=(Authentication|PermissionDenied|Connectivity|Timeout|InvalidInput|Unsupported|NotFound|Conflict|Capacity|Corruption|Protocol|Throttled|Cancelled|Internal|connection_unknown)$/\1/p' "$log" | head -1)"
      reason="${reason:-storage_error}"
      ;;
  esac
  echo "HDFS cleanup failed: $reason (exit $status)" >&2
}

require_hdfs_credentials() {
  : "${LAB_HDFS_CONFIG_DIR:?LAB_HDFS_CONFIG_DIR is required}"
  : "${LAB_HDFS_KEYTAB:?LAB_HDFS_KEYTAB is required}"
  [[ -r "$LAB_HDFS_CONFIG_DIR/core-site.xml" && -r "$LAB_HDFS_CONFIG_DIR/hdfs-site.xml" ]] || {
    echo "LAB_HDFS_CONFIG_DIR must contain readable core-site.xml and hdfs-site.xml" >&2
    return 2
  }
  [[ -r "$LAB_HDFS_KEYTAB" ]] || {
    echo "LAB_HDFS_KEYTAB is not readable" >&2
    return 2
  }
}

require_single_matrix_credentials() {
  local cifs_enabled="${1:-true}"
  if [[ "$cifs_enabled" == true ]]; then
    : "${LAB_CIFS_NIGHTLY_USERNAME:?LAB_CIFS_NIGHTLY_USERNAME is required}"
    : "${LAB_CIFS_NIGHTLY_PASSWORD:?LAB_CIFS_NIGHTLY_PASSWORD is required}"
  fi
  require_s3_credentials
  require_hdfs_credentials
  : "${LAB_DXN_S3_ACCESS_KEY:?LAB_DXN_S3_ACCESS_KEY is required}"
  : "${LAB_DXN_S3_SECRET_KEY:?LAB_DXN_S3_SECRET_KEY is required}"
}

hdfs_run_root() {
  local run_id="$1"
  validate_run_id "$run_id"
  python3 - "$LAB_HDFS_LOCATION" "$LAB_HDFS_ADMIN_USER" "$run_id" "${LAB_HDFS_BASE_PATH:-/tmp/terrasync-nightly}" <<'PY'
import re
import sys
import urllib.parse

location, admin_user, run_id, base_path = sys.argv[1:]
parsed = urllib.parse.urlsplit(location)
if parsed.scheme != "hdfs" or parsed.password is not None or parsed.hostname is None:
    raise SystemExit("LAB_HDFS_LOCATION must be a password-free HDFS URL")
user = urllib.parse.quote(admin_user, safe="")
# Nameservice IDs are configuration keys, so preserve their original case.
# Accessing parsed.port also validates a present port before retaining the authority.
_ = parsed.port
authority = parsed.netloc.rsplit("@", 1)[-1]
if not re.fullmatch(r"/[A-Za-z0-9._/-]+", base_path) or any(
    part in ("", ".", "..") for part in base_path[1:].split("/")
):
    raise SystemExit("LAB_HDFS_BASE_PATH must be a normalized absolute path")
print(f"hdfs://{user}@{authority}{base_path}/{run_id}/hdfs")
PY
}

ssh_lab() {
  local host="$1"
  shift
  ssh -i "$LAB_SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "$LAB_SSH_USER@$host" "$@"
}

ssh_lab_root() {
  local host="$1"
  shift
  ssh -i "$LAB_SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "$LAB_ROOT_SSH_USER@$host" "$@"
}

validate_run_id() {
  local run_id="$1"
  [[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
    echo "unsafe run id: $run_id" >&2
    return 2
  }
}
