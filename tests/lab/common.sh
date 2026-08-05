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
LAB_NFS41_EXPORT="${LAB_NFS41_EXPORT:-/srv/nfs/v4}"
LAB_S3_BUCKET="${LAB_S3_BUCKET:-terrasync-ci}"
LAB_CLICKHOUSE_DSN="${LAB_CLICKHOUSE_DSN:-http://10.131.9.11:8123}"
LAB_CLICKHOUSE_USER="${LAB_CLICKHOUSE_USER:-default}"
LAB_CLICKHOUSE_PASSWORD="${LAB_CLICKHOUSE_PASSWORD:-}"
LAB_FIXTURE_UID="${LAB_FIXTURE_UID:-1000}"
LAB_FIXTURE_GID="${LAB_FIXTURE_GID:-1000}"

clickhouse_query() {
  curl --noproxy '*' --fail --silent --show-error \
    --user "$LAB_CLICKHOUSE_USER:$LAB_CLICKHOUSE_PASSWORD" \
    --data-binary "$1" "$LAB_CLICKHOUSE_DSN/"
}

clickhouse_database_name() {
  local run_id="$1"
  printf 'terrasync_ci_%s' "${run_id//[^A-Za-z0-9_]/_}"
}

require_s3_credentials() {
  : "${LAB_S3_ACCESS_KEY:?LAB_S3_ACCESS_KEY is required}"
  : "${LAB_S3_SECRET_KEY:?LAB_S3_SECRET_KEY is required}"
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
