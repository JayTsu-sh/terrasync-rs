#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"

database_name="$(clickhouse_database_name "$run_id")"
clickhouse_query "DROP DATABASE IF EXISTS $database_name" || true

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT" "$LAB_WORKER_MGMT"; do
  ssh_lab "$host" "rm -rf -- '/var/lib/terrasync-ci/$run_id'"
done

if [[ -n "${LAB_S3_ACCESS_KEY:-}" && -n "${LAB_S3_SECRET_KEY:-}" ]]; then
  for endpoint in "$LAB_SOURCE_DATA" "$LAB_DEST_DATA"; do
    python3 "$(dirname "$0")/s3_helper.py" abort-multipart-prefix \
      --endpoint "$endpoint" --bucket "$LAB_S3_BUCKET" --prefix "ci/$run_id/"
    python3 "$(dirname "$0")/s3_helper.py" delete-prefix \
      --endpoint "$endpoint" --bucket "$LAB_S3_BUCKET" --prefix "ci/$run_id/"
  done
fi

rm -rf -- "/tmp/terrasync-lab/$run_id"

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT"; do
  ssh_lab_root "$host" \
    "rm -rf -- '$LAB_NFS3_EXPORT/ci/$run_id' '$LAB_NFS41_EXPORT/ci/$run_id'"
done
