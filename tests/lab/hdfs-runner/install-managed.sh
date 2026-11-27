#!/usr/bin/env bash
set -euo pipefail
umask 022

[[ $EUID == 0 ]] || { echo 'Host installation requires root' >&2; exit 2; }
[[ $# == 2 && "$2" =~ ^[0-9a-f]{64}$ ]] || {
  echo 'Usage: install-managed.sh RUNNER_USER APPROVED_CLICKHOUSE_SHA256' >&2; exit 2;
}
runner="$1"
uid="$(id -u "$runner")"
[[ "$uid" == 1000 ]] || { echo 'The HDFS tunnel requires runner UID 1000' >&2; exit 2; }
source_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
printf '%s  /usr/bin/clickhouse\n' "$2" | sha256sum --check --status
manager() {
  runuser -u "$runner" -- env XDG_RUNTIME_DIR="/run/user/$uid" \
    DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$uid/bus" /usr/bin/systemctl --user "$@"
}

loginctl enable-linger "$runner"
systemctl start "user@$uid.service"
# An update must not replace the worker while a managed attempt is active.
active_units="$(manager list-units --all --state=active,activating,deactivating --no-legend 'terrasync-hdfs-attempt@*.service')"
[[ -z "$active_units" ]] || { echo 'A managed HDFS attempt is active' >&2; exit 1; }

install -d -m 0755 /opt/terrasync-hdfs-managed /etc/systemd/user
install -m 0644 "$source_dir/../hdfs_lab.py" "$source_dir/../hdfs_lab_clickhouse.py" \
  "$source_dir/../evidence-matrix.json" /opt/terrasync-hdfs-managed/
printf '%s\n' "$2" > /opt/terrasync-hdfs-managed/clickhouse.sha256
install -m 0644 "$source_dir"/systemd/* /etc/systemd/user/
install -m 0644 "$source_dir"/system/*.mount /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now "$(systemd-escape --path --suffix=mount /run/terrasync-hdfs-managed)"
manager daemon-reload
manager enable --now terrasync-hdfs-reaper.timer
