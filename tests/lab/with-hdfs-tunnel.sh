#!/usr/bin/env bash
# Run a lab command with per-process HDFS name resolution and an SSH data tunnel.
set -euo pipefail
umask 077

managed=false
launching=false
cancel_pending=false
ssh_options=()
tunnel_pid=""
command_pid=""
terminate_command() {
  if [[ "$managed" == true && "$launching" == true ]]; then
    cancel_pending=true
    return
  fi
  trap '' INT TERM HUP
  if [[ -n "$command_pid" ]]; then
    if [[ "$managed" == true ]]; then
      # The supervisor sends TERM to this PID. Keep SSH up while the gate
      # group exits. The manager owns the stop deadline and cgroup teardown.
      kill -TERM -- "-$command_pid" 2>/dev/null || kill -TERM "$command_pid" 2>/dev/null || true
      wait "$command_pid" 2>/dev/null || true
      kill -KILL -- "-$command_pid" 2>/dev/null || true
    else
      kill -TERM -- "-$command_pid" 2>/dev/null || true
      # HDFS and DXN cleanup each have bounded deadlines. Prevent a stuck child
      # from keeping the tunnel and shared lab lock alive indefinitely.
      python3 -c 'import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_DFL)
time.sleep(230)
try:
    os.killpg(int(sys.argv[1]), signal.SIGKILL)
except ProcessLookupError:
    pass' "$command_pid" &
      local watchdog=$!
      wait "$command_pid" 2>/dev/null || true
      kill "$watchdog" 2>/dev/null || true
      wait "$watchdog" 2>/dev/null || true
    fi
    command_pid=""
  fi
  exit 143
}

cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n "$tunnel_pid" ]]; then kill "$tunnel_pid" 2>/dev/null || true; wait "$tunnel_pid" 2>/dev/null || true; fi
  rm -rf -- "$runtime"
  exit "$status"
}

if [[ -v LAB_HDFS_MANAGED_DIR ]]; then
  managed=true
  managed_root="$LAB_HDFS_MANAGED_DIR"
  [[ "$managed_root" == /* && -d "$managed_root" && ! -L "$managed_root" && -O "$managed_root" &&
    "$(stat -c %a -- "$managed_root")" == 700 ]] || {
    echo 'LAB_HDFS_MANAGED_DIR must be an absolute, private attempt directory owned by the runner' >&2
    exit 2
  }
  runtime="$managed_root/tunnel"
  mkdir -- "$runtime"
  trap cleanup EXIT
  trap terminate_command INT TERM HUP
  # Use the pinned gateway and host key directly, independent of SSH client config.
  ssh_options=(-F /dev/null -o GlobalKnownHostsFile=/dev/null)
fi

ci_dir="${LAB_HDFS_CI_DIR:-$HOME/.local/share/terrasync-hdfs-ci}"
nss="$ci_dir/pkg/usr/lib/x86_64-linux-gnu/libnss_wrapper.so"
for file in "$ci_dir/tunnel_key" "$ci_dir/known_hosts" "$ci_dir/client.keytab" "$nss"; do
  [[ -r "$file" ]] || { echo "HDFS tunnel prerequisite unavailable: $file" >&2; exit 2; }
done
[[ $# -gt 0 ]] || { echo 'HDFS tunnel requires a command' >&2; exit 2; }
# The shared runner allows other OS users. Refuse to expose the forwarded ports
# unless the root-owned, narrow output rules are still installed. Jobs running
# as github-runner already share the same runner credentials and trust boundary.
[[ "$(id -u)" == 1000 ]] || { echo 'HDFS tunnel requires the CI uid 1000' >&2; exit 2; }
rules="$(sudo -n /usr/sbin/nft list table inet terrasync_hdfs)" || {
  echo 'HDFS tunnel firewall cannot be verified' >&2; exit 2;
}
normalized_rules="$(printf '%s' "$rules" | python3 -c 'import sys; print(" ".join(sys.stdin.read().split()))')"
expected_rules='table inet terrasync_hdfs { chain output { type filter hook output priority filter; policy accept; ip daddr { 127.0.0.160, 127.0.0.162 } tcp dport 8020 meta skuid != 1000 reject with icmp port-unreachable ip daddr { 127.0.0.161, 127.0.0.163, 127.0.0.164 } tcp dport 9866 meta skuid != 1000 reject with icmp port-unreachable ip daddr 127.0.0.164 tcp dport 10088 meta skuid != 1000 reject with icmp port-unreachable } }'
[[ "$normalized_rules" == "$expected_rules" ]] || { echo 'HDFS tunnel firewall configuration differs from the approved rules' >&2; exit 2; }
if [[ "$managed" == false ]]; then
  runtime="$(mktemp -d "${RUNNER_TEMP:-/tmp}/terrasync-hdfs-tunnel.XXXXXXXX")"
  trap terminate_command INT TERM HUP
  trap cleanup EXIT
fi

# Only these processes resolve the data-network hostnames to loopback forwards.
# Keep existing host entries for the other lab backends; strip comments because
# libnss-wrapper rejects them in NSS_WRAPPER_HOSTS.
sed -E '/^[[:space:]]*(#|$)/d; s/[[:space:]]+#.*$//' /etc/hosts > "$runtime/hosts"
cat >> "$runtime/hosts" <<'HOSTS'
127.0.0.160 rocky97-no2-01.hdfs.internal rocky97-no2-01
127.0.0.161 rocky97-no2-02.hdfs.internal rocky97-no2-02
127.0.0.162 rocky97-no3-01.hdfs.internal rocky97-no3-01
127.0.0.163 rocky97-no3-02.hdfs.internal rocky97-no3-02
127.0.0.164 rocky97-no4-01.hdfs.internal rocky97-no4-01
HOSTS
cat > "$runtime/krb5.conf" <<'KRB'
[libdefaults]
 default_realm = HDFS.INTERNAL
 dns_lookup_kdc = false
 rdns = false
 dns_canonicalize_hostname = false
 udp_preference_limit = 1
[realms]
 HDFS.INTERNAL = {
  kdc = 127.0.0.164:10088
 }
[domain_realm]
 .hdfs.internal = HDFS.INTERNAL
KRB
cat > "$runtime/core-site.xml" <<'XML'
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://hdfscluster</value></property>
  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
  <property><name>hadoop.rpc.protection</name><value>authentication</value></property>
</configuration>
XML
cat > "$runtime/hdfs-site.xml" <<'XML'
<configuration>
  <property><name>dfs.nameservices</name><value>hdfscluster</value></property>
  <property><name>dfs.ha.namenodes.hdfscluster</name><value>nn1,nn2</value></property>
  <property><name>dfs.namenode.rpc-address.hdfscluster.nn1</name><value>rocky97-no2-01.hdfs.internal:8020</value></property>
  <property><name>dfs.namenode.rpc-address.hdfscluster.nn2</name><value>rocky97-no3-01.hdfs.internal:8020</value></property>
  <property><name>dfs.client.failover.proxy.provider.hdfscluster</name><value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value></property>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
  <property><name>dfs.data.transfer.protection</name><value>authentication</value></property>
</configuration>
XML
launching=true
ssh "${ssh_options[@]}" -N -T -i "$ci_dir/tunnel_key" \
  -o BatchMode=yes -o IdentitiesOnly=yes -o ConnectTimeout=10 -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes \
  -o "UserKnownHostsFile=$ci_dir/known_hosts" \
  -L 127.0.0.160:8020:10.30.3.160:8020 \
  -L 127.0.0.162:8020:10.30.3.162:8020 \
  -L 127.0.0.161:9866:10.30.3.161:9866 \
  -L 127.0.0.163:9866:10.30.3.163:9866 \
  -L 127.0.0.164:9866:10.30.3.164:9866 \
  -L 127.0.0.164:10088:10.30.3.164:88 \
  terrasync-fwd@10.128.135.164 &
tunnel_pid=$!
launching=false
[[ "$cancel_pending" == false ]] || terminate_command
ready=false
for _ in $(seq 1 100); do
  kill -0 "$tunnel_pid" 2>/dev/null || { wait "$tunnel_pid" || true; echo 'HDFS tunnel failed to start' >&2; exit 1; }
  if (echo > /dev/tcp/127.0.0.160/8020) 2>/dev/null && (echo > /dev/tcp/127.0.0.164/10088) 2>/dev/null; then
    ready=true
    break
  fi
  sleep 0.1
done
if [[ "$managed" == true && "$ready" == false ]]; then
  echo 'HDFS tunnel did not become ready' >&2
  exit 1
fi
(echo > /dev/tcp/127.0.0.160/8020) 2>/dev/null || { echo 'HDFS tunnel did not become ready' >&2; exit 1; }
export LD_PRELOAD="$nss${LD_PRELOAD:+:$LD_PRELOAD}" NSS_WRAPPER_HOSTS="$runtime/hosts"
export NSS_WRAPPER_PASSWD=/etc/passwd NSS_WRAPPER_GROUP=/etc/group KRB5_CONFIG="$runtime/krb5.conf"
export LAB_HDFS_LOCATION='hdfs://terrasync-lab-ci%40HDFS.INTERNAL@hdfscluster/'
export LAB_HDFS_ADMIN_USER='terrasync-lab-ci@HDFS.INTERNAL' LAB_HDFS_CONFIG_DIR="$runtime"
export LAB_HDFS_KEYTAB="$ci_dir/client.keytab" LAB_HDFS_BASE_PATH='/tmp/terrasync-nightly/terrasync-lab-ci'
export LAB_HDFS_FINGERPRINT='hdfscluster-kerberos/10.30.3.160+10.30.3.162/ssh-gateway-10.128.135.164'
launching=true
setsid "$@" &
command_pid=$!
launching=false
[[ "$cancel_pending" == false ]] || terminate_command
command_status=0
wait "$command_pid" || command_status=$?
if [[ "$managed" == true ]]; then
  kill -KILL -- "-$command_pid" 2>/dev/null || true
fi
command_pid=""
(( command_status == 0 )) || exit "$command_status"
kill -0 "$tunnel_pid" 2>/dev/null || { echo 'HDFS tunnel exited during the command' >&2; exit 1; }
