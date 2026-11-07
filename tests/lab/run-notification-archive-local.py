#!/usr/bin/env python3
"""Exercise notification storage in isolated real single/replicated ClickHouse services."""

import os
import http.client
import http.server
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]


def docker(*arguments):
    return subprocess.check_output(["docker", *arguments], text=True).strip()


def configuration(replica):
    http_port, native_port, interserver_port = (8123, 9000, 9009) if replica == 1 else (8124, 9001, 9010)
    keeper = """
      <keeper_server><tcp_port>9181</tcp_port><server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>9234</port></server></raft_configuration>
      </keeper_server>""" if replica == 1 else ""
    return f"""<clickhouse>
      <logger><level>warning</level></logger>
      <listen_host replace="1">127.0.0.1</listen_host>
      <http_port>{http_port}</http_port><tcp_port>{native_port}</tcp_port>
      <mysql_port remove="1"/><postgresql_port remove="1"/>
      <interserver_http_port>{interserver_port}</interserver_http_port>
      <interserver_http_host>127.0.0.1</interserver_http_host>
      <max_server_memory_usage>1073741824</max_server_memory_usage>
      <max_thread_pool_size>192</max_thread_pool_size>
      <background_schedule_pool_size>4</background_schedule_pool_size>
      <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
      <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
      <mark_cache_size>33554432</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size>
      <macros><shard>01</shard><replica>r{replica}</replica></macros>
      <zookeeper><node><host>127.0.0.1</host><port>9181</port></node></zookeeper>
      <remote_servers><notification_cluster><shard><internal_replication>true</internal_replication>
        <replica><host>127.0.0.1</host><port>9000</port></replica>
        <replica><host>127.0.0.1</host><port>9001</port></replica>
      </shard></notification_cluster></remote_servers>
      {keeper}
    </clickhouse>"""


def run_isolated():
    image = os.environ.get("NOTIFICATION_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:26.3-alpine")
    containers = []
    with tempfile.TemporaryDirectory(prefix="ts-notification-archive-") as directory:
        try:
            for replica in (1, 2):
                config = Path(directory) / f"r{replica}.xml"
                config.write_text(configuration(replica))
                network = "none" if replica == 1 else f"container:{containers[0]}"
                container = docker(
                    "run", "-d", "--name", f"ts-notification-r{replica}-{uuid.uuid4().hex[:12]}",
                    "--network", network, "--memory=1536m", "--cpus=2",
                    "--tmpfs", "/var/lib/clickhouse:rw,size=384m,mode=1777",
                    "--tmpfs", "/var/log/clickhouse-server:rw,size=32m,mode=1777",
                    "--label", "codex.task=notification-archive", "-e", "CLICKHOUSE_SKIP_USER_SETUP=1",
                    "--mount", f"type=bind,src={config},dst=/etc/clickhouse-server/config.d/notifications.xml,readonly",
                    image,
                )
                containers.append(container)
            print(f"isolated notification image={docker('inspect', containers[0], '--format', '{{.Image}}')}", flush=True)
            pid = docker("inspect", containers[0], "--format", "{{.State.Pid}}")
            suite = [flag for flag in ("--copy-generations", "--rename-generations", "--retention-events", "--source-absence") if flag in sys.argv]
            subprocess.run(["nsenter", "-t", pid, "-n", sys.executable, __file__, "--inside", *suite], cwd=ROOT, check=True)
        except BaseException:
            for container in containers:
                print(docker("logs", "--tail=25", container), file=sys.stderr)
            raise
        finally:
            for container in reversed(containers):
                docker("rm", "-f", "-v", container)


def run_copy_generations():
    """Reuse the owned ClickHouse pair for copy-generation state and read faults."""
    state = "normal"
    guard = threading.Lock()
    release = threading.Event()

    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            nonlocal state
            if self.path not in ("/normal", "/disconnect", "/stall"):
                self.send_error(404)
                return
            with guard:
                state = self.path[1:]
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                chunks = []
                while True:
                    count = int(self.rfile.readline().split(b";", 1)[0], 16)
                    if count == 0:
                        while self.rfile.readline() not in (b"\r\n", b""):
                            pass
                        break
                    chunks.append(self.rfile.read(count))
                    assert self.rfile.read(2) == b"\r\n"
                body = b"".join(chunks)
            else:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with guard:
                fault = state
            if fault == "disconnect":
                self.close_connection = True
                return
            if fault == "stall":
                release.wait(10)
                self.close_connection = True
                return
            connection = http.client.HTTPConnection("127.0.0.1", 8123, timeout=15)
            try:
                headers = {key: value for key, value in self.headers.items()
                           if key.lower() not in ("host", "connection", "content-length", "transfer-encoding")}
                headers["Content-Length"] = str(len(body))
                connection.request("POST", self.path, body=body, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("content-length", "transfer-encoding", "connection"):
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                connection.close()

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    proxy.daemon_threads = True
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    environment = dict(os.environ)
    address = f"127.0.0.1:{proxy.server_port}"
    environment["LAB_COPY_GENERATION_DSN"] = f"http://{address}"
    environment["LAB_COPY_GENERATION_CONTROL"] = address
    try:
        if "--source-absence" in sys.argv:
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                "orchestrator::absence_tests::real_source_absence_guards_target_mutations", "--", "--ignored", "--exact", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            return
        if "--retention-events" in sys.argv:
            run_retention_events(environment)
            return
        if "--rename-generations" in sys.argv:
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
                "real_rename_after_failed_comparison_of_the_same_item",
                "--", "--ignored", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                "real_incremental_move_still_applies_rename_without_early_source_purge",
                "--", "--ignored", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            return
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
            "real_copy_generation_policies", "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "real_copy_source_policy_transitions_preserve_target_data",
            "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
    finally:
        release.set()
        proxy.shutdown()
        proxy.server_close()


def run_retention_events(environment):
    reports = []

    class Callback(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            reports.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    callback = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Callback)
    callback.daemon_threads = True
    threading.Thread(target=callback.serve_forever, daemon=True).start()
    environment["LAB_TARGET_RETENTION_CALLBACK_URL"] = f"http://127.0.0.1:{callback.server_port}/progress"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "real_retention_events_preserve_targets_and_archive_successful_callbacks",
            "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        assert len(reports) == 4, reports
        events = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", [])]
        assert len(events) == 4, events
        assert all(report["target_preserved_count"] == 2 and report["detail"]["error_count"] == 0 and report["detail"]["deleted_count"] == 0 for report in reports), reports
        assert all(event["code"] == "target_preserved" and event["retention"]["source_presence"] == "unknown" and event["retention"]["reason"] == "source_cleanup_policy" for event in events), events
        assert all(report["diagnostics_complete"] for report in reports if not report.get("diagnostics")), reports
        print("independent HTTP callback inspection deployments=2 retained_entries=2 errors=0 details=4 HTTP_204=accepted", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()


def run_inside():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for port in (8123, 8124):
        for _ in range(100):
            try:
                with opener.open(f"http://127.0.0.1:{port}/ping", timeout=1) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError(f"ClickHouse readiness timed out on port {port}")
    if any(flag in sys.argv for flag in ("--copy-generations", "--rename-generations", "--retention-events", "--source-absence")):
        run_copy_generations()
        return
    environment = dict(os.environ)
    environment["LAB_NOTIFICATION_CLICKHOUSE_DSN"] = "http://127.0.0.1:8123"
    environment["LAB_NOTIFICATION_CLICKHOUSE_USER"] = "default"
    environment.pop("LAB_NOTIFICATION_CLICKHOUSE_PASSWORD", None)
    environment.pop("LAB_NOTIFICATION_CLICKHOUSE_CLUSTER", None)
    fault = "normal"
    release = threading.Event()
    observed = threading.Event()
    lock = threading.Lock()

    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            nonlocal fault, release
            if self.path == "/observed":
                body = b"yes" if observed.is_set() else b"no"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if not self.path.startswith("/control?mode="):
                self.send_error(404)
                return
            with lock:
                release.set()
                release = threading.Event()
                fault = self.path.split("=", 1)[1]
                observed.clear()
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def do_POST(self):
            nonlocal fault
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                chunks = []
                while True:
                    count = int(self.rfile.readline().split(b";", 1)[0], 16)
                    if count == 0:
                        while True:
                            trailer = self.rfile.readline()
                            if trailer in (b"\r\n", b""):
                                break
                        break
                    chunks.append(self.rfile.read(count))
                    assert self.rfile.read(2) == b"\r\n"
                body = b"".join(chunks)
            else:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            decoded = urllib.parse.unquote(self.path)
            is_insert = "INSERT" in decoded.upper() and "notifications_" in decoded
            with lock:
                mode = fault if is_insert else "normal"
                unblock = release
                if is_insert:
                    observed.set()
                    # 首次归档正常，第二次 ACK 写入丢失响应。
                    if mode == "ack_lost_reply":
                        mode, fault = "normal", "lost_ack"
                    elif not mode.startswith("persistent_"):
                        fault = "normal"
                mode = mode.removeprefix("persistent_")
            if mode == "disconnect":
                self.connection.close()
                return
            if mode == "stall":
                unblock.wait(15)
                self.connection.close()
                return
            upstream = http.client.HTTPConnection("127.0.0.1", 8123, timeout=10)
            try:
                headers = {key: value for key, value in self.headers.items() if key.lower() not in ("content-length", "transfer-encoding", "connection")}
                headers["Content-Length"] = str(len(body))
                upstream.request("POST", self.path, body=body, headers=headers)
                response = upstream.getresponse()
                payload = response.read()
                if mode == "lost_ack":
                    assert response.status == 200, (response.status, payload[:512])
                    self.connection.close()
                    return
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("content-length", "transfer-encoding", "connection"):
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            finally:
                upstream.close()

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    proxy.daemon_threads = True
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    environment["LAB_NOTIFICATION_PROXY_ADDR"] = f"127.0.0.1:{proxy.server_port}"
    try:
        for mode in ("single", "cluster"):
            if mode == "cluster":
                environment["LAB_NOTIFICATION_CLICKHOUSE_CLUSTER"] = "notification_cluster"
            print(f"notification archive deployment={mode}", flush=True)
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
                "catalogue::notifications::tests::real_",
                "--", "--ignored", "--test-threads=1", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                "consumer::stats::delivery::tests::real_",
                "--", "--ignored", "--test-threads=1", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                "consumer::stats::journal::tests::real_",
                "--", "--ignored", "--test-threads=1", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
    finally:
        release.set()
        proxy.shutdown()
        proxy.server_close()


if __name__ == "__main__":
    if "--inside" in sys.argv:
        run_inside()
    else:
        run_isolated()
