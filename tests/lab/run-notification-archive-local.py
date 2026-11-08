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
            if any(flag in sys.argv for flag in ("--version-catalogue", "--mutable-catalogue", "--version-metadata", "--copy-object-cleanup")):
                containers.append(docker(
                    "run", "-d", "--name", f"ts-version-catalogue-{uuid.uuid4().hex[:12]}",
                    "--network", f"container:{containers[0]}", "--memory=768m", "--cpus=1",
                    "--tmpfs", "/data:rw,size=128m,mode=1777", "--label", "codex.task=version-catalogue",
                    "-e", "RUSTFS_ADDRESS=0.0.0.0:19000", "-e", "RUSTFS_CONSOLE_ADDRESS=0.0.0.0:19001",
                    "-e", "RUSTFS_ACCESS_KEY=migration-test", "-e", "RUSTFS_SECRET_KEY=migration-test-isolated-only",
                    os.environ.get("MIGRATION_S3_IMAGE", "rustfs/rustfs:latest"), "/data",
                ))
                print(f"isolated S3 image={docker('inspect', containers[-1], '--format', '{{.Image}}')}", flush=True)
            print(f"isolated notification image={docker('inspect', containers[0], '--format', '{{.Image}}')}", flush=True)
            pid = docker("inspect", containers[0], "--format", "{{.State.Pid}}")
            suite = [flag for flag in ("--copy-generations", "--copy-content", "--copy-ready-cleanup", "--copy-object-cleanup", "--catalogue-recovery", "--consumer-recovery", "--copy-execution", "--copy-data-cancel", "--copy-file-list-cancel", "--rename-generations", "--retention-events", "--source-absence", "--request-policy", "--copy-contracts", "--version-catalogue", "--mutable-catalogue", "--version-metadata", "--scan-wire") if flag in sys.argv]
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
    batch_stats = {"writes": 0, "lost": 0, "tokens": set()}
    comparison_stats = {"reads": 0, "first": None}
    guard = threading.Lock()
    release = threading.Event()

    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            nonlocal state
            if self.path == "/compare_stats":
                with guard:
                    first = comparison_stats["first"]
                    payload = json.dumps({"reads": comparison_stats["reads"], "elapsed_ms": None if first is None else int((time.monotonic() - first) * 1000)}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path == "/batch_stats":
                with guard:
                    payload = json.dumps({"writes": batch_stats["writes"], "lost": batch_stats["lost"], "tokens": len(batch_stats["tokens"])}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path not in ("/normal", "/disconnect", "/stall", "/startup_disconnect", "/startup_stall", "/running_disconnect", "/running_stall", "/reject_prepare",
                                 "/compare_flash", "/compare_lost", "/compare_disconnect", "/compare_stall", "/compare_recover_stall", "/compare_reject",
                                 "/batch_flash", "/batch_lost", "/batch_disconnect", "/batch_stall", "/batch_recover_stall", "/batch_reject",
                                 "/retirement_flash", "/retirement_disconnect", "/retirement_stall", "/retirement_lost", "/retirement_cancel"):
                self.send_error(404)
                return
            with guard:
                state = self.path[1:]
                batch_stats.update(writes=0, lost=0, tokens=set())
                comparison_stats.update(reads=0, first=None)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            nonlocal state
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
            query = urllib.parse.unquote_plus(self.path).encode() + body[:256]
            is_retirement = b"ALTER TABLE" in query.upper() and b"COPY_SEQUENCE>0" in query.upper()
            is_compare = b"AS CHANGE_CODE" in query.upper()
            is_insert = b"INSERT INTO" in query.upper() and b"BASE_" in query.upper()
            with guard:
                fault = state
                if fault.startswith("retirement_"):
                    if not is_retirement:
                        fault = "normal"
                    else:
                        fault = "batch_" + fault.removeprefix("retirement_")
                        if state in ("retirement_flash", "retirement_lost"):
                            state = "normal"
                        if fault == "batch_cancel":
                            fault = "batch_stall"
                if is_insert:
                    batch_stats["writes"] += 1
                    token = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get("insert_deduplication_token", [])
                    batch_stats["tokens"].update(token)
                comparison_fault = fault.startswith("compare_")
                if is_compare:
                    comparison_stats["reads"] += 1
                    if comparison_fault and comparison_stats["first"] is None:
                        comparison_stats["first"] = time.monotonic()
                if comparison_fault:
                    if not is_compare:
                        fault = "normal"
                    else:
                        fault = "batch_" + fault.removeprefix("compare_")
                        if state in ("compare_flash", "compare_lost", "compare_recover_stall"):
                            state = "normal"
                if fault.startswith("batch_") and not comparison_fault and not is_retirement:
                    if not is_insert:
                        fault = "normal"
                    elif fault in ("batch_flash", "batch_lost", "batch_recover_stall"):
                        state = "normal"
                # 接受记录已入库后，仅拒绝本次基线准备，不阻塞后续终态归档。
                if fault == "reject_prepare":
                    query = urllib.parse.unquote(self.path).encode() + body
                    if b"ALTER TABLE" in query and b"copy_sequence <" in query:
                        state = "normal"
                    else:
                        fault = "normal"
                # 运行期只中断数据路径，通知可用；执行开始取消后再恢复数据连接。
                if fault.startswith("running_"):
                    if b"KILL QUERY" in query.upper():
                        # 只在数据执行已经请求停止时恢复；通知通道可用不能掩盖数据故障。
                        state = fault = "normal"
                    elif "notifications_" in urllib.parse.unquote(self.path) or b"notifications_" in body:
                        fault = "normal"
                    else:
                        fault = fault.split("_", 1)[1]
                # 启动探测持续失败；进入通知归档后恢复数据库，单独验证最终通知链路。
                if fault.startswith("startup_"):
                    if "notifications_" in urllib.parse.unquote(self.path) or b"notifications_" in body:
                        state = fault = "normal"
                    else:
                        fault = fault.removeprefix("startup_")
            if fault in ("disconnect", "batch_disconnect", "batch_flash"):
                self.close_connection = True
                return
            if fault in ("reject_prepare", "batch_reject"):
                self.send_error(403, "owned prepare rejection" if fault == "reject_prepare" else ("owned comparison rejection" if comparison_fault else "owned batch rejection"))
                return
            if fault in ("stall", "batch_stall", "batch_recover_stall"):
                release.wait(1.2 if fault == "batch_recover_stall" else 10)
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
                if fault == "batch_lost":
                    assert response.status == 200, (response.status, payload[:512])
                    with guard:
                        batch_stats["lost"] += 1
                    self.close_connection = True
                    return
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
        if "--copy-object-cleanup" in sys.argv:
            from copy_object_cleanup import run
            run(ROOT, environment)
            return
        if "--copy-ready-cleanup" in sys.argv:
            from copy_ready_cleanup import run
            run(ROOT, environment)
            return
        if "--copy-content" in sys.argv:
            run_copy_content(environment)
            return
        if "--copy-data-cancel" in sys.argv or "--copy-file-list-cancel" in sys.argv:
            run_copy_data_cancellation(environment, file_list="--copy-file-list-cancel" in sys.argv)
            return
        if "--copy-execution" in sys.argv:
            run_copy_execution(environment)
            return
        if "--consumer-recovery" in sys.argv:
            run_consumer_recovery(environment)
            return
        if "--catalogue-recovery" in sys.argv:
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
                "real_copy_batches_recover_with_frozen_facts", "--", "--ignored", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            return
        if "--scan-wire" in sys.argv:
            environment["LAB_CLICKHOUSE_DSN"] = environment["LAB_COPY_GENERATION_DSN"]
            environment["LAB_CLICKHOUSE_CLUSTER"] = "notification_cluster"
            for test in ("cluster_fresh_schema_includes_candidate_hash_on_every_replica",
                         "cluster_native_key_stage_replays_events_before_direct_base_write",
                         "cluster_shared_native_observations_roundtrip_merge_replay_and_deletions"):
                subprocess.run([
                    "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
                    "scan::cluster::tests::" + test, "--", "--ignored", "--exact", "--nocapture",
                ], cwd=ROOT, env=environment, check=True)
            return
        if "--version-catalogue" in sys.argv:
            run_version_catalogue(environment)
            return
        if "--version-metadata" in sys.argv:
            from version_metadata_catalogue import run
            run(ROOT, environment)
            return
        if "--mutable-catalogue" in sys.argv:
            from mutable_object_catalogue import run
            run(ROOT, environment)
            return
        if "--copy-contracts" in sys.argv:
            environment["LAB_CATALOGUE_SINGLE"] = "1"
            environment["LAB_CLICKHOUSE_DSN"] = environment["LAB_COPY_GENERATION_DSN"]
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
                "catalogue::catalogue_tests", "--", "--ignored", "--nocapture",
                "--test-threads=1", "--skip", "cluster_catalogue_",
            ], cwd=ROOT, env=environment, check=True)
            return
        if "--request-policy" in sys.argv:
            run_request_policy(environment)
            return
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
                "real_retained_incremental_applies_confirmed_rename",
                "--", "--ignored", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            return
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "db", "--lib",
            "real_source_deleted_base_retirement", "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
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



def run_copy_content(environment):
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
    try:
        with tempfile.TemporaryDirectory(prefix="ts-copy-content-") as directory:
            environment["LAB_COPY_CONTENT_ROOT"] = directory
            environment["LAB_COPY_CONTENT_CALLBACK"] = f"http://127.0.0.1:{callback.server_port}/callback"
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                "real_incremental_move_checks_existing_data_before_source_removal",
                "--", "--ignored", "--nocapture", "--test-threads=1",
            ], cwd=ROOT, env=environment, check=True)
            summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
            assert len(summaries) == 12 and all(report["diagnostics_complete"] for report in summaries), summaries
            failed = [report for report in summaries if report["migration"]["phase"] == "failed"]
            assert len(failed) == 4 and all(report["task_error_count"] == report["target_preserved_count"] == 1 for report in failed), failed
            events = {event["event_id"]: event for report in reports
                      for event in (report.get("diagnostics") or {}).get("events", [])}
            retained = [event for event in events.values()
                        if event["code"] == "target_preserved" and event.get("retention", {}).get("scope") == "entry"]
            assert len(retained) == 4, retained
            for event in retained:
                assert event["retention"]["source_path"] == event["retention"]["target_path"] == "payload", event
                assert "Data mismatch" in event["message"], event
            for deployment in ("single", "cluster"):
                for case in ("unchanged", "corrupt", "metadata"):
                    root = Path(directory) / f"{deployment}-{case}"
                    source = root / "source" / "payload"
                    assert (root / "target" / "payload").read_bytes() == (b"source!" if case == "unchanged" else b"damaged")
                    assert not source.exists() if case == "unchanged" else source.read_bytes() == b"source!"
            print("independent content migration inspection deployments=2 rounds=12 corrupted_sources_retained=4 confirmed_sources_removed=2 callback_causes=4 HTTP_204=accepted final_reports_saved=true", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()


def run_version_catalogue(environment):
    import boto3
    from botocore.config import Config
    for _ in range(100):
        try:
            with urllib.request.urlopen("http://127.0.0.1:19000/health/ready", timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        time.sleep(0.25)
    else:
        raise RuntimeError("owned S3 readiness timed out")
    client = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", aws_access_key_id="migration-test",
                          aws_secret_access_key="migration-test-isolated-only", region_name="us-east-1",
                          config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3))
    buckets = {kind: f"catalogue-{kind}-{uuid.uuid4().hex[:12]}" for kind in ("source", "latest", "history")}
    for kind, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if kind != "latest":
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        environment["LAB_VERSION_CATALOGUE_" + kind.upper()] = f"s3://migration-test:migration-test-isolated-only@{bucket}.127.0.0.1:19000"
    seeded = {prefix: set() for prefix in ("single", "cluster")}
    for prefix in ("single", "cluster"):
        for version in range(4):
            saved = client.put_object(Bucket=buckets["source"], Key=prefix + "/payload", Body=f"payload-{version}".encode(), Tagging=f"raw={version}")
            seeded[prefix].add(saved["VersionId"])
    fault_prefixes = set()
    data_refusals = {prefix: 0 for prefix in seeded}
    fault_guard = threading.Lock()
    class SourceProxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            parsed = urllib.parse.urlsplit(self.path)
            arguments = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            prefix = next((prefix for prefix in seeded if f"/{prefix}/" in parsed.path), None)
            version = arguments.get("versionId", [None])[0]
            is_data = self.command == "GET" and version is not None and not any(key in arguments for key in ("tagging", "attributes", "acl"))
            with fault_guard:
                deny = prefix in fault_prefixes and version not in seeded.get(prefix, set()) and is_data
                if deny:
                    data_refusals[prefix] += 1
            if deny:
                body = b"<Error><Code>AccessDenied</Code><Message>injected new version data permission denial</Message></Error>"
                self.send_response(403)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            upstream = http.client.HTTPConnection("127.0.0.1", 19000, timeout=10)
            try:
                upstream.request(self.command, self.path, headers=dict(self.headers))
                response = upstream.getresponse()
                body = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("connection", "content-length", "transfer-encoding"):
                        self.send_header(key, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                upstream.close()

        do_GET = do_HEAD = handle_request

    source_proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SourceProxy)
    source_proxy.daemon_threads = True
    threading.Thread(target=source_proxy.serve_forever, daemon=True).start()
    environment["LAB_VERSION_CATALOGUE_FAULT_SOURCE"] = f"s3://migration-test:migration-test-isolated-only@{buckets['source']}.127.0.0.1:{source_proxy.server_port}"
    reports = []
    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path not in ("/marker/single", "/marker/cluster", "/data-fault/single", "/data-fault/cluster"):
                self.send_error(404)
                return
            prefix = self.path.rsplit("/", 1)[1]
            if self.path.startswith("/marker/"):
                client.delete_object(Bucket=buckets["source"], Key=prefix + "/payload")
            else:
                with fault_guard:
                    fault_prefixes.add(prefix)
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            reports.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Control)
    control.daemon_threads = True
    threading.Thread(target=control.serve_forever, daemon=True).start()
    environment["LAB_VERSION_CATALOGUE_CONTROL"] = f"127.0.0.1:{control.server_port}"
    environment["LAB_VERSION_CATALOGUE_CALLBACK"] = f"http://127.0.0.1:{control.server_port}/callback"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "copy_execution::version_tests::real_versioned_copy_catalogues_follow_destination_history", "--", "--ignored", "--exact", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        for prefix in ("single", "cluster"):
            key = prefix + "/payload"
            assert client.get_object(Bucket=buckets["latest"], Key=key)["Body"].read() == b"final-payload"
            versions = client.list_object_versions(Bucket=buckets["history"], Prefix=key)
            assert len(versions.get("Versions", [])) == 6 and not versions.get("DeleteMarkers"), versions
            contents = [client.get_object(Bucket=buckets["history"], Key=key, VersionId=version["VersionId"])["Body"].read()
                        for version in versions["Versions"]]
            assert contents == [b"final-payload", b"changed-payload", b"payload-3", b"payload-2", b"payload-1", b"payload-0"], contents
            failed = client.list_object_versions(Bucket=buckets["history"], Prefix=prefix + "/failure/payload")
            assert len(failed.get("Versions", [])) == 4 and not failed.get("DeleteMarkers"), failed
            assert data_refusals[prefix] == 1, data_refusals
            source = client.list_object_versions(Bucket=buckets["source"], Prefix=key)
            assert len(source.get("Versions", [])) == 6 and len(source.get("DeleteMarkers", [])) == 1, source
        print("independent S3 catalogue inspection deployments=2 latest_current_payload=true target_history_versions=6 no_duplicate_replay=true source_versions_retained=6 source_delete_markers=1 latest_marker_target_preserved=true ordered_payloads=true failed_target_versions=4 denied_data_reads_per_failed_entry=1", flush=True)
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 18 and all(report["task_error_count"] == 0 and report["diagnostics_complete"] for report in summaries), summaries
        failures = [report for report in summaries if report["detail"]["error_count"] == 1]
        assert len(failures) == 2 and all(report["job_id"] == "version_failed_history" and report["migration"]["phase"] == "failed" for report in failures), failures
        errors = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "copy_version_failed"]
        assert len(errors) == 4 and all("AccessDenied" in event["message"] for event in errors), errors
        assert sum("blocked_by_prior_version" in event["message"] for event in errors) == 2, errors
        retained = [report for report in summaries if report["target_preserved_count"] == 1]
        assert len(retained) == 2, retained
        markers = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", [])
                   if event.get("retention", {}).get("source_path") == "payload"]
        assert len(markers) == 2 and all(event["retention"]["reason"] == "source_deletion_unconfirmed" for event in markers), markers
        print("independent S3 callback inspection successful_rounds=16 failed_entries=2 version_error_details=4 blocked_versions=2 latest_delete_marker_retention_entries=2 task_errors=0 HTTP_204=accepted", flush=True)
    finally:
        control.shutdown()
        control.server_close()
        source_proxy.shutdown()
        source_proxy.server_close()


def run_copy_data_cancellation(environment, file_list=False):
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
    environment["LAB_COPY_DATA_CANCEL_CALLBACK"] = f"http://127.0.0.1:{callback.server_port}/callback"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "real_database_fault_cancels_file_list_direct_and_checkpoint_data" if file_list else "real_database_fault_cancels_active_direct_and_checkpoint_data", "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 32 and all(report["diagnostics_complete"] for report in summaries), summaries
        failed = [report for report in summaries if report["task_error_count"] == 1]
        assert len(failed) == 16 and all(report["migration"]["phase"] == "failed" for report in failed), failed
        for failed_report in failed:
            attempt = failed_report["callback_identity"]["attempt_id"]
            causes = [event for report in reports
                      if report["callback_identity"]["attempt_id"] == attempt
                      for event in (report.get("diagnostics") or {}).get("events", [])]
            assert any("ClickHouse unavailable after" in event["message"] for event in causes), causes
        resumed = [report for report in summaries if report["migration"]["generation"] >= 2 and report["task_error_count"] == 0]
        # 增量基线轮只有 generation=1，成功的后续轮都来自检查点恢复。
        assert len(resumed) == 8 and all(report["detail"]["transferred_bytes"] < 256 * 1024 * 1024 for report in resumed), resumed
        print("independent data cancellation callback inspection deployments=2 failed_runs=16 checkpoint_resumptions=8 HTTP_204=accepted diagnostics_complete=true", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()


def run_copy_execution(environment):
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
    environment["LAB_COPY_EXECUTION_CALLBACK"] = f"http://127.0.0.1:{callback.server_port}/callback"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "real_copy_comparison_uses_task_database_execution", "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 24 and all(report["diagnostics_complete"] for report in summaries), summaries
        failed = [report for report in summaries if report["task_error_count"] == 1]
        assert len(failed) == 6 and all(report["migration"]["phase"] == "failed" for report in failed), failed
        attempts = [report for report in summaries if report["migration"]["generation"] == 2]
        assert len(attempts) == 12, attempts
        recovered = [report for report in attempts if report["task_error_count"] == 0]
        assert len(recovered) == 6 and all(report["migration"]["phase"] == "completed" and report["detail"]["new_count"] == 4 for report in recovered), recovered
        causes = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", [])
                  if "owned comparison rejection" in event["message"]]
        assert len(causes) >= 2, causes
        print("independent task execution callback inspection deployments=2 recovered_comparisons=6 failed_comparisons=6 original_rejection_preserved=true HTTP_204=accepted", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()


def run_consumer_recovery(environment):
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
    environment["LAB_COPY_CONSUMER_CALLBACK"] = f"http://127.0.0.1:{callback.server_port}/callback"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "real_copy_consumers_recover_and_stop_after_final_failure", "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 6 and all(report["diagnostics_complete"] for report in summaries), summaries
        failed = [report for report in summaries if report["task_error_count"] == 1]
        assert len(failed) == 4 and all(report["migration"]["phase"] == "failed" for report in failed), failed
        causes = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", [])
                  if "owned batch rejection" in event["message"]]
        assert len(causes) >= 4, causes
        print("independent consumer callback inspection deployments=2 full_and_incremental=true failed_runs=4 original_database_cause=true HTTP_204=accepted", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()


def run_request_policy(environment):
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
    environment["LAB_REQUEST_POLICY_CALLBACK_URL"] = f"http://127.0.0.1:{callback.server_port}/progress"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "real_copy_requests_freeze_cleanup_and_report_preflight_rejections",
            "--", "--ignored", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        rejected = [report for report in reports if report["job_id"].startswith(("local_preflight_", "remote_policy_"))]
        final = [report for report in rejected if report["is_final"]]
        summaries = [report for report in final if not report.get("diagnostics")]
        assert len(summaries) == 18, reports
        assert all(report["task_error_count"] == 1 and report["diagnostics_complete"] for report in summaries), summaries
        assert all(report["detail"].get("new_count", 0) == 0 for report in final), final
        events = [event for report in rejected for event in (report.get("diagnostics") or {}).get("events", [])]
        assert len(events) == 18, events
        assert sum("双进程模式尚未提供" in event["message"] for event in events) == 2, events
        assert sum("Invalid remote address" in event["message"] for event in events) == 2, events
        local = [report for report in summaries if report["job_id"].startswith("local_preflight_")]
        assert len(local) == 14, local
        planned = [report for report in reports if report["job_id"].startswith("plan_window_")
                   and report["is_final"] and not report.get("diagnostics")]
        successful = [report for report in planned if report["task_error_count"] == 0]
        bindings = [report for report in planned if report["task_error_count"] == 1]
        assert len(successful) == 48 and len(bindings) == 6, planned
        expected_modes = ["full", "incremental", "incremental", "incremental", "incremental", "full", "full", "incremental"]
        expected_bytes = [1, 0, 1, 0, 0, 1, 0, 0]
        for report in successful:
            migration = report["migration"]
            position = migration["generation"] - 1
            assert migration["plan"]["mode"] == expected_modes[position], report
            assert migration["plan"]["target_cleanup"] == "preserve", report
            assert migration["phase"] == "completed" and report["diagnostics_complete"], report
            assert report["detail"]["transferred_bytes"] == expected_bytes[position], report
        print("independent generation-plan HTTP inspection successful_runs=48 binding_rejections=6 include_exclude_combined=true plans_and_data_bytes_match=true", flush=True)
        edges = [report for report in reports if report["job_id"].startswith("plan_edge_")
                 and report["is_final"] and not report.get("diagnostics")]
        assert len(edges) == 16 and all(report["diagnostics_complete"] for report in edges), edges
        failed = [report for report in edges if report["task_error_count"] == 1]
        assert len(failed) == 6, failed
        accepted_failures = [report for report in failed if report.get("migration")]
        assert len(accepted_failures) == 2, failed
        assert all(report["migration"]["phase"] == "failed"
                   and not report["migration"]["baseline_prepared"] for report in accepted_failures), accepted_failures
        print("independent lifecycle HTTP inspection prepare_failure=2 mirror_conflict=2 binding_after_base_drop=2 recovered_successful_runs=10", flush=True)
        retired = [report for report in reports if report["job_id"].startswith("retirement_callback_")
                   and report["is_final"] and not report.get("diagnostics")]
        assert len(retired) == 12 and all(report["diagnostics_complete"] for report in retired), retired
        failures = [report for report in retired if report["task_error_count"] == 1]
        successes = [report for report in retired if report["task_error_count"] == 0]
        assert len(failures) == len(successes) == 6, retired
        for report in failures:
            assert report["migration"]["phase"] == "failed", report
            assert report["detail"]["error_count"] == 0, report
            attempt = report["callback_identity"]["attempt_id"]
            causes = [event for delivered in reports if delivered["callback_identity"]["attempt_id"] == attempt
                      for event in (delivered.get("diagnostics") or {}).get("events", [])]
            assert any("base 清理失败" in event["message"] for event in causes), causes
        assert all(report["migration"]["phase"] == "completed" and report["migration"]["plan"]["mode"] == "full" for report in successes), successes
        print("independent retirement HTTP inspection failed_runs=6 recovered_full_runs=6 base_cleanup_cause_delivered=true final_reports_archived=true HTTP_204=accepted", flush=True)
        serialized = json.dumps(reports)
        assert "policy-secret" not in serialized and "policy-access" not in serialized and "policy auth secret" not in serialized, serialized
        print("independent request-policy HTTP inspection deployments=2 rejections=18 local_preflight=14 task_errors_each=1 HTTP_204=accepted credentials=redacted", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()


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
    if any(flag in sys.argv for flag in ("--copy-generations", "--copy-content", "--copy-ready-cleanup", "--copy-object-cleanup", "--catalogue-recovery", "--consumer-recovery", "--copy-execution", "--copy-data-cancel", "--copy-file-list-cancel", "--rename-generations", "--retention-events", "--source-absence", "--request-policy", "--copy-contracts", "--version-catalogue", "--mutable-catalogue", "--version-metadata", "--scan-wire")):
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
