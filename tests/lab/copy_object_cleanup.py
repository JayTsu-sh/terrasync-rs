"""Real unversioned S3 source cleanup, isolated services and independent final reads."""
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
import uuid

import boto3
from botocore.config import Config


BODY = b"ordinary object data"
NESTED = b"nested object data"
SLASH = b"nonempty directory object"


def run(repository, environment):
    database_upstream = urllib.parse.urlsplit(environment["LAB_COPY_GENERATION_DSN"])
    client = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", aws_access_key_id="migration-test",
                          aws_secret_access_key="migration-test-isolated-only", region_name="us-east-1",
                          config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3))
    for _ in range(60):
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(0.25)
    else:
        raise RuntimeError("owned S3 readiness timeout")
    buckets = {role: f"objclean-{role}-{uuid.uuid4().hex[:12]}" for role in ("source", "latest", "history")}
    for role, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if role == "history":
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        client.put_object(Bucket=bucket, Key="sentinel", Body=b"retain sentinel")
    guard = threading.Lock()
    active = [None]
    states = {}
    reports = []
    stopped = threading.Event()
    with tempfile.TemporaryDirectory(prefix="ts-object-cleanup-") as directory:
        root = Path(directory)

        def inspect(prefix, mode):
            state = states[prefix]
            target = state["target"]
            keys = state["keys"]
            remaining = {item["Key"][len(prefix) + 1:]: item for item in client.list_objects_v2(
                Bucket=buckets["source"], Prefix=prefix + "/").get("Contents", [])}
            expected_remaining = ({"slash/"} if target == "local" and state["initial"] == "normal" else set()) if mode in ("normal", "lostack") else set(keys)
            assert set(remaining) == expected_remaining, (prefix, mode, remaining, expected_remaining)
            for key, body in keys.items():
                if key in remaining:
                    assert client.get_object(Bucket=buckets["source"], Key=f"{prefix}/{key}")["Body"].read() == body
                if target == "local":
                    if key != "slash/":
                        assert (root / prefix / "target" / key).read_bytes() == body
                elif mode == "data":
                    assert not client.list_objects_v2(Bucket=buckets[target], Prefix=f"{prefix}/{key}").get("Contents"), (prefix, key)
                else:
                    assert client.get_object(Bucket=buckets[target], Key=f"{prefix}/{key}")["Body"].read() == body
                    if mode == "normal":
                        assert client.get_object_tagging(Bucket=buckets[target], Key=f"{prefix}/{key}")["TagSet"] == [{"Key": "proof", "Value": "raw"}]
            if target == "local":
                assert state["source_tags"] == 0, (prefix, "Local target must not request source tags", state)
            if target == "history":
                history = client.list_object_versions(Bucket=buckets[target], Prefix=prefix + "/")
                assert not history.get("DeleteMarkers"), (prefix, history)
                assert len(history.get("Versions", [])) == len(keys), (prefix, history)
                assert {record["Key"] for record in history["Versions"]} == {prefix + "/" + key for key in keys}
            if state["initial"] == "lostack":
                assert state["forwarded_deletes"] == 1
            if mode in ("data", "metadata"):
                assert state["forwarded_deletes"] == 0
            state["inspections"] += 1
            print(f"independent object cleanup prefix={prefix} mode={mode} source_remaining={sorted(remaining)} target_data_verified=true fault_to_final_report_ms={state.get('fault_to_final_report_ms')}", flush=True)

        class Relay(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.path.startswith(("/arm/", "/recover/", "/inspect/")):
                    action, prefix, mode = self.path.strip("/").split("/")
                    if action == "arm":
                        target = prefix.split("-")[1]
                        keys = {"payload": BODY}
                        if mode == "normal" and target != "latest":
                            keys.update({"tree/file": NESTED, "slash/": SLASH})
                        for key, body in keys.items():
                            client.put_object(Bucket=buckets["source"], Key=f"{prefix}/{key}", Body=body, Tagging="proof=raw")
                        with guard:
                            assert prefix not in states
                            states[prefix] = dict(initial=mode, mode=mode, target=target, keys=keys,
                                                  forwarded_deletes=0, source_tags=0, affected=0,
                                                  first_fault=None, inspections=0, fact_released=False,
                                                  deleted=threading.Event())
                            active[0] = prefix
                    elif action == "recover":
                        with guard:
                            states[prefix]["mode"] = "normal"
                    else:
                        inspect(prefix, mode)
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.relay()

            def do_POST(self):
                if self.path == "/callback":
                    report = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    reports.append(report)
                    if report["is_final"] and not report.get("diagnostics") and report["migration"]["phase"] == "failed":
                        prefix = report["job_id"].removeprefix("objclean_").replace("_", "-")
                        with guard:
                            state = states[prefix]
                            if state["first_fault"] is not None:
                                state["fault_to_final_report_ms"] = round((time.monotonic() - state["first_fault"]) * 1000)
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.relay()

            def read_body(self):
                if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                    return self.rfile.read(int(self.headers.get("Content-Length", "0")))
                chunks = []
                while True:
                    count = int(self.rfile.readline().split(b";", 1)[0], 16)
                    if not count:
                        while self.rfile.readline() not in (b"\r\n", b""):
                            pass
                        return b"".join(chunks)
                    chunks.append(self.rfile.read(count))
                    assert self.rfile.read(2) == b"\r\n"

            def relay(self):
                parsed = urllib.parse.urlsplit(self.path)
                path = urllib.parse.unquote(parsed.path)
                query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                parts = path.strip("/").split("/", 1)
                database = parts[0] not in buckets.values()
                source = parts[0] == buckets["source"]
                with guard:
                    prefix = active[0]
                    state = states.get(prefix)
                    mode = state["mode"] if state else "normal"
                    deleting = source and self.command == "DELETE"
                    if source and self.command == "GET" and "tagging" in query:
                        state["source_tags"] += 1
                    data_failure = not source and self.command == "PUT" and "tagging" not in query and mode == "data"
                    metadata_failure = not source and self.command == "PUT" and "tagging" in query and mode == "metadata"
                    affected = (deleting and mode != "normal") or data_failure or metadata_failure
                    if affected:
                        state["affected"] += 1
                        state["first_fault"] = state["first_fault"] or time.monotonic()
                body = self.read_body()
                if database:
                    sql = query.get("query", [""])[0].upper()
                    held = state and state["initial"] == "normal" and state["target"] == "latest" and not state["fact_released"] and "INSERT INTO" in sql and "FORMAT ROWBINARY" in sql and "BASE_OBJCLEAN_" in sql
                    if held:
                        assert state["deleted"].wait(8), (prefix, "source deletion must not wait for database fact receipt")
                        inspect(prefix, "normal")
                        state["fact_released"] = True
                        print(f"independent ordinary S3 source deleted before fact forwarded prefix={prefix}", flush=True)
                if affected and mode in ("readonly", "data", "metadata"):
                    payload = f"<Error><Code>AccessDenied</Code><Message>owned object {mode} refusal</Message></Error>".encode()
                    self.send_response(403)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if deleting and mode in ("disconnect", "stall"):
                    if mode == "stall":
                        stopped.wait(15)
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                connection = http.client.HTTPConnection(database_upstream.hostname if database else "127.0.0.1",
                                                        database_upstream.port if database else 19000, timeout=15 if database else 5)
                try:
                    headers = {name: value for name, value in self.headers.items()
                               if name.lower() not in ("connection", "content-length", "transfer-encoding")}
                    headers["Content-Length"] = str(len(body))
                    connection.request(self.command, self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    payload = response.read()
                    if deleting:
                        with guard:
                            state["forwarded_deletes"] += 1
                            state["deleted"].set()
                    if deleting and mode == "lostack":
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for name, value in response.getheaders():
                        if name.lower() not in ("connection", "content-length", "transfer-encoding"):
                            self.send_header(name, value)
                    self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    connection.close()

            do_HEAD = do_PUT = do_DELETE = relay

        relay = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay)
        relay.daemon_threads = True
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        address = f"127.0.0.1:{relay.server_port}"
        environment = dict(environment, S3_CONNECT_TIMEOUT="1", S3_READ_TIMEOUT="1", S3_OPERATION_TIMEOUT="2",
                           LAB_OBJECT_CLEANUP_CONTROL=address, LAB_OBJECT_CLEANUP_CALLBACK=f"http://{address}/callback",
                           LAB_OBJECT_CLEANUP_ROOT=str(root), LAB_COPY_GENERATION_DSN=f"http://{address}")
        for role, bucket in buckets.items():
            environment["LAB_OBJECT_CLEANUP_" + role.upper()] = f"s3://migration-test:migration-test-isolated-only@{bucket}.127.0.0.1:{relay.server_port}"
        try:
            subprocess.run(["cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                            "copy_execution::object_cleanup_tests::real_unversioned_object_source_cleanup_and_failures",
                            "--", "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                           cwd=repository, env=environment, check=True, timeout=360)
            assert len(states) == 24 and all(state["inspections"] == (2 if state["initial"] != "normal" or state["target"] == "latest" else 1) for state in states.values()), states
            assert sum(state["fact_released"] for state in states.values()) == 4, states
            assert all(state["affected"] for state in states.values() if state["initial"] != "normal"), states
            summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
            assert len(summaries) == 36 and all(report["diagnostics_complete"] for report in summaries), summaries
            assert sum(report["migration"]["phase"] == "failed" for report in summaries) == 12
            events = {event["event_id"]: event for report in reports
                      for event in (report.get("diagnostics") or {}).get("events", [])}
            purge = [event for event in events.values() if event.get("retention", {}).get("reason") == "source_deletion_unconfirmed"]
            assert len(purge) == 8 and all(event["retention"]["source_presence"] == "unknown" and event["path"] == "payload" for event in purge), purge
            for mode in ("data", "metadata"):
                failures = [report for report in summaries if report["job_id"].endswith("_" + mode) and report["migration"]["phase"] == "failed"]
                assert len(failures) == 2 and all(report["detail"]["error_count"] == 1 for report in failures), failures
                detailed = {event["event_id"]: event for report in reports if report["job_id"].endswith("_" + mode)
                            for event in (report.get("diagnostics") or {}).get("events", []) if event["code"] == "copy_version_failed"}
                assert len(detailed) == 2, detailed
                assert all(event["path"] == "payload" and "HTTP=403" in event["message"] and "code=AccessDenied" in event["message"]
                           and "backend=S3" in event["message"] and "phase=" in event["message"] for event in detailed.values()), detailed
                if mode == "metadata":
                    assert all("phase=target_metadata_apply" in event["message"] for event in detailed.values()), detailed
                print(f"callback object failure mode={mode} details=" + json.dumps([event["message"] for event in detailed.values()]), flush=True)
            for role, bucket in buckets.items():
                assert client.get_object(Bucket=bucket, Key="sentinel")["Body"].read() == b"retain sentinel"
                assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
            print("independent ordinary object cleanup deployments=2 normal=12 failures=12 recovery=12 HTTP_204=true final_reports_saved=true base_empty=true all_recovery_generations_full=true", flush=True)
        finally:
            stopped.set()
            relay.shutdown()
            relay.server_close()
