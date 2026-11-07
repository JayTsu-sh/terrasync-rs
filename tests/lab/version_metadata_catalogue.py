"""Real version metadata catalogue retries: no body replay, stable target versions and callbacks."""
import http.client
import http.server
import json
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid


def run(root, environment):
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
    buckets = {kind: f"metadata-{kind}-{uuid.uuid4().hex[:12]}" for kind in ("source", "latest", "history")}
    for kind, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if kind != "latest":
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    cases = [f"{deployment}/{history}/{verify}" for deployment in ("single", "cluster")
             for history in ("latest", "history") for verify in ("quick", "full")]
    seeded, modes, counts = {}, {}, {}
    for prefix in cases:
        seeded[prefix] = [client.put_object(Bucket=buckets["source"], Key=prefix + "/payload",
                          Body=f"version-{index}".encode(), Tagging=f"raw=source-{index}")["VersionId"] for index in range(4)]
        modes[prefix] = "normal"
        counts[prefix] = {"source_data": 0, "target_data": 0, "target_get": 0, "source_denied": 0, "target_denied": 0, "target_tags": 0}
    guard = threading.Lock()
    writes = []

    class Proxy(http.server.BaseHTTPRequestHandler):
        side = None

        def log_message(self, *_):
            pass

        def request_body(self):
            if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                return self.rfile.read(int(self.headers.get("Content-Length", "0")))
            chunks = []
            while True:
                line = self.rfile.readline()
                chunks.append(line)
                size = int(line.split(b";", 1)[0], 16)
                if size == 0:
                    while True:
                        trailer = self.rfile.readline()
                        chunks.append(trailer)
                        if trailer in (b"\r\n", b""):
                            return b"".join(chunks)
                chunks.append(self.rfile.read(size + 2))

        def handle_request(self):
            body = self.request_body()
            parsed = urllib.parse.urlsplit(self.path)
            arguments = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            prefix = next((prefix for prefix in cases if parsed.path.endswith("/" + prefix + "/payload")), None)
            tagging = "tagging" in arguments
            deny = False
            if prefix:
                with guard:
                    mode = modes[prefix]
                    if self.side == "source" and self.command == "GET" and not tagging:
                        counts[prefix]["source_data"] += 1
                    if self.side == "target" and self.command == "GET" and not tagging:
                        counts[prefix]["target_get"] += 1
                    if self.side == "target" and self.command == "PUT":
                        counts[prefix]["target_tags" if tagging else "target_data"] += 1
                        if tagging:
                            writes.append((prefix, arguments.get("versionId")))
                    deny = (self.side == "source" and self.command == "GET" and tagging and mode == "source-fail") or (
                        self.side == "target" and self.command == "PUT" and tagging and mode == "target-fail")
                    if deny:
                        counts[prefix]["source_denied" if self.side == "source" else "target_denied"] += 1
            if deny:
                payload = b"<Error><Code>AccessDenied</Code><Message>injected metadata permission refusal</Message></Error>"
                self.send_response(403)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            connection = http.client.HTTPConnection("127.0.0.1", 19000, timeout=10)
            try:
                connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                response = connection.getresponse()
                payload = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("connection", "content-length", "transfer-encoding"):
                        self.send_header(key, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(payload)
            finally:
                connection.close()

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

    class Source(Proxy):
        side = "source"

    class Target(Proxy):
        side = "target"

    reports = []

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            mode, prefix = self.path.lstrip("/").split("/", 1)
            if prefix not in cases or mode not in ("source-fail", "target-fail", "normal", "old-tags"):
                self.send_error(404)
                return
            if mode == "old-tags":
                client.put_object_tagging(Bucket=buckets["source"], Key=prefix + "/payload", VersionId=seeded[prefix][0],
                                          Tagging={"TagSet": [{"Key": "raw", "Value": "changed-old"}]})
            with guard:
                modes[prefix] = "normal" if mode == "old-tags" else mode
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            reports.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    source = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Source)
    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Target)
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Control)
    servers = (source, target, control)
    for server in servers:
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
    environment.update({
        "LAB_VERSION_METADATA_CATALOGUE_SOURCE": f"s3://migration-test:migration-test-isolated-only@{buckets['source']}.127.0.0.1:{source.server_port}",
        "LAB_VERSION_METADATA_CATALOGUE_LATEST": f"s3://migration-test:migration-test-isolated-only@{buckets['latest']}.127.0.0.1:{target.server_port}",
        "LAB_VERSION_METADATA_CATALOGUE_HISTORY": f"s3://migration-test:migration-test-isolated-only@{buckets['history']}.127.0.0.1:{target.server_port}",
        "LAB_VERSION_METADATA_CATALOGUE_CONTROL": f"127.0.0.1:{control.server_port}",
        "LAB_VERSION_METADATA_CATALOGUE_CALLBACK": f"http://127.0.0.1:{control.server_port}/progress",
    })
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "copy_execution::version_metadata_tests::historical_metadata_repair_preserves_versions_and_data_after_repeated_failures",
            "--", "--ignored", "--exact", "--nocapture",
        ], cwd=root, env=environment, check=True)
        for prefix in cases:
            history = "/history/" in prefix
            versions = client.list_object_versions(Bucket=buckets["history" if history else "latest"], Prefix=prefix + "/payload")
            records = versions.get("Versions", [])
            assert len(records) == (4 if history else 1) and not versions.get("DeleteMarkers"), (prefix, versions)
            actual = [client.get_object(Bucket=buckets["history" if history else "latest"], Key=prefix + "/payload", VersionId=item["VersionId"])["Body"].read() for item in records]
            assert actual == ([f"version-{index}".encode() for index in (3, 2, 1, 0)] if history else [b"version-3"]), (prefix, actual)
            for index, item in enumerate(records):
                raw = f"source-{3 - index}" if history else "source-3"
                if history and index == 3:
                    raw = "changed-old"
                assert client.get_object_tagging(Bucket=buckets["history" if history else "latest"], Key=item["Key"], VersionId=item["VersionId"])["TagSet"] == [{"Key": "raw", "Value": raw}]
            expected_data = 4 if history else 1
            assert counts[prefix]["source_data"] == counts[prefix]["target_data"] == expected_data, (prefix, counts[prefix])
            assert counts[prefix]["target_get"] == (expected_data if prefix.endswith("full") else 0), (prefix, counts[prefix])
            assert counts[prefix]["source_denied"] == counts[prefix]["target_denied"] == 2 * expected_data, (prefix, counts[prefix])
            for selected, ids in writes:
                if selected == prefix:
                    assert (ids is not None and ids[0] in {item["VersionId"] for item in records}) if history else ids is None
            source_records = client.list_object_versions(Bucket=buckets["source"], Prefix=prefix + "/payload")
            assert len(source_records.get("Versions", [])) == 4 and not source_records.get("DeleteMarkers")
            print(f"independent metadata catalogue prefix={prefix} versions={len(records)} source_retained=4 repeated_failures=4 body_replays=0 current=version-3", flush=True)
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 56 and all(report["task_error_count"] == 0 and report["diagnostics_complete"] for report in summaries), summaries
        failed = [report for report in summaries if report["detail"]["error_count"] == 1]
        assert len(failed) == 32 and all(report["migration"]["phase"] == "failed" for report in failed), failed
        events = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", [])]
        version_errors = [event for event in events if event["code"] == "copy_version_failed"]
        scan_errors = [event for event in events if event["code"] == "Scan"]
        retained = [event for event in events if event["code"] == "target_preserved"]
        assert len(version_errors) == 80 and len(scan_errors) == len(retained) == 40
        assert len(events) == len(version_errors) + len(scan_errors) + len(retained)
        assert all("source_metadata" in event["message"] or "target_metadata_apply" in event["message"] for event in version_errors)
        assert all("Tags" in event["message"] and "PermissionDenied" in event["message"] and "version:" in event["message"] for event in scan_errors)
        assert all(event["retention"]["source_presence"] == "unknown" for event in retained)
        print("independent metadata callback summaries=56 failed_entries=32 detailed_version_causes=80 source_scan_causes=40 conservative_ranges=40 task_errors=0 HTTP_204=accepted", flush=True)
    finally:
        for server in reversed(servers):
            server.shutdown()
            server.server_close()
