"""Owned S3 identity regression: real ETags and data, fixed LastModified precision."""

import http.client
import http.server
import json
import re
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
    buckets = {kind: f"mutable-{kind}-{uuid.uuid4().hex[:12]}" for kind in ("ordinary", "null", "latest", "history")}
    for kind, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if kind in ("null", "history"):
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        if kind == "null":
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    cases = [(deployment, kind, history) for deployment in ("single", "cluster")
             for kind in ("ordinary", "null") for history in (False, True)]
    original_etags = {}
    source_reads = {}
    reads_guard = threading.Lock()
    for deployment, kind, history in cases:
        prefix = f"{deployment}/{kind}/{str(history).lower()}"
        saved = client.put_object(Bucket=buckets[kind], Key=prefix + "/payload", Body=b"payload-old", Tagging="raw=retained")
        original_etags[prefix] = saved["ETag"]
        source_reads[prefix] = {"data": 0, "tags": 0}
        if kind == "null":
            listed = client.list_object_versions(Bucket=buckets[kind], Prefix=prefix + "/payload")
            assert [version["VersionId"] for version in listed.get("Versions", [])] == ["null"], listed

    class SourceProxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            parsed = urllib.parse.urlsplit(self.path)
            prefix = next((prefix for prefix in source_reads if parsed.path.endswith("/" + prefix + "/payload")), None)
            arguments = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if self.command == "GET" and prefix is not None:
                with reads_guard:
                    source_reads[prefix]["tags" if "tagging" in arguments else "data"] += 1
            connection = http.client.HTTPConnection("127.0.0.1", 19000, timeout=10)
            try:
                connection.request(self.command, self.path, headers=dict(self.headers))
                response = connection.getresponse()
                body = response.read()
                # 仅固定时间精度，数据、ETag、版本号及标签均来自真实存储。
                if "xml" in response.getheader("Content-Type", ""):
                    body = re.sub(rb"<LastModified>[^<]*</LastModified>",
                                  b"<LastModified>2024-01-01T00:00:00.000Z</LastModified>", body)
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("connection", "content-length", "transfer-encoding"):
                        self.send_header(key, "Mon, 01 Jan 2024 00:00:00 GMT" if key.lower() == "last-modified" else value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                connection.close()

        do_GET = do_HEAD = handle_request

    reports = []

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            prefix = self.path.removeprefix("/replace/")
            parts = prefix.split("/")
            if not self.path.startswith("/replace/") or len(parts) != 3 or prefix not in original_etags:
                self.send_error(404)
                return
            saved = client.put_object(Bucket=buckets[parts[1]], Key=prefix + "/payload", Body=b"payload-new", Tagging="raw=retained")
            assert saved["ETag"] != original_etags[prefix], saved
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            reports.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    source = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SourceProxy)
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Control)
    for server in (source, control):
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
    for kind, bucket in buckets.items():
        port = source.server_port if kind in ("ordinary", "null") else 19000
        environment["LAB_MUTABLE_CATALOGUE_" + kind.upper()] = f"s3://migration-test:migration-test-isolated-only@{bucket}.127.0.0.1:{port}"
    environment["LAB_MUTABLE_CATALOGUE_CONTROL"] = f"127.0.0.1:{control.server_port}"
    environment["LAB_MUTABLE_CATALOGUE_CALLBACK"] = f"http://127.0.0.1:{control.server_port}/callback"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "copy_execution::mutable_tests::real_mutable_object_identities_are_compared_even_with_equal_size_time_and_version",
            "--", "--ignored", "--exact", "--nocapture",
        ], cwd=root, env=environment, check=True)
        for deployment, kind, history in cases:
            key = f"{deployment}/{kind}/{str(history).lower()}/payload"
            target = buckets["history" if history else "latest"]
            assert client.get_object(Bucket=target, Key=key)["Body"].read() == b"payload-new"
            assert client.get_object_tagging(Bucket=target, Key=key)["TagSet"] == [{"Key": "raw", "Value": "retained"}]
            assert client.get_object(Bucket=buckets[kind], Key=key)["Body"].read() == b"payload-new"
            if history:
                listed = client.list_object_versions(Bucket=target, Prefix=key)
                versions = listed.get("Versions", [])
                assert len(versions) == 2 and not listed.get("DeleteMarkers"), listed
                contents = [client.get_object(Bucket=target, Key=key, VersionId=version["VersionId"])["Body"].read()
                            for version in versions]
                assert contents == [b"payload-new", b"payload-old"], contents
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 24 and all(report["task_error_count"] == 0 and report["diagnostics_complete"]
                                          and report["detail"]["error_count"] == 0 for report in summaries), summaries
        changes = [report for report in summaries if report["migration"]["generation"] == 2]
        assert len(changes) == 8 and all(report["detail"]["changed_count"] == 1
                                         and report["detail"]["transferred_bytes"] == 11 for report in changes), changes
        unchanged = [report for report in summaries if report["migration"]["generation"] == 3]
        assert len(unchanged) == 8 and all(report["detail"]["transferred_bytes"] == 0 for report in unchanged), unchanged
        assert all(reads == {"data": 2, "tags": 3} for reads in source_reads.values()), source_reads
        print("independent mutable-object inspection deployments=2 cases=8 rounds=24 source_types=ordinary,null same_size_mtime_version=true changed_data_republished=true unchanged_bytes=0 source_data_GET_per_case=2 source_tags_GET_per_case=3 target_history_versions=2 tags_and_source_retained=true HTTP_204=accepted", flush=True)
    finally:
        for server in (control, source):
            server.shutdown()
            server.server_close()
