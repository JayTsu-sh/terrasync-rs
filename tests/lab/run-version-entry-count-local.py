#!/usr/bin/env python3
"""Verify app copy events and entry counters against isolated real versioned S3 buckets.

Requires docker, nsenter, boto3, cached Rust dependencies and a cached RustFS image.
"""

import os
import http.client
import http.server
import json
from pathlib import Path
import subprocess
import sys
import time
import threading
import urllib.request
import urllib.parse
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]


def docker(*arguments):
    return subprocess.check_output(["docker", *arguments], text=True).strip()


def run_isolated():
    image = os.environ.get("MIGRATION_S3_IMAGE", "rustfs/rustfs:latest")
    container = docker(
        "run", "-d", "--name", f"dm-nightly-entry-count-{uuid.uuid4().hex[:12]}",
        "--network", "none", "--memory=768m", "--cpus=1",
        "--tmpfs", "/data:rw,size=128m,mode=1777", "--label", "codex.task=version-entry-count",
        "-e", "RUSTFS_ADDRESS=0.0.0.0:9000", "-e", "RUSTFS_ACCESS_KEY=migration-test",
        "-e", "RUSTFS_SECRET_KEY=migration-test-isolated-only", image, "/data",
    )
    try:
        print(f"isolated entry-count image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
        pid = docker("inspect", container, "--format", "{{.State.Pid}}")
        subprocess.run(["nsenter", "-t", pid, "-n", sys.executable, __file__, "--inside"], cwd=ROOT, check=True)
    finally:
        docker("rm", "-f", "-v", container)


def run_inside():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(60):
        try:
            with opener.open("http://127.0.0.1:9000/health/ready", timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        time.sleep(0.25)
    else:
        raise RuntimeError("isolated S3 readiness timed out")
    client = boto3.client(
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="migration-test",
        aws_secret_access_key="migration-test-isolated-only", region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3),
    )
    run_id = uuid.uuid4().hex[:12]
    buckets = {kind: f"migration-{kind}-{run_id}" for kind in ("source", "latest", "history")}
    for kind, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if kind != "latest":
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    for _ in range(20):
        client.put_object(Bucket=buckets["source"], Key="payload", Body=b"payload")
    environment = dict(os.environ)
    for kind, variable in (("source", "LAB_MIGRATION_SOURCE_URL"), ("latest", "LAB_MIGRATION_LATEST_URL"), ("history", "LAB_MIGRATION_HISTORY_URL")):
        environment[variable] = f"s3://migration-test:migration-test-isolated-only@{buckets[kind]}.127.0.0.1:9000"
    subprocess.run([
        "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
        "storage_tests::s3_versions::full_copy_counts_versioned_source_by_delivered_entry",
        "--", "--ignored", "--exact", "--nocapture",
    ], cwd=ROOT, env=environment, check=True)
    subprocess.run([
        "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
        "storage_tests::s3_versions::required_consumer_failure_stops_history_worker_before_next_version_or_entry",
        "--", "--ignored", "--exact", "--nocapture",
    ], cwd=ROOT, env=environment, check=True)
    published = client.list_object_versions(Bucket=buckets["history"], Prefix="consumer-closed/payload")
    assert len(published.get("Versions", [])) == 1 and not published.get("DeleteMarkers"), published
    retained = client.list_object_versions(Bucket=buckets["source"], Prefix="payload")
    assert len(retained.get("Versions", [])) == 20 and not retained.get("DeleteMarkers"), retained
    print("independent S3 inspection closed_consumer target_versions=1 source_versions=20 next_version_or_entry=not_started", flush=True)
    tag_refusals = 0
    data_refusals = 0
    fault = "tags"
    counter_lock = threading.Lock()

    class DeniedTags(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            nonlocal tag_refusals, data_refusals
            is_data = self.command == "GET" and "versionId" in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query) and "tagging" not in self.path
            if (fault == "tags" and "tagging" in self.path) or (fault == "data" and is_data):
                with counter_lock:
                    if fault == "tags":
                        tag_refusals += 1
                    else:
                        data_refusals += 1
                payload = f"<Error><Code>AccessDenied</Code><Message>injected source {fault} permission denial</Message></Error>".encode()
                self.send_response(403)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
            try:
                upstream.request(self.command, self.path, headers=dict(self.headers))
                response = upstream.getresponse()
                payload = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                        self.send_header(key, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            finally:
                upstream.close()

        do_GET = do_HEAD = handle_request

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), DeniedTags)
    proxy.daemon_threads = True
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    callback_reports = []

    class Callback(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            report = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            callback_reports.append(report)
            body = json.dumps({"batch_id": report.get("diagnostics", {}).get("batch_id")}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    callback = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Callback)
    callback.daemon_threads = True
    threading.Thread(target=callback.serve_forever, daemon=True).start()
    try:
        environment["LAB_MIGRATION_CALLBACK_URL"] = f"http://127.0.0.1:{callback.server_port}/progress"
        environment["LAB_MIGRATION_DENIED_SOURCE_URL"] = f"s3://migration-test:migration-test-isolated-only@{buckets['source']}.127.0.0.1:{proxy.server_port}"
        environment["LAB_MIGRATION_FAILED_URL"] = environment["LAB_MIGRATION_HISTORY_URL"] + "/metadata-failure"
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "storage_tests::s3_versions::metadata_failures_copy_every_version_but_count_one_failed_entry",
            "--", "--ignored", "--exact", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        assert tag_refusals == 20, tag_refusals
        assert len(callback_reports) == 1, callback_reports
        report = callback_reports[0]
        assert report["is_final"] and report["detail"]["error_count"] == 1, report
        errors = [event for event in report["diagnostics"]["events"] if event["code"] == "copy_version_failed"]
        assert len(errors) == 20 and all("PermissionDenied" in event["message"] and "phase=source_metadata" in event["message"] for event in errors), errors
        print("real HTTP callback failed_entries=1 version_error_details=20 ACK=confirmed", flush=True)
        failed_records = client.list_object_versions(Bucket=buckets["history"], Prefix="metadata-failure/payload").get("Versions", [])
        assert len(failed_records) == 20, len(failed_records)
        assert client.get_object(Bucket=buckets["history"], Key="metadata-failure/payload")["Body"].read() == b"payload"
        print("independent S3 inspection metadata_failures=20 target_data_versions=20 failed_entries=1", flush=True)
        fault = "data"
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "storage_tests::s3_versions::data_failure_blocks_later_versions_and_counts_each_delivered_entry_once",
            "--", "--ignored", "--exact", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        assert data_refusals == 2, data_refusals
        for cap in (20, 10):
            listed = client.list_object_versions(Bucket=buckets["history"], Prefix=f"data-failure-{cap}/payload")
            assert not listed.get("Versions") and not listed.get("DeleteMarkers"), listed
        print("independent S3 inspection data_failures=2 later_version_reads=0 published_versions=0", flush=True)
    finally:
        proxy.shutdown()
        proxy.server_close()
        callback.shutdown()
        callback.server_close()
    for kind, expected in (("source", 20), ("latest", 1), ("history", 20)):
        listed = client.list_object_versions(Bucket=buckets[kind], Prefix="payload")
        records = listed.get("Versions", []) + listed.get("DeleteMarkers", [])
        assert len(records) == expected, (kind, len(records), expected)
        assert client.get_object(Bucket=buckets[kind], Key="payload")["Body"].read() == b"payload"
        print(f"independent S3 inspection kind={kind} version_records={len(records)} payload=verified", flush=True)


if __name__ == "__main__":
    if "--inside" in sys.argv:
        run_inside()
    else:
        run_isolated()
