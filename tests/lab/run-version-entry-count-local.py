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
    subprocess.run([
        "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
        "storage_tests::s3_versions::segmented_full_copy_counts_one_logical_entry_and_keeps_every_version",
        "--", "--ignored", "--exact", "--nocapture",
    ], cwd=ROOT, env=environment, check=True)
    for cap in (1, 7):
        key = f"segment-success-{cap}/payload"
        versions = client.list_object_versions(Bucket=buckets["history"], Prefix=key)
        assert len(versions.get("Versions", [])) == 20 and not versions.get("DeleteMarkers"), versions
        assert client.get_object(Bucket=buckets["history"], Key=key)["Body"].read() == b"payload"
    print("independent S3 inspection segmented_full caps=1,7 logical_entries=1 target_versions=20", flush=True)
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
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

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
        assert len(callback_reports) == 2, callback_reports
        report = callback_reports[-1]
        assert report["is_final"] and report["diagnostics_complete"] and report["detail"]["error_count"] == 1, report
        assert all(item["detail"]["error_count"] == 1 for item in callback_reports), callback_reports
        errors = [event for item in callback_reports for event in (item.get("diagnostics") or {}).get("events", []) if event["code"] == "copy_version_failed"]
        assert len(errors) == 20 and all("PermissionDenied" in event["message"] and "phase=source_metadata" in event["message"] for event in errors), errors
        print("real HTTP callback failed_entries=1 version_error_details=20 HTTP_204=accepted summary_complete=true", flush=True)
        failed_records = client.list_object_versions(Bucket=buckets["history"], Prefix="metadata-failure/payload").get("Versions", [])
        assert len(failed_records) == 20, len(failed_records)
        assert client.get_object(Bucket=buckets["history"], Key="metadata-failure/payload")["Body"].read() == b"payload"
        print("independent S3 inspection metadata_failures=20 target_data_versions=20 failed_entries=1", flush=True)
        for cap in (1, 7):
            key = f"metadata-failure/segments-{cap}/payload"
            versions = client.list_object_versions(Bucket=buckets["history"], Prefix=key)
            assert len(versions.get("Versions", [])) == 20 and not versions.get("DeleteMarkers"), versions
            assert client.get_object(Bucket=buckets["history"], Key=key)["Body"].read() == b"payload"
        print("independent S3 inspection segmented_metadata caps=1,7 failed_entries=1 target_data_versions=20 extra_tag_reads=0", flush=True)
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

    for index in range(20):
        client.put_object(Bucket=buckets["source"], Key="slash/a/", Body=f"slash-{index}".encode())
    client.put_object(Bucket=buckets["source"], Key="slash/a/file", Body=b"child")
    client.put_object(Bucket=buckets["source"], Key="slash/empty/", Body=b"")
    slash_before = [item for item in client.list_object_versions(Bucket=buckets["source"], Prefix="slash/a/").get("Versions", []) if item["Key"] == "slash/a/"]
    assert slash_before
    slash_history_count = len(slash_before)
    print(f"slash fixture source_versions={slash_history_count} put_requests=20; provider may store slash keys as one null version", flush=True)
    environment["LAB_MIGRATION_SLASH_SOURCE_URL"] = environment["LAB_MIGRATION_SOURCE_URL"] + "/slash"
    callback_reports.clear()
    callback = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Callback)
    callback.daemon_threads = True
    threading.Thread(target=callback.serve_forever, daemon=True).start()
    environment["LAB_MIGRATION_CALLBACK_URL"] = f"http://127.0.0.1:{callback.server_port}/progress"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "storage_tests::s3_versions::nonempty_slash_objects_skip_filesystems_but_copy_complete_keys_and_history_to_objects",
            "--", "--ignored", "--exact", "--nocapture",
        ], cwd=ROOT, env=environment, check=True)
        assert len(callback_reports) == 2, callback_reports
        assert all(report["skipped_count"] == 1 and report["detail"]["error_count"] == 0 for report in callback_reports)
        events = [event for report in callback_reports for event in (report.get("diagnostics") or {}).get("events", [])]
        assert len(events) == 1 and events[0]["code"] == "copy_skipped" and events[0]["path"] == "a/", events
        assert callback_reports[-1]["diagnostics_complete"]
        for kind, count in (("source", slash_history_count), ("latest", 1), ("history", slash_history_count)):
            versions = client.list_object_versions(Bucket=buckets[kind], Prefix="slash/a/").get("Versions", [])
            assert len([item for item in versions if item["Key"] == "slash/a/"]) == count, (kind, versions)
            assert client.get_object(Bucket=buckets[kind], Key="slash/a/")["Body"].read() == b"slash-19"
            assert client.get_object(Bucket=buckets[kind], Key="slash/a/file")["Body"].read() == b"child"
            assert client.get_object(Bucket=buckets[kind], Key="slash/empty/")["Body"].read() == b""
        slash_after = [item for item in client.list_object_versions(Bucket=buckets["source"], Prefix="slash/a/").get("Versions", []) if item["Key"] == "slash/a/"]
        assert {(item["VersionId"], item["Size"]) for item in slash_before} == {(item["VersionId"], item["Size"]) for item in slash_after}
        print(f"independent S3 inspection slash_key latest=1 history={slash_history_count} source={slash_history_count} child=preserved empty_marker=preserved callback_skip=1 errors=0", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()

    # 输出范围省略的属性不应发送请求；若误发则由真实服务前代理明确拒绝。
    for _ in range(20):
        client.put_object(Bucket=buckets["source"], Key="metadata-scope/payload", Body=b"scope")
    scope_records = client.list_object_versions(Bucket=buckets["source"], Prefix="metadata-scope/payload")["Versions"]
    assert len(scope_records) == 20
    latest_id = next(item["VersionId"] for item in scope_records if item["IsLatest"])
    metadata_requests = []
    denied_metadata = []

    class ScopedMetadata(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            object_path = urllib.parse.unquote(parsed.path)
            if "tagging" in query:
                record = (object_path, query.get("versionId", [None])[0])
                metadata_requests.append(record)
                if object_path.endswith("/slash/a/") or (object_path.endswith("/metadata-scope/payload") and record[1] != latest_id):
                    denied_metadata.append(record)
                    payload = b"<Error><Code>AccessDenied</Code><Message>unneeded metadata read</Message></Error>"
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

    scope_proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ScopedMetadata)
    scope_proxy.daemon_threads = True
    threading.Thread(target=scope_proxy.serve_forever, daemon=True).start()
    try:
        scope_root = f"s3://migration-test:migration-test-isolated-only@{buckets['source']}.127.0.0.1:{scope_proxy.server_port}"
        for prefix, test_name, expected_reads in (
            ("slash", "omitted_directory_object_metadata_does_not_read_denied_tags_or_hide_children", 2),
            ("metadata-scope", "latest_version_metadata_scope_keeps_all_history_without_reading_old_tags", 1),
        ):
            metadata_requests.clear()
            environment["LAB_METADATA_SCOPE_SOURCE_URL"] = f"{scope_root}/{prefix}"
            subprocess.run([
                "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                f"storage_tests::s3_metadata_scope::{test_name}",
                "--", "--ignored", "--exact", "--nocapture",
            ], cwd=ROOT, env=environment, check=True)
            assert len(metadata_requests) == expected_reads, metadata_requests
            assert not denied_metadata, denied_metadata
            print(f"independent S3 metadata inspection prefix={prefix} tag_requests={expected_reads} unnecessary_reads=0", flush=True)
    finally:
        scope_proxy.shutdown()
        scope_proxy.server_close()


if __name__ == "__main__":
    if "--inside" in sys.argv:
        run_inside()
    else:
        run_isolated()
