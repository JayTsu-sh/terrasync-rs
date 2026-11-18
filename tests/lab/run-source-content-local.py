#!/usr/bin/env python3
"""Exercise existing-target content verification and failures in an owned RustFS fixture.

Requires a prebuilt app lib test binary, cached RustFS image, docker, nsenter and boto3.
The relay rejects mutations. No shared service or firewall is changed.
"""

import argparse
import http.client
import http.server
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid

import boto3
from botocore.config import Config


TEST = "source_cleanup::content_tests::real_existing_s3_content_verification_gates_source_removal"
PAYLOAD = b"verified payload"


def docker(*arguments):
    return subprocess.check_output(["timeout", "30", "docker", *arguments], text=True).strip()


def isolated(arguments):
    binary = str(Path(arguments.binary).resolve())
    listed = subprocess.check_output(["timeout", "10", binary, "--list"], text=True)
    assert f"{TEST}: test" in listed.splitlines(), "the exact ignored test must exist"
    name = f"terrasync-content-{uuid.uuid4().hex[:12]}"
    image = os.environ.get("CONTENT_TEST_S3_IMAGE", "rustfs/rustfs:latest")
    container = docker(
        "run", "-d", "--pull", "never", "--name", name, "--network", "none",
        "--memory=768m", "--cpus=1", "--tmpfs", "/data:rw,size=128m,mode=1777",
        "--label", "codex.task=source-content-verification", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
        "-e", "RUSTFS_ACCESS_KEY=content-test", "-e", "RUSTFS_SECRET_KEY=content-test-isolated-only",
        image, "/data",
    )
    try:
        print(f"owned content image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
        pid = docker("inspect", container, "--format", "{{.State.Pid}}")
        subprocess.run(["timeout", "120", "nsenter", "-t", pid, "-n", sys.executable,
                        __file__, "--inside", "--binary", binary], check=True)
    finally:
        docker("rm", "-f", "-v", container)


def inside(arguments):
    client = boto3.client(
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="content-test",
        aws_secret_access_key="content-test-isolated-only", region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=1, read_timeout=1),
    )
    for _ in range(60):
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(0.25)
    else:
        raise RuntimeError("owned RustFS readiness timed out")
    bucket = f"content-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    stopped = threading.Event()
    lock = threading.Lock()
    state = {"mode": "normal", "data_get": [], "writes": 0, "active": 0}
    with tempfile.TemporaryDirectory(prefix="terrasync-source-content-") as directory:
        root = Path(directory)
        control = root / "control"
        seen = root / "seen"

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
                data_get = self.command == "GET" and self.path.partition("?")[0].endswith("/payload")
                data_get = data_get and not {"tagging", "acl", "uploads", "uploadId"}.intersection(query)
                mutation = self.command in ("PUT", "POST", "DELETE", "PATCH")
                with lock:
                    state["active"] += 1
                    state["writes"] += int(mutation)
                    if data_get:
                        state["data_get"].append(query.get("versionId", []))
                    mode = control.read_text().strip()
                try:
                    if mutation:
                        self.send_error(403, "content verification cannot mutate target")
                        return
                    if data_get:
                        seen.write_text("seen")
                        if mode == "readonly":
                            body = b"<Error><Code>AccessDenied</Code><Message>injected content read denied</Message></Error>"
                            self.send_response(403)
                            self.send_header("Content-Length", str(len(body)))
                            self.end_headers()
                            self.wfile.write(body)
                            return
                        if mode in ("disconnect", "stall", "cancel", "flash", "recover_stall"):
                            if mode not in ("disconnect", "flash"):
                                self.connection.settimeout(0.1)
                                deadline = time.monotonic() + 10
                                while not stopped.is_set() and time.monotonic() < deadline:
                                    try:
                                        if not self.connection.recv(1):
                                            break
                                    except socket.timeout:
                                        pass
                                    except OSError:
                                        break
                            return
                    upstream = http.client.HTTPConnection("127.0.0.1", 9000, timeout=3)
                    try:
                        upstream.request(self.command, self.path, headers=dict(self.headers))
                        response = upstream.getresponse()
                        body = response.read()
                        self.send_response(response.status)
                        for key, value in response.getheaders():
                            if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                                self.send_header(key, value)
                        self.send_header("Content-Length", response.getheader("Content-Length", "0")
                                         if self.command == "HEAD" else str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                    finally:
                        upstream.close()
                finally:
                    self.close_connection = True
                    with lock:
                        state["active"] -= 1

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            for mode in ("quick", "normal", "mismatch", "readonly", "disconnect", "stall", "cancel", "flash", "recover_stall"):
                prefix = f"case-{mode}"
                target_body = b"corrupt! payload" if mode == "mismatch" else PAYLOAD
                versions = [client.put_object(Bucket=bucket, Key=f"{prefix}/payload", Body=target_body)["VersionId"]
                            for _ in range(3)]
                sentinel = client.put_object(Bucket=bucket, Key=f"{prefix}/sentinel", Body=b"retained")["VersionId"]
                source = root / prefix
                source.mkdir()
                payload = source / "payload"
                payload.write_bytes(PAYLOAD)
                payload.chmod(0o640)
                before = payload.stat()
                seen.unlink(missing_ok=True)
                control.write_text(mode)
                with lock:
                    state["data_get"] = []
                    state["writes"] = 0
                environment = dict(os.environ, S3_CONNECT_TIMEOUT="1", S3_OPERATION_TIMEOUT="2", S3_READ_TIMEOUT="1")
                environment.update(
                    LAB_CONTENT_SOURCE=str(source), LAB_CONTENT_FAULT=mode, LAB_CONTENT_SEEN=str(seen),
                    LAB_CONTENT_CONTROL=str(control),
                    LAB_CONTENT_TARGET=f"s3://content-test:content-test-isolated-only@{bucket}.127.0.0.1:{server.server_port}/{prefix}",
                )
                subprocess.run(["timeout", "15", arguments.binary, TEST, "--ignored", "--exact", "--nocapture"],
                               env=environment, check=True)
                deadline = time.monotonic() + 2
                while state["active"] and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert state["active"] == 0, "test process exit must release relay connections"
                gets = state["data_get"]
                assert state["writes"] == 0, state
                assert (len(gets) == 0) if mode == "quick" else (len(gets) > 0), gets
                assert all(selected == [versions[-1]] for selected in gets), (mode, versions, gets)
                if mode in ("quick", "normal", "flash", "recover_stall"):
                    assert not payload.exists(), mode
                else:
                    after = payload.stat()
                    assert payload.read_bytes() == PAYLOAD
                    assert (after.st_mode, after.st_size, after.st_mtime_ns, after.st_ctime_ns) == (
                        before.st_mode, before.st_size, before.st_mtime_ns, before.st_ctime_ns), mode
                records = client.list_object_versions(Bucket=bucket, Prefix=f"{prefix}/")
                assert not records.get("DeleteMarkers"), records
                assert {record["VersionId"] for record in records["Versions"]} == {*versions, sentinel}, records
                for version in versions:
                    assert client.get_object(Bucket=bucket, Key=f"{prefix}/payload", VersionId=version)["Body"].read() == target_body
                assert client.get_object(Bucket=bucket, Key=f"{prefix}/sentinel")["Body"].read() == b"retained"
                assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
                print(f"independent content fault={mode} pinned_GETs={len(gets)} writes=0 versions=3 sentinel=retained connections=closed", flush=True)
        finally:
            stopped.set()
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    inside(arguments) if arguments.inside else isolated(arguments)
