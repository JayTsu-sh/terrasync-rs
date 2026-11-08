#!/usr/bin/env python3
"""Check ordinary and cancelled initialization against an owned RustFS container.

Requires a prebuilt app lib test binary, cached RustFS image, docker, nsenter and boto3.
No shared server, firewall, source tree or bucket is modified.
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


TEST = "orchestrator::cancellation_tests::real_storage_initialization_observes_cancellation_without_source_or_object_mutation"


def docker(*arguments):
    return subprocess.check_output(["timeout", "30", "docker", *arguments], text=True).strip()


def isolated(arguments):
    binary = str(Path(arguments.binary).resolve())
    listed = subprocess.check_output(["timeout", "10", binary, "--list"], text=True)
    assert f"{TEST}: test" in listed.splitlines(), "the exact ignored test must exist"
    name = f"data-mover-worker-cancel-{uuid.uuid4().hex[:12]}"
    image = os.environ.get("WORKER_CANCELLATION_S3_IMAGE", "rustfs/rustfs:latest")
    container = docker(
        "run", "-d", "--pull", "never", "--name", name, "--network", "none",
        "--memory=768m", "--cpus=1", "--tmpfs", "/data:rw,size=128m,mode=1777",
        "--label", "codex.task=copy-worker-cancellation", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
        "-e", "RUSTFS_ACCESS_KEY=worker-test", "-e", "RUSTFS_SECRET_KEY=worker-test-isolated-only",
        image, "/data",
    )
    try:
        print(f"isolated initialization image={docker('inspect', container, '--format', '{{.Image}}')}", flush=True)
        pid = docker("inspect", container, "--format", "{{.State.Pid}}")
        subprocess.run(["timeout", "90", "nsenter", "-t", pid, "-n", sys.executable,
                        __file__, "--inside", "--binary", binary], check=True)
    finally:
        docker("rm", "-f", "-v", container)


def inside(arguments):
    client = boto3.client(
        "s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="worker-test",
        aws_secret_access_key="worker-test-isolated-only", region_name="us-east-1",
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
    bucket = f"data-mover-worker-cancel-{uuid.uuid4().hex[:12]}"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    original = client.put_object(Bucket=bucket, Key="sentinel", Body=b"unchanged target")
    stopped = threading.Event()
    lock = threading.Lock()
    state = {"mode": "normal", "active": 0, "requests": 0, "writes": 0, "data_get": 0}
    with tempfile.TemporaryDirectory(prefix="terrasync-worker-cancel-") as directory:
        root = Path(directory)
        source = root / "source"
        source.mkdir()
        payload = source / "file"
        payload.write_bytes(b"retained source")
        attributes = payload.stat()
        seen = root / "request-seen"

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def handle_request(self):
                parsed = urllib.parse.urlsplit(self.path)
                query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                components = parsed.path.strip("/").split("/")
                with lock:
                    state["active"] += 1
                    state["requests"] += 1
                    mutation = self.command in ("PUT", "POST", "DELETE", "PATCH")
                    state["writes"] += int(mutation)
                    state["data_get"] += int(self.command == "GET" and len(components) >= 2
                                             and not {"acl", "tagging", "uploads", "uploadId"}.intersection(query))
                    mode = state["mode"]
                try:
                    seen.write_text("seen")
                    if mutation:
                        self.send_error(403, "initialization may not mutate the owned bucket")
                    elif mode == "cancel":
                        self.connection.settimeout(0.1)
                        while not stopped.is_set():
                            try:
                                if not self.connection.recv(1):
                                    break
                            except socket.timeout:
                                pass
                            except OSError:
                                break
                    else:
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
            for mode in ("normal", "cancel"):
                seen.unlink(missing_ok=True)
                with lock:
                    state["mode"] = mode
                    state["requests"] = 0
                environment = dict(os.environ, S3_CONNECT_TIMEOUT="5", S3_OPERATION_TIMEOUT="5", S3_READ_TIMEOUT="5")
                environment.update(
                    LAB_WORKER_SOURCE_ROOT=str(source), LAB_WORKER_FAULT=mode,
                    LAB_WORKER_REQUEST_SEEN=str(seen),
                    LAB_WORKER_TARGET_URL=f"s3://worker-test:worker-test-isolated-only@{bucket}.127.0.0.1:{server.server_port}/target",
                )
                subprocess.run(["timeout", "20", arguments.binary, TEST, "--ignored", "--exact", "--nocapture"],
                               env=environment, check=True)
                deadline = time.monotonic() + 2
                while state["active"] and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert state["active"] == 0, "process exit must close the proxy connection"
                assert state["requests"] > 0, "the real S3 initialization request must begin"
                assert state["writes"] == state["data_get"] == 0, state
                after = payload.stat()
                assert payload.read_bytes() == b"retained source"
                assert (after.st_mode, after.st_size, after.st_mtime_ns, after.st_ctime_ns) == (
                    attributes.st_mode, attributes.st_size, attributes.st_mtime_ns, attributes.st_ctime_ns)
                versions = client.list_object_versions(Bucket=bucket)
                assert len(versions.get("Versions", [])) == 1 and not versions.get("DeleteMarkers")
                assert versions["Versions"][0]["VersionId"] == original["VersionId"]
                assert client.get_object(Bucket=bucket, Key="sentinel")["Body"].read() == b"unchanged target"
                assert not client.list_multipart_uploads(Bucket=bucket).get("Uploads")
                print(f"independent initialization fault={mode} requests={state['requests']} source=unchanged target=unchanged writes=0 data_GET=0 connections=closed", flush=True)
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
