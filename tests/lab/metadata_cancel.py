"""阻塞独占 S3 的 tags 写入或回读，核验真实取消、原副本及历史绑定。"""
import http.client
import http.server
import json
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid


def run(repository, environment):
    import boto3
    from botocore.config import Config
    for _ in range(100):
        try:
            with urllib.request.urlopen("http://127.0.0.1:19000/health/ready", timeout=1) as reply:
                if reply.status == 200:
                    break
        except OSError:
            pass
        time.sleep(0.2)
    else:
        raise RuntimeError("owned S3 readiness timed out")
    client = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", aws_access_key_id="migration-test",
                          aws_secret_access_key="migration-test-isolated-only", region_name="us-east-1",
                          config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=2, read_timeout=3))
    buckets = {kind: f"cancel-{kind}-{uuid.uuid4().hex[:12]}" for kind in ("source", "latest", "history")}
    for kind, bucket in buckets.items():
        client.create_bucket(Bucket=bucket)
        if kind == "history":
            client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    guard = threading.Lock()
    cases = {}
    for workflow in ("ordinary", "selected", "historical"):
        for applying in (True, False):
            name = workflow + ("-apply" if applying else "-verify")
            key = name + "/payload"
            client.put_object(Bucket=buckets["source"], Key=key, Body=b"published payload", Tagging="raw=source")
            bucket = buckets["history" if workflow == "historical" else "latest"]
            original = client.put_object(Bucket=bucket, Key=key, Body=b"published payload", Tagging="raw=old")
            if workflow == "historical":
                head = client.put_object(Bucket=bucket, Key=key, Body=b"head stays intact", Tagging="raw=head")
            else:
                head = original
            cases[name] = dict(name=name, workflow=workflow, applying=applying, bucket=bucket,
                               target_version=original.get("VersionId") if workflow == "historical" else None,
                               head=head.get("VersionId"), armed=False, observed=False, release=threading.Event(),
                               data_reads=0, data_writes=0, deletes=0, held=0, selected=[])

    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def body(self):
            if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                return self.rfile.read(int(self.headers.get("Content-Length", "0")))
            chunks = []
            while True:
                line = self.rfile.readline()
                chunks.append(line)
                size = int(line.split(b";", 1)[0], 16)
                if not size:
                    while True:
                        trailer = self.rfile.readline()
                        chunks.append(trailer)
                        if trailer in (b"\r\n", b""):
                            return b"".join(chunks)
                chunks.append(self.rfile.read(size + 2))

        def handle_request(self):
            body = self.body()
            parsed = urllib.parse.urlsplit(self.path)
            args = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            name = next((name for name in cases if parsed.path.endswith("/" + name + "/payload")), None)
            state = cases.get(name)
            hold = False
            if state is not None:
                with guard:
                    tagging = "tagging" in args
                    if not tagging and self.command == "GET":
                        state["data_reads"] += 1
                    if not tagging and self.command == "PUT":
                        state["data_writes"] += 1
                    if self.command == "DELETE":
                        state["deletes"] += 1
                    if tagging:
                        state["selected"].append(args.get("versionId", [None])[0])
                    hold = state["armed"] and tagging and self.command == ("PUT" if state["applying"] else "GET")
                    if hold:
                        state["held"] += 1
                        state["observed"] = True
            if hold:
                assert state["release"].wait(10), name
                return
            connection = http.client.HTTPConnection("127.0.0.1", 19000, timeout=10)
            try:
                connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                response = connection.getresponse()
                payload = response.read()
                self.send_response(response.status)
                for header, value in response.getheaders():
                    if header.lower() not in ("connection", "content-length", "transfer-encoding"):
                        self.send_header(header, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(payload)
            finally:
                connection.close()

        do_GET = do_HEAD = do_PUT = do_DELETE = handle_request

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            action, name = self.path.strip("/").split("/", 1)
            state = cases[name]
            with guard:
                if action == "arm":
                    state["armed"] = True
                elif action == "normal":
                    state["armed"] = False
                    state["release"].set()
                else:
                    assert action == "observed"
                ready = action != "observed" or state["observed"]
            self.send_response(204 if ready else 503)
            self.send_header("Content-Length", "0")
            self.end_headers()

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Control)
    for server in (proxy, control):
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
    definitions = [dict(name=name, workflow=state["workflow"], applying=state["applying"],
                        target_version=state["target_version"],
                        source=f"s3://migration-test:migration-test-isolated-only@{buckets['source']}.127.0.0.1:19000/{name}",
                        target=f"s3://migration-test:migration-test-isolated-only@{state['bucket']}.127.0.0.1:{proxy.server_port}/{name}")
                   for name, state in cases.items()]
    environment = dict(environment, LAB_METADATA_CANCEL_CASES=json.dumps(definitions),
                       LAB_METADATA_CANCEL_CONTROL=f"127.0.0.1:{control.server_port}")
    try:
        subprocess.run(["cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                        "file_copy::verification_cancel_tests::real_metadata_only_cancellation_preserves_data_and_target_versions",
                        "--", "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                       cwd=repository, env=environment, check=True, timeout=120)
        for name, state in cases.items():
            assert state["held"] == 1 and state["observed"], (name, state)
            assert state["data_reads"] == state["data_writes"] == state["deletes"] == 0, (name, state)
            assert all(version == state["target_version"] for version in state["selected"]), (name, state)
            key = name + "/payload"
            options = {"VersionId": state["target_version"]} if state["target_version"] else {}
            assert client.get_object(Bucket=buckets["source"], Key=key)["Body"].read() == b"published payload"
            assert client.get_object(Bucket=state["bucket"], Key=key, **options)["Body"].read() == b"published payload"
            assert client.get_object_tagging(Bucket=state["bucket"], Key=key, **options)["TagSet"] == [{"Key": "raw", "Value": "source"}]
            if options:
                records = client.list_object_versions(Bucket=state["bucket"], Prefix=key)
                assert len(records.get("Versions", [])) == 2 and not records.get("DeleteMarkers"), records
                assert client.head_object(Bucket=state["bucket"], Key=key)["VersionId"] == state["head"]
                assert client.get_object_tagging(Bucket=state["bucket"], Key=key)["TagSet"] == [{"Key": "raw", "Value": "head"}]
            print(f"independent metadata cancellation case={name} held=1 data_reads=0 data_writes=0 deletes=0 target_version_intact=true recovered_tags=true", flush=True)
    finally:
        for state in cases.values():
            state["release"].set()
        for server in (proxy, control):
            server.shutdown()
            server.server_close()
