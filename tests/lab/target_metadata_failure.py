"""Deny target tagging on an owned real S3 service and inspect data and callbacks."""
import http.client
import http.server
import json
import subprocess
import threading
import urllib.parse


def run(root, environment, client, buckets):
    counts = {"tags": 0, "data": 0, "verify": 0}
    guard = threading.Lock()

    class Target(http.server.BaseHTTPRequestHandler):
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
            tagging = "tagging" in arguments
            if self.command == "PUT" and tagging:
                with guard:
                    counts["tags"] += 1
                payload = b"<Error><Code>AccessDenied</Code><Message>injected target tag permission denial</Message></Error>"
                self.send_response(403)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if parsed.path.endswith("/payload"):
                with guard:
                    if self.command == "PUT" and not tagging:
                        counts["data"] += 1
                    if self.command == "GET" and not tagging:
                        counts["verify"] += 1
            connection = http.client.HTTPConnection("127.0.0.1", 9000, timeout=5)
            try:
                connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                response = connection.getresponse()
                payload = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                        self.send_header(key, value)
                self.send_header("Content-Length", response.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            finally:
                connection.close()

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

    reports = []

    class Callback(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            reports.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Target)
    callback = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Callback)
    for server in (target, callback):
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
    environment["LAB_TARGET_METADATA_URL"] = f"s3://migration-test:migration-test-isolated-only@{buckets['history']}.127.0.0.1:{target.server_port}/target-metadata-failure"
    environment["LAB_TARGET_METADATA_CALLBACK"] = f"http://127.0.0.1:{callback.server_port}/progress"
    try:
        subprocess.run([
            "cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
            "storage_tests::s3_versions::target_metadata::target_metadata_failure_keeps_every_data_version_and_reports_one_failed_entry",
            "--", "--ignored", "--exact", "--nocapture",
        ], cwd=root, env=environment, check=True)
        for suffix in ("whole-false", "whole-true", "segments-1", "segments-7"):
            key = "target-metadata-failure/" + suffix + "/payload"
            listed = client.list_object_versions(Bucket=buckets["history"], Prefix=key)
            versions = listed.get("Versions", [])
            assert len(versions) == 20 and not listed.get("DeleteMarkers"), listed
            for version in versions:
                assert client.get_object(Bucket=buckets["history"], Key=key, VersionId=version["VersionId"])["Body"].read() == b"payload"
        source = client.list_object_versions(Bucket=buckets["source"], Prefix="payload")
        assert len(source.get("Versions", [])) == 20 and not source.get("DeleteMarkers"), source
        summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
        assert len(summaries) == 2 and all(report["detail"]["error_count"] == 1 and report["detail"]["transferred_bytes"] == 140
                                          and report["task_error_count"] == 0 and report["diagnostics_complete"] for report in summaries), reports
        errors = [event for report in reports for event in (report.get("diagnostics") or {}).get("events", [])]
        assert len(errors) == 40 and all("phase=target_metadata_apply" in event["message"] and "AccessDenied" in event["message"] for event in errors), errors
        assert counts == {"tags": 80, "data": 80, "verify": 20}, counts
        print("independent target-metadata inspection whole=quick,full segments=1,7 failed_entries_each=1 retained_versions_each=20 source_versions=20 data_PUT=80 tag_refusals=80 full_verify_GET=20 callback_details=40 HTTP_204=accepted", flush=True)
    finally:
        for server in (callback, target):
            server.shutdown()
            server.server_close()
