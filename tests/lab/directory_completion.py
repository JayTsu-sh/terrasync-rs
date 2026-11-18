"""仅在独占目录注入权限故障，独立核验每页 callback。"""

import http.server
import json
from pathlib import Path
import subprocess
import tempfile
import threading


def run(repository, environment):
    reports = []
    guard = threading.Lock()

    class Callback(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            assert self.path == "/callback"
            body = self.rfile.read(int(self.headers["Content-Length"]))
            with guard:
                reports.append(json.loads(body))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    callback = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Callback)
    callback.daemon_threads = True
    threading.Thread(target=callback.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="ts-directory-finalize-") as directory:
            root = Path(directory)
            root.chmod(0o777)
            environment = dict(environment, LAB_DIRECTORY_ROOT=str(root),
                               LAB_DIRECTORY_CALLBACK=f"http://127.0.0.1:{callback.server_port}/callback")
            subprocess.run(["cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                            "copy_execution::directory_completion_tests::real_directory_metadata_failures_are_all_reported_and_recoverable",
                            "--", "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                           cwd=repository, env=environment, check=True, timeout=360)
            summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
            assert len(summaries) == 10, summaries
            failed = [report for report in summaries if report["migration"]["phase"] == "failed"]
            assert len(failed) == 4, failed
            for report in failed:
                assert report["detail"]["error_count"] == 70 and report["task_error_count"] == 0, report
                assert report["diagnostics_complete"], report
                attempt = report["callback_identity"]["attempt_id"]
                pages = [payload["diagnostics"] for payload in reports
                         if payload["callback_identity"]["attempt_id"] == attempt and payload.get("diagnostics")]
                all_events = {event["event_id"]: event for page in pages for event in page["events"]}
                events = {identity: event for identity, event in all_events.items() if event["code"] == "Copy"}
                retained = [event for event in all_events.values() if event["code"] != "Copy"]
                assert len(events) == 70 and len(pages) >= 2, (report, pages)
                assert all(event["code"] == "target_preserved" and event.get("retention") for event in retained), retained
                assert {event["path"] for event in events.values()} == {f"d{index:03}" for index in range(70)}, events
                assert all(event["code"] == "Copy" and "directory_metadata_finalize" in event["message"]
                           and "backend=Local" in event["message"] for event in events.values()), events
                assert all(page["omitted_count"] == 0 for page in pages), pages
            assert all(report["diagnostics_complete"] for report in summaries), summaries
            for name in ("single_full", "single_incremental", "cluster_full", "cluster_incremental"):
                case = root / name
                assert list((case / "source").iterdir()) == [], name
                for index in range(71):
                    assert (case / "target" / f"d{index:03}" / "payload").read_bytes() == b"directory completion payload"
                    assert int((case / "target" / f"d{index:03}").stat().st_mtime) == 1_700_000_000
            print("independent directory completion deployments=2 full_failures=2 incremental_failures=2 "
                  "bootstrap=2 recovery=4 failed_entry_events=280 no_omissions=true counts_survive_204=true "
                  "source_preserved_until_ready=true target_data_preserved=true final_reports_saved=true", flush=True)
    finally:
        callback.shutdown()
        callback.server_close()
