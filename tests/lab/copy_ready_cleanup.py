"""Hold only owned base fact writes; inspect source deletion before allowing a receipt."""

import http.client
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import urllib.parse


BODY = b"ready data"


def run(repository, environment):
    upstream = urllib.parse.urlsplit(environment["LAB_COPY_GENERATION_DSN"])
    reports = []
    guard = threading.Lock()
    cases = {}
    active = [None]
    with tempfile.TemporaryDirectory(prefix="ts-ready-cleanup-") as directory:
        root = Path(directory)
        root.chmod(0o777)

        class Relay(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                parts = self.path.strip("/").split("/")
                if len(parts) != 3 or parts[0] not in ("arm", "finish"):
                    self.send_error(404)
                    return
                _, deployment, name = parts
                key = (deployment, name)
                with guard:
                    if parts[0] == "arm":
                        assert key not in cases
                        cases[key] = {"released": False, "observed": None, "error": None, "inserts": 0,
                                      "tokens": [], "lost": False, "fault_started": None,
                                      "first_retry_ms": None,
                                      "stop_stalls": threading.Event()}
                        active[0] = key
                    else:
                        state = cases[key]
                        assert state["error"] is None, state
                        if name not in ("data", "metadata"):
                            assert state["observed"] is not None and state["released"], state
                        if state["fault_started"] is not None:
                            elapsed_ms = round((time.monotonic() - state["fault_started"]) * 1000)
                            assert elapsed_ms < 8000, (key, elapsed_ms)
                            print(f"incremental fact fault deployment={deployment} case={name} "
                                  f"fault_to_first_retry_ms={state['first_retry_ms']} fault_to_final_ms={elapsed_ms}", flush=True)
                        state["stop_stalls"].set()
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def body(self):
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

            def inspect_before_receipt(self, key):
                deployment, name = key
                case = root / f"{deployment}-{name}"
                names = ["tree/a", "b"] if name == "directory" else (["a", "b", "c", "d"] if name == "fallback" else ["a", "b"])
                expected_missing = 0 if name == "directory" else 2
                deadline = time.monotonic() + 3
                if name == "directory":
                    time.sleep(0.25)
                while True:
                    missing = sum(not (case / "source" / path).exists() for path in names)
                    if missing == expected_missing:
                        break
                    if time.monotonic() >= deadline:
                        ready = sum((case / "target" / path).is_file()
                                    and (case / "target" / path).read_bytes() == BODY for path in names)
                        raise AssertionError((key, "source missing before receipt", missing, expected_missing, "targets_ready", ready))
                    time.sleep(0.005)
                for path in names:
                    assert (case / "target" / path).read_bytes() == BODY, (key, path)
                if name.startswith("incremental_"):
                    assert (case / "source/previous").read_bytes() == b"unchanged"
                    assert (case / "target/previous").read_bytes() == b"unchanged"
                if name == "directory":
                    assert (case / "source/tree").is_dir()
                observed = {"source_missing": missing, "total": len(names), "forwarded_before_proof": False}
                with guard:
                    cases[key]["observed"] = observed
                print(f"independent before-receipt deployment={deployment} case={name} source_missing={missing}/{len(names)} targets_ready=true base_fact_forwarded=false", flush=True)

            def do_POST(self):
                body = self.body()
                if self.path == "/callback":
                    reports.append(json.loads(body))
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                sql = urllib.parse.unquote_plus(self.path).upper()
                with guard:
                    key = active[0]
                    match = key is not None and f"BASE_READY_{key[0]}_{key[1]}".upper() in sql
                    held = match and "INSERT INTO" in sql and "FORMAT ROWBINARY" in sql
                    state = cases.get(key)
                    hold = held and key[1] not in ("data", "metadata") and not state["released"]
                    if held:
                        state["inserts"] += 1
                        parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                        state["tokens"].append(parameters.get("insert_deduplication_token", []))
                if hold:
                    try:
                        self.inspect_before_receipt(key)
                    except BaseException as error:
                        with guard:
                            state["error"] = repr(error)
                        print(f"independent before-receipt assertion failed {error!r}", flush=True)
                        self.send_error(500, "owned source readiness proof failed")
                        return
                    finally:
                        with guard:
                            state["released"] = True
                    if key[1] in ("reject", "incremental_reject"):
                        self.send_error(403, "owned ready fact rejection")
                        return
                if held and key[1] in ("incremental_disconnect", "incremental_stall"):
                    with guard:
                        now = time.monotonic()
                        if state["fault_started"] is None:
                            state["fault_started"] = now
                        elif state["first_retry_ms"] is None:
                            state["first_retry_ms"] = round((now - state["fault_started"]) * 1000)
                    if key[1] == "incremental_stall":
                        state["stop_stalls"].wait(10)
                    self.close_connection = True
                    return
                connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=15)
                try:
                    headers = {name: value for name, value in self.headers.items()
                               if name.lower() not in ("host", "connection", "content-length", "transfer-encoding")}
                    headers["Content-Length"] = str(len(body))
                    connection.request("POST", self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    payload = response.read()
                    if hold and key[1] == "incremental_lost":
                        assert response.status == 200, (response.status, payload[:512])
                        with guard:
                            state["lost"] = True
                        self.close_connection = True
                        return
                    self.send_response(response.status)
                    for name, value in response.getheaders():
                        if name.lower() not in ("connection", "content-length", "transfer-encoding"):
                            self.send_header(name, value)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    connection.close()

        relay = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay)
        relay.daemon_threads = True
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        environment = dict(environment)
        address = f"127.0.0.1:{relay.server_port}"
        environment.update(LAB_READY_ROOT=str(root), LAB_READY_CONTROL=address,
                           LAB_READY_CALLBACK=f"http://{address}/callback", LAB_COPY_GENERATION_DSN=f"http://{address}")
        try:
            subprocess.run(["cargo", "test", "--offline", "--locked", "-p", "app", "--lib",
                            "copy_execution::ready_cleanup_tests::real_full_move_ready_window_does_not_wait_for_fact_receipts",
                            "--", "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                           cwd=repository, env=environment, check=True, timeout=240)
            assert len(cases) == 22 and all(state["error"] is None for state in cases.values()), cases
            for (deployment, name), state in cases.items():
                case = root / f"{deployment}-{name}"
                paths = ["tree/a", "b"] if name == "directory" else (["a", "b", "c", "d"] if name == "fallback" else ["a", "b"])
                for path in paths:
                    source, target = case / "source" / path, case / "target" / path
                    if name in ("data", "metadata"):
                        assert source.read_bytes() == BODY
                    else:
                        assert not source.exists()
                    if name == "data":
                        assert not target.exists()
                    else:
                        assert target.read_bytes() == BODY
            summaries = [report for report in reports if report["is_final"] and not report.get("diagnostics")]
            assert len(summaries) == 32 and all(report["diagnostics_complete"] for report in summaries), summaries
            full = [report for report in summaries if "_incremental_" not in report["job_id"]]
            assert len(full) == 12, full
            failed = [report for report in full if report["migration"]["phase"] == "failed"]
            assert len(failed) == 6, failed
            events = {event["event_id"]: event for report in reports
                      for event in (report.get("diagnostics") or {}).get("events", [])}
            rejection = [event for event in events.values() if "owned ready fact rejection" in event["message"]]
            assert len(rejection) >= 2, events
            for name in ("data", "metadata"):
                matches = [report for report in summaries if report["job_id"].endswith("_" + name)]
                assert len(matches) == 2 and all(report["detail"]["error_count"] == 2 for report in matches), matches
            for (deployment, name), state in cases.items():
                if not name.startswith("incremental_"):
                    continue
                final, = [report for report in summaries
                          if report["job_id"] == f"ready_{deployment}_{name}" and report["migration"]["generation"] == 2]
                failed = name in ("incremental_reject", "incremental_disconnect", "incremental_stall")
                assert final["migration"]["phase"] == ("failed" if failed else "completed"), final
                assert final["task_error_count"] == int(failed), final
                assert final["detail"]["transferred_bytes"] == 2 * len(BODY), final
                assert final["detail"]["new_count"] == final["detail"]["changed_count"] == 1, final
                if name == "incremental_lost":
                    assert state["lost"] and state["inserts"] == 2, state
                    first, retry = state["tokens"]
                    assert len(first) == 1 and first == retry, state
            print("independent ready-window inspection deployments=2 full_runs=12 incremental_runs=10 source_deleted_before_unforwarded_fact=true incremental_unchanged_deferred=true lost_receipt_replayed_once=2 HTTP_204_and_final_reports=true", flush=True)
        finally:
            relay.shutdown()
            relay.server_close()
