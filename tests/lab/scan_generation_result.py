"""Verify persisted scan generation results through the CLI and real ClickHouse."""

from datetime import datetime
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import urllib.parse
import uuid

from copy_takeover import generations, request_body, rows, sql


class Fault:
    def __init__(self, mode=None):
        self.mode = mode
        self.writes = 0
        self.published = 0
        self.probes = 0
        self.cleanups = 0
        self.disconnected_at = None
        self.first_probe_at = None
        self.scan_exit_at = None
        self.stopped_at = None
        self.cancel_ready = threading.Event()
        self.held = None
        self.replayed = False
        self.database = None


class Relay(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def disconnect(self):
        self.close_connection = True
        self.connection.shutdown(socket.SHUT_RDWR)

    def hold_until_cancel(self, fault):
        fault.cancel_ready.set()
        time.sleep(1.5)
        self.disconnect()

    def do_POST(self):
        body = request_body(self)
        if self.path == "/callback":
            self.server.callbacks.append(json.loads(body))
            self.send_response(204)
            self.end_headers()
            return
        query = urllib.parse.unquote_plus(self.path).upper() + body[:200].decode(errors="ignore").upper()
        fault = self.server.fault
        write = "INSERT INTO STATE (JOB_ID,GENERATION,EPOCH,VER)" in query or (
            "INSERT INTO STATE_SCAN_RESULT " in query and "SCAN_EPOCH" in query
        )
        if "LOG_COMMENT=TERRASYNC-SCAN-" in query and "SELECT 1" in query:
            if fault.mode == "preflight":
                self.send_error(403, "owned fixture rejects the initial scanner ping")
                return
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in ("host", "content-length", "connection", "transfer-encoding")}
        headers["Content-Length"] = str(len(body))
        rollback = " DROP PARTITION " in query and any(table in query for table in ("BASE_LOCAL", "BASE_SCAN_RESULT"))
        if fault.held and not fault.replayed and (
            fault.mode == "rollback" and rollback or fault.mode == "resume-commit" and write
        ):
            if rollback:
                assert manifest(fault.database, self.server.mode)["phase"] == 4
            check_delayed_replay(fault.held)
            fault.replayed = True
        if write:
            fault.writes += 1
            if fault.mode == "missing":
                fault.held = (self.path, body, headers)
                fault.disconnected_at = time.monotonic()
                self.disconnect()
                return
        readback = "SELECT SCAN_STATE,SCAN_EPOCH FROM STATE_SCAN_RESULT" in query or (
            "SELECT JOB_ID,GENERATION,EPOCH,VER FROM " in query and ".STATE FINAL" in query
        )
        if readback:
            if fault.mode == "resume-refused":
                fault.probes += 1
                self.send_error(403, "owned fixture rejects resume commit readback")
                return
            if fault.disconnected_at is not None:
                fault.probes += 1
                if fault.first_probe_at is None:
                    fault.first_probe_at = time.monotonic()
                if fault.mode == "pending":
                    self.disconnect()
                    return
                if fault.mode == "pending-timeout":
                    time.sleep(1.5)
                    self.disconnect()
                    return
        if fault.published and "DROP TABLE IF EXISTS " in query and "SCAN_CUT_" in query:
            if fault.mode == "cleanup":
                fault.cleanups += 1
                self.send_error(403, "owned fixture denies post-commit snapshot cleanup")
                return
            if fault.mode == "cancel-cleanup":
                self.hold_until_cancel(fault)
                return

        upstream = http.client.HTTPConnection("127.0.0.1", 8123, timeout=30)
        try:
            upstream.request("POST", self.path, body=body, headers=headers)
            response = upstream.getresponse()
            payload = response.read()
            if fault.mode == "cancel-before" and response.status == 200 and not write and "INSERT INTO" in query and "SCAN_RESUME" in query:
                if manifest(fault.database, self.server.mode)["phase"] == 1:
                    self.hold_until_cancel(fault)
                    return
            if write and response.status == 200:
                fault.published += 1
                if fault.mode == "cancel-commit":
                    self.hold_until_cancel(fault)
                    return
                if fault.mode in ("verified", "pending", "pending-timeout", "gateway"):
                    fault.disconnected_at = time.monotonic()
                    if fault.mode == "gateway":
                        self.send_error(504, "owned gateway lost the upstream commit response")
                    else:
                        self.disconnect()
                    return
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in ("content-length", "transfer-encoding", "connection"):
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            upstream.close()


def archived_reports(database, job):
    if sql(f"EXISTS TABLE {database}.notifications_{job}").strip() == "0":
        return []
    return [
        json.loads(json.loads(row["payload_json"])["payload_json"])
        for row in rows(
            f"SELECT payload_json FROM {database}.notifications_{job} FINAL WHERE kind=2"
        )
    ]


def check_report(report, outcome, *, failed):
    assert report["is_final"] and report.get("generation_commit_result") == outcome, report
    assert "generation_committed" not in report, report
    if failed is not None:
        assert (report["detail"]["error_count"] > 0 or report["task_error_count"] > 0) == failed, report


def run_scan(binary, work, config, source, database, name, relay, *, resume=False,
             fault=None, outcome="committed", failed=False, kind="Incremental", callback_driver=None, replay=None):
    job = "scan_result"
    command = [str(binary), "-c", str(config), "scan", "--id", job]
    if resume:
        command.append("--resume")
    environment = {key: value for key, value in os.environ.items() if not key.startswith("TERRASYNC_LAB_SCAN_")}
    if fault in ("before", "rollback"):
        environment["TERRASYNC_LAB_SCAN_FAIL_AFTER_FINALIZING"] = "1"
    # 将每个子进程的内部诊断日志放到本轮证据目录，与同键的其他实验隔离。
    log_root = work / name
    environment["CARGO_MANIFEST_DIR"] = str(log_root / "fixture")
    scanner_log = log_root / "logs" / job / "app.log"
    seen = {report["callback_identity"]["attempt_id"] for report in archived_reports(database, job)}
    relay.fault = Fault(fault)
    relay.fault.held = replay
    relay.fault.database = database
    relay.callbacks.clear()
    if callback_driver:
        environment.pop("LAB_SCAN_CALLBACK_RESUME", None)
        command = [str(callback_driver), "--ignored", "--exact", "scan_generation_callback_entrypoint", "--nocapture"]
        environment.update(
            LAB_SCAN_CALLBACK_CONFIG=str(config),
            LAB_SCAN_CALLBACK_SOURCE=str(source),
            LAB_SCAN_CALLBACK_JOB_DIR=str(work / "jobs" / job),
            LAB_SCAN_CALLBACK_URL=f"http://127.0.0.1:{relay.server_port}/callback",
        )
        if resume:
            environment["LAB_SCAN_CALLBACK_RESUME"] = "1"
    with (work / f"{name}.log").open("wb") as log:
        with subprocess.Popen([*command, str(source)], cwd=work, env=environment,
                              stdout=log, stderr=subprocess.STDOUT) as process:
            try:
                if fault in ("cancel-before", "cancel-commit", "cancel-cleanup"):
                    assert relay.fault.cancel_ready.wait(15), (name, log.name)
                    when = time.monotonic()
                    process.send_signal(signal.SIGINT)
                    result = process.wait(timeout=2)
                    relay.fault.stopped_at = time.monotonic() - when
                else:
                    result = process.wait(timeout=60)
                relay.fault.scan_exit_at = time.monotonic()
            except BaseException:
                process.kill()
                process.wait()
                raise
    assert (result != 0) == (failed is not False), (name, result, log.name)
    new = [report for report in archived_reports(database, job)
           if report["callback_identity"]["attempt_id"] not in seen]
    assert new, (name, log.name)
    for report in new:
        check_report(report, outcome, failed=failed)
        assert report["detail"]["kind"] == kind, (name, report)
    output = Path(log.name).read_text()
    if outcome is None:
        assert "Generation commit result:" not in output, name
    else:
        assert f"Generation commit result: {outcome}" in output, name
    if fault in ("cancel-before", "cancel-commit", "cancel-cleanup"):
        assert "ClickHouse request termination could not be confirmed" in output, (name, log.name)
    if callback_driver:
        received = [report for report in relay.callbacks if report["is_final"]]
        assert received, (name, log.name)
        for report in received:
            check_report(report, outcome, failed=failed)
            assert report in new, (name, report)
        (work / f"{name}-callbacks.json").write_text(json.dumps(received, ensure_ascii=False, indent=2) + "\n")
        assert all("generation_commit_result" not in report for report in relay.callbacks if not report["is_final"])
    report = max(new, key=lambda value: value["callback_identity"]["sequence"])
    (work / f"{name}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    result = relay.fault
    lower = None if result.first_probe_at is None else result.scan_exit_at - result.first_probe_at
    upper = None if result.disconnected_at is None else result.scan_exit_at - result.disconnected_at
    recovery_window = None
    if fault in ("pending", "pending-timeout", "missing"):
        assert 2.6 <= lower <= upper < 5.5, (name, lower, upper)
        if not callback_driver:
            log_lines = scanner_log.read_text().splitlines()
            started, = [line for line in log_lines if "ClickHouse connection recovery started" in line]
            expired, = [line for line in log_lines if "ClickHouse connection recovery window exhausted" in line]
            recovery_window = (datetime.fromisoformat(expired.split()[0]) - datetime.fromisoformat(started.split()[0])).total_seconds()
            assert 2.9 <= recovery_window <= 3.25, (name, recovery_window)
    print(f"{work.parent.name}/{work.name} {name} {outcome} failed={failed} writes={result.writes} "
          f"readbacks={result.probes} recovery_seconds=({lower},{upper}) internal_window={recovery_window} "
          f"cancel_seconds={result.stopped_at}", flush=True)
    return report


def manifest(database, mode):
    table = "scan_resume FINAL WHERE job_id='scan_result'" if mode == "cluster" else "scan_resume_scan_result FINAL"
    current, = rows(f"SELECT event_time,phase,baseline_snapshot FROM {database}.{table}")
    return current


def evidence(database, mode):
    if mode == "cluster":
        state = "state FINAL WHERE job_id='scan_result'"
        epoch = "epoch"
    else:
        state = "state_scan_result FINAL WHERE baseline_committed"
        epoch = "scan_epoch"
    current = manifest(database, mode)
    committed, = rows(f"SELECT {epoch} AS epoch FROM {database}.{state}")
    return int(current["event_time"]), int(committed["epoch"]), current["phase"]


def check_baseline_snapshot(database, mode):
    token = manifest(database, mode)["baseline_snapshot"]
    table, = rows(f"SELECT name FROM system.tables WHERE database='{database}' AND endsWith(name,'{token}_base')")
    predicate = "WHERE job_id='scan_result' AND is_deleted=0" if mode == "cluster" else ""
    entries = rows(f"SELECT relative_path,size FROM {database}.{table['name']} FINAL {predicate} ORDER BY relative_path")
    assert [(row["relative_path"], int(row["size"])) for row in entries] == [("file", 8)], entries


def check_delayed_replay(held):
    path, body, headers = held
    upstream = http.client.HTTPConnection("127.0.0.1", 8123, timeout=5)
    try:
        upstream.request("POST", path, body=body, headers=headers)
        response = upstream.getresponse()
        payload = response.read()
        assert response.status >= 400 and b"Scan manifest changed before commit" in payload, (response.status, payload)
    finally:
        upstream.close()


def check_handoff(binary, work, config, source, database, relay):
    target = work / "target"
    target.mkdir()
    relay.fault = Fault()
    with (work / "copy-handoff.log").open("wb") as log:
        subprocess.run([str(binary), "-c", str(config), "sync", "--id", "scan_result", str(source), str(target)],
                       cwd=work, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
    manifest = generations(database, "scan_result")[-1]
    assert manifest["phase"] == "completed" and manifest["plan"]["mode"] == "full", manifest
    assert {path.name: path.read_bytes() for path in target.iterdir()} == {"file": b"baseline", "added": b"new"}
    print(f"{work.name} scan-to-copy handoff completed", flush=True)


def run_mode(binary, work, mode, database, relay, callback_driver, *, full_only=False):
    relay.mode = mode
    source = work / "source"
    source.mkdir(parents=True)
    (source / "file").write_bytes(b"baseline")
    config = work / "config.toml"
    config.write_text(f"""
[log]
level = "info"
[scan]
concurrency = 2
[database]
enabled = true
batch_size = 10
[database.clickhouse]
mode = "{mode}"
{('cluster = "notification_cluster"' if mode == "cluster" else '')}
dsn = "http://127.0.0.1:{relay.server_port}"
database = "{database}"
username = "default"
dial_timeout = 1
read_timeout = 1
""")

    def scan(name, **kwargs):
        return run_scan(binary, work, config, source, database, name, relay, callback_driver=callback_driver, **kwargs)

    if callback_driver or full_only:
        full_attempt = scan("full-before", resume=True, fault="before", outcome="not_committed",
                            failed=True, kind="Full")["callback_identity"]
        full_retry = scan("full-retry", resume=True, kind="Full")["callback_identity"]
        assert full_retry["round_id"] == full_attempt["round_id"]
    else:
        scan("full", kind="Full")
    full, baseline, phase = evidence(database, mode)
    assert full == baseline and phase == 2
    scan("preflight", fault="preflight", outcome="not_committed", failed=True, kind="Full")
    assert relay.fault.writes == 0 and evidence(database, mode) == (full, full, 2)
    if full_only:
        return

    (source / "added").write_bytes(b"new")
    pre = scan("before", resume=True, fault="before", outcome="not_committed", failed=True)["callback_identity"]
    active, baseline, phase = evidence(database, mode)
    assert baseline == full and active > full and phase == 1
    assert relay.fault.writes == 0
    check_baseline_snapshot(database, mode)
    retry = scan("before-retry", resume=True)["callback_identity"]
    assert pre["round_id"] == retry["round_id"] and pre["attempt_id"] != retry["attempt_id"]
    assert evidence(database, mode) == (active, active, 2)

    scan("cleanup", fault="cleanup", failed=True)
    committed, baseline, phase = evidence(database, mode)
    assert committed == baseline and phase == 2 and committed > active
    assert relay.fault.writes == relay.fault.published == 1 and relay.fault.cleanups > 0

    scan("verified", fault="verified")
    verified, baseline, phase = evidence(database, mode)
    assert verified == baseline and phase == 2 and verified > committed
    assert relay.fault.writes == relay.fault.published == 1 and relay.fault.probes > 0
    scan("gateway", fault="gateway")
    gateway, baseline, phase = evidence(database, mode)
    assert gateway == baseline and phase == 2 and gateway > verified
    assert relay.fault.writes == relay.fault.published == 1 and relay.fault.probes > 0

    fault = "pending-timeout" if callback_driver else "pending"
    pending = scan("pending", resume=True, fault=fault, outcome="pending_confirmation", failed=True)["callback_identity"]
    pending_epoch, baseline, phase = evidence(database, mode)
    assert pending_epoch == baseline and phase == 1 and pending_epoch > gateway
    assert relay.fault.writes == relay.fault.published == 1 and relay.fault.probes > 0

    unbound = scan("pending-restart-preflight", resume=True, fault="preflight", kind="Full",
                   outcome=None, failed=True)
    assert "generation_commit_result" not in unbound and evidence(database, mode) == (pending_epoch, pending_epoch, 1)
    refused = scan("pending-restart-refused", resume=True, fault="resume-refused",
                   outcome="pending_confirmation", failed=True)
    assert refused["callback_identity"]["round_id"] == pending["round_id"]
    assert refused["detail"]["scanned_files"] == 2 and relay.fault.writes == 0 and relay.fault.probes == 1
    recovered = scan("pending-retry", resume=True)["callback_identity"]
    assert pending["round_id"] == recovered["round_id"] and pending["attempt_id"] != recovered["attempt_id"]
    assert evidence(database, mode) == (pending_epoch, pending_epoch, 2)
    assert relay.fault.writes == 0

    missing = scan("missing", resume=True, fault="missing", outcome="pending_confirmation", failed=True)["callback_identity"]
    active, baseline, phase = evidence(database, mode)
    assert active > pending_epoch and baseline == pending_epoch and phase == 1
    assert relay.fault.writes == 1 and relay.fault.published == 0 and relay.fault.probes > 0
    held = relay.fault.held
    retry = scan("missing-retry", resume=True, fault="resume-commit", replay=held)["callback_identity"]
    assert missing["round_id"] == retry["round_id"] and missing["attempt_id"] != retry["attempt_id"]
    assert evidence(database, mode) == (active, active, 2)
    assert relay.fault.writes == relay.fault.published == 1 and relay.fault.replayed
    check_delayed_replay(held)
    assert evidence(database, mode) == (active, active, 2)

    if callback_driver is None:
        scan("held-before-rollback", fault="missing", outcome="pending_confirmation", failed=True)
        skipped, baseline, phase = evidence(database, mode)
        assert skipped > active and baseline == active and phase == 1
        held = relay.fault.held
        scan("rollback", fault="rollback", outcome="not_committed", failed=True, replay=held)
        assert relay.fault.replayed and relay.fault.writes == 0
        restarted, baseline, phase = evidence(database, mode)
        assert restarted > skipped and baseline == active and phase == 1

        scan("rollback-retry")
        restored, baseline, phase = evidence(database, mode)
        assert restored == baseline and restored > restarted and phase == 2
        scan("cancel-before", resume=True, fault="cancel-before", outcome="not_committed", failed=True)
        assert relay.fault.writes == 0 and relay.fault.stopped_at < 2
        precommit, baseline, phase = evidence(database, mode)
        assert precommit > restored and baseline == restored and phase == 1
        scan("cancel-commit", resume=True, fault="cancel-commit", outcome="pending_confirmation", failed=True)
        assert relay.fault.published == 1 and relay.fault.stopped_at < 2
        cancelled, baseline, phase = evidence(database, mode)
        assert (cancelled, phase) == (baseline, 1)
        scan("cancel-commit-retry", resume=True)
        assert evidence(database, mode) == (cancelled, cancelled, 2) and relay.fault.writes == 0
        scan("cancel-cleanup", fault="cancel-cleanup", failed=True)
        assert relay.fault.published == 1 and relay.fault.stopped_at < 2
        committed, baseline, phase = evidence(database, mode)
        assert committed == baseline and phase == 2 and committed > cancelled
        check_handoff(binary, work, config, source, database, relay)


def check_stream(binary, callback_driver, logs, relay):
    work = logs / "streaming"
    source = work / "source"
    source.mkdir(parents=True)
    (source / "file").write_text("stream")
    config = work / "config.toml"
    config.write_text("[database]\nenabled = false\n")
    with (work / "cli.log").open("wb") as log:
        subprocess.run([str(binary), "-c", str(config), "scan", "--id", "scan_result", str(source)],
                       cwd=work, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=15)
    assert "Generation commit result" not in (work / "cli.log").read_text()
    relay.callbacks.clear()
    environment = dict(os.environ, LAB_SCAN_CALLBACK_CONFIG=str(config), LAB_SCAN_CALLBACK_SOURCE=str(source),
                       LAB_SCAN_CALLBACK_JOB_DIR=str(work / "jobs" / "scan_result"),
                       LAB_SCAN_CALLBACK_URL=f"http://127.0.0.1:{relay.server_port}/callback")
    environment.pop("LAB_SCAN_CALLBACK_RESUME", None)
    with (work / "callback.log").open("wb") as log:
        subprocess.run([str(callback_driver), "--ignored", "--exact", "scan_generation_callback_entrypoint"],
                       cwd=work, env=environment, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=15)
    final = [report for report in relay.callbacks if report["is_final"]]
    assert final
    for report in final:
        check_report(report, None, failed=False)
        assert "generation_commit_result" not in report, report
    (work / "callbacks.json").write_text(json.dumps(final, ensure_ascii=False, indent=2) + "\n")
    print("streaming CLI and HTTP report omit generation result", flush=True)


def run():
    binary = Path(os.environ["LAB_TERRASYNC_BIN"]).resolve(strict=True)
    callback_driver = Path(os.environ["LAB_SCAN_CALLBACK_DRIVER"]).resolve(strict=True)
    logs = Path(os.environ["LAB_SCAN_RESULT_LOG_DIR"]).resolve()
    logs.mkdir(parents=True, exist_ok=False)
    for path in (binary, callback_driver):
        with path.open("rb") as executable:
            print(f"{path.name} sha256={hashlib.file_digest(executable, 'sha256').hexdigest()}", flush=True)
    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay) as relay:
        relay.fault = Fault()
        relay.callbacks = []
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        try:
            for mode in ("single", "cluster"):
                cluster = " ON CLUSTER notification_cluster" if mode == "cluster" else ""
                for group, driver in (("cli", None), ("cli-resume", None), ("callback", callback_driver)):
                    database = f"ts_scan_result_{uuid.uuid4().hex}"
                    sql(f"CREATE DATABASE {database}{cluster}")
                    try:
                        run_mode(binary, logs / group / mode, mode, database, relay, driver,
                                 full_only=group == "cli-resume")
                    finally:
                        sql(f"DROP DATABASE {database}{cluster} SYNC")
            check_stream(binary, callback_driver, logs, relay)
        finally:
            relay.shutdown()
