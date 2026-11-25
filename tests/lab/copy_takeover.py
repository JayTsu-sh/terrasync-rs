"""Exercise a real CLI takeover and late ClickHouse writes on owned fixtures."""

import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import urllib.parse
import uuid


class WriteGate:
    def __init__(self, job, hold_state):
        self.job = job
        self.hold_state = hold_state
        self.received = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.status = None
        self.payload = None


def sql(query, port=8123):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=90)
    try:
        connection.request("POST", "/", body=query.encode())
        response = connection.getresponse()
        body = response.read().decode()
        assert response.status == 200, (response.status, query, body)
        return body
    finally:
        connection.close()


def rows(query, port=8123):
    return [json.loads(line) for line in sql(query + " FORMAT JSONEachRow", port).splitlines()]


def request_body(handler):
    if handler.headers.get("Transfer-Encoding", "").lower() != "chunked":
        return handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
    chunks = []
    while True:
        length = int(handler.rfile.readline().split(b";", 1)[0], 16)
        if not length:
            while handler.rfile.readline() not in (b"\r\n", b"", b"\n"):
                pass
            return b"".join(chunks)
        chunks.append(handler.rfile.read(length))
        assert handler.rfile.read(2) == b"\r\n"


def wait_for(predicate, label, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(label)


def archive(database, job, execution_id):
    result = rows(
        f"SELECT payload_json FROM {database}.notifications_{job} FINAL "
        f"WHERE kind=2 AND attempt_id='{execution_id}' ORDER BY sequence DESC LIMIT 1"
    )
    assert len(result) == 1, (job, execution_id, result)
    return json.loads(json.loads(result[0]["payload_json"])["payload_json"])


def generations(database, job):
    result = rows(f"SELECT descriptor FROM {database}.generations_{job} FINAL ORDER BY sequence")
    return [json.loads(row["descriptor"]) for row in result]


def base(database, job, local=False, port=8123):
    table = f"__catalogue_base_{job}" if local else f"base_{job}"
    return rows(
        f"SELECT relative_path,copy_sequence,copy_execution_id FROM {database}.{table} FINAL ORDER BY relative_path",
        port,
    )


def check_route(database, job, shards, selected, expected):
    if selected is None:
        assert [row["relative_path"] for row in base(database, job)] == expected
        return
    replica_counts = []
    for shard in range(shards):
        for replica in range(2):
            facts = base(database, job, local=True, port=8123 + 2 * shard + replica)
            assert [fact["relative_path"] for fact in facts] == (expected if shard == selected else []), facts
            replica_counts.append(len(facts))
    print(f"takeover route job={job} shard={selected + 1} replica_rows={replica_counts}", flush=True)


def check_completed(database, job, sequence, mode):
    current = generations(database, job)[-1]
    assert (current["sequence"], current["phase"], current["plan"]["mode"]) == (sequence, "completed", mode), current
    execution_id = current["request"]["execution_id"]
    report = archive(database, job, execution_id)
    assert report["is_final"] and report["task_error_count"] == 0, report
    # CLI 无 callback。计划内保留事件允许留在已归档的最终报告中。
    diagnostics = report.get("diagnostics") or {}
    assert all(event["code"] == "target_preserved" for event in diagnostics.get("events", [])), report
    assert report["migration"]["execution_id"] == execution_id, report
    return current, report["detail"]


def record_contents(directory):
    return {path.name: path.read_bytes() for path in directory.iterdir()}


def run_case(binary, logs, relay_port, set_gate, database, job, shards, selected, checkpoint, separate_directory,
             hold_state=False):
    cluster = selected is not None
    case_logs = logs / job
    case_logs.mkdir()
    with tempfile.TemporaryDirectory(prefix=f"ts-{job}-") as path:
        fixture = Path(path)
        source, target, work, alternate = [fixture / name for name in ("source", "target", "work", "alternate")]
        for directory in (source, target, work, alternate):
            directory.mkdir()
        (source / "changed").write_bytes(b"baseline value")
        (source / "unchanged").write_bytes(b"stable")

        for retain in (True, False):
            config = fixture / f"retain-{retain}.toml"
            config.write_text(f"""
[log]
level = "info"
[sync]
is_source_reserved = {str(retain).lower()}
enable_integrity_check = true
concurrency = 2
[scan]
concurrency = 2
[database]
enabled = true
batch_size = 1000
[database.clickhouse]
mode = "{'cluster' if cluster else 'single'}"
{('cluster = "notification_cluster"' if cluster else '')}
dsn = "http://127.0.0.1:{relay_port}"
database = "{database}"
username = "default"
dial_timeout = 5
read_timeout = 60
""")

        def launch(name, retain, directory=work, subcommand="sync"):
            command = [str(binary), "-c", str(fixture / f"retain-{retain}.toml"), subcommand, "--id", job]
            if checkpoint:
                command.append("--resume")
            operands = [str(source), str(target)] if subcommand == "sync" else [str(source)]
            with (case_logs / f"{name}.log").open("wb") as output:
                return subprocess.Popen(command + operands, cwd=directory,
                                        stdout=output, stderr=subprocess.STDOUT)

        def finish(process, name):
            try:
                return process.wait(timeout=90)
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError(f"{job} {name} timed out. See {case_logs}") from error

        assert finish(launch("baseline", True), "baseline") == 0, case_logs
        check_completed(database, job, 1, "full")
        check_route(database, job, shards, selected, ["changed", "unchanged"])

        changed, added, fresh = b"published by old execution", b"already moved", b"written after takeover"
        (source / "changed").write_bytes(changed)
        (source / "added").write_bytes(added)
        gate = WriteGate(job, hold_state)
        set_gate(gate)
        old = launch("old", False)
        try:
            assert gate.received.wait(30), f"no old database write intercepted. See {case_logs}"
            if not hold_state:
                assert b"\x07changed" in gate.payload and b"\x05added" in gate.payload, case_logs
            wait_for(lambda: {path.name for path in source.iterdir()} == {"unchanged"},
                     f"ready files were not cleaned before receipt. See {case_logs}")
            assert record_contents(target) == {"changed": changed, "added": added, "unchanged": b"stable"}
            previous = generations(database, job)[-1]
            assert (previous["sequence"], previous["phase"], previous["plan"]["mode"]) == (2, "prepared", "incremental")
            previous_id = previous["request"]["execution_id"]

            started = time.monotonic()
            contender_dir = alternate if separate_directory else work
            contender_code = finish(launch("contender", True, contender_dir), "contender")
            elapsed_ms = round((time.monotonic() - started) * 1000)
            contender_log = (case_logs / "contender.log").read_text()
            locked = contender_code != 0 and "已有执行" in contender_log
            latest = generations(database, job)[-1]
            assert old.poll() is None, f"old process ended before the contender. See {case_logs}"
            print(f"takeover live_owner job={job} different_job_dir={separate_directory} "
                  f"contender_status={contender_code} lock_rejected={locked} "
                  f"current_generation={latest['sequence']} elapsed_ms={elapsed_ms}", flush=True)
            if not locked or latest != previous:
                return ["live_owner_other_directory" if separate_directory else "live_owner_same_directory"]
            if separate_directory:
                code = finish(launch("scan_contender", True, alternate, "scan"), "scan contender")
                assert code != 0 and "已有执行" in (case_logs / "scan_contender.log").read_text(), case_logs
                assert generations(database, job)[-1] == previous and old.poll() is None, case_logs
                print(f"takeover scan_contender job={job} locked=true current_generation=2", flush=True)
            assert record_contents(source) == {"unchanged": b"stable"}
            assert record_contents(target) == {"changed": changed, "added": added, "unchanged": b"stable"}

            killed = time.monotonic()
            old.kill()
            assert old.wait(timeout=5) == -signal.SIGKILL
            (source / "changed").write_bytes(fresh)
            assert finish(launch("successor", True, contender_dir), "successor") == 0, case_logs
            successor, detail = check_completed(database, job, 3, "full")
            elapsed_ms = round((time.monotonic() - killed) * 1000)
            successor_id = successor["request"]["execution_id"]
            assert successor_id != previous_id and successor["plan"]["target_cleanup"] == "preserve", successor
            assert detail["transferred_bytes"] == len(fresh) + len(b"stable"), detail
            assert record_contents(target) == {"changed": fresh, "added": added, "unchanged": b"stable"}
            check_route(database, job, shards, selected, ["changed", "unchanged"])
            print(f"takeover after_sigkill job={job} successor_generation=3 successor_mode=full "
                  f"published_bytes={detail['transferred_bytes']} recovered_ms={elapsed_ms}", flush=True)

            storage = ([(f"{database}.base_{job}", 8123)] if selected is None else
                       [(f"{database}.__catalogue_base_{job}", 8123 + 2 * selected + replica)
                        for replica in range(2)])
            if not hold_state:
                for table, port in storage:
                    sql(f"ALTER TABLE {table} MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool=1", port)

            # Send the exact old request after the new generation has completed.
            gate.release.set()
            assert gate.finished.wait(30) and gate.status == 200, (gate.status, case_logs)
            if hold_state:
                state = rows(f"SELECT copy_execution_id,baseline_committed FROM {database}.state_{job} FINAL WHERE id=1")
                state_preserved = state == [{"copy_execution_id": successor_id, "baseline_committed": True}]
                print(f"takeover late_state job={job} old_commit_status={gate.status} "
                      f"successor_state_preserved={state_preserved}", flush=True)
                if not state_preserved:
                    return ["late_old_state_overwrote_successor"]
            else:
                by_path = {row["relative_path"]: row for row in base(database, job)}
                assert by_path["changed"]["copy_execution_id"] == successor_id, by_path
                assert by_path["added"]["copy_execution_id"] == previous_id, by_path
                for table, port in storage:
                    physical = rows(f"SELECT copy_execution_id FROM {table} WHERE relative_path='changed'", port)
                    assert {row["copy_execution_id"] for row in physical} == {previous_id, successor_id}, physical

            assert finish(launch("next_incremental", True, contender_dir), "next incremental") == 0, case_logs
            _, detail = check_completed(database, job, 4, "incremental")
            assert (target / "changed").read_bytes() == fresh
            late_target_retained = (target / "added").exists()
            if late_target_retained:
                assert (target / "added").read_bytes() == added
            print(f"takeover subsequent_incremental job={job} held_state={hold_state} "
                  f"old_only_target_retained={late_target_retained} "
                  f"reported_deleted={detail['deleted_count']}", flush=True)
            if not late_target_retained or detail["deleted_count"] != 0:
                return ["late_old_fact_reintroduced_deletion"]
            assert detail["transferred_bytes"] == 0, detail

            # An incremental after another restart must remember the last full boundary
            # and still reconcile a real deletion based on the retained catalogue.
            (source / "changed").unlink()
            assert finish(launch("real_deletion", True, contender_dir), "real deletion") == 0, case_logs
            _, detail = check_completed(database, job, 5, "incremental")
            assert detail["deleted_count"] == 1 and detail["transferred_bytes"] == 0, detail
            assert record_contents(target) == {"unchanged": b"stable", "added": added}
            # 基线之外的旧版本也须随路径删除，否则普通扫描可再次观察到该路径。
            for table, port in storage:
                stale = rows(f"SELECT copy_execution_id FROM {table} WHERE relative_path='changed'", port)
                assert not stale, (job, table, port, stale)
            print(f"takeover valid_deletion job={job} reported_deleted={detail['deleted_count']} "
                  f"stale_target_retained=true transferred_bytes={detail['transferred_bytes']}", flush=True)
            return []
        finally:
            if old.poll() is None:
                old.kill()
                old.wait(timeout=5)
            gate.release.set()
            gate.finished.wait(10)
            set_gate(None)


def run(shards):
    binary = Path(os.environ["LAB_TERRASYNC_BIN"]).resolve(strict=True)
    logs = Path(os.environ["LAB_TAKEOVER_LOG_DIR"]).resolve()
    logs.mkdir(parents=True, exist_ok=False)
    selected_gate = None
    guard = threading.Lock()

    def set_gate(gate):
        nonlocal selected_gate
        with guard:
            selected_gate = gate

    class Relay(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = request_body(self)
            query = (urllib.parse.unquote_plus(self.path).encode() + body[:160]).upper()
            with guard:
                gate = selected_gate
                held = False
                if gate is not None and not gate.received.is_set() and b"INSERT INTO" in query:
                    if gate.hold_state:
                        held = (f"STATE_{gate.job}".upper().encode() in query
                                and b"BASELINE_COMMITTED" in query and b",TRUE," in query)
                    else:
                        held = f"BASE_{gate.job}".upper().encode() in query and b"FORMAT ROWBINARY" in query
                if held:
                    gate.payload = body
                    gate.received.set()
            if held and not gate.release.wait(120):
                self.send_error(504, "owned fixture did not release the held write")
                return
            connection = http.client.HTTPConnection("127.0.0.1", 8123, timeout=90)
            try:
                headers = {key: value for key, value in self.headers.items()
                           if key.lower() not in ("host", "content-length", "connection", "transfer-encoding")}
                headers["Content-Length"] = str(len(body))
                connection.request("POST", self.path, body=body, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                if held:
                    gate.status = response.status
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("content-length", "connection", "transfer-encoding"):
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                connection.close()
                if held:
                    gate.finished.set()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with binary.open("rb") as executable:
        binary_hash = hashlib.file_digest(executable, "sha256").hexdigest()
    print(f"takeover CLI sha256={binary_hash} raw_logs={logs}", flush=True)
    failures = []
    try:
        for cluster in (False, True):
            database = f"ts_takeover_{uuid.uuid4().hex}"
            on_cluster = " ON CLUSTER notification_cluster" if cluster else ""
            sql(f"CREATE DATABASE {database}{on_cluster}")
            try:
                for separate_dir, hold_state, checkpoint in (
                    (False, False, False), (True, False, True), (True, True, False)
                ):
                    variant = "state" if hold_state else "other" if separate_dir else "same"
                    prefix = f"takeover_{'cluster' if cluster else 'single'}_{variant}"
                    placements = rows(
                        f"SELECT number,cityHash64(concat('{prefix}_',toString(number))) % {shards} AS shard FROM numbers(64)"
                    )
                    locations = range(shards) if cluster else (None,)
                    for selected in locations:
                        number = next(row["number"] for row in placements
                                      if selected is None or int(row["shard"]) == selected)
                        job = f"{prefix}_{number}"
                        results = run_case(binary, logs, server.server_port, set_gate, database, job,
                                           shards, selected, checkpoint, separate_dir, hold_state)
                        failures.extend((job, result) for result in results)
            finally:
                sql(f"DROP DATABASE {database}{on_cluster} SYNC")
    finally:
        server.shutdown()
        server.server_close()
    assert not failures, f"takeover contract failures: {failures}"
