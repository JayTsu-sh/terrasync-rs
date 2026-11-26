"""Exercise copy commit evidence through separate CLI processes and real ClickHouse."""

from datetime import datetime
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import threading
import time
import urllib.parse
import uuid

from copy_takeover import archive, generations, request_body, rows, sql
from copy_generation_nfs import nfs_tunnel


class Fault:
    def __init__(self, mode=None):
        self.mode = mode
        self.writes = 0
        self.published = 0
        self.readbacks = 0
        self.denied = 0
        self.silent_since = None
        self.first_readback = None
        self.readback_disconnects = []
        self.query_tag = None
        self.cancel_started = None
        self.cancel_status = None
        self.active_queries_before_cancel = 0


class Relay(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def deny(self, fault):
        fault.denied += 1
        payload = b"Code: 497. Migration fixture rejected this statement before forwarding"
        self.send_response(403)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def observe_disconnect(self, fault):
        try:
            assert self.rfile.read(1) == b""
        except ConnectionResetError:
            pass
        fault.readback_disconnects.append(time.monotonic())

    def do_POST(self):
        body = request_body(self)
        if self.path == "/callback":
            self.server.callbacks.append(json.loads(body))
            self.send_response(204)
            self.end_headers()
            return
        self.handle_database(body)

    def handle_database(self, body):
        query = (urllib.parse.unquote_plus(self.path).encode() + body[:256]).upper()
        fault = self.server.fault
        commit = b"INSERT INTO STATE_MIG_RESULT " in query and b"BASELINE_COMMITTED" in query and b",TRUE," in query
        readback = b"SELECT SCAN_STATE,COPY_EXECUTION_ID,STATE_VERSION,COPY_BASELINE_SEQUENCE FROM STATE_MIG_RESULT FINAL" in query
        hold_readback = fault.published and readback and fault.mode == "pending"
        cancelling = fault.query_tag is not None and b"KILL QUERY" in query and fault.query_tag.upper().encode() in query
        if cancelling and fault.cancel_started is None:
            fault.cancel_started = time.monotonic()
            fault.active_queries_before_cancel = int(sql(
                f"SELECT count() FROM system.processes WHERE Settings['log_comment']='{fault.query_tag}'"
            ))
        if commit:
            fault.writes += 1
            if fault.mode == "before":
                self.deny(fault)
                return
        if fault.published and readback:
            fault.readbacks += 1
            if fault.first_readback is None:
                fault.first_readback = time.monotonic()
            if hold_readback:
                if fault.query_tag is None:
                    parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                    fault.query_tag, = parameters["log_comment"]
                # 让真实查询跨过恢复截止时间，并允许服务器在行之间响应取消。
                delayed = body.replace(
                    b"WHERE id=1",
                    b"WHERE id=1 AND (SELECT sum(sleepEachRow(0.05)) FROM numbers(100) SETTINGS max_block_size=1)=0",
                )
                assert delayed != body, body
                body = delayed
        if fault.published and fault.mode == "cleanup" and b"DELETE WHERE COPY_SEQUENCE>0 AND COPY_SEQUENCE<=" in query:
            self.deny(fault)
            return
        disconnect = None
        if hold_readback:
            # 独立观察客户端断开，避免把等待 ClickHouse 的耗时算进取消响应。
            self.close_connection = True
            self.connection.settimeout(5)
            disconnect = threading.Thread(target=self.observe_disconnect, args=(fault,))
            disconnect.start()
        upstream = http.client.HTTPConnection("127.0.0.1", 8123, timeout=30)
        try:
            headers = {key: value for key, value in self.headers.items()
                       if key.lower() not in ("host", "content-length", "connection", "transfer-encoding")}
            headers["Content-Length"] = str(len(body))
            upstream.request("POST", self.path, body=body, headers=headers)
            response = upstream.getresponse()
            payload = response.read()
            if cancelling:
                fault.cancel_status = response.status
            if hold_readback:
                return
            if commit and response.status == 200:
                fault.published += 1
                if fault.mode == "pending":
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                if fault.mode == "verified":
                    self.send_error(504, "owned gateway lost the upstream commit response")
                    return
                if fault.mode == "silent":
                    fault.silent_since = time.monotonic()
                    time.sleep(3)
                    self.close_connection = True
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
            if disconnect is not None:
                disconnect.join()


def contents(path):
    return {file.name: (file.read_bytes(), stat.S_IMODE(file.stat().st_mode)) for file in path.iterdir()}


class Scenario:
    def __init__(self, binary, work, database, cluster, relay, nfs_port, callback_driver):
        self.binary, self.work, self.database, self.relay = binary, work, database, relay
        self.cluster = cluster
        self.source, self.target = work / "source", work / "target"
        self.stable_renames = nfs_port is not None
        if self.stable_renames:
            subpath = f"{'cluster' if cluster else 'single'}/{work.name}"
            prefix = os.environ["LAB_COPY_RESULT_NFS_PREFIX"]
            self.source = Path(os.environ["LAB_COPY_RESULT_NFS_VIEW"]) / subpath
            self.source_uri = f"nfs://127.0.0.1:{nfs_port}/srv/nfs/v4:/ci/{prefix}/{subpath}?version=4.1&noresvport=true&uid=0&gid=0"
        else:
            self.source_uri = str(self.source)
        self.source.mkdir(parents=True)
        self.target.mkdir(parents=True)
        self.sequence = 0
        self.callback_driver = callback_driver
        self.attempts = set()
        self.configs = {}
        for retain in (True, False):
            config = work / f"retain-{retain}.toml"
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
batch_size = 20
[database.clickhouse]
mode = "{'cluster' if cluster else 'single'}"
{('cluster = "notification_cluster"' if cluster else '')}
dsn = "http://127.0.0.1:{relay.server_port}"
database = "{database}"
username = "default"
dial_timeout = 3
read_timeout = 2
""")
            self.configs[retain] = config

    def write(self, path, value, mode=0o640):
        entry = self.source / path
        entry.write_bytes(value)
        entry.chmod(mode)

    def sync(self, name, *, mode="incremental", outcome="committed", failure=None, retain=True, options=()):
        fault = self.relay.fault = Fault(failure)
        self.sequence += 1
        self.expected_source = contents(self.source) if retain else {}
        command = [str(self.binary), "-c", str(self.configs[retain]), "sync", "--id", "mig_result",
                   *options, self.source_uri, str(self.target)]
        environment = dict(os.environ, CARGO_MANIFEST_DIR=str(self.work / name / "fixture"))
        callback = name == "interrupted" and self.callback_driver is not None
        if callback:
            command = [str(self.callback_driver), "--ignored", "--exact", "copy_generation_callback_entrypoint", "--nocapture"]
            self.relay.callbacks.clear()
            environment.update(
                LAB_COPY_RESULT_CONFIG=str(self.configs[retain]),
                LAB_COPY_RESULT_JOB_DIR=str(self.work / "jobs" / "replicate_mig_result"),
                LAB_COPY_RESULT_SOURCE=self.source_uri,
                LAB_COPY_RESULT_TARGET=str(self.target),
                LAB_COPY_RESULT_CALLBACK=f"http://127.0.0.1:{self.relay.server_port}/callback",
            )
        with (self.work / f"{name}.log").open("wb") as log:
            run = subprocess.run(command, cwd=self.work, env=environment, stdout=log,
                                 stderr=subprocess.STDOUT, timeout=60)
        assert contents(self.source) == self.expected_source, (name, contents(self.source), self.expected_source)
        failed = failure in ("before", "pending", "cleanup")
        assert (run.returncode != 0) == failed, (name, run.returncode, log.name)
        generation = generations(self.database, "mig_result")[-1]
        execution_id = generation["request"]["execution_id"]
        assert execution_id not in self.attempts, generation
        self.attempts.add(execution_id)
        assert generation["sequence"] == self.sequence and generation["plan"]["mode"] == mode, generation
        assert generation["phase"] == ("failed" if failed else "completed"), generation
        report = archive(self.database, "mig_result", execution_id)
        assert report["is_final"] and report["generation_commit_result"] == outcome, report
        assert report["migration"]["execution_id"] == execution_id, report
        assert report["migration"]["generation"] == self.sequence and report["migration"]["phase"] == generation["phase"], report
        assert (report["task_error_count"] > 0) == failed, report
        assert "generation_committed" not in report, report
        assert f"Generation commit result: {outcome}" in Path(log.name).read_text()
        if callback:
            reports = self.relay.callbacks[:]
            assert all("generation_commit_result" not in received for received in reports if not received["is_final"]), reports
            final = [received for received in reports if received["is_final"]]
            assert final and report == final[-1], (report, final)
            assert all(received["generation_commit_result"] == outcome for received in final), final
            (self.work / f"{name}-callbacks.json").write_text(json.dumps(final, ensure_ascii=False, indent=2) + "\n")
        state, = rows(f"SELECT scan_state,baseline_committed,copy_execution_id,state_version,copy_baseline_sequence "
                      f"FROM {self.database}.state_mig_result FINAL")
        assert state["copy_execution_id"] == execution_id, state
        assert int(state["state_version"]) == 3 * self.sequence + (failure != "before"), state
        assert bool(state["baseline_committed"]) == (failure != "before"), state
        if mode == "full":
            self.baseline = self.sequence
        assert int(state["copy_baseline_sequence"]) == self.baseline, state
        facts = rows(f"SELECT relative_path,copy_sequence,copy_execution_id,copy_path_confirmed,copy_data_confirmed "
                     f"FROM {self.database}.base_mig_result FINAL ORDER BY relative_path")
        assert all(self.baseline <= int(fact["copy_sequence"]) <= self.sequence for fact in facts), facts
        assert all(fact["copy_execution_id"] in self.attempts for fact in facts), facts
        events = []
        if mode == "incremental":
            events = rows(f"SELECT operation_type,relative_path,comment,source_presence,target_outcome "
                          f"FROM {self.database}.incremental_mig_result FINAL WHERE copy_execution_id='{execution_id}' "
                          f"ORDER BY operation_type,relative_path")
            event_keys = {(event["operation_type"], event["relative_path"]) for event in events}
            assert len(event_keys) == len(events), events
        result = {"generation": generation, "report": report, "state": state, "facts": facts, "events": events}
        (self.work / f"{name}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        assert fault.writes == 1 and fault.published == (failure != "before"), vars(fault)
        if failure in ("before", "cleanup"):
            assert fault.denied == 1, vars(fault)
        if failure in ("verified", "pending", "silent"):
            assert fault.readbacks > 0, vars(fault)
        if failure == "silent":
            detection = fault.first_readback - fault.silent_since
            assert 1.8 <= detection < 3, detection
            print(f"silent commit response detected after {detection:.3f}s", flush=True)
        if failure == "pending":
            assert fault.cancel_status == 200 and fault.active_queries_before_cancel > 0, vars(fault)
            assert len(fault.readback_disconnects) == fault.readbacks >= 2, vars(fault)
            assert fault.readback_disconnects[0] < fault.cancel_started, vars(fault)
            disconnect_delay = fault.readback_disconnects[-1] - fault.cancel_started
            assert 0 <= disconnect_delay < 0.75, vars(fault)
            processes = "clusterAllReplicas('notification_cluster', system.processes)" if self.cluster else "system.processes"
            assert sql(f"SELECT count() FROM {processes} WHERE Settings['log_comment']='{fault.query_tag}'").strip() == "0"
            print(f"scoped cancellation cleared {fault.active_queries_before_cancel} live readback queries; "
                  f"client disconnect observed {disconnect_delay:.3f}s after the relay received KILL QUERY", flush=True)
        if failure == "pending" and not callback:
            path = self.work / name / "logs" / "mig_result" / "app.log"
            lines = path.read_text().splitlines()
            started, = [line for line in lines if "ClickHouse connection recovery started" in line]
            exhausted, = [line for line in lines if "ClickHouse connection recovery window exhausted" in line]
            window = (datetime.fromisoformat(exhausted.split()[0]) - datetime.fromisoformat(started.split()[0])).total_seconds()
            assert 2.9 < window < 3.25, window
            print(f"copy recovery window {window:.3f}s", flush=True)
        print(f"{self.work.parent.name}/{self.work.name} {name} {mode} {outcome} "
              f"transferred={report['detail']['transferred_bytes']} events={[(e['operation_type'], e['relative_path']) for e in events]}", flush=True)
        return result

    def check_target(self, previous=None):
        expected = {} if previous is None else dict(previous)
        expected.update(self.expected_source)
        expected["untracked"] = (b"external target", 0o644)
        assert contents(self.target) == expected, (contents(self.target), expected)


def check_delta(result, expected_bytes, *, new, changed, deleted, renamed):
    detail = result["report"]["detail"]
    assert detail["transferred_bytes"] == expected_bytes, detail
    assert (detail["new_count"], detail["changed_count"], detail["deleted_count"], detail["renamed_count"]) == (
        new, changed, deleted, renamed,
    ), detail


def run_case(case, failure):
    for name, body in [("updated", b"before"), ("rename-old", b"rename body"), ("delete-me", b"gone"),
                       ("mode", b"same body"), ("stable", b"stable")]:
        case.write(name, body)
    marker = case.target / "untracked"
    marker.write_bytes(b"external target")
    marker.chmod(0o644)
    full = case.sync("full", mode="full")
    assert full["report"]["detail"]["transferred_bytes"] == sum(len(value) for value, _ in contents(case.source).values())
    case.check_target()

    case.write("updated", b"successful change")
    case.write("early", b"new before fault")
    first = case.sync("successful-incremental")
    check_delta(first, len(b"successful change") + len(b"new before fault"), new=1, changed=1, deleted=0, renamed=0)
    case.check_target()

    case.write("updated", b"changed under fault")
    case.write("fault-new", b"new under fault")
    (case.source / "mode").chmod(0o600)
    if failure == "cleanup":
        # 本轮选择删源，后续应按已有策略重新全量并保住目标。
        old_target = contents(case.source)
        interrupted = case.sync("interrupted", failure=failure, retain=False)
        check_delta(interrupted, len(b"changed under fault") + len(b"new under fault"),
                    new=1, changed=2, deleted=0, renamed=0)
        facts = interrupted["facts"]
        assert {fact["relative_path"] for fact in facts} == set(old_target), facts
        assert all(fact["copy_path_confirmed"] and fact["copy_data_confirmed"] for fact in facts), facts
        case.check_target(old_target)
        case.write("restarted", b"new after move")
        restarted = case.sync("retry", mode="full")
        assert restarted["generation"]["plan"]["target_cleanup"] == "preserve"
        assert restarted["report"]["detail"]["transferred_bytes"] == len(b"new after move")
        case.check_target(old_target)
        later_rename = "restarted"
    else:
        old_target = {}
        (case.source / "rename-old").rename(case.source / "rename-new")
        (case.source / "delete-me").unlink()
        outcome = {"before": "not_committed", "pending": "pending_confirmation",
                   "verified": "committed", "silent": "committed"}[failure]
        interrupted = case.sync("interrupted", failure=failure, outcome=outcome)
        case.check_target()
        renamed = int(case.stable_renames)
        check_delta(interrupted, len(b"changed under fault") + len(b"new under fault") + (1 - renamed) * len(b"rename body"),
                    new=2 - renamed, changed=2, deleted=2 - renamed, renamed=renamed)
        if case.stable_renames:
            old_and_new = {(event["operation_type"], event["relative_path"], event["comment"]) for event in interrupted["events"]}
            assert ("rename", "rename-new", "rename-old") in old_and_new, old_and_new
        restarted = case.sync("retry")
        check_delta(restarted, 0, new=0, changed=0, deleted=0, renamed=0)
        case.check_target()
        later_rename = "early"

    case.write("later-new", b"put after recovery")
    (case.source / later_rename).chmod(0o604)
    later = case.sync("later-incremental")
    check_delta(later, len(b"put after recovery"), new=1, changed=1, deleted=0, renamed=0)
    case.check_target(old_target)

    (case.source / later_rename).rename(case.source / "last-rename")
    (case.source / "later-new").unlink()
    last = case.sync("last-incremental")
    renamed_bytes = len(b"new after move" if failure == "cleanup" else b"new before fault")
    renamed = int(case.stable_renames)
    check_delta(last, (1 - renamed) * renamed_bytes, new=1 - renamed, changed=0, deleted=2 - renamed, renamed=renamed)
    case.check_target(old_target)

    kept = contents(case.target)
    if (case.source / "mode").exists():
        (case.source / "mode").unlink()
    excluded = case.sync("excluded", options=("--exclude", 'name=="mode"'))
    assert excluded["generation"]["plan"]["target_cleanup"] == "preserve"
    check_delta(excluded, 0, new=0, changed=0, deleted=0, renamed=0)
    assert contents(case.target) == kept


def check_unpersisted(binary, work):
    source = work / "source"
    source.mkdir(parents=True)
    target = work / "target"
    target.mkdir()
    (source / "payload").write_bytes(b"streamed")
    config = work / "config.toml"
    config.write_text("[database]\nenabled=false\n[sync]\nis_source_reserved=true\n")
    with (work / "sync.log").open("wb") as log:
        subprocess.run([str(binary), "-c", str(config), "sync", "--id", "stream_copy", str(source), str(target)],
                       cwd=work, stdout=log, stderr=subprocess.STDOUT, timeout=30, check=True)
    assert (target / "payload").read_bytes() == b"streamed"
    assert "Generation commit result:" not in (work / "sync.log").read_text()
    print("copy without ClickHouse omits the generation result", flush=True)


def run():
    binary = Path(os.environ["LAB_TERRASYNC_BIN"]).resolve(strict=True)
    driver = os.environ.get("LAB_COPY_RESULT_CALLBACK_DRIVER")
    driver = Path(driver).resolve(strict=True) if driver else None
    logs = Path(os.environ["LAB_COPY_RESULT_LOG_DIR"]).resolve()
    logs.mkdir(parents=True, exist_ok=False)
    with binary.open("rb") as executable:
        print("migration CLI sha256=" + hashlib.file_digest(executable, "sha256").hexdigest(), flush=True)
    with nfs_tunnel() as nfs_port, http.server.ThreadingHTTPServer(("127.0.0.1", 0), Relay) as relay:
        relay.callbacks = []
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        try:
            for cluster in (False, True):
                on_cluster = " ON CLUSTER notification_cluster" if cluster else ""
                failures = ("pending",) if nfs_port is not None else ("before", "verified", "pending", "silent", "cleanup")
                for failure in failures:
                    database = f"ts_mig_result_{uuid.uuid4().hex}"
                    sql(f"CREATE DATABASE {database}{on_cluster}")
                    try:
                        run_case(Scenario(binary, logs / ("cluster" if cluster else "single") / failure,
                                          database, cluster, relay, nfs_port, driver), failure)
                    finally:
                        sql(f"DROP DATABASE {database}{on_cluster} SYNC")
            check_unpersisted(binary, logs / "unpersisted")
        finally:
            relay.shutdown()
