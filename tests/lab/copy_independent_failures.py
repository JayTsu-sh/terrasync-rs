"""验证后台失败的独立计数、诊断重放及增量阶段屏障。"""
import http.server
import json
import os
from pathlib import Path
import subprocess
import threading
import urllib.parse
import uuid

from copy_generation_result import Fault, Relay, Scenario, contents
from copy_takeover import archive, generations, request_body, rows, sql


class FailureRelay(Relay):
    def do_POST(self):
        if self.path == "/callback" and self.server.exercise:
            report = json.loads(request_body(self))
            self.server.callbacks.append(report)
            batch = report.get("diagnostics")
            if batch and report["is_final"] and not self.server.replayed:
                self.server.replayed = batch["batch_id"]
                self.send_error(503, "owned callback retry")
            else:
                self.send_response(204)
                self.end_headers()
            return

        super().do_POST()

    def handle_database(self, body):
        # 两个 worker 都进入真实比较后，由 ClickHouse 分别返回查询错误。
        query = (urllib.parse.unquote_plus(self.path).encode() + body[:256]).upper()
        if self.server.exercise and b"KILL QUERY" in query:
            self.server.cancel_seen.set()
        if (self.server.exercise and self.server.cancel_seen.is_set() and b"DROP TABLE IF EXISTS" in query
                and b"TEMP_" in query and b"LOG_COMMENT=" not in query):
            self.server.shutdown_drops += 1
        if self.server.exercise == "compare" and b"AS CHANGE_CODE" in query:
            with self.server.lock:
                selector = self.server.comparisons
                self.server.comparisons += 1
                parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                self.server.query_tags.update(parameters["log_comment"])
            self.server.gate.wait(timeout=5)
            if selector == 1:
                assert self.server.cancel_seen.wait(timeout=3)
            body = (b"SELECT owned_unknown_column", b"SELEC owned_malformed_projection")[selector]
        if self.server.exercise == "exclude" and b"INSERT INTO" in query:
            statement = query + body.upper()
            if b"NOT IN" in statement and b"EXCLUDE_" in statement:
                self.server.exclusion_failure += 1
                body = b"SELECT owned_exclusion_publish_failure"
        super().handle_database(body)


def interrupted(case):
    case.relay.fault = Fault()
    config = case.configs[True]
    config.write_text(config.read_text().replace("batch_size = 20", "batch_size = 1"))
    environment = dict(os.environ, CARGO_MANIFEST_DIR=str(case.work / "interrupted" / "fixture"))
    command = [str(case.binary), "-c", str(config), "sync", "--id", "mig_result", case.source_uri, str(case.target)]
    if case.callback_driver:
        command = [str(case.callback_driver), "--ignored", "--exact", "copy_generation_callback_entrypoint", "--nocapture"]
        environment.update(LAB_COPY_RESULT_CONFIG=str(config),
                           LAB_COPY_RESULT_JOB_DIR=str(case.work / "jobs" / "replicate_mig_result"),
                           LAB_COPY_RESULT_SOURCE=case.source_uri, LAB_COPY_RESULT_TARGET=str(case.target),
                           LAB_COPY_RESULT_CALLBACK=f"http://127.0.0.1:{case.relay.server_port}/callback")
    with (case.work / "interrupted.log").open("wb") as log:
        result = subprocess.run(command, cwd=case.work, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=35)

    assert result.returncode != 0
    assert case.relay.fault.writes == 0, vars(case.relay.fault)
    committed, failed = generations(case.database, "mig_result")
    assert committed["phase"] == "completed" and failed["phase"] == "failed"
    attempt = failed["request"]["execution_id"]
    state, = rows(f"SELECT baseline_committed,copy_execution_id,state_version FROM {case.database}.state_mig_result FINAL")
    assert state["copy_execution_id"] == attempt and not state["baseline_committed"]
    assert int(state["state_version"]) == 3 * failed["sequence"]
    system_tables = "clusterAllReplicas('notification_cluster', system.tables)" if case.cluster else "system.tables"
    assert sql(f"SELECT count() FROM {system_tables} WHERE database='{case.database}' "
               "AND multiMatchAny(name, ['^temp_', '^__catalogue_temp_', '^exclude_', '^__catalogue_exclude_'])").strip() == "0"
    system_processes = "clusterAllReplicas('notification_cluster', system.processes)" if case.cluster else "system.processes"
    for tag in case.relay.query_tags:
        assert sql(f"SELECT count() FROM {system_processes} WHERE Settings['log_comment']='{tag}'").strip() == "0"
    report = archive(case.database, "mig_result", attempt)
    details = rows(f"SELECT payload_json FROM {case.database}.notifications_mig_result FINAL "
                   f"WHERE kind=1 AND attempt_id='{attempt}'")
    events = [json.loads(detail["payload_json"]) for detail in details]
    result = {"report": report, "events": events, "state": state, "callbacks": case.relay.callbacks}
    (case.work / "evidence.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    assert report["generation_commit_result"] == "not_committed" and report["migration"]["phase"] == "failed", report
    return result


def verify(case):
    case.write("keep", b"unchanged")
    case.write("missing", b"baseline payload")
    case.sync("baseline", mode="full")
    baseline = contents(case.target)
    (case.source / "missing").unlink()
    for index in range(24):
        case.write(f"new-{index}", f"source payload {index}".encode())
    source = contents(case.source)
    case.relay.exercise = "compare"
    result = interrupted(case)
    assert case.relay.comparisons == 2, case.relay.comparisons
    assert case.relay.shutdown_drops == (2 if case.cluster else 1), case.relay.shutdown_drops
    assert contents(case.source) == source and contents(case.target) == baseline
    report, events = result["report"], result["events"]
    assert report["task_error_count"] == 2 and report["detail"]["error_count"] == 0, result
    errors = [event for event in events if event["origin"] == "incremental_compare"]
    assert {e["code"] for e in errors} == {"clickhouse_server_47", "clickhouse_server_62"}, errors
    assert len({e["event_id"] for e in errors}) == len(errors) == 2, errors
    assert any("owned_unknown_column" in e["message"] for e in errors), errors
    assert any("owned_malformed_projection" in e["message"] for e in errors), errors
    if case.callback_driver:
        finals = [report for report in case.relay.callbacks if report["is_final"]]
        replayed = [report for report in finals if (report.get("diagnostics") or {}).get("batch_id") == case.relay.replayed]
        assert len(replayed) >= 2 and replayed[0] == replayed[1], replayed
        assert all(report["task_error_count"] == 2 for report in finals), finals
        replayed_ids = {event["event_id"] for event in replayed[0]["diagnostics"]["events"]}
        assert {event["event_id"] for event in errors} <= replayed_ids, replayed
        assert finals[-1] == report and report["diagnostics_complete"], finals
    else:
        assert not report["diagnostics_complete"]
    # 同一作业无故障重跑能删掉该目标，证明前一轮确实挡住了候选删除。
    case.relay.exercise = False
    case.sequence += 1
    case.attempts.add(report["migration"]["execution_id"])
    retry = case.sync("retry")
    assert retry["report"]["detail"]["deleted_count"] == 1 and not (case.target / "missing").exists()
    print(f"cluster={case.cluster} independent task errors=2, staging drained, eligible target deletion blocked, baseline not submitted", flush=True)


def verify_reported_entry(case):
    case.write("missing", b"baseline payload")
    case.sync("baseline", mode="full")
    (case.source / "missing").unlink()
    case.write("occupied", b"new source file")
    occupied = case.target / "occupied"
    occupied.mkdir()
    marker = occupied / "owned"
    marker.write_bytes(b"pre-existing destination content")
    result = interrupted(case)
    report = result["report"]
    assert report["detail"]["error_count"] == 1 and report["task_error_count"] == 0, result
    events = [event for event in result["events"] if event["code"] == "Copy"]
    assert len(events) == 1 and events[0]["path"] == "occupied", events
    assert marker.read_bytes() == b"pre-existing destination content"
    assert (case.target / "missing").read_bytes() == b"baseline payload"
    assert (case.source / "occupied").read_bytes() == b"new source file"
    print(f"cluster={case.cluster} reported copy entry errors=1, task errors=0", flush=True)


def verify_exclusion_failure(case):
    case.write("missing", b"baseline payload")
    case.sync("baseline", mode="full")
    (case.source / "missing").unlink()
    case.write("new", b"new file requires exclusion")
    case.relay.exercise = "exclude"
    result = interrupted(case)
    assert case.relay.exclusion_failure == 1
    assert result["report"]["task_error_count"] == 1, result
    errors = [event for event in result["events"] if event["code"] == "clickhouse_server_47"]
    assert len(errors) == 1 and "owned_exclusion_publish_failure" in errors[0]["message"], result
    assert (case.target / "missing").read_bytes() == b"baseline payload"
    assert not (case.target / "new").exists()
    print(f"cluster={case.cluster} exclusion publisher failed, both staging tables drained", flush=True)


def run():
    binary = Path(os.environ["LAB_TERRASYNC_BIN"]).resolve(strict=True)
    driver = os.environ.get("LAB_COPY_RESULT_CALLBACK_DRIVER")
    driver = Path(driver).resolve(strict=True) if driver else None
    logs = Path(os.environ["LAB_COPY_RESULT_LOG_DIR"]).resolve()
    logs.mkdir(parents=True, exist_ok=False)
    for cluster in (False, True):
        on_cluster = " ON CLUSTER notification_cluster" if cluster else ""
        for name, check in (("background", verify), ("entry", verify_reported_entry), ("exclude", verify_exclusion_failure)):
            database = f"ts_copy_errors_{uuid.uuid4().hex}"
            sql(f"CREATE DATABASE {database}{on_cluster}")
            try:
                with http.server.ThreadingHTTPServer(("127.0.0.1", 0), FailureRelay) as relay:
                    relay.exercise, relay.replayed, relay.comparisons = False, None, 0
                    relay.query_tags = set()
                    relay.exclusion_failure = 0
                    relay.cancel_seen, relay.shutdown_drops = threading.Event(), 0
                    relay.callbacks, relay.gate, relay.lock = [], threading.Barrier(2), threading.Lock()
                    threading.Thread(target=relay.serve_forever, daemon=True).start()
                    try:
                        check(Scenario(binary, logs / ("cluster" if cluster else "single") / name,
                                       database, cluster, relay, None, driver))
                    finally:
                        relay.shutdown()
            finally:
                sql(f"DROP DATABASE {database}{on_cluster} SYNC")
