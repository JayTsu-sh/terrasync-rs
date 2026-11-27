#!/usr/bin/env python3
"""Durable progress and redacted evidence for the single-process lab matrix."""

import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


CLASSES = set(
    "Authentication PermissionDenied Connectivity Timeout InvalidInput Unsupported NotFound "
    "Conflict Capacity Corruption Protocol Throttled Cancelled Internal".split()
)
OPERATIONS = set("Connect Traverse Observe Read Prepare Write Verify Publish Namespace Metadata".split())
PHASES = set(
    "endpoint cell_path load_receipt persist_receipt connect connect_cell claim_scope create_scope "
    "create_marker observe_scope observe_marker observe_data check_ownership delete_data delete_marker "
    "delete_scope confirm_absence create_cell observe_source cleanup_stage Preflight Prepare Transfer Verify Publish "
    "Metadata Describe Checkpoint".split()
)
CAUSES = set(
    "TimedOut ConnectionLost SessionInvalidated OutcomeUnknown smb_logon_rejected smb_access_denied "
    "smb_io_timeout smb_session_lost smb_status smb_deadline smb_transport".split()
)
OUTCOMES = {"unknown", "not_indicated", "changed_or_unknown", "published"}


def read(path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def durable_write(path, contents):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        tmp = Path(handle.name)
        try:
            json.dump(contents, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(tmp, path)
            sync_directory(path.parent)
        finally:
            tmp.unlink(missing_ok=True)


def sync_directory(path):
    directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def init(evidence, run_id, commit, dependency, source_state, profiles, deferred):
    durable_write(evidence.parent / "run.json", {
        "schema_version": 2,
        "repository": "JayTsu-sh/terrasync-rs",
        "exact_commit": commit or None,
        "dependency_commits": {"data-mover-rs": dependency},
        "source_state": source_state,
        "run_id": run_id,
        "mode": "terrasync_single_process",
        "profiles": profiles.split(","),
        "deferred_profiles": deferred.split(",") if deferred else [],
        "producer": "running",
        "touched": [],
    })
    sync_directory(evidence.parent.parent)
    sync_directory(evidence.parent.parent.parent)
    publish(evidence, "")


def mark(evidence, field, value):
    path = evidence.parent / "run.json"
    run = read(path)
    if field == "touched":
        if value in run[field]:
            return
        run[field].append(value)
    else:
        run[field] = value
    if field == "source_state" and value != "clean":
        run["exact_commit"] = None
    durable_write(path, run)


def begin(evidence, operation, mutates, *endpoints):
    files = sorted(evidence.glob(f"{operation}.*.json"))
    path = evidence / f"{operation}.{len(files) + 1:04d}.json"
    status = {
        "operation": operation,
        "endpoints": list(endpoints),
        "status": "running",
        "write_outcome": "unknown" if mutates == "true" else "not_indicated",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "started_ns": time.monotonic_ns(),
    }
    durable_write(path, status)
    print(path)


def redacted_diagnostics(log):
    rules = {
        "phase": PHASES,
        "role": {"source", "destination", "orchestration"},
        "endpoint": {None, "10.128.61.200", "10.128.61.201"},
        "class": CLASSES | {None},
        "operation": OPERATIONS | {None},
        "provenance": {"typed", "classified", "adapter_diagnostic", "unclassified"},
        "cause": CAUSES | {None},
        "write_outcome": OUTCOMES,
    }
    with open(log, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith("MATRIX_DIAGNOSTIC="):
                try:
                    item = json.loads(line.partition("=")[2])
                except ValueError:
                    continue
                if (
                    isinstance(item, dict)
                    and item.keys() == rules.keys()
                    and all(isinstance(value, (str, type(None))) and value in rules[key] for key, value in item.items())
                ):
                    yield item
            elif line.startswith("HDFS_CLEANUP_FAILURE="):
                reason = line.rstrip().partition("=")[2]
                if reason in CLASSES or reason == "connection_unknown":
                    yield {"class": reason, "operation": "Namespace", "role": "destination"}


def finish(path, exit_code, log):
    path = Path(path)
    status = read(path)
    exit_code = int(exit_code)
    diagnostics = list(redacted_diagnostics(log))
    status["duration_ms"] = (time.monotonic_ns() - status.pop("started_ns")) // 1_000_000
    status["exit_code"] = exit_code
    status["diagnostics"] = diagnostics
    if exit_code == 0:
        status["status"] = "passed"
        if status["write_outcome"] == "unknown":
            status["write_outcome"] = "acknowledged"
    else:
        cause = {124: "Timeout", 137: "Killed", 130: "Cancelled", 143: "Cancelled"}.get(exit_code)
        cause = cause or next((item["class"] for item in diagnostics if item["class"]), "unclassified")
        status.update(status="failed", cause=cause)
        if diagnostics and exit_code not in (124, 130, 137, 143):
            status["write_outcome"] = max(
                (item.get("write_outcome", status["write_outcome"]) for item in diagnostics),
                key=("not_indicated", "published", "changed_or_unknown", "unknown").index,
            )
        print(f"{status['operation']} failed: {cause} (exit {exit_code})", file=sys.stderr)
    durable_write(path, status)


def cell(evidence, source, destination, outcome, started, completed, fingerprint, expected, actual):
    name = f"{source}__{destination}.json"
    durable_write(evidence / name, {
        "gate_id": f"TS-SINGLE/{source}__{destination}",
        "source_profile": source,
        "destination_profile": destination,
        "outcome": outcome,
        "fixture_set": "single-process-functional-v1",
        "started_at": started,
        "completed_at": completed,
        "environment_fingerprint": fingerprint,
        "artifact_links": [f"artifact:{name}"],
        "expected_sha256": expected,
        "readback_sha256": actual or None,
    })


def interrupt(evidence):
    for path in attempt_paths(evidence):
        attempt = read(path)
        if attempt["status"] == "running":
            attempt.pop("started_ns")
            attempt["status"] = "interrupted"
            durable_write(path, attempt)


def attempt_paths(evidence):
    return sorted(evidence.glob("*.[0-9][0-9][0-9][0-9].json"))


def publish(evidence, workflow_outcome):
    report = read(evidence.parent / "run.json")
    touched = report.pop("touched")
    producer = report.pop("producer")
    workflow_outcome = workflow_outcome or report.pop("workflow_outcome", "")
    for field in ("workflow_outcome", "hdfs_target", "dxn_target"):
        report.pop(field, None)
    operations = [read(path) for path in attempt_paths(evidence)]
    latest = {op["operation"]: op for op in operations}

    def status(name):
        return latest.get(name, {}).get("status", "not_attempted")

    def combined(names):
        statuses = {status(name) for name in names}
        for result in ("failed", "interrupted", "running", "not_attempted"):
            if result in statuses:
                return result
        return "passed"

    receipt = evidence.parent / "cifs-receipt.json"
    ownership = {}
    if receipt.exists():
        try:
            candidate = read(receipt)
            if (
                isinstance(candidate, dict)
                and candidate.get("run_id") == report["run_id"]
                and re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", str(candidate.get("scope", "")))
                and isinstance(candidate.get("scope"), str)
            ):
                ownership = candidate
        except (OSError, ValueError):
            pass
    cifs_roles = {role: status(f"cifs.delete.{role}") for role in ("source", "destination")}
    report["cifs"] = {
        "scope": ownership.get("scope"),
        "run_id": ownership.get("run_id"),
        "setup": combined(("cifs.prepare.source", "cifs.claim.source", "cifs.verify.destination")),
        "cleanup": cifs_roles,
    }
    cleanup = {
        "hdfs": status("hdfs.delete") if "hdfs" in touched else "not_attempted",
        "s3_dxn": combined(("dxn.abort", "dxn.delete", "dxn.verify")) if "dxn" in touched else "not_attempted",
        "cifs": combined(("cifs.delete.source", "cifs.delete.destination")),
    }
    cells = [read(path) for path in sorted(path for path in evidence.glob("*__*.json") if path.name.count(".") == 1)]
    required_cleanup = ("hdfs", "s3_dxn") if report["deferred_profiles"] else ("hdfs", "s3_dxn", "cifs")
    if workflow_outcome == "cancelled" or producer == "cancelled":
        overall = "cancelled"
    elif producer == "finished" and workflow_outcome in ("", "success"):
        cifs_ready = report["deferred_profiles"] or (ownership and report["cifs"]["setup"] == "passed")
        overall = "completed" if (
            report["source_state"] == "clean"
            and cifs_ready
            and all(cleanup[name] == "passed" for name in required_cleanup)
            and len(cells) == len(report["profiles"]) ** 2
            and all(item["outcome"] == "passed" for item in cells)
        ) else "failed"
    else:
        overall = "incomplete"
    report.update(status=overall, cleanup=cleanup, cells=cells, operations=operations)
    durable_write(evidence / "matrix.json", report)


def main(args):
    command, root, *values = args
    evidence = Path(root)
    if command == "init":
        init(evidence, *values)
    elif command == "mark":
        mark(evidence, *values)
    elif command == "begin":
        begin(evidence, *values)
    elif command == "finish":
        finish(*values)
    elif command == "cell":
        cell(evidence, *values)
    elif command == "publish":
        publish(evidence, *values)
    elif command == "interrupt":
        interrupt(evidence)
    elif command in {"status", "retryable"}:
        latest = sorted(evidence.glob(f"{values[0]}.*.json"))
        attempt = read(latest[-1]) if latest else {}
        if command == "status":
            print(attempt.get("status", "not_attempted"))
        else:
            cause = attempt.get("cause")
            unknown_write = attempt.get("write_outcome") == "unknown" and cause not in {
                "Authentication", "PermissionDenied", "Conflict",
            }
            sys.exit(attempt.get("status") != "failed" or not (
                unknown_write or cause in {"Connectivity", "Timeout", "Killed"}
            ))
    elif command == "field":
        print(read(evidence.parent / "run.json").get(values[0], ""))
    elif command == "touched":
        sys.exit(values[0] not in read(evidence.parent / "run.json")["touched"])
    else:
        raise ValueError(f"unknown evidence command {command}")


if __name__ == "__main__":
    main(sys.argv[1:])
