#!/usr/bin/env python3
"""Validate an independently produced terrasync single-process evidence report."""

import json
import re
import sys
from pathlib import Path


PROFILES = (
    "local",
    "nfs3",
    "nfs40",
    "nfs41",
    "cifs_fas2750",
    "s3_standard",
    "s3_dxn",
    "hdfs",
)
DEFERRED_PROFILES = ("cifs_fas2750",)
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"[0-9a-f]{64}")
RUN_ID = re.compile(r"(nightly|release)-[A-Za-z0-9._-]{1,80}")
CELL_FIELDS = (
    "gate_id",
    "source_profile",
    "destination_profile",
    "outcome",
    "fixture_set",
    "started_at",
    "completed_at",
    "environment_fingerprint",
    "artifact_links",
)


def validate_report(report):
    if report.get("schema_version") != 2:
        raise ValueError("schema_version must be 2")
    if report.get("repository") != "JayTsu-sh/terrasync-rs":
        raise ValueError("repository must identify terrasync-rs")
    if report.get("mode") != "terrasync_single_process":
        raise ValueError("mode must be terrasync_single_process")
    if report.get("source_state") != "clean":
        raise ValueError("exact-commit evidence requires a clean source")
    if not SHA.fullmatch(str(report.get("exact_commit", ""))):
        raise ValueError("terrasync exact commit is required")
    dependency = report.get("dependency_commits", {}).get("data-mover-rs", "")
    if not SHA.fullmatch(str(dependency)):
        raise ValueError("data-mover-rs exact commit is required")
    run_id = report.get("run_id")
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise ValueError("valid run_id is required")
    if report.get("status") != "completed":
        raise ValueError("matrix run must be completed")

    deferred = tuple(report.get("deferred_profiles", ()))
    profiles = tuple(report.get("profiles", PROFILES))
    if deferred:
        if deferred != DEFERRED_PROFILES:
            raise ValueError("only the explicitly deferred CIFS profile is permitted")
        expected_profiles = tuple(profile for profile in PROFILES if profile not in deferred)
        if profiles != expected_profiles:
            raise ValueError("deferred profile report has an invalid active profile set")
    elif profiles != PROFILES:
        raise ValueError("non-default profile set requires an explicit deferred profile")

    if report.get("cleanup") != {"hdfs": "passed", "s3_dxn": "passed", "cifs": "not_attempted" if deferred else "passed"}:
        raise ValueError("HDFS, DXN and active CIFS cleanup must pass")

    cifs = report.get("cifs", {})
    if not isinstance(cifs, dict):
        raise ValueError("CIFS evidence must be an object")
    if deferred:
        if cifs != {
            "scope": None,
            "run_id": None,
            "setup": "not_attempted",
            "cleanup": {"source": "not_attempted", "destination": "not_attempted"},
        }:
            raise ValueError("deferred CIFS must have no setup or cleanup activity")
    else:
        scope = cifs.get("scope")
        # Scope is opaque to the report reader. The CIFS driver associates it with the run.
        if cifs.get("run_id") != run_id or not isinstance(scope, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", scope):
            raise ValueError("CIFS scope must identify this matrix run")
        if cifs.get("setup") != "passed" or cifs.get("cleanup") != {"source": "passed", "destination": "passed"}:
            raise ValueError("CIFS ownership verification and cleanup on both endpoints must pass")

    cells = report.get("cells")
    expected_count = len(profiles) ** 2
    if not isinstance(cells, list) or len(cells) != expected_count:
        raise ValueError(f"report must contain exactly {expected_count} cells")
    expected = {(source, destination) for source in profiles for destination in profiles}
    actual = set()
    artifact_identities = set()
    for index, cell in enumerate(cells):
        missing = [field for field in CELL_FIELDS if field not in cell]
        if missing:
            raise ValueError(f"cell {index} is missing fields: {', '.join(missing)}")
        pair = (cell["source_profile"], cell["destination_profile"])
        if pair in actual:
            raise ValueError("cells must equal the complete ordered profile product")
        actual.add(pair)
        expected_gate = f"TS-SINGLE/{pair[0]}__{pair[1]}"
        if cell["gate_id"] != expected_gate:
            raise ValueError(f"cell {index} has invalid gate_id")
        if cell["outcome"] != "passed":
            raise ValueError(f"{expected_gate} did not pass")
        expected_hash = cell.get("expected_sha256")
        if not isinstance(expected_hash, str) or not DIGEST.fullmatch(expected_hash) or cell.get("readback_sha256") != expected_hash:
            raise ValueError(f"{expected_gate} requires matching fixture and readback checksums")
        if not all(str(cell[field]).strip() for field in CELL_FIELDS[4:8]):
            raise ValueError(f"{expected_gate} has blank evidence fields")
        links = cell["artifact_links"]
        if not isinstance(links, list) or not links or not all(str(link).strip() for link in links):
            raise ValueError(f"{expected_gate} requires artifact links")
        identity = tuple(links)
        if identity in artifact_identities:
            raise ValueError("each cell requires an independent artifact identity")
        artifact_identities.add(identity)
    if actual != expected or len(actual) != len(cells):
        raise ValueError("cells must equal the complete ordered profile product")


def validate(path):
    with Path(path).open(encoding="utf-8") as handle:
        report = json.load(handle)
    validate_report(report)
    return report


def main(argv):
    if len(argv) != 2:
        raise SystemExit("usage: validate-single-process-matrix.py REPORT.json")
    report = validate(Path(argv[1]))
    deferred = report.get("deferred_profiles", [])
    print(
        f"terrasync single-process matrix: {len(report.get('profiles', PROFILES))} profiles, "
        f"{len(report['cells'])} independent cells, deferred={','.join(deferred) or 'none'}"
    )


if __name__ == "__main__":
    try:
        main(sys.argv)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"single-process matrix error: {error}", file=sys.stderr)
        raise SystemExit(1)
