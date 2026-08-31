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
SHA = re.compile(r"^[0-9a-f]{40}$")
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
    if report.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if report.get("repository") != "JayTsu-sh/terrasync-rs":
        raise ValueError("repository must identify terrasync-rs")
    if report.get("mode") != "terrasync_single_process":
        raise ValueError("mode must be terrasync_single_process")
    if not SHA.fullmatch(str(report.get("exact_commit", ""))):
        raise ValueError("terrasync exact commit is required")
    dependency = report.get("dependency_commits", {}).get("data-mover-rs", "")
    if not SHA.fullmatch(str(dependency)):
        raise ValueError("data-mover-rs exact commit is required")
    if not str(report.get("run_id", "")).strip():
        raise ValueError("run_id is required")

    cells = report.get("cells")
    if not isinstance(cells, list) or len(cells) != 64:
        raise ValueError("report must contain exactly 64 cells")
    expected = {(source, destination) for source in PROFILES for destination in PROFILES}
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
    validate(Path(argv[1]))
    print("terrasync single-process matrix: 8 profiles, 64 independent cells")


if __name__ == "__main__":
    try:
        main(sys.argv)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"single-process matrix error: {error}", file=sys.stderr)
        raise SystemExit(1)
