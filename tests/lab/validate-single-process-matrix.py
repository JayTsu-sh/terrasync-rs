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
