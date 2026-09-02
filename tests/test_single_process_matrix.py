import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
VALIDATOR_PATH = ROOT / "tests" / "lab" / "validate-single-process-matrix.py"


def load_validator():
    spec = importlib.util.spec_from_file_location("single_matrix_validator", VALIDATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SingleProcessMatrixTest(unittest.TestCase):
    def setUp(self):
        self.validator = load_validator()
        self.profiles = list(self.validator.PROFILES)

    def report(self):
        cells = []
        for source in self.profiles:
            for destination in self.profiles:
                cells.append({
                    "gate_id": f"TS-SINGLE/{source}__{destination}",
                    "source_profile": source,
                    "destination_profile": destination,
                    "outcome": "passed",
                    "fixture_set": "single-process-functional-v1",
                    "started_at": "2026-08-31T00:00:00Z",
                    "completed_at": "2026-08-31T00:00:01Z",
                    "environment_fingerprint": f"lab/{source}/{destination}",
                    "artifact_links": [f"cell:{source}__{destination}"],
                })
        return {
            "schema_version": 1,
            "repository": "JayTsu-sh/terrasync-rs",
            "exact_commit": "a" * 40,
            "dependency_commits": {"data-mover-rs": "b" * 40},
            "run_id": "release-matrix-test",
            "mode": "terrasync_single_process",
            "cells": cells,
        }

    def validate(self, report):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "report.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            return self.validator.validate(path)

    def test_complete_64_cell_report_passes(self):
        self.validator.validate_report(self.report())

    def test_duplicate_pair_is_rejected(self):
        report = self.report()
        report["cells"][-1] = dict(report["cells"][0])
        with self.assertRaisesRegex(ValueError, "complete ordered profile product"):
            self.validator.validate_report(report)

    def test_missing_exact_dependency_commit_is_rejected(self):
        report = self.report()
        report["dependency_commits"]["data-mover-rs"] = "moving-main"
        with self.assertRaisesRegex(ValueError, "exact commit"):
            self.validator.validate_report(report)

    def test_explicit_cifs_deferral_allows_only_the_non_cifs_product(self):
        report = self.report()
        active_profiles = [profile for profile in self.profiles if profile != "cifs_fas2750"]
        report["profiles"] = active_profiles
        report["deferred_profiles"] = ["cifs_fas2750"]
        report["cells"] = [
            cell
            for cell in report["cells"]
            if cell["source_profile"] in active_profiles and cell["destination_profile"] in active_profiles
        ]
        self.validator.validate_report(report)

    def test_subset_without_explicit_cifs_deferral_is_rejected(self):
        report = self.report()
        active_profiles = [profile for profile in self.profiles if profile != "cifs_fas2750"]
        report["profiles"] = active_profiles
        report["cells"] = [
            cell
            for cell in report["cells"]
            if cell["source_profile"] in active_profiles and cell["destination_profile"] in active_profiles
        ]
        with self.assertRaisesRegex(ValueError, "explicit deferred profile"):
            self.validator.validate_report(report)

    def test_validator_cli_reports_explicit_cifs_deferral(self):
        report = self.report()
        active_profiles = [profile for profile in self.profiles if profile != "cifs_fas2750"]
        report["profiles"] = active_profiles
        report["deferred_profiles"] = ["cifs_fas2750"]
        report["cells"] = [
            cell
            for cell in report["cells"]
            if cell["source_profile"] in active_profiles and cell["destination_profile"] in active_profiles
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "report.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(VALIDATOR_PATH), str(path)],
                check=True,
                capture_output=True,
                text=True,
            )
        self.assertIn("7 profiles, 49 independent cells, deferred=cifs_fas2750", result.stdout)

    def test_each_cell_requires_independent_artifact_identity(self):
        report = self.report()
        report["cells"][1]["artifact_links"] = report["cells"][0]["artifact_links"]
        with self.assertRaisesRegex(ValueError, "artifact identity"):
            self.validator.validate_report(report)

    def test_runner_and_nightly_own_the_eight_profile_gate(self):
        runner = (ROOT / "tests" / "lab" / "run-single-process-matrix.sh").read_text(encoding="utf-8")
        workflow = (ROOT / ".github" / "workflows" / "nightly.yml").read_text(encoding="utf-8")
        release = (ROOT / ".github" / "workflows" / "release-validation.yml").read_text(encoding="utf-8")
        self.assertIn("profiles=(local nfs3 nfs40 nfs41 cifs_fas2750 s3_standard s3_dxn hdfs)", runner)
        self.assertIn("run-single-process-matrix.sh", workflow)
        self.assertIn("run-single-process-matrix.sh", release)
        self.assertNotIn("continue-on-error", workflow)
        self.assertNotIn("run_terrasync sync", runner)
        self.assertNotIn("run_terrasync integrity-check", runner)
        self.assertIn("single_process_matrix", runner)
        self.assertNotIn("terrasync rm", runner)
        self.assertIn('if [[ "$source" == "local" ]]; then', runner)
        self.assertIn('mkdir -p "$source_root"', runner)
        self.assertIn('if [[ "$destination" == "local" ]]; then', runner)
        self.assertIn('mkdir -p "$destination_root"', runner)
        self.assertIn('nfs3|nfs40|nfs41)', runner)
        self.assertIn("path=\"${root%%\\?*}\"", runner)
        self.assertIn("printf '%s/%s/%s?%s'", runner)
        self.assertIn("TS_SINGLE_DEFER_CIFS", runner)
        self.assertIn("deferred_profiles", runner)
        self.assertIn("TS-SINGLE 49-cell matrix (CIFS deferred)", workflow)
        self.assertTrue(VALIDATOR_PATH.stat().st_mode & 0o111)
        self.assertTrue((ROOT / "tests" / "lab" / "run-single-process-matrix.sh").stat().st_mode & 0o111)

    def test_hdfs_defaults_use_the_kerberos_ha_nameservice(self):
        common = (ROOT / "tests" / "lab" / "common.sh").read_text(encoding="utf-8")
        self.assertIn(
            "hdfs://hdfs%2Fterrasync-runner%40HDFS.LOCAL@hdfs-ha/",
            common,
        )
        self.assertIn("hdfs/terrasync-runner@HDFS.LOCAL", common)
        self.assertNotIn("hdfs://root@10.131.9.30:9000/", common)


if __name__ == "__main__":
    unittest.main()
