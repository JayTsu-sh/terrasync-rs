import importlib.util
import os
import json
import pathlib
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import uuid
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
VALIDATOR_PATH = ROOT / "tests" / "lab" / "validate-single-process-matrix.py"
LIFECYCLE_PATH = ROOT / "tests" / "lab" / "run-and-reconcile-single-process-matrix.sh"


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
                    "expected_sha256": "d" * 64,
                    "readback_sha256": "d" * 64,
                })
        run_id = "release-matrix-test"
        return {
            "schema_version": 2,
            "source_state": "clean",
            "repository": "JayTsu-sh/terrasync-rs",
            "exact_commit": "a" * 40,
            "dependency_commits": {"data-mover-rs": "b" * 40},
            "run_id": run_id,
            "status": "completed",
            "cifs": {
                "scope": "opaque-cifs-scope",
                "run_id": run_id,
                "setup": "passed",
                "cleanup": {"source": "passed", "destination": "passed"},
            },
            "mode": "terrasync_single_process",
            "cleanup": {"hdfs": "passed", "s3_dxn": "passed", "cifs": "passed"},
            "operations": [
                {"operation": name, "status": "passed", "exit_code": 0, "write_outcome": outcome}
                for name, outcome in (
                    ("hdfs.delete", "acknowledged"),
                    ("dxn.abort", "acknowledged"),
                    ("dxn.delete", "acknowledged"),
                    ("dxn.verify", "not_indicated"),
                    ("cifs.prepare.source", "not_indicated"),
                    ("cifs.claim.source", "acknowledged"),
                    ("cifs.verify.destination", "not_indicated"),
                    ("cifs.delete.source", "acknowledged"),
                    ("cifs.delete.destination", "acknowledged"),
                )
            ],
            "cells": cells,
        }

    def validate(self, report):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "report.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            return self.validator.validate(path)

    def test_cleanup_diagnostics_only_use_typed_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "cleanup.log"
            log.write_text("Authentication in path\nHDFS_CLEANUP_FAILURE=Capacity\nsecret-value\n")
            command = 'source tests/lab/common.sh; report_hdfs_cleanup_failure 1 "$1"'
            result = subprocess.run(["bash", "-c", command, "_", str(log)], cwd=ROOT, capture_output=True, text=True)
            self.assertIn("Capacity", result.stderr)
            self.assertNotIn("Authentication", result.stderr)
            self.assertNotIn("secret-value", result.stderr)

    def test_complete_64_cell_report_passes(self):
        self.validator.validate_report(self.report())

    def test_latest_operation_attempt_must_support_the_cleanup_summary(self):
        for operation in ("cifs.delete.source", "hdfs.delete", "dxn.verify", "cifs.verify.destination"):
            with self.subTest(operation=operation):
                report = self.report()
                success = next(item for item in report["operations"] if item["operation"] == operation)
                failed = {**success, "status": "failed", "exit_code": 1, "write_outcome": "unknown"}
                report["operations"].append(failed)
                with self.assertRaisesRegex(ValueError, operation):
                    self.validator.validate_report(report)
                if operation == "cifs.delete.source":
                    success = {**success, "write_outcome": "not_indicated"}
                report["operations"].append(dict(success))
                self.validator.validate_report(report)

    def test_cleanup_summary_requires_operation_evidence_with_a_known_outcome(self):
        for replacement in (None, {"write_outcome": "unknown"}, {"exit_code": 1}, {"status": "interrupted"}):
            with self.subTest(replacement=replacement):
                report = self.report()
                success = report["operations"].pop()
                if replacement is not None:
                    report["operations"].append({**success, **replacement})
                with self.assertRaisesRegex(ValueError, "cifs.delete.destination"):
                    self.validator.validate_report(report)

    def test_failed_cleanup_rejects_an_otherwise_passing_matrix(self):
        report = self.report()
        report["cleanup"]["hdfs"] = "failed"
        with self.assertRaisesRegex(ValueError, "cleanup must pass"):
            self.validator.validate_report(report)

    def test_failed_cifs_cleanup_rejects_full_matrix(self):
        report = self.report()
        report["cleanup"]["cifs"] = "failed"
        with self.assertRaisesRegex(ValueError, "cleanup must pass"):
            self.validator.validate_report(report)

    def test_full_gate_requires_verified_scope_and_both_cleanup_roles(self):
        for key, value, message in (
            ("scope", "../peer", "scope must identify this matrix run"),
            ("setup", "verify_failed", "ownership verification and cleanup"),
            ("cleanup", {"source": "passed", "destination": "failed"}, "cleanup on both endpoints"),
        ):
            with self.subTest(key=key):
                report = self.report()
                report["cifs"][key] = value
                with self.assertRaisesRegex(ValueError, message):
                    self.validator.validate_report(report)

    def test_dirty_source_cannot_claim_an_exact_commit(self):
        report = self.report()
        report["source_state"] = "dirty"
        with self.assertRaisesRegex(ValueError, "clean source"):
            self.validator.validate_report(report)

    def test_cancelled_64_cell_report_is_rejected_even_if_cleanup_passes(self):
        report = self.report()
        report["status"] = "cancelled"
        with self.assertRaisesRegex(ValueError, "must be completed"):
            self.validator.validate_report(report)

    def test_passing_cell_without_matching_readback_is_rejected(self):
        report = self.report()
        report["cells"][0]["readback_sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "matching fixture and readback"):
            self.validator.validate_report(report)

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
        report["cleanup"]["cifs"] = "not_attempted"
        cifs_operations = [item for item in report["operations"] if item["operation"].startswith("cifs.")]
        report["operations"] = [item for item in report["operations"] if item not in cifs_operations]
        report["cifs"] = {
            "scope": None,
            "run_id": None,
            "setup": "not_attempted",
            "cleanup": {"source": "not_attempted", "destination": "not_attempted"},
        }
        report["cells"] = [
            cell
            for cell in report["cells"]
            if cell["source_profile"] in active_profiles and cell["destination_profile"] in active_profiles
        ]
        self.validator.validate_report(report)
        report["operations"].append(cifs_operations[0])
        with self.assertRaisesRegex(ValueError, "deferred CIFS"):
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
        report["operations"] = [item for item in report["operations"] if not item["operation"].startswith("cifs.")]
        report["cleanup"]["cifs"] = "not_attempted"
        report["cifs"] = {
            "scope": None,
            "run_id": None,
            "setup": "not_attempted",
            "cleanup": {"source": "not_attempted", "destination": "not_attempted"},
        }
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
        self.assertIn("run-single-process-matrix.sh", LIFECYCLE_PATH.read_text())
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
        self.assertIn("TS-SINGLE 64-cell matrix", workflow)
        self.assertNotIn('TS_SINGLE_DEFER_CIFS: "true"', workflow)
        for workflow_text in (workflow, release):
            matrix = workflow_text.index("id: single_matrix")
            reconcile = workflow_text.index("id: reconcile_matrix", matrix)
            finalize = workflow_text.index("--report-only", reconcile)
            upload = workflow_text.index("Upload TS-SINGLE matrix evidence", finalize)
            self.assertIn("if: ${{ always() }}", workflow_text[:matrix])
            self.assertIn(f'/tmp/terrasync-lab-tests.lock tests/lab/{LIFECYCLE_PATH.name}', workflow_text[matrix:reconcile])
            self.assertIn("always()", workflow_text[reconcile:finalize])
            self.assertIn("steps.single_matrix.outputs.reconciled != 'true'", workflow_text[reconcile:finalize])
            self.assertIn("always()", workflow_text[upload:])
            self.assertIn("~/.local/state/terrasync-single-matrix/", workflow_text[upload:])
        for secret in ("LAB_CIFS_NIGHTLY_USERNAME", "LAB_CIFS_NIGHTLY_PASSWORD"):
            self.assertIn(f"${{{{ secrets.{secret} }}}}", workflow)
            self.assertIn(f"${{{{ secrets.{secret} }}}}", release)
        self.assertTrue(VALIDATOR_PATH.stat().st_mode & 0o111)
        self.assertTrue((ROOT / "tests" / "lab" / "run-single-process-matrix.sh").stat().st_mode & 0o111)

    def test_release_ref_preflight_rejects_legacy_producer_before_lab_setup(self):
        release = (ROOT / ".github" / "workflows" / "release-validation.yml").read_text()
        start = release.index("      - name: Verify release ref supports the matrix lifecycle")
        end = release.index("\n      - ", start + 1)
        self.assertLess(end, release.index("tests/lab/health-check.sh"))
        self.assertIn("ref: ${{ inputs.ref || github.sha }}", release[:start])
        command = textwrap.dedent(release[start:end].partition("        run: |\n")[2])
        with tempfile.TemporaryDirectory() as directory:
            lab = pathlib.Path(directory) / "tests" / "lab"
            lab.mkdir(parents=True)
            (lab / "run-single-process-matrix.sh").touch(mode=0o700)
            for stage in ("legacy", "missing reconciliation", "compatible"):
                if stage == "missing reconciliation":
                    shutil.copy2(LIFECYCLE_PATH, lab)
                if stage == "compatible":
                    (lab / "reconcile-single-process-matrix.sh").touch(mode=0o700)
                    (lab / VALIDATOR_PATH.name).touch(mode=0o700)
                result = subprocess.run(["bash", "-ec", command], cwd=directory, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0 if stage == "compatible" else 2, result.stdout + result.stderr)
                if stage != "compatible":
                    self.assertIn("The selected release ref does not support", result.stdout)

    @unittest.skipUnless(sys.platform.startswith("linux"), "lab locking uses Linux flock")
    def test_producer_and_reconciler_share_the_lock_and_preserve_producer_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            lifecycle = pathlib.Path(shutil.copy2(LIFECYCLE_PATH, root))
            producer = root / "run-single-process-matrix.sh"
            reconcile = root / "reconcile-single-process-matrix.sh"
            producer.write_text('#!/bin/bash\nexit "$PRODUCER_STATUS"\n')
            reconcile.write_text(textwrap.dedent('''\
                #!/bin/bash
                if flock --nonblock "$LOCK_FILE" true; then exit 10; fi
                printf '%s\\n' "$*" >> "$EVENTS"
            '''))
            for script in (producer, reconcile):
                script.chmod(0o700)
            lock, events, output = (root / name for name in ("lock", "events", "output"))
            for code, outcome in ((0, "success"), (7, "failure")):
                env = {
                    **os.environ,
                    "LOCK_FILE": str(lock),
                    "EVENTS": str(events),
                    "GITHUB_OUTPUT": str(output),
                    "PRODUCER_STATUS": str(code),
                }
                result = subprocess.run(
                    ["flock", "--no-fork", str(lock), str(lifecycle), "release-fixture", "--direct"],
                    env=env,
                    timeout=10,
                )
                self.assertEqual(result.returncode, code)
                self.assertEqual(events.read_text().splitlines()[-1], f"release-fixture --direct {outcome}")
                self.assertEqual(output.read_text().splitlines()[-2:], [f"producer_outcome={outcome}", "reconciled=true"])

    def test_hdfs_run_root_preserves_nameservice_case(self):
        command = (
            'source tests/lab/common.sh; '
            'LAB_HDFS_LOCATION="hdfs://HdfsCluster/"; '
            'LAB_HDFS_ADMIN_USER="ci@EXAMPLE"; hdfs_run_root nightly-case'
        )
        result = subprocess.run(["bash", "-c", command], cwd=ROOT, capture_output=True, text=True, check=True)
        self.assertIn("@HdfsCluster/", result.stdout)

    def test_hdfs_defaults_use_the_kerberos_ha_nameservice(self):
        common = (ROOT / "tests" / "lab" / "common.sh").read_text(encoding="utf-8")
        self.assertIn(
            "hdfs://hdfs%2Fterrasync-runner%40HDFS.LOCAL@hdfs-ha/",
            common,
        )
        self.assertIn("hdfs/terrasync-runner@HDFS.LOCAL", common)
        self.assertNotIn("hdfs://root@10.131.9.30:9000/", common)



MOCK_CARGO = """
import json, os, sys
if sys.argv[1] == "metadata":
    print(json.dumps({"target_directory": os.environ["MOCK_ROOT"] + "/target"}))
elif sys.argv[1] == "build":
    assert "LAB_CIFS_NIGHTLY_PASSWORD" not in os.environ
    open(os.environ["MOCK_ROOT"] + "/built", "w").close()
else:
    raise AssertionError(sys.argv)
"""

MOCK_PYTHON = """
import os, sys
if len(sys.argv) > 1 and sys.argv[1].endswith("/s3_helper.py"):
    sys.exit(0)
os.execv(REAL_PYTHON, [REAL_PYTHON, *sys.argv[1:]])
"""

MOCK_GIT = """
import os, pathlib, sys
args = sys.argv[1:]
mode = os.environ.get("MOCK_MODE", "")
if args == ["rev-parse", "HEAD"]:
    print("a" * 40)
elif args == ["--no-optional-locks", "status", "--porcelain", "--untracked-files=all"]:
    if mode == "dirty" or (mode == "dirty_build" and (pathlib.Path(os.environ["MOCK_ROOT"]) / "built").exists()):
        print("?? untracked-source.rs")
else:
    raise AssertionError(args)
"""

MOCK_DRIVER = r"""
import json, os, pathlib, shutil, signal, sys, time
root = pathlib.Path(os.environ["MOCK_ROOT"])
mode = os.environ.get("MOCK_MODE", "")
args = sys.argv[1:]
receipt = pathlib.Path(os.environ["TS_SINGLE_CIFS_RECEIPT"])
run_id = os.environ["TS_SINGLE_MATRIX_RUN_ID"]

def record(event):
    with (root / "events").open("a") as handle:
        handle.write(json.dumps(event) + "\n")

def diagnostic(phase, role, cause_class, write_outcome="not_indicated", cause=None):
    print("MATRIX_DIAGNOSTIC=" + json.dumps({
        "phase": phase, "role": role, "endpoint": "10.128.61.200" if role == "source" else "10.128.61.201",
        "class": cause_class, "operation": "Namespace", "provenance": "adapter_diagnostic" if cause else "typed",
        "cause": cause, "write_outcome": write_outcome,
    }), file=sys.stderr)
    print("MATRIX_DIAGNOSTIC=" + json.dumps({"phase": "redact-this-password"}), file=sys.stderr)
    print("redact-this-password", file=sys.stderr)
    sys.exit(1)

def save(state):
    receipt.write_text(json.dumps(state))

if args[0] == "--delete-hdfs-run":
    sys.exit(0)
elif len(args) == 3:
    action, actual_run, role = args
    assert actual_run == run_id
    record(args)
    if action == "--prepare-cifs-run":
        assert role == "source"
        save({"run_id": run_id, "scope": "mock-scope", "phase": "Prepared", "token": "b" * 32})
        sys.exit(0)
    state = json.loads(receipt.read_text())
    scope = root / "share" / state["scope"]
    marker = scope / ".owner"
    if action == "--claim-cifs-run":
        assert role == "source" and state["phase"] == "Prepared"
        state["phase"] = "Creating"
        save(state)
        if mode == "kill_scope_create":
            (root / "ready").touch()
            signal.pause()
        scope.mkdir(parents=True)
        if mode == "collision":
            (scope / "peer-data").write_text("foreign")
            diagnostic("create_scope", role, "Conflict")
        state["phase"] = "Created"
        save(state)
        marker.mkdir()
        if mode == "kill_claim":
            (root / "ready").touch()
            signal.pause()
        state["phase"] = "Claimed"
        save(state)
    elif action == "--verify-cifs-run":
        assert role == "destination" and marker.is_dir() and state["phase"] == "Claimed"
        if mode == "inaccessible":
            diagnostic("observe_marker", role, "PermissionDenied")
        state["phase"] = "Ready"
        save(state)
    elif action in ("--delete-cifs-run", "--confirm-cifs-run"):
        if mode == "cleanup_failure" and role == "source":
            diagnostic("connect", role, "Connectivity")
        if state["phase"] == "Creating":
            diagnostic("check_ownership", role, "Conflict", "unknown", "OutcomeUnknown")
        if not scope.is_dir():
            sys.exit(0)
        assert action == "--delete-cifs-run"
        if marker.exists():
            assert state["phase"] in ("Created", "Claimed", "Ready", "Finalizing")
            state["phase"] = "Finalizing"
            save(state)
            marker.rmdir()
            if mode == "kill_cleanup":
                (root / "ready").touch()
                signal.pause()
            if mode == "lost_delete_ack" and role == "source":
                diagnostic("delete_marker", role, "Protocol", "unknown", "OutcomeUnknown")
        assert state["phase"] == "Finalizing"
        scope.rmdir()
    else:
        raise AssertionError(args)
else:
    source, source_role, source_path, destination, destination_role, destination_path, job = args
    record(args)
    if "cifs_fas2750" in (source, destination):
        assert json.loads(receipt.read_text())["phase"] == "Ready"
        report = json.loads((receipt.parent / "evidence" / "matrix.json").read_text())
        assert report["cifs"]["setup"] == "passed"
    if mode == "cancel" and destination == "cifs_fas2750":
        def stop(*_):
            record(["transfer-stopped"])
            sys.exit(1)
        signal.signal(signal.SIGTERM, stop)
        (root / "ready").touch()
        signal.pause()
    if mode == "transfer_failure" and destination == "cifs_fas2750":
        diagnostic("Publish", destination_role, "Connectivity", "unknown")
    def payload(profile, role, path):
        if profile == "local":
            return pathlib.Path(path) / "payload.bin"
        if profile == "cifs_fas2750":
            return root / "storage" / profile / path / role / "payload.bin"
        return root / "storage" / f"{profile}-{role}" / path.replace(":", "_").lstrip("/") / "payload.bin"
    target = payload(destination, destination_role, destination_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(payload(source, source_role, source_path), target)
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "lab runner uses Linux process and tool semantics")
class MatrixRunnerTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        self.run_id = f"nightly-cifs-mock-{uuid.uuid4().hex}"
        self.lab_root = pathlib.Path("/tmp/terrasync-lab") / self.run_id
        self.addCleanup(shutil.rmtree, self.lab_root, ignore_errors=True)
        self.state = self.root / "state" / self.run_id
        self.report_path = self.state / "evidence" / "matrix.json"
        self.receipt = self.state / "cifs-receipt.json"
        self.install("bin/git", MOCK_GIT)
        self.install("bin/cargo", MOCK_CARGO)
        self.install("bin/python3", f"REAL_PYTHON = {sys.executable!r}\n" + MOCK_PYTHON)
        self.install("target/release/examples/single_process_matrix", MOCK_DRIVER)
        for name in ("core-site.xml", "hdfs-site.xml", "test.keytab"):
            (self.root / name).touch()
        self.env = {
            **os.environ,
            "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
            "RUNNER_TEMP": str(self.root),
            "TS_SINGLE_STATE_ROOT": str(self.state.parent),
            "MOCK_ROOT": str(self.root),
            "TS_SINGLE_DEFER_CIFS": "false",
            "LAB_HDFS_LOCATION": "hdfs://hdfs-fixture/",
            "LAB_HDFS_ADMIN_USER": "test",
            "LAB_HDFS_CONFIG_DIR": str(self.root),
            "LAB_HDFS_KEYTAB": str(self.root / "test.keytab"),
            "LAB_HDFS_BASE_PATH": "/tmp/hdfs-fixture",
            "LAB_S3_ACCESS_KEY": "mock",
            "LAB_S3_SECRET_KEY": "mock",
            "LAB_DXN_S3_ENDPOINT": "http://dxn-fixture",
            "LAB_DXN_S3_BUCKET": "dxn-fixture",
            "LAB_DXN_S3_ACCESS_KEY": "mock",
            "LAB_DXN_S3_SECRET_KEY": "mock",
            "LAB_CIFS_NIGHTLY_USERNAME": r"LIZYAD\tsnightly",
            "LAB_CIFS_NIGHTLY_PASSWORD": "redact-this-password",
        }

    def install(self, name, program):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"#!{sys.executable}\n" + program)
        path.chmod(0o700)

    def start(self, mode, *, reconcile=False, workflow_outcome="", reconcile_mode="--direct", lifecycle=False):
        script = "reconcile-single-process-matrix.sh" if reconcile else "run-single-process-matrix.sh"
        arguments = [str(ROOT / "tests" / "lab" / script), self.run_id]
        env = {**self.env, "MOCK_MODE": mode}
        if reconcile:
            arguments += [reconcile_mode, workflow_outcome]
        if lifecycle:
            arguments = ["flock", "--no-fork", str(self.root / "lab.lock"), str(LIFECYCLE_PATH), self.run_id]
            env["GITHUB_OUTPUT"] = str(self.root / "workflow-output")
        return subprocess.Popen(
            arguments,
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )

    def finish(self, process):
        try:
            stdout, stderr = process.communicate(timeout=80)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
        self.assertNotIn("redact-this-password", stdout + stderr)
        self.assertTrue(self.report_path.exists(), stdout + stderr)
        report = json.loads(self.report_path.read_text())
        for path in self.report_path.parent.glob("*.json"):
            self.assertNotIn("redact-this-password", path.read_text())
        events_file = self.root / "events"
        events = [json.loads(line) for line in events_file.read_text().splitlines()] if events_file.exists() else []
        return report, events, stdout, stderr

    def run_mock(self, mode, *, reconcile=True):
        with self.start(mode) as producer:
            report, events, stdout, stderr = self.finish(producer)
        if not reconcile:
            return producer, report, events, stdout, stderr
        with self.start(mode, reconcile=True, workflow_outcome="success" if producer.returncode == 0 else "failure") as process:
            report, events, reconcile_stdout, reconcile_stderr = self.finish(process)
        return process, report, events, stdout + reconcile_stdout, stderr + reconcile_stderr

    def test_64_cells_use_owned_paths_derived_from_cell_and_role(self):
        with self.start("", lifecycle=True) as process:
            report, events, stdout, stderr = self.finish(process)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertIn("64 independent cells", stdout)
        self.assertEqual((self.root / "workflow-output").read_text(), "producer_outcome=success\nreconciled=true\n")
        load_validator().validate_report(report)
        self.assertEqual(report["cifs"]["run_id"], self.run_id)
        self.assertEqual(events[:3], [
            ["--prepare-cifs-run", self.run_id, "source"],
            ["--claim-cifs-run", self.run_id, "source"],
            ["--verify-cifs-run", self.run_id, "destination"],
        ])
        self.assertIn([
            "cifs_fas2750", "source", "cifs_fas2750__cifs_fas2750",
            "cifs_fas2750", "destination", "cifs_fas2750__cifs_fas2750",
            "matrix-copy-cifs_fas2750__cifs_fas2750",
        ], events)
        self.assertEqual([event[2] for event in events if event[0] == "--delete-cifs-run"], ["source", "destination"])
        self.assertTrue(self.receipt.exists())
        self.assertFalse(self.lab_root.exists() and (self.lab_root / "single-process-matrix").exists())

    def test_legacy_credentials_do_not_satisfy_the_full_gate(self):
        self.env["LAB_CIFS_USERNAME"] = self.env.pop("LAB_CIFS_NIGHTLY_USERNAME")
        self.env["LAB_CIFS_PASSWORD"] = self.env.pop("LAB_CIFS_NIGHTLY_PASSWORD")
        process, report, events, _, stderr = self.run_mock("", reconcile=False)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("LAB_CIFS_NIGHTLY_USERNAME is required", stderr)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["cifs"]["setup"], "not_attempted")
        self.assertEqual(events, [])

    def test_dirty_checkout_is_rejected_before_remote_operations(self):
        process, report, events, _, stderr = self.run_mock("dirty", reconcile=False)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("clean source checkout", stderr)
        self.assertEqual(report["source_state"], "dirty")
        self.assertIsNone(report["exact_commit"])
        self.assertEqual(events, [])

    def test_source_change_during_build_clears_exact_commit_before_claim(self):
        process, report, events, _, stderr = self.run_mock("dirty_build", reconcile=False)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("source changed during the build", stderr)
        self.assertEqual(report["source_state"], "dirty")
        self.assertIsNone(report["exact_commit"])
        self.assertEqual(events, [])

    def test_collision_grants_no_delete_authority(self):
        process, report, events, _, stderr = self.run_mock("collision")
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("Conflict", stderr)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["cifs"]["setup"], "failed")
        self.assertEqual(report["cleanup"]["cifs"], "failed")
        self.assertEqual((self.root / "share/mock-scope/peer-data").read_text(), "foreign")
        self.assertEqual([event[0] for event in events], [
            "--prepare-cifs-run", "--claim-cifs-run", "--delete-cifs-run", "--delete-cifs-run",
        ])

    def test_inaccessible_second_endpoint_rejects_cells_and_preserves_diagnostic(self):
        _, report, _, _, stderr = self.run_mock("inaccessible")
        self.assertIn("PermissionDenied", stderr)
        self.assertEqual(report["cifs"]["setup"], "failed")
        self.assertEqual(report["cells"], [])
        self.assertEqual(report["cifs"]["cleanup"], {"source": "passed", "destination": "passed"})
        diagnostic = next(op["diagnostics"][0] for op in report["operations"] if op["operation"] == "cifs.verify.destination")
        self.assertEqual(
            (diagnostic["endpoint"], diagnostic["phase"], diagnostic["class"], diagnostic["write_outcome"]),
            ("10.128.61.201", "observe_marker", "PermissionDenied", "not_indicated"),
        )

    def test_cleanup_failure_is_retained_after_a_successful_reconciliation(self):
        process, report, _, _, stderr = self.run_mock("cleanup_failure")
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("Connectivity", stderr)
        self.assertEqual(report["cifs"]["cleanup"], {"source": "failed", "destination": "passed"})
        self.assertEqual(report["status"], "failed")
        with self.start("", reconcile=True) as retry:
            report, _, _, stderr = self.finish(retry)
        self.assertEqual(retry.returncode, 0, stderr)
        load_validator().validate_report(report)
        attempts = [op for op in report["operations"] if op["operation"] == "cifs.delete.source"]
        self.assertEqual([op["status"] for op in attempts], ["failed", "failed", "passed"])
        self.assertEqual(attempts[0]["write_outcome"], "not_indicated")
        self.assertTrue(self.receipt.exists())

    def test_missing_receipt_blocks_reconciliation_with_a_claim_in_evidence(self):
        self.run_mock("inaccessible", reconcile=False)
        self.receipt.unlink()
        with self.start("", reconcile=True, workflow_outcome="failure") as process:
            report, events, _, stderr = self.finish(process)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("requires the private receipt", stderr)
        self.assertEqual(report["cleanup"]["cifs"], "not_attempted")
        self.assertNotIn("--delete-cifs-run", [event[0] for event in events])

    def test_peer_cleanup_rechecks_source_after_lost_delete_ack(self):
        self.run_mock("inaccessible", reconcile=False)
        with self.start("lost_delete_ack", reconcile=True, workflow_outcome="failure") as process:
            report, events, _, stderr = self.finish(process)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(report["cleanup"]["cifs"], "passed")
        source = [op for op in report["operations"] if op["operation"] == "cifs.delete.source"]
        self.assertEqual([op["status"] for op in source], ["failed", "passed"])
        self.assertEqual(source[0]["write_outcome"], "unknown")
        self.assertEqual(source[0]["cause"], "Protocol")
        self.assertEqual(source[0]["diagnostics"][0]["cause"], "OutcomeUnknown")
        self.assertEqual(events[-1], ["--confirm-cifs-run", self.run_id, "source"])

    def test_failed_transfer_carries_phase_and_unknown_write_to_evidence(self):
        process, report, _, _, _ = self.run_mock("transfer_failure")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["status"], "failed")
        operation = next(op for op in report["operations"] if op["operation"] == "local__cifs_fas2750.copy")
        self.assertEqual(operation["write_outcome"], "unknown")
        self.assertEqual(operation["diagnostics"][0]["phase"], "Publish")
        cell = next(item for item in report["cells"] if item["gate_id"] == "TS-SINGLE/local__cifs_fas2750")
        self.assertEqual(cell["outcome"], "failed")
        self.assertIsNone(cell["readback_sha256"])

    def stop_when_ready(self, process, sig):
        deadline = time.monotonic() + 20
        try:
            while not (self.root / "ready").exists():
                self.assertIsNone(process.poll())
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            if sig == signal.SIGKILL:
                os.killpg(process.pid, sig)
            else:
                process.send_signal(sig)
            process.communicate(timeout=10)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)

    def test_graceful_cancel_reaps_active_transfer_before_reconciliation(self):
        with self.start("cancel", lifecycle=True) as producer:
            self.stop_when_ready(producer, signal.SIGTERM)
        self.assertEqual(producer.returncode, 143)
        self.assertEqual((self.root / "workflow-output").read_text(), "producer_outcome=cancelled\n")
        with self.start("", reconcile=True, workflow_outcome="cancelled") as process:
            report, events, _, stderr = self.finish(process)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(report["status"], "cancelled")
        self.assertEqual(report["cleanup"]["cifs"], "passed")
        transfer = next(op for op in report["operations"] if op["operation"] == "local__cifs_fas2750.copy")
        self.assertEqual((transfer["status"], transfer["cause"], transfer["write_outcome"]), ("failed", "Cancelled", "unknown"))
        self.assertLess(events.index(["transfer-stopped"]), next(i for i, event in enumerate(events) if event[0] == "--delete-cifs-run"))

    def test_kill_during_claim_recovers_from_durable_receipt_after_runtime_loss(self):
        with self.start("kill_claim") as producer:
            self.stop_when_ready(producer, signal.SIGKILL)
        self.assertTrue(self.receipt.exists())
        shutil.rmtree(self.lab_root)
        with self.start("", reconcile=True, workflow_outcome="cancelled") as process:
            report, _, _, stderr = self.finish(process)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(report["status"], "cancelled")
        self.assertEqual(report["cells"], [])
        self.assertEqual(report["cifs"]["setup"], "interrupted")
        self.assertEqual(report["cleanup"]["cifs"], "passed")
        self.assertFalse((self.root / "share/mock-scope").exists())
        claim = next(op for op in report["operations"] if op["operation"] == "cifs.claim.source")
        self.assertEqual(claim["write_outcome"], "unknown")

    def test_unconfirmed_scope_create_keeps_driver_and_can_never_pass_by_absence(self):
        with self.start("kill_scope_create") as producer:
            self.stop_when_ready(producer, signal.SIGKILL)
        scope = self.root / "share" / "mock-scope"
        self.assertFalse(scope.exists())
        for late_commit in (False, True):
            if late_commit:
                scope.mkdir(parents=True)
            with self.start("", reconcile=True, workflow_outcome="cancelled") as process:
                report, _, _, stderr = self.finish(process)
            self.assertNotEqual(process.returncode, 0, stderr)
            self.assertEqual(report["cifs"]["cleanup"], {"source": "failed", "destination": "failed"})
            self.assertTrue(self.receipt.exists())
            self.assertTrue((self.state / "driver").exists())
            self.assertEqual(scope.exists(), late_commit)
            deletes = [op for op in report["operations"] if op["operation"].startswith("cifs.delete.")]
            self.assertTrue(deletes and all(op["write_outcome"] == "unknown" for op in deletes))

    def test_report_only_finalizes_interrupted_operations_without_remote_calls(self):
        with self.start("kill_claim") as producer:
            self.stop_when_ready(producer, signal.SIGKILL)
        self.env["RUNNER_TEMP"] = str(self.root / "unavailable-runner-temp")
        with self.start("", reconcile=True, workflow_outcome="cancelled", reconcile_mode="--report-only") as process:
            report, events, _, stderr = self.finish(process)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(report["status"], "cancelled")
        self.assertEqual(report["cifs"]["setup"], "interrupted")
        self.assertEqual(report["cleanup"]["cifs"], "not_attempted")
        self.assertEqual([event[0] for event in events], ["--prepare-cifs-run", "--claim-cifs-run"])

    def test_kill_after_marker_delete_preserves_the_interrupted_attempt_and_retries(self):
        self.run_mock("inaccessible", reconcile=False)
        with self.start("kill_cleanup", reconcile=True, workflow_outcome="failure") as interrupted:
            self.stop_when_ready(interrupted, signal.SIGKILL)
        self.assertEqual(json.loads(self.receipt.read_text())["phase"], "Finalizing")
        with self.start("", reconcile=True) as process:
            report, _, _, stderr = self.finish(process)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["cleanup"]["cifs"], "passed")
        attempts = [op for op in report["operations"] if op["operation"] == "cifs.delete.source"]
        self.assertEqual([op["status"] for op in attempts], ["interrupted", "passed"])


if __name__ == "__main__":
    unittest.main()
