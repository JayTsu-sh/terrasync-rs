import fcntl
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

LAB = Path(__file__).resolve().parent / "lab"
RUN_ID = "nightly-hdfs-test"
HDFS_SCOPE = f"nightly-{hashlib.sha256(RUN_ID.encode()).hexdigest()}-e2e"
HDFS_TARGET = {
    "base_path": "/fixtures/terrasync-nightly",
    "authority": "hdfs-ha",
    "routing_fingerprint": "test-route",
}


def load_controller():
    spec = importlib.util.spec_from_file_location("hdfs_lab", LAB / "hdfs_lab.py")
    controller = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller)
    return controller


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


class FakeManager:
    def __init__(self, unit, lab_lock):
        self.unit = unit
        self.lab_lock = lab_lock
        self.state = {
            "ActiveState": "inactive",
            "LoadState": "loaded",
            "FileDescriptorStorePreserve": "yes",
            "Job": "",
            "NFileDescriptorStore": "0",
            "ControlGroup": "",
            "InvocationID": "",
        }
        self.commands = []
        self.on_start = None
        self.before_clean = None
        self.lock_stream = None

    def hold_lock(self):
        self.lock_stream = self.lab_lock.open("a+")
        fcntl.flock(self.lock_stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.state["NFileDescriptorStore"] = "1"

    def close(self):
        if self.lock_stream is not None:
            self.lock_stream.close()
            self.lock_stream = None

    def __call__(self, command, *arguments, required=True):
        if command == "show":
            if arguments[0] != self.unit:
                raise AssertionError(arguments)
            properties = "\n".join(
                f"{key}={value}" for key, value in self.state.items()
            )
            return subprocess.CompletedProcess((command, *arguments), 0, properties, "")

        expected = {
            "start": ("--no-block", self.unit),
            "stop": ("--no-block", self.unit),
            "reset-failed": (self.unit,),
            "clean": ("--what=fdstore", self.unit),
        }
        if arguments != expected.get(command):
            raise AssertionError((command, arguments))
        self.commands.append(command)
        if command == "start" and self.on_start is not None:
            self.on_start()
        if command == "reset-failed":
            self.state["ActiveState"] = "inactive"
        if command == "clean":
            if self.before_clean is not None:
                self.before_clean()
            self.close()
            self.state["NFileDescriptorStore"] = "0"
        return subprocess.CompletedProcess((command, *arguments), 0, "", "")


class HdfsLabTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.controller = load_controller()
        self.state = self.root / "state"
        self.attempt = self.state / RUN_ID
        self.work = self.attempt / "work"
        self.scratch_root = self.root / "scratch"
        self.scratch = self.scratch_root / RUN_ID
        self.output = self.root / "output"
        self.certification = self.output / f"{RUN_ID}-hdfs.json"
        self.diagnostics = self.output / f"{RUN_ID}-hdfs-cleanup.json"
        self.lab_lock = self.root / "lab.lock"
        self.cgroup_root = self.root / "cgroup"
        unit = self.controller.unit(RUN_ID)
        self.group = f"/lab.slice/{unit}"
        self.events = self.cgroup_root / self.group.lstrip("/") / "cgroup.events"
        self.manager = FakeManager(unit, self.lab_lock)
        self.addCleanup(self.manager.close)
        self.candidate = {
            "terrasync_candidate": "a" * 40,
            "data_mover_candidate": "b" * 40,
        }

        self.now = 1000
        clock = SimpleNamespace(
            time=lambda: self.now, monotonic=lambda: self.now, sleep=self.advance
        )
        self.enterContext(
            patch.multiple(
                self.controller,
                STATE=self.state,
                SCRATCH=self.scratch_root,
                INSTALL=LAB,
                LAB_LOCK=self.lab_lock,
                CGROUP=self.cgroup_root,
                BOOT="current-boot",
                systemctl=self.manager,
                service_commands=self.service_commands,
                source_identity=lambda: self.candidate,
                time=clock,
            )
        )
        self.target = self.root / "target"
        for binary in self.controller.RELEASE_BINARIES:
            path = self.target / "release" / binary
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
            path.chmod(0o700)
        self.receipt = self.target / self.controller.BUILD_RECEIPT
        self.enterContext(
            patch.dict(os.environ, {"CARGO_TARGET_DIR": str(self.target)})
        )
        self.write_build_receipt()
        self.addCleanup(os.umask, os.umask(0o077))
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            self.addCleanup(signal.signal, sig, signal.getsignal(sig))

    def advance(self, seconds):
        self.now += seconds

    def cli(self, *arguments):
        with patch.object(sys, "argv", ["hdfs_lab.py", *arguments]):
            return self.controller.main()

    def reconcile(self, *options):
        return self.cli("reconcile", RUN_ID, "--output", str(self.output), *options)

    def record(self):
        return read_json(self.attempt / "state.json")

    def write_build_receipt(self):
        artifacts = [
            self.cargo_artifact(self.target / "release" / binary)
            for binary in self.controller.RELEASE_BINARIES
        ]
        with patch.object(
            self.controller.subprocess, "run", side_effect=artifacts
        ) as cargo:
            self.assertEqual(self.cli("build"), 0)
        self.assertEqual(cargo.call_count, 2)

    @staticmethod
    def cargo_artifact(path):
        return subprocess.CompletedProcess(
            "cargo",
            0,
            json.dumps({"reason": "compiler-artifact", "executable": str(path)}),
        )

    def worker_state(self, *, populated, active="active", **changes):
        record = self.record()
        record.update(invocation="invocation-1", cgroup=self.group, **changes)
        write_json(self.attempt / "state.json", record)
        self.manager.state.update(
            ActiveState=active,
            ControlGroup=self.group,
            InvocationID=record["invocation"],
        )
        self.set_population(populated)

    def set_population(self, populated):
        self.events.parent.mkdir(parents=True, exist_ok=True)
        self.events.write_text(
            f"populated {int(populated)}\nfrozen 0\n", encoding="utf-8"
        )

    def write_hdfs_cleanup(self, status):
        write_json(
            self.work / "hdfs-cleanup.json",
            {
                "schema_version": 1,
                "run_id": RUN_ID,
                "status": status,
                "scope": HDFS_SCOPE,
                "target": HDFS_TARGET,
            },
        )

    def finish_worker(self, *, test="passed", hdfs="passed"):
        self.worker_state(
            populated=False,
            active="inactive",
            test=test,
            stage="hdfs_gate",
            service_exit={
                "service_result": "success",
                "exit_code": "exited",
                "exit_status": "0",
            },
        )
        record = self.record()
        self.write_hdfs_cleanup(hdfs)
        matrix = read_json(LAB / "evidence-matrix.json")
        gate = {
            "schema_version": 1,
            "run_id": RUN_ID,
            "terrasync_candidate": record["terrasync_candidate"],
            "data_mover_candidate": record["data_mover_candidate"],
            "gates": dict.fromkeys(matrix["required_gates"], "pass"),
            "remote_role_isolation": matrix["required_role_isolation"],
            "cleanup": {"hdfs": "passed"},
            "payload_sha256": hashlib.sha256(
                f"terrasync-hdfs-{RUN_ID}\n".encode()
            ).hexdigest(),
        }
        write_json(self.work / "gate-report.json", gate)
        return gate

    def assert_withheld(self):
        diagnostic = read_json(self.diagnostics)
        self.assertEqual(diagnostic["local_cleanup"], "passed")
        self.assertEqual(diagnostic["certification"], "withheld")
        self.assertFalse(self.certification.exists())
        return diagnostic

    def service_commands(self, run_id):
        return [
            [
                [
                    "/usr/bin/python3",
                    [
                        "/usr/bin/python3",
                        "-E",
                        "-s",
                        "-B",
                        str(LAB / "hdfs_lab.py"),
                        operation,
                        run_id,
                    ],
                    False,
                ]
            ]
            for operation in ("worker", "worker-exit")
        ]

    def test_run_rejects_additional_service_command_before_staging(self):
        commands = self.service_commands(RUN_ID)
        commands[1].insert(0, ["/usr/bin/true", ["/usr/bin/true"], False])
        with (
            patch.object(self.controller, "service_commands", return_value=commands),
            self.assertRaisesRegex(RuntimeError, "incompatible"),
        ):
            self.controller.run(RUN_ID, self.output)
        self.assertFalse(self.attempt.exists())
        diagnostic = read_json(self.diagnostics)
        self.assertEqual(diagnostic["stage"], "admission_preflight")
        self.assertEqual(diagnostic["reason"], "incompatible_service_commands")
        self.assertEqual(diagnostic["test"], "failed")
        self.assertEqual(diagnostic["hdfs_cleanup"], "not_attempted")
        self.assertEqual(diagnostic["certification"], "withheld")
        self.assertFalse(self.certification.exists())

    def test_cancel_during_preflight_keeps_diagnostic_without_admission(self):
        def interrupt(run_id):
            signal.raise_signal(signal.SIGTERM)
            return self.service_commands(run_id)

        with patch.object(self.controller, "service_commands", side_effect=interrupt):
            self.assertEqual(self.controller.run(RUN_ID, self.output), 143)
        self.assertFalse(self.attempt.exists())
        self.assertEqual(self.manager.commands, [])
        diagnostic = read_json(self.diagnostics)
        self.assertEqual(diagnostic["stage"], "admission_preflight")
        self.assertEqual(diagnostic["test"], "cancelled")
        self.assertEqual(diagnostic["hdfs_cleanup"], "not_attempted")

    def test_running_attempt_is_only_observed_by_a_same_id_caller(self):
        self.controller.claim(RUN_ID)
        self.worker_state(populated=True, test="running")
        original = self.record()
        self.advance(10)

        def cancel_observer(seconds):
            self.advance(seconds)
            signal.raise_signal(signal.SIGTERM)

        with patch.object(self.controller.time, "sleep", side_effect=cancel_observer):
            self.assertEqual(self.controller.run(RUN_ID, self.output), 1)
        self.assertEqual(self.record(), original)
        self.assertEqual(read_json(self.diagnostics), original)
        self.assertEqual(self.manager.commands, [])
        self.assertFalse(self.certification.exists())

    def test_run_requires_build_receipt_before_staging(self):
        self.receipt.unlink()
        self.manager.on_start = self.finish_worker
        with self.assertRaisesRegex(RuntimeError, "build receipt"):
            self.controller.run(RUN_ID, self.output)
        self.assertFalse(self.attempt.exists())
        self.assertEqual(self.manager.commands, [])
        self.assertEqual(read_json(self.diagnostics)["stage"], "staging")

    def test_run_rejects_receipt_from_another_checkout(self):
        self.candidate["terrasync_candidate"] = "c" * 40
        with self.assertRaisesRegex(RuntimeError, "another checkout"):
            self.controller.run(RUN_ID, self.output)
        self.assertFalse(self.attempt.exists())
        self.assertEqual(self.manager.commands, [])

    def test_run_rejects_changed_binary_and_removes_staged_files(self):
        (self.target / "release/terrasync").write_bytes(b"other compilation")
        with self.assertRaisesRegex(RuntimeError, "staged terrasync"):
            self.controller.run(RUN_ID, self.output)
        self.assertFalse(self.attempt.exists())
        self.assertEqual(self.manager.commands, [])

    def test_build_does_not_leave_receipt_after_failed_rebuild(self):
        with (
            patch.object(
                self.controller.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, "cargo"),
            ),
            self.assertRaises(subprocess.CalledProcessError),
        ):
            self.cli("build")
        self.assertFalse(self.receipt.exists())

    def test_build_rejects_cargo_output_in_another_target_layout(self):
        artifact = self.cargo_artifact(
            self.target / "x86_64-unknown-linux-gnu/release/terrasync"
        )
        with (
            patch.object(self.controller.subprocess, "run", return_value=artifact),
            self.assertRaisesRegex(RuntimeError, "expected host executable"),
        ):
            self.cli("build")
        self.assertTrue((self.target / "release/terrasync").exists())
        self.assertFalse(self.receipt.exists())

    def test_observer_does_not_reexport_a_certification_without_build_proof(self):
        self.manager.on_start = self.finish_worker
        self.assertEqual(self.controller.run(RUN_ID, self.output), 0)
        record = self.record()
        record.pop("binary_sha256")
        write_json(self.attempt / "state.json", record)

        self.assertEqual(self.controller.run(RUN_ID, self.output), 1)
        self.assertFalse(self.certification.exists())
        self.assertEqual(self.record(), record)
        diagnostic = self.assert_withheld()
        self.assertEqual(
            diagnostic["certification_reason"], "build_provenance_unavailable"
        )

    def test_same_run_invokes_once_after_start_response_is_lost(self):
        def start():
            self.worker_state(populated=True, test="running")
            raise subprocess.TimeoutExpired("systemctl start", 8)

        def finish_after_observation(seconds):
            self.advance(seconds)
            self.finish_worker()

        self.manager.on_start = start
        with patch.object(
            self.controller.time, "sleep", side_effect=finish_after_observation
        ) as wait:
            self.assertEqual(self.controller.run(RUN_ID, self.output), 0)
            wait.assert_called_once()
        published = read_json(self.certification)
        self.certification.unlink()
        self.assertEqual(self.controller.run(RUN_ID, self.output), 0)
        self.assertEqual(self.manager.commands, ["start"])
        self.assertEqual(read_json(self.certification), published)
        self.assertEqual(self.record()["invocation"], "invocation-1")
        self.assertFalse(self.work.exists())

    def test_worker_refuses_second_invocation_for_claimed_run(self):
        self.controller.claim(RUN_ID)
        read_text = Path.read_text

        def read_cgroup(path, *args, **kwargs):
            if path == Path("/proc/self/cgroup"):
                return f"0::{self.group}\n"
            return read_text(path, *args, **kwargs)

        with (
            patch.dict(
                sys.modules, {"hdfs_lab_clickhouse": SimpleNamespace(start=None)}
            ),
            patch.object(Path, "read_text", read_cgroup),
            patch.object(
                self.controller, "receive_lab_lock", return_value=False
            ) as acquire,
        ):
            with patch.dict(os.environ, {"INVOCATION_ID": "invocation-1"}):
                self.assertEqual(self.cli("worker", RUN_ID), 143)
            with patch.dict(os.environ, {"INVOCATION_ID": "invocation-2"}):
                self.assertEqual(self.cli("worker", RUN_ID), 1)
            acquire.assert_called_once()
        self.assertEqual(self.record()["invocation"], "invocation-1")

    def test_sweep_recovers_abandoned_caller_after_lease_expires(self):
        self.controller.claim(RUN_ID)
        self.worker_state(populated=True, test="running")

        self.assertEqual(self.cli("sweep"), 0)
        self.assertEqual(self.record()["admission"], "open")
        self.assertEqual(self.manager.commands, [])

        self.advance(self.controller.LEASE + 1)
        self.assertEqual(self.cli("sweep"), 0)
        self.assertEqual(self.record()["admission"], "closed")
        self.assertTrue(self.record()["cancel_requested"])
        self.assertEqual(self.manager.commands, ["stop"])
        self.assertTrue(self.work.is_dir())

        self.manager.state["ActiveState"] = "inactive"
        self.set_population(False)
        self.assertEqual(self.cli("sweep"), 0)
        self.assertEqual(self.reconcile(), 0)
        self.assertEqual(self.assert_withheld()["test"], "cancelled")
        self.assertFalse(self.work.exists())

    def test_sweep_recovers_previous_boot_without_waiting_for_current_lease(self):
        with patch.object(self.controller, "BOOT", "previous-boot"):
            self.controller.claim(RUN_ID)
            self.worker_state(populated=True, test="running")
        self.events.unlink()
        self.manager.state.update(
            ActiveState="inactive", ControlGroup="", InvocationID=""
        )
        self.scratch.mkdir(parents=True)
        unrelated = self.scratch_root / "another-attempt"
        unrelated.mkdir()

        self.assertEqual(self.cli("sweep"), 0)
        self.assertEqual(self.reconcile(), 0)
        diagnostic = self.assert_withheld()
        self.assertEqual(diagnostic["boot_id"], "previous-boot")
        self.assertEqual(diagnostic["test"], "cancelled")
        self.assertEqual(self.manager.commands, [])
        self.assertFalse(self.work.exists())
        self.assertFalse(self.scratch.exists())
        self.assertTrue(unrelated.is_dir())

    def test_reconcile_preserves_files_and_lock_until_cgroup_drains(self):
        self.controller.claim(RUN_ID)
        self.worker_state(populated=True, active="failed", test="running")
        self.manager.state["ControlGroup"] = ""
        self.manager.hold_lock()
        self.scratch.mkdir(parents=True)
        files = (self.work / "worker-state", self.scratch / "clickhouse-data")
        for path in files:
            path.write_bytes(b"owned by the running cgroup")

        self.assertEqual(self.reconcile(), 1)
        self.assertEqual(read_json(self.diagnostics)["local_cleanup"], "pending")
        self.assertEqual(self.manager.commands, [])
        self.events.unlink()
        self.assertEqual(self.reconcile(), 1)
        self.assertEqual(self.manager.commands, [])
        for path in files:
            self.assertEqual(path.read_bytes(), b"owned by the running cgroup")
        with self.lab_lock.open("a+") as contender:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def cleaned_before_unlock():
                self.assertFalse(self.work.exists())
                self.assertFalse(self.scratch.exists())
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

            self.manager.before_clean = cleaned_before_unlock
            self.set_population(False)
            self.assertEqual(self.reconcile(), 0)
            self.assertEqual(self.manager.commands, ["reset-failed", "clean"])
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertEqual(self.assert_withheld()["test"], "unknown")

    def test_cancelled_run_with_passing_gate_never_publishes_success(self):
        def start():
            self.finish_worker()
            signal.raise_signal(signal.SIGTERM)

        self.manager.on_start = start
        self.output.mkdir()
        write_json(self.certification, {"stale": True})
        self.assertEqual(self.controller.run(RUN_ID, self.output), 143)
        diagnostic = self.assert_withheld()
        self.assertEqual(diagnostic["test"], "cancelled")
        self.assertEqual(diagnostic["hdfs_cleanup"], "passed")

    def test_signal_during_final_cleanup_never_publishes_success(self):
        self.manager.on_start = self.finish_worker
        self.manager.hold_lock()
        self.manager.before_clean = lambda: signal.raise_signal(signal.SIGTERM)

        status = self.controller.run(RUN_ID, self.output)
        self.assertEqual((status, self.certification.exists()), (143, False))
        self.assertEqual(self.assert_withheld()["test"], "cancelled")

    def test_unknown_hdfs_cleanup_survives_recovery_without_retry_or_delete(self):
        self.controller.claim(RUN_ID)
        self.worker_state(
            populated=False,
            active="failed",
            test="running",
            stage="hdfs_gate",
            hdfs_cleanup="unknown",
        )
        self.write_hdfs_cleanup("unknown")

        # 恢复阶段不能再次启动 gate，也不能为未知结果执行远端删除。
        with patch.object(
            subprocess, "Popen", side_effect=AssertionError("unexpected subprocess")
        ):
            self.assertEqual(self.reconcile(), 1)
            self.assertEqual(self.cli("sweep"), 0)
            self.assertEqual(self.controller.run(RUN_ID, self.output), 1)
        self.assertEqual(self.manager.commands, ["reset-failed"])
        diagnostic = self.assert_withheld()
        self.assertEqual(diagnostic["test"], "unknown")
        self.assertEqual(diagnostic["hdfs_cleanup"], "unknown")
        self.assertEqual(diagnostic["hdfs_scope"], HDFS_SCOPE)
        self.assertEqual(diagnostic["hdfs_target"], HDFS_TARGET)
        self.assertFalse(self.work.exists())

    def test_certification_publishes_matching_gate_with_both_cleanups(self):
        def complete():
            self.finish_worker()
            self.scratch.mkdir(parents=True)

        self.manager.on_start = complete

        self.assertEqual(self.controller.run(RUN_ID, self.output), 0)
        gate = read_json(self.attempt / "gate.json")
        self.assertFalse(self.work.exists())
        self.assertFalse(self.scratch.exists())
        self.assertEqual(read_json(self.attempt / "gate.json"), gate)
        self.assertEqual(
            read_json(self.certification),
            {
                **gate,
                "cleanup": {"hdfs": "passed", "clickhouse": "passed"},
            },
        )
        diagnostic = read_json(self.diagnostics)
        self.assertEqual(diagnostic["local_cleanup"], "passed")
        self.assertEqual(diagnostic["hdfs_cleanup"], "passed")
        self.assertEqual(diagnostic["certification"], "published")

    def assert_rejects_evidence(self, filename, changes):
        def finish_with_bad_evidence():
            self.finish_worker()
            path = self.work / filename
            if changes is None:
                path.unlink()
            else:
                report = read_json(path)
                report.update(changes)
                write_json(path, report)

        self.manager.on_start = finish_with_bad_evidence
        status = self.controller.run(RUN_ID, self.output)
        self.assertEqual((status, self.certification.exists()), (1, False))
        self.assert_withheld()

    def test_diagnostics_cannot_finalize_a_lost_caller(self):
        self.controller.claim(RUN_ID)
        self.finish_worker()
        self.assertEqual(self.reconcile(), 0)
        self.assertFalse(self.certification.exists())
        self.assertEqual(self.controller.run(RUN_ID, self.output), 1)
        self.assertFalse(self.certification.exists())

    def test_worker_pass_report_cannot_override_failed_service(self):
        self.controller.claim(RUN_ID)
        self.finish_worker()
        self.manager.state.update(ActiveState="failed")
        self.assertEqual(self.reconcile(), 0)
        self.assertEqual(self.assert_withheld()["test"], "unknown")

    def test_cancel_after_local_recovery_withholds_pending_certification(self):
        self.controller.claim(RUN_ID)
        self.finish_worker()
        self.assertEqual(self.cli("sweep"), 0)
        self.assertEqual(self.reconcile("--cancel"), 0)
        self.assertEqual(self.assert_withheld()["test"], "cancelled")

    def test_reaper_removes_unpublished_staging(self):
        self.work.mkdir(parents=True)
        (self.work / "terrasync").write_bytes(b"staged only")
        self.assertEqual(self.cli("sweep"), 0)
        self.assertFalse(self.attempt.exists())
        self.assertFalse(self.manager.commands)

    def test_interrupted_work_removal_preserves_hdfs_uncertainty(self):
        self.controller.claim(RUN_ID)
        self.finish_worker(test="failed", hdfs="unknown")
        original = self.controller.shutil.rmtree

        def interrupt(path):
            if path == self.work:
                (self.work / "hdfs-cleanup.json").unlink()
                raise OSError("interrupted removing workspace")
            return original(path)

        with (
            patch.object(self.controller.shutil, "rmtree", side_effect=interrupt),
            self.assertRaises(OSError),
        ):
            self.reconcile()
        self.assertEqual(self.reconcile(), 1)
        self.assertEqual(self.assert_withheld()["hdfs_cleanup"], "unknown")

    def test_certification_requires_gate_report(self):
        self.assert_rejects_evidence("gate-report.json", None)

    def test_certification_requires_matching_gate_run(self):
        self.assert_rejects_evidence("gate-report.json", {"run_id": "other-run"})

    def test_certification_requires_matching_terrasync_revision(self):
        self.assert_rejects_evidence(
            "gate-report.json", {"terrasync_candidate": "0" * 40}
        )

    def test_certification_requires_matching_data_mover_revision(self):
        self.assert_rejects_evidence(
            "gate-report.json", {"data_mover_candidate": "0" * 40}
        )

    def test_certification_requires_passed_hdfs_cleanup(self):
        self.assert_rejects_evidence("hdfs-cleanup.json", {"status": "failed"})

    def test_certification_requires_matching_hdfs_cleanup_run(self):
        self.assert_rejects_evidence("hdfs-cleanup.json", {"run_id": "other-run"})

    def test_certification_requires_all_required_gates_to_pass(self):
        matrix = read_json(LAB / "evidence-matrix.json")
        gates = dict.fromkeys(matrix["required_gates"], "pass")
        gates["single_process_hdfs_to_hdfs"] = "failed"
        self.assert_rejects_evidence("gate-report.json", {"gates": gates})


class HdfsGateTest(unittest.TestCase):
    def run_gate(self, cli_script, matrix_script=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = root / "attempt"
            attempt.mkdir(mode=0o700)
            state = root / "state"
            state.mkdir(mode=0o700)
            config = root / "config"
            config.mkdir()
            for name in ("core-site.xml", "hdfs-site.xml"):
                (config / name).touch()
            keytab = root / "fixture.keytab"
            keytab.touch()
            log = root / "calls"
            cli = root / "terrasync"
            cli.write_text(cli_script, encoding="utf-8")
            cli.chmod(0o700)
            matrix = root / "matrix"
            matrix.write_text(matrix_script or cli_script, encoding="utf-8")
            matrix.chmod(0o700)
            candidate = "a" * 40
            result = subprocess.run(
                [str(LAB / "run-hdfs-e2e.sh"), "nightly-negative-timeout"],
                env=os.environ
                | {
                    "LAB_HDFS_MANAGED_DIR": str(attempt),
                    "LAB_HDFS_STATE_DIR": str(state),
                    "LAB_HDFS_CONFIG_DIR": str(config),
                    "LAB_HDFS_KEYTAB": str(keytab),
                    "LAB_HDFS_LOCATION": "hdfs://lab@hdfs-fixture/",
                    "LAB_HDFS_ADMIN_USER": "lab",
                    "LAB_HDFS_BASE_PATH": "/tmp/lab-fixture",
                    "LAB_HDFS_FINGERPRINT": "fixture",
                    "LAB_CLICKHOUSE_DSN": "http://127.0.0.1:18123",
                    "LAB_CLICKHOUSE_USER": "fixture",
                    "LAB_CLICKHOUSE_PASSWORD": "test-password",
                    "LAB_PROBE_LOG": str(log),
                    "TERRASYNC_BINARY": str(cli),
                    "TERRASYNC_SINGLE_MATRIX_BINARY": str(matrix),
                    "TERRASYNC_CANDIDATE": candidate,
                    "DATA_MOVER_CANDIDATE": candidate,
                },
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            return (
                result,
                log.read_text().splitlines(),
                read_json(state / "hdfs-cleanup.json"),
                (state / "gate-report.json").exists(),
            )

    def test_unexpected_negative_check_exit_stops_before_hdfs_writes(self):
        result, calls, cleanup, certified = self.run_gate(
            "#!/bin/sh\n"
            'echo "$3" >> "$LAB_PROBE_LOG"\n'
            "echo 'Error: Configuration error: HDFS Source role configuration is required' >&2\n"
            "exit 124\n"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "did not produce the expected rejection (exit 124)", result.stderr
        )
        self.assertEqual(calls, ["scan"])
        self.assertEqual(cleanup["status"], "not_attempted")
        self.assertFalse(certified)

    def test_killed_receiver_leaves_hdfs_cleanup_unknown(self):
        result, calls, cleanup, certified = self.run_gate(
            """#!/bin/sh
echo "$3" >> "$LAB_PROBE_LOG"
case "$3" in
  scan)
    case "$2" in
      */missing.toml)
        echo 'Error: Configuration error: HDFS Source role configuration is required' >&2 ;;
      */invalid.toml)
        echo 'Error: failed to connect Hdfs backend: HDFS get storage root failed (Authentication): authentication failed' >&2 ;;
    esac
    exit 1 ;;
  sync)
    case " $* " in *" --remote "*) exit 1 ;; esac ;;
  integrity-check) printf 'Checked: 1\\nAll Passed ✓\\n' ;;
  serve)
    trap '' TERM
    printf ready > "$7"
    while :; do sleep 0.1; done ;;
esac
""",
            """#!/bin/sh
if [ "$1" = --delete-hdfs-run ]; then
  echo delete >> "$LAB_PROBE_LOG"
else
  printf 'terrasync-hdfs-nightly-negative-timeout\\n' > "$6/payload.bin"
fi
""",
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(calls.count("sync"), 3, calls)
        self.assertIn("serve", calls)
        self.assertNotIn("delete", calls)
        self.assertEqual(cleanup["status"], "unknown")
        self.assertEqual(cleanup["reason"], "receiver_terminated")
        self.assertFalse(certified)


if __name__ == "__main__":
    unittest.main()
