"""Own the HDFS certification attempt outside the Actions step lifetime."""

import argparse
import contextlib
import ctypes
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
INSTALL = Path("/opt/terrasync-hdfs-managed")
STATE = Path.home() / ".local/state/terrasync-hdfs-managed"
LAB_LOCK = Path("/tmp/terrasync-lab-tests.lock")
CLICKHOUSE = Path("/usr/bin/clickhouse")
SCRATCH = Path("/run/terrasync-hdfs-managed")
CGROUP = Path("/sys/fs/cgroup")
RUN_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")
BOOT = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
HEARTBEAT = 5
LEASE = 90
ADMISSION_LIMIT = 30
LOCK_LIMIT = 120
ATTEMPT_LIMIT = 2400
TERMINAL = {"passed", "failed", "cancelled", "unknown"}
RELEASE_BINARIES = ("terrasync", "examples/single_process_matrix")
BUILD_RECEIPT = "terrasync-lab-build.json"


class AdmissionError(RuntimeError):
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def atomic_json(path, data, *, can_commit=lambda: True):
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if not can_commit():
            return False
        temporary.replace(path)
        sync_directory(path.parent)
        return True
    finally:
        temporary.unlink(missing_ok=True)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_optional(path):
    try:
        return read_json(path)
    except FileNotFoundError:
        return None


def unit(run_id):
    # The instance argument is constrained by RUN_ID and is never a path.
    return f"terrasync-hdfs-attempt@{run_id}.service"


def systemctl(*arguments, required=True):
    result = subprocess.run(
        ["/usr/bin/systemctl", "--user", *arguments],
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )
    if required and result.returncode:
        raise RuntimeError(f"systemd {arguments[0]} failed: {result.stderr.strip()}")
    return result


def unit_state(run_id):
    result = systemctl(
        "show",
        unit(run_id),
        "-p",
        "ActiveState",
        "-p",
        "LoadState",
        "-p",
        "Job",
        "-p",
        "NFileDescriptorStore",
        "-p",
        "FileDescriptorStorePreserve",
        "-p",
        "ControlGroup",
        "-p",
        "InvocationID",
    )
    return dict(
        line.split("=", 1) for line in result.stdout.splitlines() if "=" in line
    )


def service_commands(run_id, check_cancelled):
    bus = ["/usr/bin/busctl", "--user", "--json=short"]
    service = "org.freedesktop.systemd1"
    raw_path = subprocess.check_output(
        [
            *bus,
            "call",
            service,
            "/org/freedesktop/systemd1",
            f"{service}.Manager",
            "LoadUnit",
            "s",
            unit(run_id),
        ],
        text=True,
        timeout=8,
    )
    check_cancelled()
    (path,) = json.loads(raw_path)["data"]
    raw_commands = subprocess.check_output(
        [
            *bus,
            "get-property",
            service,
            path,
            f"{service}.Service",
            "ExecStart",
            "ExecStopPost",
        ],
        text=True,
        timeout=8,
    )
    check_cancelled()
    return [json.loads(line)["data"] for line in raw_commands.splitlines()]


def validate_service_commands(run_id, check_cancelled):
    expected = [
        [
            [
                "/usr/bin/python3",
                [
                    "/usr/bin/python3",
                    "-E",
                    "-s",
                    "-B",
                    str(INSTALL / "hdfs_lab.py"),
                    operation,
                    run_id,
                ],
                False,
            ]
        ]
        for operation in ("worker", "worker-exit")
    ]
    actual = [
        [command[:3] for command in entries]
        for entries in service_commands(run_id, check_cancelled)
    ]
    if actual != expected:
        raise AdmissionError(
            "incompatible_service_commands",
            "the runner has incompatible HDFS service commands",
        )


def quiescent(state, record):
    if state.get("ActiveState") not in {"inactive", "failed"} or state.get("Job"):
        return False
    if record["boot_id"] != BOOT:
        return not state.get("ControlGroup")
    group = record.get("cgroup")
    if not group:
        return not state.get("ControlGroup")
    # The worker saves its kernel-provided cgroup before opening any resource.
    path = CGROUP / group.removeprefix("/") / "cgroup.events"
    try:
        return "populated 0" in path.read_text().splitlines()
    except FileNotFoundError:
        return not path.parent.exists()


@contextlib.contextmanager
def locked():
    STATE.mkdir(parents=True, mode=0o700, exist_ok=True)
    st = STATE.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise RuntimeError(
            "expected a private, runner-owned persistent state directory"
        )
    with (STATE / "registry.lock").open("a+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def source_identity():
    repo = HERE.parent.parent
    git = ["git", "-C", str(repo)]
    commit = subprocess.check_output(
        [*git, "rev-parse", "HEAD"], text=True, timeout=5
    ).strip()
    if subprocess.check_output(
        [*git, "status", "--porcelain", "--untracked-files=no"], text=True, timeout=5
    ):
        raise AdmissionError(
            "dirty_checkout", "HDFS certification requires a clean tracked checkout"
        )
    mover = re.search(
        r"data-mover-rs\.git\?rev=([0-9a-f]{40})#",
        (repo / "Cargo.lock").read_text(),
    )
    if not mover:
        raise RuntimeError("data-mover revision is missing from Cargo.lock")
    return {"terrasync_candidate": commit, "data_mover_candidate": mover[1]}


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def cargo_binary(expected, *arguments):
    result = subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--message-format=json-render-diagnostics",
            *arguments,
        ],
        cwd=HERE.parent.parent,
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    paths = []
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if event.get("reason") == "compiler-artifact" and event.get("executable"):
            paths.append(Path(event["executable"]).resolve())
    if paths != [expected]:
        raise RuntimeError(
            f"Cargo did not produce the expected host executable {expected}"
        )
    return sha256(expected)


def prepare_build():
    target = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    receipt = target / BUILD_RECEIPT
    receipt.unlink(missing_ok=True)
    candidate = source_identity()
    cli, matrix = RELEASE_BINARIES
    release = target / "release"
    binaries = {
        cli: cargo_binary(
            release / cli, "--features", "lab-fault-injection", "--bin", "terrasync"
        ),
        matrix: cargo_binary(
            release / matrix, "-p", "app", "--example", "single_process_matrix"
        ),
    }
    if candidate != source_identity():
        raise RuntimeError("the checkout changed during the lab build")
    atomic_json(receipt, {"candidate": candidate, "binaries": binaries})
    return 0


def claim(run_id):
    attempt = STATE / run_id
    with locked():
        record = read_optional(attempt / "state.json")
        if record is not None:
            return record, False
        candidate = source_identity()
        target = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
        receipt = read_optional(target / BUILD_RECEIPT)
        if (
            not isinstance(receipt, dict)
            or receipt.get("candidate") != candidate
            or not isinstance(receipt.get("binaries"), dict)
        ):
            raise AdmissionError(
                "build_receipt_mismatch",
                "the lab build receipt is missing or for another checkout",
            )
        # A launcher interrupted before publication could only have staged inert files.
        if attempt.exists():
            shutil.rmtree(attempt)
        attempt.mkdir(mode=0o700)
        work = attempt / "work"
        work.mkdir(mode=0o700)
        try:
            for script in ("with-hdfs-tunnel.sh", "run-hdfs-e2e.sh", "common.sh"):
                shutil.copy2(HERE / script, work / script)
            for binary in RELEASE_BINARIES:
                staged = work / Path(binary).name
                shutil.copy2(target / "release" / binary, staged)
                expected_digest = receipt["binaries"].get(binary)
                if not os.access(staged, os.X_OK) or sha256(staged) != expected_digest:
                    raise AdmissionError(
                        "build_binary_mismatch",
                        f"the staged {binary} does not match the lab build receipt",
                    )
        except (OSError, RuntimeError):
            shutil.rmtree(attempt)
            raise
        record = {
            "schema_version": 1,
            "run_id": run_id,
            "boot_id": BOOT,
            "created": time.time(),
            "lease": time.monotonic(),
            **candidate,
            "binary_sha256": {
                binary: receipt["binaries"][binary] for binary in RELEASE_BINARIES
            },
            "instances": {"clickhouse": "attempt_private", "hdfs": "lab_kerberos"},
            "admission": "open",
            "test": "pending",
            "local_cleanup": "pending",
            "hdfs_cleanup": "not_attempted",
            "certification": "pending",
        }
        atomic_json(attempt / "state.json", record)
        sync_directory(STATE)
        sync_directory(STATE.parent)
        return record, True


def note(run_id, **changes):
    with locked():
        path = STATE / run_id / "state.json"
        record = read_json(path)
        record.update(changes)
        atomic_json(path, record)


def receive_lab_lock(run_id, cancelled):
    # Duplicate the flock FD into the service manager before any child starts.
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
    with os.fdopen(os.open(LAB_LOCK, flags, 0o600)) as stream:
        st = os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
            raise RuntimeError("the shared lab lock is not a runner-owned file")
        deadline = time.monotonic() + LOCK_LIMIT
        while True:
            if cancelled():
                return False
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("lab lock wait exceeded 120 seconds")
                time.sleep(0.25)
        lib = ctypes.CDLL("libsystemd.so.0")
        fd = ctypes.c_int(stream.fileno())
        lib.sd_notify_barrier.argtypes = [ctypes.c_int, ctypes.c_uint64]
        stored = lib.sd_pid_notify_with_fds(
            0, 0, b"FDSTORE=1\nFDNAME=lab-lock\nFDPOLL=0", ctypes.byref(fd), 1
        )
        if stored != 1 or lib.sd_notify_barrier(0, 2_000_000) != 1:
            raise RuntimeError("could not transfer the lab lock to systemd")
        retained = unit_state(run_id)
        if (
            retained.get("FileDescriptorStorePreserve") != "yes"
            or retained.get("NFileDescriptorStore") != "1"
        ):
            raise RuntimeError("systemd did not retain the lab lock")
        # Close only this copy. LOCK_UN would unlock the shared description.
        return True


def worker(run_id):
    from hdfs_lab_clickhouse import start

    attempt = STATE / run_id
    work = attempt / "work"
    state_path = attempt / "state.json"
    interrupted = False

    def cancel(*_):
        nonlocal interrupted
        interrupted = True

    def cancelled():
        return interrupted or read_json(state_path)["admission"] != "open"

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, cancel)
    with locked():
        record = read_json(state_path)
        invocation = os.environ["INVOCATION_ID"]
        if (
            record["boot_id"] != BOOT
            or record["admission"] != "open"
            or record.get("invocation")
        ):
            return 1
        if time.monotonic() - record["lease"] > LEASE:
            return 1
        group = Path("/proc/self/cgroup").read_text().strip().removeprefix("0::")
        if not group.startswith("/") or Path(group).name != unit(run_id):
            raise RuntimeError("worker is outside its expected cgroup")
        record.update(
            invocation=invocation, cgroup=group, test="running", stage="lab_lock"
        )
        atomic_json(state_path, record)

    try:
        if not receive_lab_lock(run_id, cancelled) or cancelled():
            note(run_id, test="cancelled")
            return 143
        note(run_id, stage="clickhouse_setup")
        scratch = SCRATCH / run_id
        root_stat = SCRATCH.lstat()
        if (
            not SCRATCH.is_mount()
            or root_stat.st_uid != os.getuid()
            or root_stat.st_mode & 0o077
        ):
            raise RuntimeError("bounded private HDFS scratch mount is unavailable")
        scratch.mkdir(mode=0o700)
        gate_dir = scratch / "gate"
        gate_dir.mkdir(mode=0o700)
        pin = (HERE / "clickhouse.sha256").read_text().strip()
        binary_stat = CLICKHOUSE.stat()
        digest = sha256(CLICKHOUSE)
        if binary_stat.st_uid != 0 or binary_stat.st_mode & 0o022 or digest != pin:
            raise RuntimeError(
                "ClickHouse binary differs from the approved installation"
            )
        note(run_id, clickhouse_sha256=digest)
        server, database = start(scratch / "clickhouse", CLICKHOUSE, cancelled)
        if cancelled():
            note(run_id, test="cancelled")
            return 143

        env = {
            key: os.environ[key]
            for key in ("HOME", "PATH", "LANG")
            if key in os.environ
        }
        env.update(
            database,
            LAB_HDFS_MANAGED_DIR=str(gate_dir),
            LAB_HDFS_STATE_DIR=str(work),
            TERRASYNC_BINARY=str(work / "terrasync"),
            TERRASYNC_SINGLE_MATRIX_BINARY=str(work / "single_process_matrix"),
            TERRASYNC_CANDIDATE=record["terrasync_candidate"],
            DATA_MOVER_CANDIDATE=record["data_mover_candidate"],
        )
        note(run_id, stage="hdfs_gate", hdfs_cleanup="unknown")
        with (scratch / "gate.log").open("wb") as log:
            gate = subprocess.Popen(
                [
                    str(work / "with-hdfs-tunnel.sh"),
                    str(work / "run-hdfs-e2e.sh"),
                    run_id,
                ],
                env=env,
                cwd=work,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
            )
        stop_deadline = None
        while gate.poll() is None:
            if stop_deadline is None and (cancelled() or server.poll() is not None):
                gate.terminate()
                stop_deadline = time.monotonic() + 65
            if stop_deadline is not None and time.monotonic() >= stop_deadline:
                # systemd terminates the entire cgroup once this main process exits.
                break
            time.sleep(0.2)
        if cancelled():
            outcome = "cancelled"
        elif gate.returncode == 0 and server.poll() is None:
            outcome = "passed"
        else:
            outcome = "failed"
        if gate.returncode is not None and server.poll() is None:
            server.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                server.wait(timeout=5)
        note(run_id, test=outcome, gate_exit=gate.returncode)
        return {"passed": 0, "cancelled": 143, "failed": 1}[outcome]
    except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
        note(
            run_id,
            test="cancelled" if cancelled() else "failed",
            reason=type(exc).__name__,
        )
        print(
            f"HDFS attempt failed during setup or execution ({type(exc).__name__})",
            file=sys.stderr,
        )
        return 143 if cancelled() else 1


def service_passed(record):
    return record.get("service_exit") == {
        "service_result": "success",
        "exit_code": "exited",
        "exit_status": "0",
    }


def build_verified(record):
    digests = record.get("binary_sha256", {})
    return (
        isinstance(digests, dict)
        and digests.keys() == set(RELEASE_BINARIES)
        and all(
            isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
            for value in digests.values()
        )
    )


def record_service_exit(run_id):
    """Record the service manager's verdict before a stopped unit can be reset."""
    with locked():
        path = STATE / run_id / "state.json"
        record = read_optional(path)
        if record is None or record.get("invocation") != os.environ["INVOCATION_ID"]:
            return 0
        fields = ("SERVICE_RESULT", "EXIT_CODE", "EXIT_STATUS")
        record["service_exit"] = {
            field.lower(): os.environ.get(field) for field in fields
        }
        atomic_json(path, record)
    return 0


def reconcile(run_id, cancel_requested=False, background=False):
    attempt = STATE / run_id
    path = attempt / "state.json"
    with locked():
        record = read_optional(path)
        if record is None:
            if background and attempt.is_dir():
                # claim() holds the same registry lock until it publishes state.
                # An unpublished staging directory cannot have an admitted worker.
                shutil.rmtree(attempt)
                sync_directory(STATE)
            return None
        if record["local_cleanup"] == "passed":
            if cancel_requested and record["certification"] == "ready":
                record.update(
                    test="cancelled", certification="withheld", cancel_requested=True
                )
                atomic_json(path, record)
            return record
        observed = unit_state(run_id)
        expired = (
            record["boot_id"] != BOOT or time.monotonic() - record["lease"] > LEASE
        )
        unfinished_start = (
            not record.get("invocation")
            and record["boot_id"] == BOOT
            and time.time() - record["created"] < ADMISSION_LIMIT
        )
        stopped = quiescent(observed, record)
        if record["admission"] == "open":
            if cancel_requested or (background and expired):
                record["cancel_requested"] = True
            elif not stopped or unfinished_start:
                return record
            record["admission"] = "closed"
            atomic_json(path, record)
        if not stopped:
            # A late start checks the closed admission record before any child starts.
            if observed.get("ActiveState") != "deactivating":
                systemctl("stop", "--no-block", unit(run_id), required=False)
            return record

        if record["test"] == "passed" and (
            observed["ActiveState"] == "failed" or not service_passed(record)
        ):
            record["test"] = "unknown"
        if record["test"] in {"pending", "running"}:
            record["test"] = (
                "cancelled" if record.get("cancel_requested") else "unknown"
            )
        elif record["test"] == "passed" and record.get("cancel_requested"):
            record["test"] = "cancelled"

        work = attempt / "work"
        if not record.get("hdfs_evidence_ingested"):
            hdfs = read_optional(work / "hdfs-cleanup.json")
            if (
                hdfs is not None
                and hdfs.get("run_id") == run_id
                and hdfs.get("status") in TERMINAL | {"not_attempted"}
            ):
                record["hdfs_cleanup"] = hdfs["status"]
                record["hdfs_cleanup_reason"] = hdfs.get("reason")
                record["hdfs_scope"] = hdfs.get("scope")
                record["hdfs_target"] = hdfs.get("target")
            elif hdfs is not None:
                record["hdfs_cleanup"] = "unknown"
            elif record["hdfs_cleanup"] == "unknown" and work.exists():
                # The gate must persist intent before issuing any remote write.
                record["hdfs_cleanup"] = "not_attempted"
            record["hdfs_evidence_ingested"] = True
        phase = read_optional(work / "gate-phase.json")
        if phase is not None and phase.get("run_id") == run_id:
            record["gate_phase"] = phase.get("phase")
        gate = read_optional(work / "gate-report.json") or read_optional(
            attempt / "gate.json"
        )
        if gate is not None:
            atomic_json(attempt / "gate.json", gate)
        atomic_json(path, record)
        scratch = SCRATCH / run_id
        if scratch.exists():
            shutil.rmtree(scratch)
        if work.exists():
            shutil.rmtree(work)
            sync_directory(attempt)

        # Preserve the lock through main-process failure and dispose the private
        # directory before releasing the systemd copy, after proving quiescence.
        if observed["ActiveState"] == "failed":
            systemctl("reset-failed", unit(run_id))
        observed = unit_state(run_id)
        if not quiescent(observed, record):
            return record
        if observed.get("NFileDescriptorStore", "0") != "0":
            systemctl("clean", "--what=fdstore", unit(run_id))
            if unit_state(run_id).get("NFileDescriptorStore", "0") != "0":
                raise RuntimeError("systemd retained the lab lock after cleanup")

        record["local_cleanup"] = "passed"
        contract = read_json(HERE / "evidence-matrix.json")
        certified = (
            record["test"] == "passed"
            and record["hdfs_cleanup"] == "passed"
            and build_verified(record)
            and gate is not None
            and gate.get("schema_version") == contract["schema_version"]
            and gate.get("run_id") == run_id
            and gate.get("cleanup") == {"hdfs": "passed"}
            and gate.get("gates") == dict.fromkeys(contract["required_gates"], "pass")
            and gate.get("remote_role_isolation") == contract["required_role_isolation"]
            and gate.get("payload_sha256")
            == hashlib.sha256(f"terrasync-hdfs-{run_id}\n".encode()).hexdigest()
            and gate.get("terrasync_candidate") == record["terrasync_candidate"]
            and gate.get("data_mover_candidate") == record["data_mover_candidate"]
        )
        record["certification"] = "ready" if certified else "withheld"
        record["finished"] = time.time()
        atomic_json(path, record)
        return record


def publish(run_id, output, *, finalize=False, cancelled=lambda: False):
    with locked():
        attempt = STATE / run_id
        record = read_optional(attempt / "state.json")
        if record is None:
            return 1
        if cancelled() and record["certification"] == "ready":
            record.update(
                test="cancelled", certification="withheld", cancel_requested=True
            )
            atomic_json(attempt / "state.json", record)
        if record["certification"] in {"ready", "published"} and not service_passed(
            record
        ):
            record.update(test="unknown", certification="withheld")
            atomic_json(attempt / "state.json", record)
        if record["certification"] in {"ready", "published"} and not build_verified(
            record
        ):
            record = {
                **record,
                "certification": "withheld",
                "certification_reason": "build_provenance_unavailable",
            }
        output.mkdir(parents=True, exist_ok=True)
        certification = output / f"{run_id}-hdfs.json"
        if record["certification"] == "published" or (
            finalize and record["certification"] == "ready"
        ):
            gate = read_json(attempt / "gate.json")
            gate["cleanup"]["clickhouse"] = "passed"
            atomic_json(certification, gate)
            if record["certification"] == "ready":
                record["certification"] = "published"
                # The rename commits this result. Honour cancellation after the new
                # record has been written and before exposing it to other callers.
                if not atomic_json(
                    attempt / "state.json", record, can_commit=lambda: not cancelled()
                ):
                    record.update(
                        test="cancelled",
                        certification="withheld",
                        cancel_requested=True,
                    )
                    atomic_json(attempt / "state.json", record)
                    certification.unlink()
                    sync_directory(output)
        else:
            certification.unlink(missing_ok=True)
        atomic_json(output / f"{run_id}-hdfs-cleanup.json", record)
        return 0 if record["certification"] == "published" else 1


def validate_installation(run_id, check_cancelled):
    # Refuse to stage anything if the installed service and manager are unavailable.
    observed = unit_state(run_id)
    check_cancelled()
    if (
        observed.get("LoadState") != "loaded"
        or observed.get("FileDescriptorStorePreserve") != "yes"
    ):
        raise AdmissionError(
            "incompatible_service_unit",
            "the runner needs terrasync-hdfs-attempt@.service with FD preservation",
        )
    validate_service_commands(run_id, check_cancelled)
    for file in ("hdfs_lab.py", "hdfs_lab_clickhouse.py", "evidence-matrix.json"):
        check_cancelled()
        if (HERE / file).read_bytes() != (INSTALL / file).read_bytes():
            raise AdmissionError(
                "installation_mismatch",
                "the runner's managed HDFS installation differs from this checkout",
            )


def admission_diagnostic(run_id, output, stage, reason, *, interrupted):
    output.mkdir(parents=True, exist_ok=True)
    atomic_json(
        output / f"{run_id}-hdfs-cleanup.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "stage": stage,
            "reason": reason,
            "test": "cancelled" if interrupted else "failed",
            "local_cleanup": "not_attempted",
            "hdfs_cleanup": "not_attempted",
            "certification": "withheld",
        },
    )


def run(run_id, output):
    if (STATE / run_id / "state.json").is_file():
        return publish(run_id, output)
    interrupted = False

    def cancel(*_):
        nonlocal interrupted
        interrupted = True

    def check_cancelled():
        if interrupted:
            raise AdmissionError("cancelled", "HDFS attempt cancelled during admission")

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, cancel)

    stage = "admission_preflight"
    try:
        check_cancelled()
        validate_installation(run_id, check_cancelled)
        check_cancelled()
        stage = "staging"
        _, new = claim(run_id)
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        ValueError,
        KeyError,
    ) as exc:
        reason = exc.reason if isinstance(exc, AdmissionError) else type(exc).__name__
        admission_diagnostic(run_id, output, stage, reason, interrupted=interrupted)
        if interrupted:
            return 143
        raise
    if not new:
        return publish(run_id, output)

    if not interrupted:
        # A timeout is ambiguous. Do not resubmit; observe this exact unit.
        try:
            systemctl("start", "--no-block", unit(run_id), required=False)
        except subprocess.TimeoutExpired:
            pass
    deadline = time.monotonic() + ATTEMPT_LIMIT
    while True:
        if interrupted or time.monotonic() >= deadline:
            wait_for_recovery(run_id, cancel_requested=True)
            publish(run_id, output)
            return 143 if interrupted else 1
        record = reconcile(run_id)
        if record["local_cleanup"] == "passed":
            result = publish(
                run_id, output, finalize=True, cancelled=lambda: interrupted
            )
            return 143 if result and interrupted else result
        with locked():
            path = STATE / run_id / "state.json"
            record = read_json(path)
            if record["admission"] == "open":
                record["lease"] = time.monotonic()
                atomic_json(path, record)
        time.sleep(HEARTBEAT)


def wait_for_recovery(run_id, cancel_requested=False):
    deadline = time.monotonic() + (80 if cancel_requested else 0)
    while True:
        record = reconcile(run_id, cancel_requested=cancel_requested, background=True)
        if (
            not record
            or record["local_cleanup"] == "passed"
            or time.monotonic() >= deadline
        ):
            return record
        time.sleep(1)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("build", "run", "worker", "worker-exit", "reconcile", "sweep"),
    )
    parser.add_argument("run_id", nargs="?")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cancel", action="store_true")
    args = parser.parse_args()
    if args.command == "build":
        return prepare_build()
    if args.command == "sweep":
        unresolved = False
        if STATE.exists():
            for entry in sorted(STATE.iterdir()):
                if RUN_ID.fullmatch(entry.name) and entry.is_dir():
                    try:
                        reconcile(entry.name, background=True)
                    except (
                        OSError,
                        RuntimeError,
                        subprocess.SubprocessError,
                        ValueError,
                    ) as exc:
                        print(
                            f"{entry.name}: reconciliation deferred ({type(exc).__name__})",
                            file=sys.stderr,
                        )
                        unresolved = True
        return int(unresolved)
    if not args.run_id or not RUN_ID.fullmatch(args.run_id):
        parser.error("invalid run ID")
    if args.command == "worker":
        return worker(args.run_id)
    if args.command == "worker-exit":
        return record_service_exit(args.run_id)
    if args.command == "run":
        if args.output is None:
            parser.error("run requires --output")
        return run(args.run_id, args.output)
    record = wait_for_recovery(args.run_id, cancel_requested=args.cancel)
    if args.output:
        if record is not None:
            publish(args.run_id, args.output)
        elif not (args.output / f"{args.run_id}-hdfs-cleanup.json").exists():
            admission_diagnostic(
                args.run_id,
                args.output,
                "admission_preflight",
                "cancelled" if args.cancel else "attempt_not_admitted",
                interrupted=args.cancel,
            )
    return (
        0
        if record
        and record["local_cleanup"] == "passed"
        and record["hdfs_cleanup"] in {"passed", "not_attempted"}
        else 1
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        ValueError,
        KeyError,
    ) as err:
        print(f"HDFS attempt controller: {err}", file=sys.stderr)
        sys.exit(1)
