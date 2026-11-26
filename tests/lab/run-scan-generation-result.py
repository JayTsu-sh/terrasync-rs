#!/usr/bin/env python3
"""Build this checkout's fault-injection binaries and run the scan generation lab."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
FEATURE = "lab-fault-injection"
CALLBACK_TEST = "scan_generation_callback_entrypoint"


def show(line):
    try:
        print(line, end="", flush=True)
    except BrokenPipeError:
        with open(os.devnull, "w") as sink:
            os.dup2(sink.fileno(), sys.stdout.fileno())


def build_binary(command, name, kind, results, binaries, environment):
    messages = results / f"{name}.cargo.jsonl"
    diagnostics = results / f"{name}.cargo.stderr.log"
    show(f"Building {name}; diagnostics: {diagnostics}\n")
    with messages.open("w") as stdout, diagnostics.open("w") as stderr:
        run_command(["cargo", *command, "--locked", "--features", FEATURE, "--message-format=json"],
                    environment=environment, stdout=stdout, stderr=stderr)
    executables = []
    with messages.open() as stdout:
        for line in stdout:
            message = json.loads(line)
            if (message["reason"] == "compiler-artifact" and message["target"]["name"] == name
                    and message["target"]["kind"] == [kind] and FEATURE in message["features"]
                    and message["executable"]):
                executables.append(Path(message["executable"]))
    if len(executables) != 1:
        raise RuntimeError(f"expected one executable for {name} with {FEATURE}, found {len(executables)}")
    executable, = executables
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError(f"Cargo artifact is not executable: {executable}")
    if kind == "bin" and not executable.resolve().is_relative_to(binaries.parent.resolve()):
        raise RuntimeError(f"CLI artifact is outside this run's Cargo target directory: {executable}")
    return Path(shutil.copy2(executable, binaries / name))


def run_command(command, *, stdout=None, stderr=None, environment=None, transcript=None, check=True):
    child = None
    interrupted = None
    forwarded = False

    def cancel(signum, _frame):
        nonlocal interrupted, forwarded
        if interrupted is None:
            interrupted = signum
        if child is not None and not forwarded:
            forwarded = True
            try:
                # Cargo 与实验室 runner 都通过 SIGINT 终止本轮子进程。
                os.killpg(child.pid, signal.SIGINT)
            except ProcessLookupError:
                pass

    handled_signals = (signal.SIGINT, signal.SIGTERM)
    previous = {signum: signal.signal(signum, cancel) for signum in handled_signals}
    try:
        if transcript is not None:
            stdout, stderr = subprocess.PIPE, subprocess.STDOUT
        with subprocess.Popen(command, cwd=ROOT, env=environment, stdout=stdout, stderr=stderr,
                              text=True, errors="replace", start_new_session=True) as child:
            if interrupted is not None:
                cancel(interrupted, None)
            if transcript is not None:
                for line in child.stdout:
                    transcript.write(line)
                    show(line)
            code = child.wait()
            if interrupted is not None:
                code = 128 + interrupted
        if check and code:
            raise subprocess.CalledProcessError(code, command)
        return code
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def run_suite(cli, driver, results):
    variables = {
        "LAB_TERRASYNC_BIN": cli,
        "LAB_SCAN_CALLBACK_DRIVER": driver,
        "LAB_SCAN_RESULT_LOG_DIR": results / "scenarios",
    }
    if "NOTIFICATION_CLICKHOUSE_IMAGE" in os.environ:
        variables["NOTIFICATION_CLICKHOUSE_IMAGE"] = os.environ["NOTIFICATION_CLICKHOUSE_IMAGE"]
    command = ["sudo", "-n", "--"] if os.geteuid() else []
    command += ["env", "-u", "PYTHONOPTIMIZE", *(f"{key}={value}" for key, value in variables.items()),
                "sh", "-c", 'umask 022; exec "$@"', "scan-generation-lab", sys.executable,
                str(ROOT / "tests/lab/run-notification-archive-local.py"), "--scan-generation-result"]
    with (results / "run.log").open("w", buffering=1) as log:
        code = run_command(command, transcript=log, check=False)
        log.write(f"Scan generation lab exit: {code}\n")
        return code


def build_and_run(results):
    metadata_file = results / "cargo-metadata.json"
    with metadata_file.open("w") as stdout, (results / "cargo-metadata.stderr.log").open("w") as stderr:
        run_command(["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"],
                    stdout=stdout, stderr=stderr)
    metadata = json.loads(metadata_file.read_text())
    target = Path(metadata["target_directory"]) / "lab-scan-generation-result"
    target.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=target) as temporary:
        run_dir = Path(temporary)
        binaries = run_dir / "binaries"
        binaries.mkdir()
        environment = dict(os.environ, CARGO_BUILD_BUILD_DIR=str(target), CARGO_TARGET_DIR=str(run_dir))
        environment.setdefault("CARGO_PROFILE_DEV_DEBUG", "0")
        environment.setdefault("CARGO_INCREMENTAL", "0")
        with (target / ".launcher.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            cli = build_binary(["build", "--bin", "terrasync"], "terrasync", "bin", results, binaries, environment)
            driver = build_binary(["test", "--profile", "dev", "-p", "app", "--test", "scan_resume", "--no-run"],
                                  "scan_resume", "test", results, binaries, environment)
        with (results / "cli-version.txt").open("w") as log:
            run_command([cli, "--version"], stdout=log, stderr=subprocess.STDOUT)
        listing = results / "callback-test-list.txt"
        with listing.open("w") as log:
            run_command([driver, "--ignored", "--exact", CALLBACK_TEST, "--list"],
                        stdout=log, stderr=subprocess.STDOUT)
        if f"{CALLBACK_TEST}: test" not in listing.read_text().splitlines():
            raise RuntimeError(f"callback driver does not contain the ignored {CALLBACK_TEST} test")
        hashes = {}
        for binary in (cli, driver):
            with binary.open("rb") as executable:
                hashes[binary.name] = hashlib.file_digest(executable, "sha256").hexdigest()
        (results / "binary-sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
        return run_suite(cli, driver, results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, help="new results directory (default: a temporary directory)")
    args = parser.parse_args()
    if args.output_dir is None:
        results = Path(tempfile.mkdtemp(prefix="terrasync-scan-generation-"))
    else:
        results = args.output_dir.resolve()
        results.mkdir(mode=0o700, parents=True)
    show(f"Scan generation evidence: {results}\n")
    try:
        return build_and_run(results)
    except (OSError, RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        (results / "launcher-error.log").write_text(f"{error}\n")
        print(f"Scan generation lab: {error}", file=sys.stderr)
        return error.returncode if isinstance(error, subprocess.CalledProcessError) else 1


def terminate(signum, _frame):
    raise SystemExit(128 + signum)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, terminate)
    try:
        sys.exit(main())
    except OSError as error:
        sys.exit(f"Scan generation lab: {error}")
    except KeyboardInterrupt:
        sys.exit(130)
