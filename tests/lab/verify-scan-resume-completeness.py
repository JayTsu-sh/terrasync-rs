#!/usr/bin/env python3
"""Verify first-pass ∪ resumed-pass output against an independent source manifest.

Uses a real CLI and ClickHouse. Merge suspension is scoped to each disposable test
table, making its physical parts a reliable per-attempt output ledger. Production
code is not instrumented. --binary must be built with lab-fault-injection.
"""
import argparse
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
import time
from datetime import datetime, timezone

import requests


def create_fixture(root, layout, links):
    root = Path(root)
    root.mkdir(parents=True)

    def write(name):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"" if name.endswith(".empty") else ("fixture:" + name).encode())

    if layout == "balanced":
        for group in range(16):
            for branch in range(10):
                for number in range(25):
                    write(f"group{group:02}/branch{branch:02}/file{number:03}.txt")
    elif layout == "flat":
        for number in range(8000):
            write(f"file{number:05}.txt")
    elif layout == "deep":
        path = ""
        for depth in range(80):
            path += f"level{depth:03}/"
            for number in range(12):
                write(path + f"file{number:03}.txt")
            if depth % 10 == 0:
                write(path + "side branch/a.txt")
                write(path + "side branch/b.txt")
    elif layout == "nfs":
        for group in range(8):
            for branch in range(10):
                for number in range(18):
                    write(f"group{group:02}/branch{branch:02}/file{number:03}.txt")
    else:
        raise ValueError(layout)
    for name in ["中文目录/带 空格.txt", "quote'dir/a\"b.txt", "line\nbreak.txt",
                 "[brackets].txt", ".hidden/hidden.txt", "zero.empty"]:
        write(name)
    (root / "empty directory").mkdir()
    os.link(root / "zero.empty", root / "hardlink.empty")
    if links:
        (root / "file-link").symlink_to("zero.empty")
        (root / "dangling-link").symlink_to("absent-target")
        (root / "directory-loop").symlink_to(".", target_is_directory=True)


def manifest(root):
    root = Path(root)
    result = {}

    def fail(error):
        raise error

    for parent, dirs, files in os.walk(root, followlinks=False, onerror=fail):
        for name in dirs + files:
            path = Path(parent) / name
            info = path.lstat()
            kind = ("directory" if stat.S_ISDIR(info.st_mode) else
                    "symlink" if stat.S_ISLNK(info.st_mode) else
                    "file" if stat.S_ISREG(info.st_mode) else "special")
            result[path.relative_to(root).as_posix()] = {
                "kind": kind, "size": info.st_size if kind == "file" else None,
            }
    return result


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def flag(value):
    return value is True or value == 1 or value == "1"


def project(rows):
    result = {}
    for row in rows:
        kind = ("directory" if flag(row["is_dir"]) else
                "symlink" if flag(row["is_symlink"]) else
                "file" if flag(row["is_regular_file"]) else "special")
        entry = {"kind": kind, "size": int(row["size"]) if kind == "file" else None}
        path = row["relative_path"]
        if path in result and result[path] != entry:
            raise AssertionError(f"inconsistent records for {path!r}")
        result[path] = entry
    return result


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def write_rows(path, rows):
    with path.open("w") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False) + "\n")


class Case:
    def __init__(self, args, name):
        self.args, self.name = args, name
        self.work = args.work_dir / name
        self.work.mkdir(parents=True, exist_ok=True)
        self.database = "ts_scan_union_" + name.replace("-", "_") + "_" + str(time.time_ns())
        self.table = self.database + ".base_union_case"
        self.remote_root = "/srv/nfs/v4/ci/" + args.run_id
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.auth = (os.environ.get("LAB_CLICKHOUSE_USER", "default"),
                             os.environ.get("LAB_CLICKHOUSE_PASSWORD", ""))
        self.process = None
        self.parts = set()
        self.union = {}
        self.attempts = []
        self.started = datetime.now(timezone.utc).isoformat()

    def query(self, sql):
        response = self.session.post(self.args.clickhouse_url, data=sql.encode(), timeout=30)
        response.raise_for_status()
        return response.text

    def rows(self, sql):
        return [json.loads(line) for line in self.query(sql + " FORMAT JSONEachRow").splitlines() if line]

    def remote(self, code):
        result = subprocess.run([
            "ssh", "-i", str(self.args.ssh_key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=8", "root@" + self.args.nfs_mgmt,
            "python3 -c " + shlex.quote(code),
        ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
        return result.stdout.decode()

    def source_manifest(self):
        if self.name != "nfs41":
            return manifest(self.work / "source")
        code = "from pathlib import Path\nimport os, stat, json\n" + inspect.getsource(manifest)
        return json.loads(self.remote(code + "\nprint(json.dumps(manifest(" + repr(self.remote_root) + ")))"))

    def prepare(self):
        layout = self.name.removeprefix("local-")
        if self.name == "nfs41":
            code = "from pathlib import Path\nimport os\n" + inspect.getsource(create_fixture)
            code += "\ncreate_fixture(" + repr(self.remote_root) + ", 'nfs', False)\n"
            code += "\nfor p in [Path(" + repr(self.remote_root) + "), *Path(" + repr(self.remote_root) + ").rglob('*')]: os.chown(p, 1000, 1000)\n"
            self.remote(code)
            self.source = ("nfs://" + self.args.nfs_data + "/srv/nfs/v4:/ci/" + self.args.run_id +
                           "?version=4.1&noresvport=true&uid=1000&gid=1000")
        else:
            create_fixture(self.work / "source", layout, True)
            self.source = str(self.work / "source")
        self.expected = self.source_manifest()
        write_json(self.work / "source-manifest.json", self.expected)
        self.expected_files = sum(row["kind"] == "file" for row in self.expected.values())
        self.query("CREATE DATABASE " + self.database)
        config = ("[database]\nenabled=true\ntype=\"clickhouse\"\nbatch_size=37\n" +
                  "[database.clickhouse]\ndsn=" + json.dumps(self.args.clickhouse_url) +
                  "\ndatabase=" + json.dumps(self.database) + "\nusername=" + json.dumps(self.session.auth[0]) +
                  "\npassword=" + json.dumps(self.session.auth[1]) + "\n[scan]\nconcurrency=" +
                  str(1 if self.name == "local-deep" else 8) + "\n")
        (self.work / "config.toml").write_text(config)

    def start(self, number):
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("TERRASYNC_LAB_SCAN_"):
                del env[key]
        env["TERRASYNC_LAB_SCAN_CHECKPOINT_EVERY_MS"] = "60000" if self.name == "local-flat" else "0"
        if self.name != "local-flat":
            env["TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS"] = "15"
        env["NO_PROXY"] = env.get("NO_PROXY", "") + ",10.131.9.11,10.10.1.12,127.0.0.1"
        env["no_proxy"] = env["NO_PROXY"]
        with (self.work / f"attempt-{number}.log").open("w") as output:
            self.process = subprocess.Popen([
                str(self.args.binary), "-c", str(self.work / "config.toml"), "scan", "--id", "union_case",
                "--resume", self.source,
            ], cwd=self.work, env=env, stdout=output, stderr=subprocess.STDOUT)

    def state(self):
        values = self.rows("SELECT version, observed_entries, length(checkpoint) AS checkpoint_bytes, phase, "
                           "generation, incremental FROM " + self.database + ".scan_resume_union_case FINAL")
        return values[0] if values else {}

    def wait_and_kill(self, fraction):
        target = math.ceil(self.expected_files * fraction)
        deadline = time.monotonic() + 180
        while True:
            if self.process.poll() is not None:
                raise AssertionError("scan exited before the planned interruption")
            if time.monotonic() >= deadline:
                raise TimeoutError("planned interruption point was not reached")
            try:
                state = self.state()
                count = int(self.query("SELECT countIf(is_regular_file) FROM " + self.table + " FINAL"))
                checkpoint_ok = self.name == "local-flat" or int(state.get("checkpoint_bytes", 0)) > 0
                if target <= count < self.expected_files and checkpoint_ok and state.get("phase") == 0:
                    break
            except requests.HTTPError:
                pass  # The CLI creates its tables on startup.
            time.sleep(.015)
        self.process.kill()
        status = self.process.wait(timeout=10)
        self.process = None
        assert status == -9
        return status

    def quiesce(self):
        self.query("SYSTEM STOP MERGES " + self.table)
        deadline = time.monotonic() + 30
        while True:
            merges = int(self.query("SELECT count() FROM system.merges WHERE database='" + self.database +
                                    "' AND table='base_union_case'"))
            inserts = int(self.query("SELECT count() FROM system.processes WHERE current_database='" +
                                     self.database + "' AND startsWith(query,'INSERT')"))
            if merges == 0 and inserts == 0:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("writes/merges did not quiesce")
            time.sleep(.03)

    def capture(self, number, status):
        self.quiesce()
        raw = self.rows("SELECT relative_path, is_dir, is_symlink, is_regular_file, size, _part AS part_name FROM " + self.table)
        current_parts = {row["part_name"] for row in raw}
        assert self.parts <= current_parts, "an earlier attempt's immutable parts disappeared"
        newly_written = [row for row in raw if row["part_name"] not in self.parts]
        current = project(newly_written)
        overlap = len(current.keys() & self.union.keys())
        new_paths = len(current.keys() - self.union.keys())
        files = {path for path, value in current.items() if value["kind"] == "file"}
        previous_files = {path for path, value in self.union.items() if value["kind"] == "file"}
        new_files = len(files - previous_files)
        overlapping_files = len(files & previous_files)
        self.union.update(current)
        self.parts = current_parts
        write_rows(self.work / f"attempt-{number}-records.jsonl", newly_written)
        state = self.state()
        state["committed_generation"] = int(self.query("SELECT scan_state FROM " + self.database + ".state_union_case FINAL"))
        self.attempts.append({
            "attempt": number, "exit_code": status, "physical_rows_written": len(newly_written),
            "unique_paths_written": len(current), "regular_files_written": sum(v["kind"] == "file" for v in current.values()),
            "overlap_with_earlier_attempts": overlap, "new_paths": new_paths,
            "new_regular_files": new_files, "overlapping_regular_files": overlapping_files,
            "cumulative_unique_paths": len(self.union), "state": state,
            "attempt_manifest_sha256": digest(current),
        })
        if status == -9:
            assert state["phase"] == 0 and state["incremental"] is False
            assert state["committed_generation"] == 0
        else:
            assert status == 0 and state["phase"] == 2 and state["committed_generation"] == 1

    def run(self):
        self.prepare()
        fractions = {"local-balanced": [.15, .5, .85], "local-flat": [.23],
                     "local-deep": [.35, .7], "nfs41": [.25, .7]}[self.name]
        for number, fraction in enumerate(fractions, 1):
            self.start(number)
            self.capture(number, self.wait_and_kill(fraction))
            print(self.name, "interruption", number, "durable paths", len(self.union), flush=True)
        self.start(len(fractions) + 1)
        status = self.process.wait(timeout=180)
        self.process = None
        self.capture(len(fractions) + 1, status)
        final_rows = self.rows("SELECT relative_path, is_dir, is_symlink, is_regular_file, size FROM " + self.table + " FINAL")
        final = project(final_rows)
        write_rows(self.work / "final-records.jsonl", final_rows)
        missing = sorted(self.expected.keys() - self.union.keys())
        extra = sorted(self.union.keys() - self.expected.keys())
        mismatches = sorted(path for path in self.expected.keys() & self.union.keys()
                            if self.expected[path] != self.union[path])
        result = {"name": self.name, "source_regular_files": self.expected_files,
                  "source_entries": len(self.expected), "attempts": self.attempts,
                  "union_entries": len(self.union), "missing_paths": missing, "extra_paths": extra,
                  "type_or_size_mismatches": mismatches,
                  "final_duplicates": len(final_rows) - len(final),
                  "source_manifest_sha256": digest(self.expected), "union_manifest_sha256": digest(self.union),
                  "final_manifest_sha256": digest(final), "started_at_utc": self.started}
        result["source_unchanged"] = self.source_manifest() == self.expected
        result["union_regular_files"] = sum(value["kind"] == "file" for value in self.union.values())
        result["final_regular_files"] = sum(value["kind"] == "file" for value in final.values())
        result["passed"] = (self.union == self.expected == final and len(final_rows) == len(final)
                            and result["source_unchanged"])
        return result

    def cleanup(self):
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.query("DROP DATABASE IF EXISTS " + self.database)
        assert self.query("SELECT count() FROM system.databases WHERE name='" + self.database + "'").strip() == "0"
        if self.name == "nfs41":
            self.remote("from pathlib import Path\nimport shutil\np=Path(" + repr(self.remote_root) +
                        ")\nif p.exists(): shutil.rmtree(p)\nassert not p.exists()")
        elif (self.work / "source").exists():
            shutil.rmtree(self.work / "source")
        (self.work / "config.toml").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--clickhouse-url", default=os.environ.get("LAB_CLICKHOUSE_DSN", "http://10.131.9.11:8123"))
    parser.add_argument("--with-nfs", action="store_true")
    parser.add_argument("--ssh-key", type=Path, default=os.environ.get("LAB_SSH_KEY"))
    parser.add_argument("--nfs-mgmt", default="10.131.9.12")
    parser.add_argument("--nfs-data", default="10.10.1.12")
    parser.add_argument("--case", action="append", choices=["local-balanced", "local-flat", "local-deep", "nfs41"])
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    args.work_dir = args.work_dir.resolve()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.run_id = "release-scan-union-" + str(time.time_ns())
    cases = args.case or ["local-balanced", "local-flat", "local-deep"] + (["nfs41"] if args.with_nfs else [])
    if "nfs41" in cases and args.ssh_key is None:
        parser.error("--ssh-key or LAB_SSH_KEY is required for NFS")
    report = {"run_id": args.run_id, "binary_sha256": hashlib.file_digest(args.binary.open("rb"), "sha256").hexdigest(),
              "invariant": "first_attempt_output UNION all_resumed_attempt_outputs = independent_source_manifest = final_database",
              "method": "Per-attempt durable records are identified by immutable ClickHouse physical parts; merges are stopped only on disposable test tables.",
              "cases": []}
    for name in cases:
        case = Case(args, name)
        try:
            result = case.run()
        except Exception as error:
            result = {"name": name, "passed": False, "error": str(error), "attempts": case.attempts}
        finally:
            try:
                case.cleanup()
                cleaned = True
            except Exception as error:
                cleaned = False
                print("cleanup failed", name, str(error), flush=True)
        result["cleaned"] = cleaned
        report["cases"].append(result)
        write_json(args.work_dir / "report.json", report)
        print(name, "PASS" if result.get("passed") else "FAIL", flush=True)
    report["passed"] = all(case.get("passed") and case["cleaned"] for case in report["cases"])
    report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_json(args.work_dir / "report.json", report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
