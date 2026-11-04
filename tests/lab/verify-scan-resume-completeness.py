#!/usr/bin/env python3
"""Verify confirmed directory-block prefixes and the final independent source manifest.

Uses a real CLI and disposable ClickHouse tables. Checkpoints may be shortened for
this functional test; its timings are not performance evidence. Production code is
not instrumented. --binary must be built with lab-fault-injection.
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
        self.confirmed = {}
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
                           "generation, incremental, checkpoint_snapshot, statistics FROM " +
                           self.database + ".scan_resume_union_case FINAL")
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

    def capture(self, number, status):
        state = self.state()
        raw_statistics = state.pop("statistics")
        scan = json.loads(raw_statistics)["report"]["scan"] if raw_statistics else None
        if status == 0:
            # 完成后 cut 可以被正常回收；最终事实以已发布的 working 表为准。
            assert state["phase"] == 2
            table = self.table
        else:
            assert status == -9 and state["phase"] == 0
            token = state["checkpoint_snapshot"]
            assert re.fullmatch(r"[0-9a-f]{32}", token), "invalid confirmed snapshot token"
            # 只定位 manifest 引用的快照，不从 working 表的物理 part 推算观察或重放。
            tables = self.rows("SELECT name FROM system.tables WHERE database='" + self.database +
                               "' AND endsWith(name, '_" + token + "_base')")
            assert len(tables) == 1, "confirmed snapshot is missing or ambiguous"
            name = tables[0]["name"]
            assert re.fullmatch(r"[a-z0-9_]+", name)
            table = self.database + "." + name
        rows = self.rows("SELECT relative_path, is_dir, is_symlink, is_regular_file, size FROM " + table + " FINAL")
        confirmed = project(rows)
        assert len(rows) == len(confirmed), "duplicate confirmed paths"
        assert all(confirmed.get(path) == value for path, value in self.confirmed.items()), "confirmed prefix regressed"
        assert all(self.expected.get(path) == value for path, value in confirmed.items()), "confirmed snapshot differs from source"
        assert int(state["observed_entries"]) == len(confirmed)
        if scan is None:
            # 初始 manifest 尚无统计报告；零游标、零标量和真实空 cut 必须同时成立。
            assert status == -9 and state["phase"] == 0
            assert int(state["checkpoint_bytes"]) == 0 and not confirmed
        else:
            assert scan["scanned_entries"] == scan["retained_entries"] == len(confirmed)
            assert scan["filtered_entries"] == 0
            expected_types = {name: 0 for name in ["directories", "regular_files", "symlinks",
                              "block_devices", "character_devices", "fifos", "sockets", "unknown"]}
            for value in confirmed.values():
                expected_types[{"directory": "directories", "file": "regular_files",
                                "symlink": "symlinks"}[value["kind"]]] += 1
            assert scan["counts"]["scanned"] == scan["counts"]["retained"] == expected_types
            assert not any(scan["counts"]["filtered"].values())
            assert scan["counts"]["regular_file_bytes"] == sum(
                value["size"] for value in confirmed.values() if value["kind"] == "file")
        state["committed_generation"] = int(self.query("SELECT scan_state FROM " + self.database +
                                                       ".state_union_case FINAL"))
        write_rows(self.work / f"attempt-{number}-confirmed-records.jsonl", rows)
        self.attempts.append({"attempt": number, "exit_code": status, "state": state,
                              "confirmed_entries": len(confirmed), "scan": scan,
                              "confirmed_manifest_sha256": digest(confirmed)})
        self.confirmed = confirmed
        if status == -9:
            assert state["incremental"] is False and state["committed_generation"] == 0
            assert scan is None or not scan["complete"]
            if self.name == "local-flat":
                assert int(state["checkpoint_bytes"]) == 0 and not confirmed
        else:
            assert state["committed_generation"] == 1
            assert scan is not None and scan["complete"] and confirmed == self.expected

    def run(self):
        self.prepare()
        fractions = {"local-balanced": [.15, .5, .85], "local-flat": [.23],
                     "local-deep": [.35, .7], "nfs41": [.25, .7]}[self.name]
        for number, fraction in enumerate(fractions, 1):
            self.start(number)
            self.capture(number, self.wait_and_kill(fraction))
            print(self.name, "interruption", number, "confirmed paths", len(self.confirmed), flush=True)
        self.start(len(fractions) + 1)
        status = self.process.wait(timeout=180)
        self.process = None
        self.capture(len(fractions) + 1, status)
        final_rows = self.rows("SELECT relative_path, is_dir, is_symlink, is_regular_file, size FROM " + self.table + " FINAL")
        final = project(final_rows)
        write_rows(self.work / "final-records.jsonl", final_rows)
        missing = sorted(self.expected.keys() - final.keys())
        extra = sorted(final.keys() - self.expected.keys())
        mismatches = sorted(path for path in self.expected.keys() & final.keys()
                            if self.expected[path] != final[path])
        result = {"name": self.name, "source_regular_files": self.expected_files,
                  "source_entries": len(self.expected), "attempts": self.attempts,
                  "confirmed_entries": len(self.confirmed), "missing_paths": missing, "extra_paths": extra,
                  "type_or_size_mismatches": mismatches,
                  "final_duplicates": len(final_rows) - len(final),
                  "source_manifest_sha256": digest(self.expected), "confirmed_manifest_sha256": digest(self.confirmed),
                  "final_manifest_sha256": digest(final), "started_at_utc": self.started}
        result["source_unchanged"] = self.source_manifest() == self.expected
        result["final_regular_files"] = sum(value["kind"] == "file" for value in final.values())
        result["passed"] = (self.confirmed == self.expected == final and len(final_rows) == len(final)
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
              "invariant": "confirmed_prefix = referenced_snapshot = checkpoint_statistics; final_database = independent_source_manifest = final_statistics",
              "method": "Inspect immutable snapshots referenced by acknowledged manifests; allow unconfirmed working tails to disappear during recovery. Never infer replay or attempt output from physical parts.",
              "functional_only": True,
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
