#!/usr/bin/env python3
"""
e2e-test-nfs-v4-incremental-scan/scripts/run.py
NFS v4.1 增量扫描 e2e 测试（与 v3 逻辑相同，URL 加 ?version=4.1）。
"""

import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS_SCRIPTS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS_SCRIPTS))

import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result

JOB_ID = "nfs-v4-incr-scan"
SANITIZED = "nfs_v4_incr_scan"
NFS_SERVER_PATH = "/export/nfsv4"
BASELINE_DIRS, BASELINE_FILES, BASELINE_SYMLINKS = 113, 335, 79
POST_DIRS, POST_FILES, POST_SYMLINKS = 114, 333, 79


def _cleanup(a, src_ip, ch_host, nfs_export):
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo ok || true"),
            ex.submit(a.clickhouse_drop_tables, ch_host, SANITIZED),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
            ex.submit(a.run_shell_quiet, "rm -rf target/debug/logs/*"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception:
                pass


def _check_incr_stats(stdout, expected):
    actual = {}
    for op in expected:
        m = re.search(rf"{op}[:\s]+(\d+)\s+total", stdout, re.IGNORECASE)
        if m:
            actual[op] = int(m.group(1))
    passed = all(actual.get(k) == v for k, v in expected.items())
    return AssertionResult(
        "incr_statistics", passed, expected, actual,
        f"{'✓' if passed else '✗'} incr_statistics: expected={expected}, actual={actual}"
    )


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V4_SOURCE_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V4_SOURCE_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_export = cfg.get("NFS_V4_EXPORT", NFS_SERVER_PATH)
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_url = f"nfs://{src_ip}{nfs_export}?version=4.1"

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, ch_host, nfs_export)

    # Setup: use nfs4 setup script from full-scan skill
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v4-full-scan" / "scripts" / "setup-nfs4-test-data.sh"
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-nfs4-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-nfs4-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return build_result(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} setup_nfs4_test_data"))
    if not setup_ok:
        return build_result(results, start)

    # 全量扫描（建基线）
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        results.append(AssertionResult("full_scan", False, {}, {}, "✗ full_scan failed"))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)
    baseline_exp = {"dirs": BASELINE_DIRS, "files": BASELINE_FILES, "symlinks": BASELINE_SYMLINKS}
    results.append(a.check_cli_scan_output(proc.stdout + proc.stderr, baseline_exp))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", baseline_exp))

    # 变更数据（使用 nfs4 专用 mutate 脚本）
    mutate_sh = _SKILL_DIR / "scripts" / "mutate-nfs4-test-data.sh"
    if not mutate_sh.exists():
        mutate_sh = _SKILL_DIR.parent / "e2e-test-nfs-v4-incremental-scan" / "scripts" / "mutate-nfs4-test-data.sh"
    try:
        a.scp_to(mutate_sh, src_ip, "/tmp/mutate-nfs4-test-data.sh")
        mut_out = a.ssh_exec(src_ip, "sudo bash /tmp/mutate-nfs4-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ mutate: {e}"))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)
    mutate_ok = "OK:" in mut_out or "OK：" in mut_out
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} mutate_nfs4_test_data"))
    if not mutate_ok:
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    # 增量扫描
    proc2 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    incr_out = proc2.stdout + proc2.stderr
    post_exp = {"dirs": POST_DIRS, "files": POST_FILES, "symlinks": POST_SYMLINKS}
    results.append(a.check_cli_scan_output(incr_out, post_exp))
    results.append(_check_incr_stats(
        incr_out, {"new": 7, "changed": 9, "renamed": 47, "deleted": 8}
    ))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", post_exp))

    _cleanup(a, src_ip, ch_host, nfs_export)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
