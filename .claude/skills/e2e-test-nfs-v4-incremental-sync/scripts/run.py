#!/usr/bin/env python3
"""
e2e-test-nfs-v4-incremental-sync/scripts/run.py
NFS v4.1 增量同步 e2e 测试。
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
from assertions import AssertionResult, TerrasyncAssertions

SYNC_JOB_ID = "nfs-v4-incr-sync"
DST_SCAN_JOB_ID = "nfs-v4-incr-sync-dst"
SANITIZED = "nfs_v4_incr_sync"
NFS_SERVER_PATH = "/export/nfsv4"
BASELINE_DIRS, BASELINE_FILES, BASELINE_SYMLINKS = 113, 335, 79
POST_DIRS, POST_FILES, POST_SYMLINKS = 114, 333, 79

_TABLES = [
    f"base_{SANITIZED}", f"state_{SANITIZED}",
    f"base_{SANITIZED}_dst", f"state_{SANITIZED}_dst",
    f"base_{SANITIZED}_verify_src", f"state_{SANITIZED}_verify_src",
    f"base_{SANITIZED}_verify_dst", f"state_{SANITIZED}_verify_dst",
]


def _cleanup(a, src_ip, dest_ip, ch_host, nfs_export):
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo ok || true"),
            ex.submit(a.ssh_exec, dest_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo ok || true"),
            *[ex.submit(a.clickhouse_query, ch_host,
                        f"DROP TABLE IF EXISTS default.{t}") for t in _TABLES],
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' -exec rm -rf {{}} +"),
            ex.submit(a.run_shell_quiet, "rm -rf target/debug/logs/*"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception:
                pass


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V4_SOURCE_IP", "NFS_V4_DEST_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V4_SOURCE_IP"]
    dest_ip = cfg["NFS_V4_DEST_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_export = cfg.get("NFS_V4_EXPORT", NFS_SERVER_PATH)
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_url = f"nfs://{src_ip}{nfs_export}?version=4.1"
    dst_url = f"nfs://{dest_ip}{nfs_export}?version=4.1"

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)

    # 创建基线数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v4-full-scan" / "scripts" / "setup-nfs4-test-data.sh"
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-nfs4-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-nfs4-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return _build(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} setup_nfs4_test_data"))
    if not setup_ok:
        return _build(results, start)

    baseline_exp = {"dirs": BASELINE_DIRS, "files": BASELINE_FILES, "symlinks": BASELINE_SYMLINKS}

    # 全量 Sync
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync",
         "--id", SYNC_JOB_ID, src_url, dst_url],
        capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        results.append(AssertionResult("full_sync", False, {}, {}, "✗ full_sync failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return _build(results, start)
    results.append(a.check_cli_sync_output(proc.stdout + proc.stderr, baseline_exp))

    # 变更数据
    mutate_sh = _SKILL_DIR.parent / "e2e-test-nfs-v4-incremental-scan" / "scripts" / "mutate-nfs4-test-data.sh"
    try:
        a.scp_to(mutate_sh, src_ip, "/tmp/mutate-nfs4-test-data.sh")
        mut_out = a.ssh_exec(src_ip, "sudo bash /tmp/mutate-nfs4-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ mutate: {e}"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return _build(results, start)
    mutate_ok = "OK:" in mut_out or "OK：" in mut_out
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} mutate_nfs4"))
    if not mutate_ok:
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return _build(results, start)

    # 增量 Sync
    proc2 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync",
         "--id", SYNC_JOB_ID, src_url, dst_url],
        capture_output=True, text=True, timeout=900)
    incr_out = proc2.stdout + proc2.stderr
    post_exp = {"dirs": POST_DIRS, "files": POST_FILES, "symlinks": POST_SYMLINKS}
    results.append(a.check_cli_scan_output(incr_out, post_exp))

    # 验证目标端
    proc3 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan",
         "--id", DST_SCAN_JOB_ID, dst_url],
        capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(proc3.stdout + proc3.stderr, post_exp))

    # Integrity Check
    for mode_flag, label in [(["--quick"], "quick"), ([], "full")]:
        p = subprocess.run(
            [binary, "-c", config, "-l", "trace", "integrity-check",
             src_url, dst_url, *mode_flag],
            capture_output=True, text=True, timeout=600)
        passed = p.returncode == 0 and "All Passed" in (p.stdout + p.stderr)
        results.append(AssertionResult(f"ic_{label}", passed, {}, {},
                                       f"{'✓' if passed else '✗'} integrity_{label}"))

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
    return _build(results, start)


def _build(results, start):
    elapsed = round(time.monotonic() - start, 1)
    passed = all(r.passed for r in results)
    for r in results:
        print(r.message)
    print(f"\n{'PASS' if passed else 'FAIL'} ({elapsed}s)")
    return {"passed": passed, "metrics": {"elapsed_sec": elapsed},
            "assertions": [{"name": r.name, "passed": r.passed, "message": r.message}
                           for r in results]}


if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
