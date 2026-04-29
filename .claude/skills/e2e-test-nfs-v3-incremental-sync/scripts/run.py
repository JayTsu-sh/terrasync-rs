#!/usr/bin/env python3
"""
e2e-test-nfs-v3-incremental-sync/scripts/run.py
NFS v3 增量同步 e2e 测试：全量 sync 建基线 → 变更 → 增量 sync → 验证 → integrity-check。
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
from protocol_constants import NfsV3 as _PC

SYNC_JOB_ID = "nfs-v3-incr-sync"
DST_SCAN_JOB_ID = "nfs-v3-incr-sync-dst"
SANITIZED = "nfs_v3_incr_sync"

BASELINE_DIRS, BASELINE_FILES, BASELINE_SYMLINKS = _PC.BASELINE_DIRS, _PC.BASELINE_FILES, _PC.BASELINE_SYMLINKS
POST_DIRS, POST_FILES, POST_SYMLINKS               = _PC.POST_DIRS, _PC.POST_FILES, _PC.POST_SYMLINKS

# CH 表（包括 verify 表）
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


def _terrasync(binary, config, *args, timeout=600):
    return subprocess.run([binary, "-c", config, "-l", "trace", *args],
                         capture_output=True, text=True, timeout=timeout)


def _check_incr_statistics(stdout: str, expected: dict) -> AssertionResult:
    actual = {}
    for op in expected:
        m = re.search(rf"{op}[:\s]+(\d+)\s+total", stdout, re.IGNORECASE)
        if m:
            actual[op] = int(m.group(1))
    passed = all(actual.get(k) == v for k, v in expected.items())
    return AssertionResult(
        "incr_sync_statistics", passed, expected, actual,
        f"{'✓' if passed else '✗'} incr_sync_statistics: expected={expected}, actual={actual}"
    )


def _dest_find(a, dest_ip, nfs_export, expected):
    cmd = (f"sudo find {nfs_export}/test-data -type d | wc -l; "
           f"sudo find {nfs_export}/test-data -type f | wc -l; "
           f"sudo find {nfs_export}/test-data -type l | wc -l")
    try:
        out = a.ssh_exec(dest_ip, cmd, timeout=60)
    except Exception as e:
        return AssertionResult("dest_find_counts", False, {}, {}, f"✗ dest_find_counts: {e}")
    lines = [ln.strip() for ln in out.strip().splitlines() if ln.strip()]
    if len(lines) < 3:
        return AssertionResult("dest_find_counts", False, {}, {},
                               "✗ dest_find_counts: unexpected output")
    actual = {"dirs": int(lines[0]), "files": int(lines[1]), "symlinks": int(lines[2])}
    passed = actual == expected
    return AssertionResult("dest_find_counts", passed, expected, actual,
                           f"{'✓' if passed else '✗'} dest_find_counts: {actual}")


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V3_SOURCE_IP", "NFS_V3_DEST_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V3_SOURCE_IP"]
    dest_ip = cfg["NFS_V3_DEST_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_export = cfg.get("NFS_V3_EXPORT", _PC.EXPORT)
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_url = f"nfs://{src_ip}{nfs_export}"
    dst_url = f"nfs://{dest_ip}{nfs_export}"

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)

    # Step 1：创建基线数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v3" / "scripts" / "setup-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {},
                                       f"✗ setup: setup-test-data.sh not found"))
        return build_result(results, start)
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return build_result(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} setup_test_data"))
    if not setup_ok:
        return build_result(results, start)

    # Step 3：全量 Sync
    proc = _terrasync(binary, config, "sync", "--id", SYNC_JOB_ID, src_url, dst_url)
    sync_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("full_sync_exit", False, {"code": 0},
                                       {"code": proc.returncode}, "✗ full_sync: failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return build_result(results, start)

    baseline_exp = {"dirs": BASELINE_DIRS, "files": BASELINE_FILES, "symlinks": BASELINE_SYMLINKS}
    results.append(a.check_cli_sync_output(sync_out, baseline_exp))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", baseline_exp))
    results.append(_dest_find(a, dest_ip, nfs_export, baseline_exp))

    # Step 4：变更源端数据（复用 incremental-scan 的 mutate 脚本）
    mutate_sh = (_SKILL_DIR.parent / "e2e-test-nfs-v3-incremental-scan" /
                 "scripts" / "mutate-test-data.sh")
    if not mutate_sh.exists():
        results.append(AssertionResult("mutate", False, {}, {},
                                       "✗ mutate: mutate-test-data.sh not found in incremental-scan"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return build_result(results, start)
    try:
        a.scp_to(mutate_sh, src_ip, "/tmp/mutate-test-data.sh")
        mut_out = a.ssh_exec(src_ip, "sudo bash /tmp/mutate-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ mutate: {e}"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return build_result(results, start)
    mutate_ok = "OK:" in mut_out or "OK：" in mut_out
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} mutate_test_data"))
    if not mutate_ok:
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return build_result(results, start)

    # Step 5：增量 Sync（同 JOB_ID，jobs/ 存在 → 自动增量）
    proc2 = _terrasync(binary, config, "sync", "--id", SYNC_JOB_ID, src_url, dst_url,
                       timeout=900)
    incr_out = proc2.stdout + proc2.stderr
    if proc2.returncode != 0:
        results.append(AssertionResult("incr_sync_exit", False, {"code": 0},
                                       {"code": proc2.returncode}, "✗ incr_sync: failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return build_result(results, start)

    post_exp = {"dirs": POST_DIRS, "files": POST_FILES, "symlinks": POST_SYMLINKS}
    results.append(a.check_cli_scan_output(incr_out, post_exp))
    results.append(_check_incr_statistics(incr_out, {"new": 7, "changed": 19, "renamed": 47, "deleted": 8}))

    # Step 6：验证目标端
    proc3 = _terrasync(binary, config, "scan", "--id", DST_SCAN_JOB_ID, dst_url, timeout=300)
    results.append(a.check_cli_scan_output(proc3.stdout + proc3.stderr, post_exp))
    results.append(_dest_find(a, dest_ip, nfs_export, post_exp))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}_dst", post_exp))

    # Step 7：Integrity Check（Quick + Full）
    for mode_flag, label in [(["--quick"], "quick"), ([], "full")]:
        proc_ic = _terrasync(binary, config, "integrity-check",
                             *([f"--id", f"nfs-v3-incr-sync-ic-{label}"] if not mode_flag else []),
                             src_url, dst_url, *mode_flag, timeout=600)
        ic_out = proc_ic.stdout + proc_ic.stderr
        passed = proc_ic.returncode == 0 and "All Passed" in ic_out
        results.append(AssertionResult(
            f"integrity_check_{label}", passed,
            {"returncode": 0, "all_passed": True}, {"returncode": proc_ic.returncode},
            f"{'✓' if passed else '✗'} integrity_check_{label}"
        ))

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
