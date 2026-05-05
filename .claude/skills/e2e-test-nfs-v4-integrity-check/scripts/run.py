#!/usr/bin/env python3
"""
e2e-test-nfs-v4-integrity-check/scripts/run.py
NFS v4.1 完整性校验 e2e 测试（多场景）。
"""

import re
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_PROJECT_ROOT = _SKILL_DIR.parent.parent.parent
_HARNESS_SCRIPTS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS_SCRIPTS))

import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result, run_terrasync_timed
from protocol_constants import NfsV4 as _PC

SYNC_JOB_ID = "nfs-v4-ic-sync"
IC_JOB_ID = "nfs-v4-ic-test"
SANITIZED_SYNC = "nfs_v4_ic_sync"
NFS_SERVER_PATH = _PC.EXPORT
_TABLES_PATTERN = "nfs_v4_ic"


def _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path):
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo find {nfs_server_path} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
            ex.submit(a.ssh_exec, dest_ip,
                      f"sudo find {nfs_server_path} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
            ex.submit(_drop_ic_tables, a, ch_host),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{_TABLES_PATTERN}*' | xargs rm -rf"),
            ex.submit(a.run_shell_quiet, "rm -rf target/debug/logs/*"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                print(f"⚠ cleanup warning: {e}", flush=True)


def _drop_ic_tables(a, ch_host):
    tables = a.clickhouse_query(
        ch_host,
        f"SELECT name FROM system.tables WHERE database='default' "
        f"AND name LIKE '%{_TABLES_PATTERN}%' FORMAT TabSeparated"
    )
    for t in tables.strip().splitlines():
        t = t.strip()
        if t:
            a.clickhouse_execute(ch_host, f"DROP TABLE IF EXISTS default.{t}")


def _ic(binary, config, src_url, dst_url, *extra_args, timeout=600):
    return run_terrasync_timed([binary, "-c", config, "-l", "trace", "integrity-check",
         src_url, dst_url, *extra_args],
        capture_output=True, text=True, timeout=timeout)


def _check_ic(out, label, expect_pass, min_mismatch=0, min_missing=0):
    all_passed = "All Passed" in out
    if expect_pass:
        passed = all_passed
        return AssertionResult(f"ic_{label}", passed, {"all_passed": True}, {},
                               f"{'✓' if passed else '✗'} ic_{label}")
    mm = re.search(r"Mismatch[:\s]+(\d+)", out, re.IGNORECASE)
    ms = re.search(r"Missing[:\s]+(\d+)", out, re.IGNORECASE)
    actual_mm = int(mm.group(1)) if mm else 0
    actual_ms = int(ms.group(1)) if ms else 0
    passed = actual_mm >= min_mismatch and actual_ms >= min_missing
    return AssertionResult(
        f"ic_{label}", passed,
        {"mismatch>=": min_mismatch, "missing>=": min_missing},
        {"mismatch": actual_mm, "missing": actual_ms},
        f"{'✓' if passed else '✗'} ic_{label}: mm={actual_mm}, ms={actual_ms}"
    )


def run(env: dict = None) -> dict:
    os.chdir(_PROJECT_ROOT)
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V4_SOURCE_IP", "NFS_V4_DEST_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V4_SOURCE_IP"]
    dest_ip = cfg["NFS_V4_DEST_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_server_path = cfg.get("NFS_V4_SERVER_PATH", NFS_SERVER_PATH)
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_url = f"nfs://{src_ip}/?version=4.1"
    dst_url = f"nfs://{dest_ip}/?version=4.1"

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path)

    # 创建测试数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v4-full-scan" / "scripts" / "setup-nfs4-test-data.sh"
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-nfs4-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-nfs4-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return build_result(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} setup_nfs4"))
    if not setup_ok:
        return build_result(results, start)

    # Sync 到目标端
    proc = run_terrasync_timed([binary, "-c", config, "-l", "trace", "sync",
         "--id", SYNC_JOB_ID, src_url, dst_url],
        capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        results.append(AssertionResult("sync", False, {}, {}, "✗ sync failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path)
        return build_result(results, start)
    results.append(AssertionResult("sync", True, {}, {}, "✓ initial_sync"))

    # 场景1: Full 一致
    r1 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-full")
    results.append(_check_ic(r1.stdout + r1.stderr, "full_consistent", True))

    # 场景2: Quick 一致
    r2 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-quick", "--quick", timeout=300)
    results.append(_check_ic(r2.stdout + r2.stderr, "quick_consistent", True))

    # 制造 Mismatch
    try:
        a.ssh_exec(dest_ip,
                   f"echo 'tampered-v4' > {nfs_server_path}/test-data/d1/d1_1/file1.txt")
    except Exception as e:
        print(f"⚠ cleanup warning: {e}", flush=True)

    r3 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-mismatch")
    results.append(_check_ic(r3.stdout + r3.stderr, "detects_mismatch", False, min_mismatch=1))

    # 制造 Missing
    try:
        a.ssh_exec(dest_ip, f"rm -f {nfs_server_path}/test-data/d2/file1.txt")
    except Exception as e:
        print(f"⚠ cleanup warning: {e}", flush=True)

    r4 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-missing")
    results.append(_check_ic(r4.stdout + r4.stderr, "detects_missing", False,
                             min_mismatch=1, min_missing=1))

    # Auto-Fix：先恢复再修改属性再 fix
    run_terrasync_timed([binary, "-c", config, "-l", "trace", "sync",
                   "--id", f"{SYNC_JOB_ID}-fix", src_url, dst_url],
                   capture_output=True, timeout=900)
    try:
        a.ssh_exec(dest_ip, f"chmod 777 {nfs_server_path}/test-data/d1/d1_1/file1.txt")
    except Exception as e:
        print(f"⚠ cleanup warning: {e}", flush=True)
    r5 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-fix", "--auto-fix")
    results.append(AssertionResult("auto_fix", r5.returncode == 0, {}, {},
                                   f"{'✓' if r5.returncode == 0 else '✗'} auto_fix"))

    r6 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-verify")
    results.append(_check_ic(r6.stdout + r6.stderr, "post_fix_verify", True))

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
