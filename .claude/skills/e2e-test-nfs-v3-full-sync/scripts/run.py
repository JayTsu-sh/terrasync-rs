#!/usr/bin/env python3
"""
e2e-test-nfs-v3-full-sync/scripts/run.py
NFS v3 全量同步 e2e 测试（源端 → 目标端）。
"""

import subprocess
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_PROJECT_ROOT = _SKILL_DIR.parent.parent.parent
_HARNESS_SCRIPTS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS_SCRIPTS))

import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result
from protocol_constants import NfsV3 as _PC

JOB_ID = "nfs-v3-full-sync"
SANITIZED = "nfs_v3_full_sync"
EXPECTED_DIRS     = _PC.BASELINE_DIRS
EXPECTED_FILES    = _PC.BASELINE_FILES
EXPECTED_SYMLINKS = _PC.BASELINE_SYMLINKS
EXPECTED_TOTAL    = _PC.BASELINE_TOTAL

# full-sync 产生的所有 CH 表（包含 integrity-check 时的 verify 表）
_TABLES = [
    f"base_{SANITIZED}", f"state_{SANITIZED}", f"tar_manifest_{SANITIZED}",
    f"base_{SANITIZED}_verify_src", f"state_{SANITIZED}_verify_src",
    f"base_{SANITIZED}_verify_dst", f"state_{SANITIZED}_verify_dst",
]


def _cleanup(a, src_ip, dest_ip, ch_host, nfs_export):
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo find {nfs_export} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
            ex.submit(a.ssh_exec, dest_ip,
                      f"sudo find {nfs_export} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
            *[ex.submit(a.clickhouse_execute, ch_host, f"DROP TABLE IF EXISTS default.{t}") for t in _TABLES],
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
            ex.submit(a.run_shell_quiet, "rm -rf target/debug/logs/*"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                print(f"⚠ cleanup warning: {e}", flush=True)


def _setup_data(a, src_ip):
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v3" / "scripts" / "setup-test-data.sh"
    if not setup_sh.exists():
        return AssertionResult("setup_test_data", False, {}, {},
                               f"✗ setup_test_data: script not found: {setup_sh}")
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-test-data.sh", timeout=120)
    except Exception as e:
        return AssertionResult("setup_test_data", False, {}, {}, f"✗ setup_test_data: {e}")
    passed = "OK:" in out or "OK：" in out
    return AssertionResult("setup_test_data", passed, {}, {},
                           f"{'✓' if passed else '✗'} setup_test_data")


def _verify_dest_find(a, dest_ip, nfs_export):
    cmd = (f"sudo find {nfs_export}/test-data -type d | wc -l; "
           f"sudo find {nfs_export}/test-data -type f | wc -l; "
           f"sudo find {nfs_export}/test-data -type l | wc -l")
    try:
        out = a.ssh_exec(dest_ip, cmd, timeout=60)
    except Exception as e:
        return AssertionResult("dest_find_counts", False, {}, {}, f"✗ dest_find_counts: {e}")
    lines = [ln.strip() for ln in out.strip().splitlines() if ln.strip()]
    if len(lines) < 3:
        return AssertionResult("dest_find_counts", False, {"lines": 3}, {"lines": len(lines)},
                               f"✗ dest_find_counts: unexpected output")
    actual = {"dirs": int(lines[0]), "files": int(lines[1]), "symlinks": int(lines[2])}
    expected = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
    passed = actual == expected
    return AssertionResult("dest_find_counts", passed, expected, actual,
                           f"{'✓' if passed else '✗'} dest_find_counts: {actual}")


def _run_integrity_check(binary, config, src_ip, dest_ip, nfs_export):
    src_url = f"nfs://{src_ip}{nfs_export}"
    dst_url = f"nfs://{dest_ip}{nfs_export}"
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "integrity-check",
         src_url, dst_url, "--quick"],
        capture_output=True, text=True, timeout=300,
    )
    out = proc.stdout + proc.stderr
    passed = proc.returncode == 0 and "All Passed" in out
    return AssertionResult(
        "integrity_check", passed, {"returncode": 0, "all_passed": True},
        {"returncode": proc.returncode},
        f"{'✓' if passed else '✗'} integrity_check: {'All Passed' if passed else 'FAILED'}"
    )


def _verify_metadata(a, dest_ip):
    verify_sh = _SKILL_DIR / "scripts" / "verify-metadata.sh"
    if not verify_sh.exists():
        return AssertionResult("metadata_verification", False, {}, {},
                               "✗ metadata_verification: verify-metadata.sh not found")
    try:
        a.scp_to(verify_sh, dest_ip, "/tmp/verify-metadata.sh")
        out = a.ssh_exec(dest_ip, "sudo bash /tmp/verify-metadata.sh", timeout=300)
    except Exception as e:
        return AssertionResult("metadata_verification", False, {}, {},
                               f"✗ metadata_verification: {e}")
    passed = "Mismatch: 0" in out and "✓" in out
    return AssertionResult("metadata_verification", passed, {"mismatches": 0}, {},
                           f"{'✓' if passed else '✗'} metadata_verification")


def run(env: dict = None) -> dict:
    os.chdir(_PROJECT_ROOT)
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

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)

    setup_r = _setup_data(a, src_ip)
    results.append(setup_r)
    if not setup_r.passed:
        return build_result(results, start)

    src_url = f"nfs://{src_ip}{nfs_export}"
    dst_url = f"nfs://{dest_ip}{nfs_export}"
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync", "--id", JOB_ID, src_url, dst_url],
        capture_output=True, text=True, timeout=600,
    )
    sync_out = proc.stdout + proc.stderr

    if proc.returncode != 0:
        results.append(AssertionResult("sync_exit_code", False, {"code": 0},
                                       {"code": proc.returncode},
                                       f"✗ sync_exit_code: {proc.returncode}"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return build_result(results, start)

    cli_r = a.check_cli_sync_output(
        sync_out, {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
    )
    results.append(cli_r)

    # 并发验证
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [
            ex.submit(_verify_dest_find, a, dest_ip, nfs_export),
            ex.submit(a.check_clickhouse_counts, ch_host, f"base_{SANITIZED}",
                      {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES,
                       "symlinks": EXPECTED_SYMLINKS}),
            ex.submit(_run_integrity_check, binary, config, src_ip, dest_ip, nfs_export),
        ]
        for f in as_completed(futs):
            results.append(f.result())

    results.append(_verify_metadata(a, dest_ip))
    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
