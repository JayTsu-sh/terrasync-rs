#!/usr/bin/env python3
"""
e2e-test-nfs-v4-full-sync/scripts/run.py
NFS v4.1 全量同步 e2e 测试（源端 → 目标端）。
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
from protocol_constants import NfsV4 as _PC

JOB_ID = "nfs-v4-full-sync"
SANITIZED = "nfs_v4_full_sync"
EXPECTED_DIRS     = _PC.BASELINE_DIRS
EXPECTED_FILES    = _PC.BASELINE_FILES
EXPECTED_SYMLINKS = _PC.BASELINE_SYMLINKS
NFS_SERVER_PATH   = _PC.EXPORT

_TABLES = [
    f"base_{SANITIZED}", f"state_{SANITIZED}", f"tar_manifest_{SANITIZED}",
    f"base_{SANITIZED}_verify_src", f"state_{SANITIZED}_verify_src",
    f"base_{SANITIZED}_verify_dst", f"state_{SANITIZED}_verify_dst",
]


def _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path):
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo find {nfs_server_path} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
            ex.submit(a.ssh_exec, dest_ip,
                      f"sudo find {nfs_server_path} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
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

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []
    src_url = f"nfs://{src_ip}/?version=4.1"
    dst_url = f"nfs://{dest_ip}/?version=4.1"

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path)

    # Setup: use the setup script in THIS skill's scripts dir
    setup_sh = _SKILL_DIR / "scripts" / "setup-nfs4-test-data.sh"
    if not setup_sh.exists():
        # Fall back to full-scan skill's setup script
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

    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync", "--id", JOB_ID, src_url, dst_url],
        capture_output=True, text=True, timeout=600,
    )
    sync_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("sync_exit", False, {"code": 0},
                                       {"code": proc.returncode}, "✗ sync failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path)
        return build_result(results, start)

    exp = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
    results.append(a.check_cli_sync_output(sync_out, exp))

    # 并发验证（dest_find + clickhouse，不与 integrity-check 并发避免 NFS server busy）
    with ThreadPoolExecutor(max_workers=2) as ex:
        def dest_find():
            cmd = (f"sudo find {nfs_server_path}/test-data -type d | wc -l; "
                   f"sudo find {nfs_server_path}/test-data -type f | wc -l; "
                   f"sudo find {nfs_server_path}/test-data -type l | wc -l")
            out = a.ssh_exec(dest_ip, cmd, timeout=60)
            lines = [ln.strip() for ln in out.strip().splitlines() if ln.strip()]
            actual = {"dirs": int(lines[0]), "files": int(lines[1]), "symlinks": int(lines[2])}
            passed = actual == exp
            return AssertionResult("dest_find_counts", passed, exp, actual,
                                   f"{'✓' if passed else '✗'} dest_find_counts: {actual}")

        futs = [
            ex.submit(dest_find),
            ex.submit(a.check_clickhouse_counts, ch_host, f"base_{SANITIZED}", exp),
        ]
        for f in as_completed(futs):
            results.append(f.result())

    # integrity-check 串行执行（避免与 dest_find SSH 并发导致 NFS4 server busy）
    p = subprocess.run(
        [binary, "-c", config, "-l", "trace", "integrity-check",
         src_url, dst_url, "--quick"],
        capture_output=True, text=True, timeout=300)
    ic_out = p.stdout + p.stderr
    ic_passed = p.returncode == 0 and "All Passed" in ic_out
    ic_msg = f"{'✓' if ic_passed else '✗'} integrity_check_quick"
    if not ic_passed:
        for line in ic_out.splitlines()[-20:]:
            if any(k in line for k in ("Passed", "Failed", "Checked", "Mismatch", "error", "ERROR", "All ")):
                ic_msg += f"\n  {line.strip()}"
    results.append(AssertionResult("integrity_check", ic_passed, {}, {}, ic_msg))

    # 元数据验证（在目标端）
    verify_sh = _SKILL_DIR / "scripts" / "verify-metadata.sh"
    if verify_sh.exists():
        try:
            a.scp_to(verify_sh, dest_ip, "/tmp/verify-metadata-v4.sh")
            mout = a.ssh_exec(dest_ip, "sudo bash /tmp/verify-metadata-v4.sh", timeout=300)
            passed = "Mismatch: 0" in mout and "✓" in mout
            results.append(AssertionResult("metadata_verification", passed, {}, {},
                                           f"{'✓' if passed else '✗'} metadata_verification"))
        except Exception as e:
            results.append(AssertionResult("metadata_verification", False, {}, {},
                                           f"✗ metadata_verification: {e}"))

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_server_path)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
