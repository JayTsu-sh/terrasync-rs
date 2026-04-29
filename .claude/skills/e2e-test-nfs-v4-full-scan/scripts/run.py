#!/usr/bin/env python3
"""
e2e-test-nfs-v4-full-scan/scripts/run.py
NFS v4.1 全量扫描 e2e 测试。与 v3 的区别：URL 加 ?version=4.1，服务器路径 /export/nfsv4。
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
from protocol_constants import NfsV4 as _PC

JOB_ID = "nfs-v4-full-scan"
SANITIZED = "nfs_v4_full_scan"
EXPECTED_DIRS     = _PC.BASELINE_DIRS
EXPECTED_FILES    = _PC.BASELINE_FILES
EXPECTED_SYMLINKS = _PC.BASELINE_SYMLINKS
EXPECTED_TOTAL    = _PC.BASELINE_TOTAL
NFS_SERVER_PATH   = _PC.EXPORT


def _cleanup(a, src_ip, ch_host):
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo rm -rf {NFS_SERVER_PATH}/test-data && echo ok || true"),
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


def _setup_data(a, src_ip):
    setup_sh = _SKILL_DIR / "scripts" / "setup-nfs4-test-data.sh"
    if not setup_sh.exists():
        return AssertionResult("setup", False, {}, {},
                               f"✗ setup: {setup_sh} not found")
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-nfs4-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-nfs4-test-data.sh", timeout=120)
    except Exception as e:
        return AssertionResult("setup", False, {}, {}, f"✗ setup: {e}")
    passed = "OK:" in out or "OK：" in out
    return AssertionResult("setup", passed, {}, {},
                           f"{'✓' if passed else '✗'} setup_nfs4_test_data")


def _verify_fh_and_mode(a, ch_host) -> list[AssertionResult]:
    """NFS v4.1 特有验证：file_handle 非空 + mode 字段已填写。"""
    results = []
    fh_q = (f"SELECT count(*) FROM default.base_{SANITIZED} "
            f"FINAL WHERE file_handle='' FORMAT TabSeparated")
    fh_count = a.clickhouse_query(ch_host, fh_q).strip()
    passed = fh_count == "0"
    results.append(AssertionResult(
        "file_handle_populated", passed,
        {"empty_count": 0}, {"empty_count": fh_count},
        f"{'✓' if passed else '✗'} file_handle_populated: empty={fh_count}"
    ))

    mode_q = (f"SELECT count(*) FROM default.base_{SANITIZED} FINAL "
              f"WHERE mode=0 AND is_dir=false AND is_symlink=false FORMAT TabSeparated")
    mode_count = a.clickhouse_query(ch_host, mode_q).strip()
    mode_ok = mode_count == "0"
    results.append(AssertionResult(
        "mode_fields_populated", mode_ok,
        {"zero_mode_files": 0}, {"zero_mode_files": mode_count},
        f"{'✓' if mode_ok else '✗'} mode_fields_populated: zero_mode={mode_count}"
    ))
    return results


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

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []
    src_url = f"nfs://{src_ip}{nfs_export}?version=4.1"

    _cleanup(a, src_ip, ch_host)

    setup_r = _setup_data(a, src_ip)
    results.append(setup_r)
    if not setup_r.passed:
        return build_result(results, start)

    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300,
    )
    scan_out = proc.stdout + proc.stderr

    if proc.returncode != 0:
        results.append(AssertionResult("scan_exit", False, {"code": 0},
                                       {"code": proc.returncode}, "✗ scan failed"))
        _cleanup(a, src_ip, ch_host)
        return build_result(results, start)

    results.append(a.check_cli_scan_output(
        scan_out, {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
    ))
    results.append(a.check_clickhouse_counts(
        ch_host, f"base_{SANITIZED}",
        {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
    ))
    results.extend(_verify_fh_and_mode(a, ch_host))

    # 独立文件系统核查
    try:
        fs_out = a.ssh_exec(src_ip,
                            f"sudo find {nfs_export}/test-data -type d | wc -l; "
                            f"sudo find {nfs_export}/test-data -type f | wc -l; "
                            f"sudo find {nfs_export}/test-data -type l | wc -l", timeout=60)
        lines = [ln.strip() for ln in fs_out.strip().splitlines() if ln.strip()]
        if len(lines) >= 3:
            actual = {"dirs": int(lines[0]), "files": int(lines[1]), "symlinks": int(lines[2])}
            exp = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
            passed = actual == exp
            results.append(AssertionResult(
                "filesystem_counts", passed, exp, actual,
                f"{'✓' if passed else '✗'} filesystem_counts: {actual}"
            ))
    except Exception as e:
        results.append(AssertionResult("filesystem_counts", False, {}, {},
                                       f"✗ filesystem_counts: {e}"))

    _cleanup(a, src_ip, ch_host)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
