#!/usr/bin/env python3
"""
e2e-test-nfs-v3-integrity-check/scripts/run.py
NFS v3 完整性校验 e2e 测试：多场景验证（一致/Mismatch/Missing/AutoFix）。

注意：本 skill 使用与全量扫描相同的测试数据集（EXPECTED_DIRS=113, FILES=335, SYMLINKS=79）。
SKILL.md 中显示的 40/117/36 为历史遗留数值，以实际 setup-test-data.sh 输出为准。
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

SYNC_JOB_ID = "nfs-v3-ic-sync"
IC_JOB_ID = "nfs-v3-ic-test"
SANITIZED_SYNC = "nfs_v3_ic_sync"

_TABLES_PATTERN = "nfs_v3_ic"


def _cleanup(a, src_ip, dest_ip, ch_host, nfs_export):
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo ok || true"),
            ex.submit(a.ssh_exec, dest_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo ok || true"),
            ex.submit(_drop_all_ic_tables, a, ch_host),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{_TABLES_PATTERN}*' | xargs rm -rf"),
            ex.submit(a.run_shell_quiet, "rm -rf target/debug/logs/*"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception:
                pass


def _drop_all_ic_tables(a, ch_host):
    """动态查出所有 nfs_v3_ic* 表并 DROP。"""
    tables = a.clickhouse_query(
        ch_host,
        f"SELECT name FROM system.tables WHERE database='default' "
        f"AND name LIKE '%{_TABLES_PATTERN}%' FORMAT TabSeparated"
    )
    for t in tables.strip().splitlines():
        t = t.strip()
        if t:
            a.clickhouse_query(ch_host, f"DROP TABLE IF EXISTS default.{t}")


def _terrasync(binary, config, *args, timeout=600):
    return subprocess.run([binary, "-c", config, "-l", "trace", *args],
                         capture_output=True, text=True, timeout=timeout)


def _check_ic_output(out: str, expect_passed: bool, label: str,
                     min_mismatch: int = 0, min_missing: int = 0) -> AssertionResult:
    """验证 integrity-check 输出是否符合预期。"""
    all_passed = "All Passed" in out

    if expect_passed:
        passed = all_passed
        msg = (f"{'✓' if passed else '✗'} ic_{label}: "
               + ("All Passed ✓" if passed else "expected All Passed but found issues"))
        return AssertionResult(f"ic_{label}", passed, {"all_passed": True}, {}, msg)

    # 期望有错误
    mismatch_m = re.search(r"Mismatch[:\s]+(\d+)", out, re.IGNORECASE)
    missing_m = re.search(r"Missing[:\s]+(\d+)", out, re.IGNORECASE)
    actual_mismatch = int(mismatch_m.group(1)) if mismatch_m else 0
    actual_missing = int(missing_m.group(1)) if missing_m else 0

    passed = actual_mismatch >= min_mismatch and actual_missing >= min_missing
    return AssertionResult(
        f"ic_{label}", passed,
        {"mismatch>=": min_mismatch, "missing>=": min_missing},
        {"mismatch": actual_mismatch, "missing": actual_missing},
        f"{'✓' if passed else '✗'} ic_{label}: mismatch={actual_mismatch}, missing={actual_missing}"
    )


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V3_SOURCE_IP", "NFS_V3_DEST_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V3_SOURCE_IP"]
    dest_ip = cfg["NFS_V3_DEST_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_export = cfg.get("NFS_V3_EXPORT", "/export/nfs")
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_url = f"nfs://{src_ip}{nfs_export}"
    dst_url = f"nfs://{dest_ip}{nfs_export}"

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)

    # Step 2a：创建测试数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v3" / "scripts" / "setup-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {},
                                       f"✗ setup: setup-test-data.sh not found"))
        return _build_result(results, start)
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return _build_result(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} setup_test_data"))
    if not setup_ok:
        return _build_result(results, start)

    # Step 2b：Sync 到目标端
    proc = _terrasync(binary, config, "sync", "--id", SYNC_JOB_ID, src_url, dst_url,
                      timeout=900)
    if proc.returncode != 0:
        results.append(AssertionResult("initial_sync", False, {"code": 0},
                                       {"code": proc.returncode}, "✗ initial_sync: failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return _build_result(results, start)
    results.append(AssertionResult("initial_sync", True, {}, {}, "✓ initial_sync: OK"))

    # Step 3：场景1 — Full 模式（应全部通过）
    proc3 = _terrasync(binary, config, "integrity-check", "--id", IC_JOB_ID,
                       src_url, dst_url, timeout=600)
    results.append(_check_ic_output(proc3.stdout + proc3.stderr, True, "full_consistent"))

    # Step 4：场景2 — Quick 模式（应全部通过）
    proc4 = _terrasync(binary, config, "integrity-check", "--id", f"{IC_JOB_ID}-quick",
                       "--quick", src_url, dst_url, timeout=300)
    results.append(_check_ic_output(proc4.stdout + proc4.stderr, True, "quick_consistent"))

    # Step 5a：在目标端修改文件（制造 Mismatch）
    try:
        a.ssh_exec(dest_ip,
                   f"echo 'tampered-content-for-integrity-check' "
                   f"> {nfs_export}/test-data/d1/d1_1/file1.txt")
    except Exception as e:
        results.append(AssertionResult("tamper_mismatch", False, {}, {}, f"✗ tamper: {e}"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return _build_result(results, start)

    # Step 5b：Full 模式（应检测到 Mismatch）
    proc5b = _terrasync(binary, config, "integrity-check", "--id", f"{IC_JOB_ID}-mismatch",
                        src_url, dst_url, timeout=600)
    results.append(_check_ic_output(proc5b.stdout + proc5b.stderr, False, "detects_mismatch",
                                    min_mismatch=1))

    # Step 5c：删除目标端文件（制造 Missing）
    try:
        a.ssh_exec(dest_ip, f"rm -f {nfs_export}/test-data/d2/file1.txt")
    except Exception as e:
        results.append(AssertionResult("tamper_missing", False, {}, {}, f"✗ tamper: {e}"))
        _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
        return _build_result(results, start)

    # Step 5d：Full 模式（应检测到 Missing + Mismatch）
    proc5d = _terrasync(binary, config, "integrity-check", "--id", f"{IC_JOB_ID}-missing",
                        src_url, dst_url, timeout=600)
    results.append(_check_ic_output(proc5d.stdout + proc5d.stderr, False, "detects_missing",
                                    min_mismatch=1, min_missing=1))

    # Step 6：Auto-Fix 模式（先恢复缺失文件，再修改属性，再 auto-fix）
    # 6a：重新 sync 恢复 missing 文件
    proc6a = _terrasync(binary, config, "sync", "--id", f"{SYNC_JOB_ID}-fix",
                        src_url, dst_url, timeout=900)
    sync_ok = proc6a.returncode == 0
    results.append(AssertionResult("restore_sync", sync_ok, {"code": 0},
                                   {"code": proc6a.returncode},
                                   f"{'✓' if sync_ok else '✗'} restore_sync"))

    # 6b：在目标端改文件属性
    try:
        a.ssh_exec(dest_ip,
                   f"chmod 777 {nfs_export}/test-data/d1/d1_1/file1.txt")
    except Exception:
        pass  # best-effort

    # 6c：Auto-Fix
    proc6c = _terrasync(binary, config, "integrity-check", "--id", f"{IC_JOB_ID}-fix",
                        "--auto-fix", src_url, dst_url, timeout=600)
    results.append(AssertionResult("auto_fix", proc6c.returncode == 0, {}, {},
                                   f"{'✓' if proc6c.returncode == 0 else '✗'} auto_fix"))

    # 6d：修复后再次校验（应全部通过）
    proc6d = _terrasync(binary, config, "integrity-check", "--id", f"{IC_JOB_ID}-verify",
                        src_url, dst_url, timeout=600)
    results.append(_check_ic_output(proc6d.stdout + proc6d.stderr, True, "post_fix_verify"))

    _cleanup(a, src_ip, dest_ip, ch_host, nfs_export)
    return _build_result(results, start)


def _build_result(results, start):
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
