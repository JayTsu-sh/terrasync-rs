#!/usr/bin/env python3
"""
e2e-test-nfs-v3-incremental-scan/scripts/run.py
NFS v3 增量扫描 e2e 测试：全量建基线 → 变更 → 增量扫描 → 验证。
"""

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

JOB_ID = "nfs-v3-incr-scan"
SANITIZED = "nfs_v3_incr_scan"

BASELINE_DIRS, BASELINE_FILES, BASELINE_SYMLINKS = _PC.BASELINE_DIRS, _PC.BASELINE_FILES, _PC.BASELINE_SYMLINKS
POST_DIRS, POST_FILES, POST_SYMLINKS               = _PC.POST_DIRS, _PC.POST_FILES, _PC.POST_SYMLINKS
POST_TOTAL                                         = _PC.POST_TOTAL

# 增量统计预期值（来自 SKILL.md Step 4b）
EXPECTED_INCR = {
    "new":     {"dirs": 2,  "files": 3,  "symlinks": 2,  "total": 7},
    "changed": {"dirs": 0,  "files": 9,  "symlinks": 0,  "total": 9},
    "renamed": {"dirs": 9,  "files": 28, "symlinks": 10, "total": 47},
    "deleted": {"dirs": 1,  "files": 5,  "symlinks": 2,  "total": 8},
}


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


def _terrasync_scan(binary, config, job_id, src_ip, nfs_export, timeout=300):
    return subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan",
         "--id", job_id, f"nfs://{src_ip}{nfs_export}"],
        capture_output=True, text=True, timeout=timeout,
    )


def _check_incr_statistics(stdout: str) -> AssertionResult:
    """验证增量扫描输出中的统计数字（New/Changed/Renamed/Deleted）。"""
    import re
    actual = {}
    for op in ("new", "changed", "renamed", "deleted"):
        # 匹配 "New:   7 total" 或 "New:          7 total"
        m = re.search(rf"{op}[:\s]+(\d+)\s+total", stdout, re.IGNORECASE)
        if m:
            actual[op] = int(m.group(1))

    if not actual:
        return AssertionResult(
            "incr_statistics", False, EXPECTED_INCR, {},
            "✗ incr_statistics: could not parse incremental statistics from output"
        )

    expected_totals = {k: v["total"] for k, v in EXPECTED_INCR.items()}
    passed = all(actual.get(k) == v for k, v in expected_totals.items())
    return AssertionResult(
        "incr_statistics", passed, expected_totals, actual,
        f"{'✓' if passed else '✗'} incr_statistics: expected={expected_totals}, actual={actual}"
    )


def _check_incr_ch_table(a, ch_host) -> list[AssertionResult]:
    """验证 CH incremental 表的分类计数（11 行）。"""
    query = (
        f"SELECT operation_type,is_dir,is_symlink,count(*) FROM default.incremental_{SANITIZED} "
        f"FINAL GROUP BY operation_type,is_dir,is_symlink "
        f"ORDER BY operation_type,is_dir,is_symlink FORMAT TabSeparated"
    )
    text = a.clickhouse_query(ch_host, query)
    rows = {}
    for line in text.strip().splitlines():
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        key = (parts[0].strip(), parts[1].strip(), parts[2].strip())
        rows[key] = int(parts[3].strip())

    expected_rows = {
        ("changed", "false", "false"): 9,
        ("changed", "true",  "false"): 2,
        ("deleted", "false", "false"): 5,
        ("deleted", "false", "true"):  2,
        ("deleted", "true",  "false"): 1,
        ("new",     "false", "false"): 3,
        ("new",     "false", "true"):  2,
        ("new",     "true",  "false"): 2,
        ("rename",  "false", "false"): 28,
        ("rename",  "false", "true"):  10,
        ("rename",  "true",  "false"): 9,
    }
    mismatches = {k: (expected_rows[k], rows.get(k)) for k in expected_rows
                  if rows.get(k) != expected_rows[k]}
    passed = not mismatches
    msg = (f"{'✓' if passed else '✗'} ch_incremental_table: "
           + (f"11 rows verified" if passed else f"mismatches={mismatches}"))
    return AssertionResult("ch_incremental_table", passed, expected_rows, rows, msg)


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V3_SOURCE_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V3_SOURCE_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_export = cfg.get("NFS_V3_EXPORT", _PC.EXPORT)
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, ch_host, nfs_export)

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

    # Step 2：全量扫描（建基线）
    proc = _terrasync_scan(binary, config, JOB_ID, src_ip, nfs_export)
    if proc.returncode != 0:
        results.append(AssertionResult("full_scan_exit", False, {"code": 0},
                                       {"code": proc.returncode}, "✗ full_scan: failed"))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    results.append(a.check_cli_scan_output(
        proc.stdout + proc.stderr,
        {"dirs": BASELINE_DIRS, "files": BASELINE_FILES, "symlinks": BASELINE_SYMLINKS}
    ))
    results.append(a.check_clickhouse_counts(
        ch_host, f"base_{SANITIZED}",
        {"dirs": BASELINE_DIRS, "files": BASELINE_FILES, "symlinks": BASELINE_SYMLINKS}
    ))

    # Step 3：变更数据
    mutate_sh = _SKILL_DIR / "scripts" / "mutate-test-data.sh"
    if not mutate_sh.exists():
        results.append(AssertionResult("mutate", False, {}, {},
                                       "✗ mutate: mutate-test-data.sh not found"))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)
    try:
        a.scp_to(mutate_sh, src_ip, "/tmp/mutate-test-data.sh")
        mut_out = a.ssh_exec(src_ip, "sudo bash /tmp/mutate-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ mutate: {e}"))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)
    mutate_ok = "OK:" in mut_out or "OK：" in mut_out
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} mutate_test_data"))
    if not mutate_ok:
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    # Step 4：增量扫描（同一 JOB_ID，jobs/ 目录存在 → 自动增量模式）
    proc2 = _terrasync_scan(binary, config, JOB_ID, src_ip, nfs_export)
    incr_out = proc2.stdout + proc2.stderr

    if proc2.returncode != 0:
        results.append(AssertionResult("incr_scan_exit", False, {"code": 0},
                                       {"code": proc2.returncode}, "✗ incr_scan: failed"))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    # 4a：Scanned 统计（遍历当前文件系统）
    results.append(a.check_cli_scan_output(
        incr_out, {"dirs": POST_DIRS, "files": POST_FILES, "symlinks": POST_SYMLINKS}
    ))

    # 4b：Incremental 统计
    results.append(_check_incr_statistics(incr_out))

    # 4c/4d：CH 表验证（并发）
    with ThreadPoolExecutor(max_workers=2) as ex:
        state_q = (f"SELECT scan_state FROM default.state_{SANITIZED} FINAL WHERE id=1 "
                   f"FORMAT TabSeparated")
        state = a.clickhouse_query(ch_host, state_q).strip()

        if state:
            count_q = (f"SELECT is_dir,is_symlink,count(*) FROM default.base_{SANITIZED} FINAL "
                       f"WHERE current_state={state} GROUP BY is_dir,is_symlink "
                       f"FORMAT TabSeparated")
            count_text = a.clickhouse_query(ch_host, count_q)
            actual_base = {}
            for line in count_text.strip().splitlines():
                p = line.split("\t")
                if len(p) < 3:
                    continue
                if p[0] == "true" and p[1] == "false":
                    actual_base["dirs"] = int(p[2])
                elif p[0] == "false" and p[1] == "true":
                    actual_base["symlinks"] = int(p[2])
                elif p[0] == "false" and p[1] == "false":
                    actual_base["files"] = int(p[2])
            exp = {"dirs": POST_DIRS, "files": POST_FILES, "symlinks": POST_SYMLINKS}
            passed = actual_base == exp
            results.append(AssertionResult(
                "ch_base_post_incr", passed, exp, actual_base,
                f"{'✓' if passed else '✗'} ch_base_post_incr: expected={exp}, actual={actual_base}"
            ))
        else:
            results.append(AssertionResult("ch_base_post_incr", False, {}, {},
                                           "✗ ch_base_post_incr: scan_state empty"))

        results.append(_check_incr_ch_table(a, ch_host))

    _cleanup(a, src_ip, ch_host, nfs_export)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
