#!/usr/bin/env python3
"""
e2e-test-nfs-v3-full-scan/scripts/run.py
NFS v3 全量扫描 e2e 测试，对应 SKILL.md 的完整流程。

独立运行：
  cd .claude/skills/e2e-test-nfs-v3-full-scan
  python scripts/run.py

harness runner 调用：
  result = run(env=cfg_dict)
"""

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# 加入 harness-run/scripts 到 path，使用共享库
_SKILL_DIR = Path(__file__).parent.parent
_HARNESS_SCRIPTS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS_SCRIPTS))

import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result

# ── 测试常量 ──────────────────────────────────────────────────────────────────
JOB_ID = "nfs-v3-full-scan"
SANITIZED_JOB_ID = "nfs_v3_full_scan"
EXPECTED_DIRS = 113
EXPECTED_FILES = 335
EXPECTED_SYMLINKS = 79
EXPECTED_TOTAL = EXPECTED_DIRS + EXPECTED_FILES + EXPECTED_SYMLINKS  # 527


# ── 内部步骤 ──────────────────────────────────────────────────────────────────

def _cleanup(a: TerrasyncAssertions, src_ip: str, ch_host: str, nfs_export: str):
    """Step 0/3：并发清理（幂等，失败静默）。"""
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo cleaned || true"),
            ex.submit(a.clickhouse_drop_tables, ch_host, SANITIZED_JOB_ID),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED_JOB_ID}*' | xargs rm -rf"),
            ex.submit(a.run_shell_quiet, "rm -rf target/debug/logs/*"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception:
                pass  # 清理步骤 best-effort


def _setup_test_data(a: TerrasyncAssertions, src_ip: str) -> AssertionResult:
    """Steps 1a-1b：上传并执行 setup-test-data.sh。"""
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v3" / "scripts" / "setup-test-data.sh"
    if not setup_sh.exists():
        return AssertionResult(
            "setup_test_data", False, {}, {},
            f"✗ setup_test_data: script not found: {setup_sh}"
        )

    # 1a: scp 上传脚本
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-test-data.sh")
    except Exception as e:
        return AssertionResult(
            "setup_test_data", False, {}, {}, f"✗ setup_test_data: scp failed: {e}"
        )

    # 1b: 执行创建测试数据
    try:
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-test-data.sh", timeout=120)
    except Exception as e:
        return AssertionResult(
            "setup_test_data", False, {}, {}, f"✗ setup_test_data: ssh exec failed: {e}"
        )

    passed = "OK:" in out or "OK：" in out
    return AssertionResult(
        "setup_test_data", passed, {"contains": "OK:"}, {"output_tail": out[-200:]},
        f"{'✓' if passed else '✗'} setup_test_data: "
        + ("test data created" if passed else "script did not report success")
    )


def _verify_state_and_total(
    a: TerrasyncAssertions, ch_host: str
) -> AssertionResult:
    """Step 2c：验证 state 表 + base 表总行数。"""
    query = (
        f"SELECT scan_state FROM default.state_{SANITIZED_JOB_ID} "
        f"FINAL WHERE id=1 FORMAT TabSeparated"
    )
    state = a.clickhouse_query(ch_host, query).strip()
    if not state:
        return AssertionResult(
            "state_table_total", False, {"scan_state": "non-empty"}, {},
            "✗ state_table_total: scan_state is empty (state table not written)"
        )

    count_q = (
        f"SELECT count(*) FROM default.base_{SANITIZED_JOB_ID} FINAL "
        f"WHERE current_state={state} FORMAT TabSeparated"
    )
    count_str = a.clickhouse_query(ch_host, count_q).strip()
    try:
        count = int(count_str)
    except ValueError:
        return AssertionResult(
            "state_table_total", False, {"total": EXPECTED_TOTAL}, {"raw": count_str},
            f"✗ state_table_total: unexpected response: {count_str!r}"
        )

    passed = count == EXPECTED_TOTAL
    return AssertionResult(
        "state_table_total", passed,
        {"total": EXPECTED_TOTAL}, {"total": count},
        f"{'✓' if passed else '✗'} state_table_total: expected={EXPECTED_TOTAL}, actual={count}"
    )


def _verify_filesystem(a: TerrasyncAssertions, src_ip: str, nfs_export: str) -> AssertionResult:
    """Step 2d：SSH 直接 find 对照 ClickHouse 计数。"""
    cmd = (
        f"sudo find {nfs_export}/test-data -type d | wc -l; "
        f"sudo find {nfs_export}/test-data -type f | wc -l; "
        f"sudo find {nfs_export}/test-data -type l | wc -l"
    )
    try:
        out = a.ssh_exec(src_ip, cmd, timeout=60)
    except Exception as e:
        return AssertionResult(
            "filesystem_counts", False, {}, {}, f"✗ filesystem_counts: SSH failed: {e}"
        )

    lines = [ln.strip() for ln in out.strip().splitlines() if ln.strip()]
    if len(lines) < 3:
        return AssertionResult(
            "filesystem_counts", False, {"lines": 3}, {"lines": len(lines)},
            f"✗ filesystem_counts: unexpected output: {out!r}"
        )
    try:
        actual = {
            "dirs": int(lines[0]),
            "files": int(lines[1]),
            "symlinks": int(lines[2]),
        }
    except ValueError:
        return AssertionResult(
            "filesystem_counts", False, {}, {"output": out},
            f"✗ filesystem_counts: could not parse integers from: {lines}"
        )

    expected = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS}
    passed = actual == expected
    return AssertionResult(
        "filesystem_counts", passed, expected, actual,
        f"{'✓' if passed else '✗'} filesystem_counts: expected={expected}, actual={actual}"
    )


def _verify_metadata(a: TerrasyncAssertions, src_ip: str) -> AssertionResult:
    """Step 2e：上传并运行 verify-metadata.sh。"""
    verify_sh = _SKILL_DIR / "scripts" / "verify-metadata.sh"
    if not verify_sh.exists():
        return AssertionResult(
            "metadata_verification", False, {}, {},
            f"✗ metadata_verification: verify-metadata.sh not found"
        )

    try:
        a.scp_to(verify_sh, src_ip, "/tmp/verify-metadata.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/verify-metadata.sh", timeout=300)
    except Exception as e:
        return AssertionResult(
            "metadata_verification", False, {}, {}, f"✗ metadata_verification: {e}"
        )

    passed = "Mismatch: 0" in out and "✓" in out
    return AssertionResult(
        "metadata_verification", passed,
        {"mismatches": 0}, {"output_tail": out[-400:]},
        f"{'✓' if passed else '✗'} metadata_verification"
        + (" — all 527 entries matched" if passed else " — see output for details")
    )


# ── 公共接口 ──────────────────────────────────────────────────────────────────

def run(env: dict = None) -> dict:
    """
    harness runner 调用入口。
    env: harness runner 注入的配置 dict；独立运行时从 .env 加载。
    返回: {"passed": bool, "metrics": {...}, "assertions": [...]}
    """
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V3_SOURCE_IP", "CLICKHOUSE_HOST")

    src_ip = cfg["NFS_V3_SOURCE_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    nfs_export = cfg.get("NFS_V3_EXPORT", "/export/nfs")
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results: list[AssertionResult] = []

    # Step 0：清理
    _cleanup(a, src_ip, ch_host, nfs_export)

    # Step 1：创建测试数据
    setup_r = _setup_test_data(a, src_ip)
    results.append(setup_r)
    if not setup_r.passed:
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    # Step 2：全量扫描
    scan_cmd = [
        binary, "-c", config, "-l", "trace",
        "scan", "--id", JOB_ID,
        f"nfs://{src_ip}{nfs_export}",
    ]
    proc = subprocess.run(scan_cmd, capture_output=True, text=True, timeout=300)
    scan_output = proc.stdout + proc.stderr

    if proc.returncode != 0:
        results.append(AssertionResult(
            "scan_exit_code", False, {"code": 0}, {"code": proc.returncode},
            f"✗ scan_exit_code: terrasync exited with {proc.returncode}"
        ))
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    # Step 2a：CLI 输出验证
    cli_r = a.check_cli_scan_output(
        scan_output,
        {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS},
    )
    results.append(cli_r)
    if not cli_r.passed:
        _cleanup(a, src_ip, ch_host, nfs_export)
        return build_result(results, start)

    # Steps 2b/2c/2d：并发验证
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {
            ex.submit(
                a.check_clickhouse_counts, ch_host, f"base_{SANITIZED_JOB_ID}",
                {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES, "symlinks": EXPECTED_SYMLINKS},
            ): "ch_counts",
            ex.submit(_verify_state_and_total, a, ch_host): "state_total",
            ex.submit(_verify_filesystem, a, src_ip, nfs_export): "filesystem",
        }
        for f in as_completed(futs):
            results.append(f.result())

    # Step 2e：元数据验证（依赖 state 表，串行）
    results.append(_verify_metadata(a, src_ip))

    # Step 3：清理
    _cleanup(a, src_ip, ch_host, nfs_export)

    return build_result(results, start)




if __name__ == "__main__":
    result = run()
    sys.exit(0 if result["passed"] else 1)
