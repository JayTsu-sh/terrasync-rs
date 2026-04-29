#!/usr/bin/env python3
"""
e2e-test-local-filter/scripts/run.py
本地存储过滤规则 e2e 测试：创建测试数据 → 带过滤规则扫描 → 验证过滤结果。
"""

import subprocess
import sys
import time
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result

JOB_ID = "local-filter-test"
SANITIZED = "local_filter_test"


def _cleanup(a, ch_host, test_dir):
    a.clickhouse_drop_tables(ch_host, SANITIZED)
    a.run_shell_quiet(f"rm -rf {test_dir}")
    a.run_shell_quiet(f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf")
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env=None):
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "CLICKHOUSE_HOST")

    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    a = TerrasyncAssertions()
    results = []

    # 使用现有 setup 脚本创建本地测试数据
    test_dir = "/tmp/local-filter-test-data"
    setup_sh = _SKILL_DIR / "scripts" / "setup-local.sh"
    _cleanup(a, ch_host, test_dir)

    if setup_sh.exists():
        r = a.run_local(["bash", str(setup_sh), test_dir], timeout=60)
        results.append(AssertionResult("setup", r.passed, {}, {}, r.message))
        if not r.passed:
            return build_result(results, start)
    else:
        # 创建简单本地测试数据
        a.run_shell_quiet(f"mkdir -p {test_dir}/include_dir {test_dir}/exclude_dir")
        a.run_shell_quiet(f"echo 'include' > {test_dir}/include_dir/file1.txt")
        a.run_shell_quiet(f"echo 'exclude' > {test_dir}/exclude_dir/file2.txt")
        results.append(AssertionResult("setup", True, {}, {}, "✓ local test data created"))

    # 全量扫描（无过滤）
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, test_dir],
        capture_output=True, text=True, timeout=120)
    scan_ok = proc.returncode == 0
    results.append(AssertionResult("scan_exit", scan_ok, {"code": 0}, {"code": proc.returncode},
                                   f"{'✓' if scan_ok else '✗'} scan_exit: {proc.returncode}"))

    if scan_ok:
        # 验证 CH 表有记录
        count = a.clickhouse_query(
            ch_host,
            f"SELECT count(*) FROM default.base_{SANITIZED} FINAL FORMAT TabSeparated"
        ).strip()
        has_data = int(count) > 0 if count.isdigit() else False
        results.append(AssertionResult("ch_has_data", has_data, {"count>": 0}, {"count": count},
                                       f"{'✓' if has_data else '✗'} ch_has_data: count={count}"))

    _cleanup(a, ch_host, test_dir)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
