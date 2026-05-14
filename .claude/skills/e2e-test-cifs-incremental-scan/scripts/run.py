#!/usr/bin/env python3
"""
e2e-test-cifs-incremental-scan/scripts/run.py
CIFS 增量扫描 e2e 测试：全量建基线 → mutate → 增量扫描 → 验证。
CIFS Fh3 增量统计（来自 mutate 脚本注释）：
  New:5(dirs=2,files=3), Changed:2(files=2), Renamed:5(dirs=1,files=4), Deleted:6(dirs=1,files=5)
"""

import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_PROJECT_ROOT = _SKILL_DIR.parent.parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result, run_terrasync_timed
from protocol_constants import Cifs as _PC

JOB_ID = "cifs-incr-scan"
SANITIZED = "cifs_incr_scan"
BASELINE_DIRS, BASELINE_FILES = _PC.BASELINE_DIRS, _PC.BASELINE_FILES
POST_DIRS, POST_FILES = _PC.POST_DIRS, _PC.POST_FILES


def _check_smbclient():
    return subprocess.run(["smbclient", "--version"], capture_output=True).returncode == 0


def _cifs_url(host, user, passwd, share, prefix="test-data"):
    return f"smb://{user}:{passwd}@{host}/{share}/{prefix}"


def _smb_rm(host, user, passwd, share, prefix="test-data"):
    subprocess.run(["smbclient", f"//{host}/{share}", "-U", f"{user}%{passwd}",
                    "-c", f"deltree {prefix}"], capture_output=True, timeout=30)


def _run_script(script_path, host, user, passwd, share):
    env_vars = {**os.environ,
                "CIFS_HOST": host, "CIFS_SHARE": share,
                "CIFS_USER": user, "CIFS_PASS": passwd}
    return subprocess.run(["bash", str(script_path)],
                          capture_output=True, text=True, timeout=180, env=env_vars)


def _cleanup(a, cfg):
    host = cfg["CIFS_SOURCE_HOST"]
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SOURCE_SHARE", _PC.SHARE); ch_host = cfg["CLICKHOUSE_HOST"]
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [
            ex.submit(_smb_rm, host, user, passwd, share),
            ex.submit(a.clickhouse_drop_tables, ch_host, SANITIZED),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try: f.result()
            except Exception as e:
                print(f"⚠ cleanup warning: {e}", flush=True)
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def _check_incr_stats(stdout, expected):
    actual = {}
    for op in expected:
        m = re.search(rf"{op}[:\s]+(\d+)\s+total", stdout, re.IGNORECASE)
        if m: actual[op] = int(m.group(1))
    passed = all(actual.get(k) == v for k, v in expected.items())
    return AssertionResult("incr_statistics", passed, expected, actual,
                           f"{'✓' if passed else '✗'} incr_statistics: expected={expected}, actual={actual}")


def run(env=None):
    os.chdir(_PROJECT_ROOT)
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "CIFS_SOURCE_HOST", "CLICKHOUSE_HOST")

    if not _check_smbclient():
        return {"passed": False, "metrics": {"elapsed_sec": 0},
                "assertions": [{"name": "smbclient", "passed": False,
                                "message": "✗ smbclient not found — install samba-client"}]}

    host = cfg["CIFS_SOURCE_HOST"]
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SOURCE_SHARE", _PC.SHARE); ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    cifs_url = _cifs_url(host, user, passwd, share)
    a = TerrasyncAssertions()
    results = []

    _cleanup(a, cfg)

    # Setup 基线数据
    setup_sh = _SKILL_DIR.parent / "_shared" / "cifs" / "setup-cifs-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {}, f"✗ {setup_sh} not found"))
        return build_result(results, start)
    p = _run_script(setup_sh, host, user, passwd, share)
    setup_ok = p.returncode == 0
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} cifs_setup"))
    if not setup_ok: return build_result(results, start)

    # 全量扫描（建基线）
    proc = run_terrasync_timed([binary, "-c", config, "-l", "trace", "scan",
                          "--id", JOB_ID, cifs_url],
                         capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        results.append(AssertionResult("full_scan", False, {}, {}, "✗ full_scan failed"))
        _cleanup(a, cfg); return build_result(results, start)
    bl = {"dirs": BASELINE_DIRS, "files": BASELINE_FILES}
    results.append(a.check_cli_scan_output(proc.stdout + proc.stderr, bl))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", bl))

    # Mutate
    mutate_sh = _SKILL_DIR.parent / "_shared" / "cifs" / "mutate-cifs-test-data.sh"
    if not mutate_sh.exists():
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ {mutate_sh} not found"))
        _cleanup(a, cfg); return build_result(results, start)
    pm = _run_script(mutate_sh, host, user, passwd, share)
    mutate_ok = pm.returncode == 0
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} cifs_mutate"))
    if not mutate_ok:
        _cleanup(a, cfg); return build_result(results, start)

    # 增量扫描
    proc2 = run_terrasync_timed([binary, "-c", config, "-l", "trace", "scan",
                           "--id", JOB_ID, cifs_url],
                          capture_output=True, text=True, timeout=300)
    incr_out = proc2.stdout + proc2.stderr
    post = {"dirs": POST_DIRS, "files": POST_FILES}
    results.append(a.check_cli_scan_output(incr_out, post))
    results.append(_check_incr_stats(incr_out, {"new": 5, "changed": 2, "renamed": 5, "deleted": 6}))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", post))

    _cleanup(a, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
