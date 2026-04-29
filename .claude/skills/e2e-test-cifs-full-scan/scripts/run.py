#!/usr/bin/env python3
"""
e2e-test-cifs-full-scan/scripts/run.py
CIFS 全量扫描 e2e 测试。
注意：需要先配置 Samba server（在 192.168.50.173 和 192.168.50.23 上）。
"""

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions

JOB_ID = "cifs-full-scan"
SANITIZED = "cifs_full_scan"
EXPECTED_DIRS = 40
EXPECTED_FILES = 117
EXPECTED_TOTAL = EXPECTED_DIRS + EXPECTED_FILES  # 157，CIFS 无 symlink
_SETUP = Path(".claude/skills/cifs-full-scan/scripts/setup-cifs-test-data.sh")


def _smb_rm(a, host, share, user, passwd, prefix="test-data"):
    """通过 smbclient 删除 CIFS 共享中的测试数据（幂等）。"""
    try:
        a.run_local(["smbclient", f"//{host}/{share}", "-U", f"{user}%{passwd}",
                     "-c", f"deltree {prefix}"], timeout=30)
    except Exception:
        pass


def _check_smbclient():
    r = subprocess.run(["smbclient", "--version"], capture_output=True)
    return r.returncode == 0


def _cifs_url(cfg, host_key="CIFS_SOURCE_HOST", prefix="test-data"):
    user = cfg.get("CIFS_USER", "administrator")
    passwd = cfg.get("CIFS_PASS", "")
    host = cfg[host_key]
    share = cfg.get("CIFS_SHARE", "testshare")
    return f"smb://{user}:{passwd}@{host}/{share}/{prefix}"


def _cleanup(a, host, ch_host, cfg):
    share = cfg.get("CIFS_SHARE", "testshare")
    user = cfg.get("CIFS_USER", "administrator")
    passwd = cfg.get("CIFS_PASS", "")
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [
            ex.submit(_smb_rm, a, host, share, user, passwd),
            ex.submit(a.clickhouse_drop_tables, ch_host, SANITIZED),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try: f.result()
            except Exception: pass
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "CIFS_SOURCE_HOST", "CLICKHOUSE_HOST")

    if not _check_smbclient():
        return {"passed": False, "metrics": {"elapsed_sec": 0},
                "assertions": [{"name": "smbclient_available", "passed": False,
                                "message": "✗ smbclient not found — install samba-client or configure Samba first"}]}

    src_host = cfg["CIFS_SOURCE_HOST"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    src_url = _cifs_url(cfg)
    a = TerrasyncAssertions(ssh_user=cfg.get("SSH_USER", "root"))
    results = []

    _cleanup(a, src_host, ch_host, cfg)

    # 上传测试数据（使用 cifs-full-scan 老格式脚本）
    setup_sh = _SKILL_DIR.parent / "cifs-full-scan" / "scripts" / "setup-cifs-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {},
                                       f"✗ setup: {setup_sh} not found"))
        return _build(results, start)

    proc_setup = subprocess.run(
        ["bash", str(setup_sh)], capture_output=True, text=True, timeout=120)
    setup_ok = proc_setup.returncode == 0
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} cifs_setup_data"))
    if not setup_ok:
        return _build(results, start)

    # 全量扫描
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        results.append(AssertionResult("scan_exit", False, {}, {}, "✗ scan failed"))
        _cleanup(a, src_host, ch_host, cfg)
        return _build(results, start)

    exp = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES}
    results.append(a.check_cli_scan_output(proc.stdout + proc.stderr, exp))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", exp))

    # file_handle 验证（CIFS 支持 Fh3 策略）
    fh_q = f"SELECT count(*) FROM default.base_{SANITIZED} FINAL WHERE file_handle='' FORMAT TabSeparated"
    fh_count = a.clickhouse_query(ch_host, fh_q).strip()
    fh_ok = fh_count == "0"
    results.append(AssertionResult("file_handle", fh_ok, {"empty": 0}, {"empty": fh_count},
                                   f"{'✓' if fh_ok else '✗'} file_handle_populated: empty={fh_count}"))

    _cleanup(a, src_host, ch_host, cfg)
    return _build(results, start)


def _build(results, start):
    elapsed = round(time.monotonic() - start, 1)
    passed = all(r.passed for r in results)
    for r in results: print(r.message)
    print(f"\n{'PASS' if passed else 'FAIL'} ({elapsed}s)")
    return {"passed": passed, "metrics": {"elapsed_sec": elapsed},
            "assertions": [{"name": r.name, "passed": r.passed, "message": r.message} for r in results]}


if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
