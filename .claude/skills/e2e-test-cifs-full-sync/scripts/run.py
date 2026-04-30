#!/usr/bin/env python3
"""
e2e-test-cifs-full-sync/scripts/run.py
CIFS 全量同步 e2e 测试（源端 → 目标端）。
Samba: 192.168.50.173/testshare → 192.168.50.23/testshare
"""

import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result
from protocol_constants import Cifs as _PC

SYNC_JOB_ID = "cifs-full-sync"
DST_SCAN_JOB_ID = "cifs-full-sync-dst"
SANITIZED = "cifs_full_sync"
EXPECTED_DIRS = _PC.BASELINE_DIRS
EXPECTED_FILES = _PC.BASELINE_FILES


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


def _drop_tables(a, ch_host):
    tables = a.clickhouse_query(ch_host,
        f"SELECT name FROM system.tables WHERE database='default' "
        f"AND name LIKE '%{SANITIZED}%' FORMAT TabSeparated")
    for t in tables.strip().splitlines():
        if t.strip():
            a.clickhouse_execute(ch_host, f"DROP TABLE IF EXISTS default.{t.strip()}")


def _cleanup(a, cfg):
    src = cfg["CIFS_SOURCE_HOST"]; dst = cfg["CIFS_DEST_HOST"]
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SHARE", _PC.SHARE); ch_host = cfg["CLICKHOUSE_HOST"]
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(_smb_rm, src, user, passwd, share),
            ex.submit(_smb_rm, dst, user, passwd, share),
            ex.submit(_drop_tables, a, ch_host),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try: f.result()
            except Exception as e:
                print(f"⚠ cleanup warning: {e}", flush=True)
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env=None):
    os.chdir(_PROJECT_ROOT)
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "CIFS_SOURCE_HOST", "CIFS_DEST_HOST", "CLICKHOUSE_HOST")

    if not _check_smbclient():
        return {"passed": False, "metrics": {"elapsed_sec": 0},
                "assertions": [{"name": "smbclient", "passed": False,
                                "message": "✗ smbclient not found — install samba-client"}]}

    src = cfg["CIFS_SOURCE_HOST"]; dst = cfg["CIFS_DEST_HOST"]
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SHARE", _PC.SHARE); ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    src_url = _cifs_url(src, user, passwd, share)
    dst_url = _cifs_url(dst, user, passwd, share)
    a = TerrasyncAssertions()
    results = []

    _cleanup(a, cfg)

    # 上传源端数据
    setup_sh = _SKILL_DIR.parent / "cifs-full-scan" / "scripts" / "setup-cifs-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {}, f"✗ {setup_sh} not found"))
        return build_result(results, start)
    p = _run_script(setup_sh, src, user, passwd, share)
    setup_ok = p.returncode == 0
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} cifs_setup: {p.stderr[-200:] if not setup_ok else ''}"))
    if not setup_ok: return build_result(results, start)

    # 全量 Sync
    proc = subprocess.run([binary, "-c", config, "-l", "trace", "sync",
                          "--id", SYNC_JOB_ID, src_url, dst_url],
                         capture_output=True, text=True, timeout=600)
    sync_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("sync_exit", False, {}, {}, "✗ cifs sync failed"))
        _cleanup(a, cfg); return build_result(results, start)

    exp = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES}
    results.append(a.check_cli_sync_output(sync_out, exp))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", exp))

    # 目标端扫描验证
    proc2 = subprocess.run([binary, "-c", config, "-l", "trace", "scan",
                           "--id", DST_SCAN_JOB_ID, dst_url],
                          capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(proc2.stdout + proc2.stderr, exp))

    # Integrity Check
    proc3 = subprocess.run([binary, "-c", config, "-l", "trace", "integrity-check",
                           src_url, dst_url, "--quick"],
                          capture_output=True, text=True, timeout=300)
    ok = proc3.returncode == 0 and "All Passed" in (proc3.stdout + proc3.stderr)
    results.append(AssertionResult("integrity_check", ok, {}, {},
                                   f"{'✓' if ok else '✗'} integrity_check_quick"))

    _cleanup(a, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
