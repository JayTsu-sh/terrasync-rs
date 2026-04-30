#!/usr/bin/env python3
"""
e2e-test-cifs-incremental-sync/scripts/run.py
CIFS 增量同步 e2e 测试：全量 sync 建基线 → mutate → 增量 sync → integrity-check。
"""

import os
import re
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

SYNC_JOB_ID = "cifs-incr-sync"
DST_SCAN_JOB_ID = "cifs-incr-sync-dst"
SANITIZED = "cifs_incr_sync"
BASELINE_DIRS, BASELINE_FILES = _PC.BASELINE_DIRS, _PC.BASELINE_FILES
POST_DIRS, POST_FILES = _PC.POST_DIRS, _PC.POST_FILES

_TABLES = [
    f"base_{SANITIZED}", f"state_{SANITIZED}",
    f"base_{SANITIZED}_dst", f"state_{SANITIZED}_dst",
    f"base_{SANITIZED}_verify_src", f"state_{SANITIZED}_verify_src",
    f"base_{SANITIZED}_verify_dst", f"state_{SANITIZED}_verify_dst",
]


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
    src = cfg["CIFS_SOURCE_HOST"]; dst = cfg["CIFS_DEST_HOST"]
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SHARE", _PC.SHARE); ch_host = cfg["CLICKHOUSE_HOST"]
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = [
            ex.submit(_smb_rm, src, user, passwd, share),
            ex.submit(_smb_rm, dst, user, passwd, share),
            *[ex.submit(a.clickhouse_execute, ch_host, f"DROP TABLE IF EXISTS default.{t}")
              for t in _TABLES],
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' -exec rm -rf {{}} +"),
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

    # 创建基线数据
    setup_sh = _SKILL_DIR.parent / "cifs-full-scan" / "scripts" / "setup-cifs-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {}, f"✗ {setup_sh} not found"))
        return build_result(results, start)
    p = _run_script(setup_sh, src, user, passwd, share)
    setup_ok = p.returncode == 0
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} cifs_setup"))
    if not setup_ok: return build_result(results, start)

    bl = {"dirs": BASELINE_DIRS, "files": BASELINE_FILES}

    # 全量 Sync
    proc = subprocess.run([binary, "-c", config, "-l", "trace", "sync",
                          "--id", SYNC_JOB_ID, src_url, dst_url],
                         capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        results.append(AssertionResult("full_sync", False, {}, {}, "✗ full_sync failed"))
        _cleanup(a, cfg); return build_result(results, start)
    results.append(a.check_cli_sync_output(proc.stdout + proc.stderr, bl))

    # Mutate 源端
    mutate_sh = _SKILL_DIR.parent / "cifs-incremental-sync" / "scripts" / "mutate-cifs-test-data.sh"
    if not mutate_sh.exists():
        # fallback to cifs-incremental-scan's mutate script
        mutate_sh = _SKILL_DIR.parent / "cifs-incremental-scan" / "scripts" / "mutate-cifs-test-data.sh"
    if not mutate_sh.exists():
        results.append(AssertionResult("mutate", False, {}, {}, "✗ mutate script not found"))
        _cleanup(a, cfg); return build_result(results, start)
    pm = _run_script(mutate_sh, src, user, passwd, share)
    mutate_ok = pm.returncode == 0
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} cifs_mutate"))
    if not mutate_ok:
        _cleanup(a, cfg); return build_result(results, start)

    # 增量 Sync
    proc2 = subprocess.run([binary, "-c", config, "-l", "trace", "sync",
                           "--id", SYNC_JOB_ID, src_url, dst_url],
                          capture_output=True, text=True, timeout=600)
    incr_out = proc2.stdout + proc2.stderr
    post = {"dirs": POST_DIRS, "files": POST_FILES}
    results.append(a.check_cli_scan_output(incr_out, post))

    # 验证目标端
    proc3 = subprocess.run([binary, "-c", config, "-l", "trace", "scan",
                           "--id", DST_SCAN_JOB_ID, dst_url],
                          capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(proc3.stdout + proc3.stderr, post))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}_dst", post))

    # Integrity Check
    for flag, label in [(["--quick"], "quick"), ([], "full")]:
        p = subprocess.run([binary, "-c", config, "-l", "trace", "integrity-check",
                           src_url, dst_url, *flag],
                          capture_output=True, text=True, timeout=300)
        ok = p.returncode == 0 and "All Passed" in (p.stdout + p.stderr)
        results.append(AssertionResult(f"ic_{label}", ok, {}, {},
                                       f"{'✓' if ok else '✗'} integrity_{label}"))

    _cleanup(a, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
