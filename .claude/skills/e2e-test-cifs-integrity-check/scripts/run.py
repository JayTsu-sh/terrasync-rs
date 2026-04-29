#!/usr/bin/env python3
"""
e2e-test-cifs-integrity-check/scripts/run.py
CIFS 完整性校验 e2e 测试：多场景（一致/Mismatch/Missing）。
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

SYNC_JOB_ID = "cifs-ic-sync"
IC_JOB_ID = "cifs-ic-test"
SANITIZED = "cifs_ic"
_TABLES_PATTERN = "cifs_ic"


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


def _drop_ic_tables(a, ch_host):
    tables = a.clickhouse_query(ch_host,
        f"SELECT name FROM system.tables WHERE database='default' "
        f"AND name LIKE '%{_TABLES_PATTERN}%' FORMAT TabSeparated")
    for t in tables.strip().splitlines():
        if t.strip():
            a.clickhouse_query(ch_host, f"DROP TABLE IF EXISTS default.{t.strip()}")


def _cleanup(a, cfg):
    src = cfg["CIFS_SOURCE_HOST"]; dst = cfg.get("CIFS_DEST_HOST", src)
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SHARE", "testshare"); ch_host = cfg["CLICKHOUSE_HOST"]
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(_smb_rm, src, user, passwd, share),
            ex.submit(_smb_rm, dst, user, passwd, share),
            ex.submit(_drop_ic_tables, a, ch_host),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{_TABLES_PATTERN}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try: f.result()
            except Exception: pass
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def _ic(binary, config, src_url, dst_url, *args, timeout=300):
    return subprocess.run([binary, "-c", config, "-l", "trace", "integrity-check",
                          src_url, dst_url, *args],
                         capture_output=True, text=True, timeout=timeout)


def _check_ic(out, label, expect_pass, min_mm=0, min_ms=0):
    all_passed = "All Passed" in out
    if expect_pass:
        return AssertionResult(f"ic_{label}", all_passed, {}, {},
                               f"{'✓' if all_passed else '✗'} ic_{label}")
    mm = int((re.search(r"Mismatch[:\s]+(\d+)", out, re.I) or [0, "0"])[1])
    ms = int((re.search(r"Missing[:\s]+(\d+)", out, re.I) or [0, "0"])[1])
    passed = mm >= min_mm and ms >= min_ms
    return AssertionResult(f"ic_{label}", passed,
                           {"mm>=": min_mm, "ms>=": min_ms},
                           {"mm": mm, "ms": ms},
                           f"{'✓' if passed else '✗'} ic_{label}: mm={mm}, ms={ms}")


def run(env=None):
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "CIFS_SOURCE_HOST", "CIFS_DEST_HOST", "CLICKHOUSE_HOST")

    if not _check_smbclient():
        return {"passed": False, "metrics": {"elapsed_sec": 0},
                "assertions": [{"name": "smbclient", "passed": False,
                                "message": "✗ smbclient not found — install samba-client"}]}

    src = cfg["CIFS_SOURCE_HOST"]; dst = cfg["CIFS_DEST_HOST"]
    user = cfg.get("CIFS_USER", "terrasync"); passwd = cfg.get("CIFS_PASS", "terrasync123")
    share = cfg.get("CIFS_SHARE", "testshare"); ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    src_url = _cifs_url(src, user, passwd, share)
    dst_url = _cifs_url(dst, user, passwd, share)
    a = TerrasyncAssertions()
    results = []

    _cleanup(a, cfg)

    # 创建测试数据
    setup_sh = _SKILL_DIR.parent / "cifs-full-scan" / "scripts" / "setup-cifs-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {}, f"✗ {setup_sh} not found"))
        return build_result(results, start)
    p = _run_script(setup_sh, src, user, passwd, share)
    setup_ok = p.returncode == 0
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} cifs_setup"))
    if not setup_ok: return build_result(results, start)

    # Sync 建基线
    proc = subprocess.run([binary, "-c", config, "-l", "trace", "sync",
                          "--id", SYNC_JOB_ID, src_url, dst_url],
                         capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        results.append(AssertionResult("sync", False, {}, {}, "✗ sync failed"))
        _cleanup(a, cfg); return build_result(results, start)
    results.append(AssertionResult("sync", True, {}, {}, "✓ initial_sync"))

    # 场景1: Full 一致
    r1 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-full")
    results.append(_check_ic(r1.stdout + r1.stderr, "full_consistent", True))

    # 场景2: Quick 一致
    r2 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-quick", "--quick")
    results.append(_check_ic(r2.stdout + r2.stderr, "quick_consistent", True))

    # 制造 Mismatch：在目标端用 smbclient 覆盖文件
    smb_tamper = subprocess.run(
        ["bash", "-c",
         f"echo 'tampered' | smbclient //{dst}/{share} -U {user}%{passwd} "
         f"-c 'put /dev/stdin test-data/d1/d1_1/file1.txt' 2>/dev/null"],
        capture_output=True, timeout=30)
    if smb_tamper.returncode != 0:
        # Alternative: use temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tf:
            tf.write("tampered-cifs-content")
            tmp = tf.name
        subprocess.run(["smbclient", f"//{dst}/{share}", "-U", f"{user}%{passwd}",
                        "-c", f"put {tmp} test-data/d1/d1_1/file1.txt"],
                       capture_output=True, timeout=30)
        os.unlink(tmp)

    r3 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-mismatch")
    results.append(_check_ic(r3.stdout + r3.stderr, "detects_mismatch", False, min_mm=1))

    # 制造 Missing：删除目标端文件
    subprocess.run(["smbclient", f"//{dst}/{share}", "-U", f"{user}%{passwd}",
                    "-c", "del test-data/d2/file1.txt"],
                   capture_output=True, timeout=30)

    r4 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-missing")
    results.append(_check_ic(r4.stdout + r4.stderr, "detects_missing", False, min_mm=1, min_ms=1))

    _cleanup(a, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
