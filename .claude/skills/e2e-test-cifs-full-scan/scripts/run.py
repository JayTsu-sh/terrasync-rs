#!/usr/bin/env python3
"""
e2e-test-cifs-full-scan/scripts/run.py
CIFS 全量扫描 e2e 测试。
注意：需要先配置 Samba server（在 192.168.50.173 和 192.168.50.23 上）。
"""

import subprocess
import os
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

JOB_ID = "cifs-full-scan"
SANITIZED = "cifs_full_scan"
EXPECTED_DIRS = _PC.BASELINE_DIRS
EXPECTED_FILES = _PC.BASELINE_FILES
EXPECTED_TOTAL = _PC.BASELINE_TOTAL
_SETUP = Path(".claude/skills/_shared/cifs/setup-cifs-test-data.sh")


def _smb_rm(a, host, share, user, passwd, prefix="test-data"):
    """通过 smbclient 删除 CIFS 共享中的测试数据（幂等）。"""
    try:
        a.run_local(["smbclient", f"//{host}/{share}", "-U", f"{user}%{passwd}",
                     "-c", f"deltree {prefix}"], timeout=30)
    except Exception as e:
        print(f"⚠ cleanup warning: {e}", flush=True)


def _check_smbclient():
    r = subprocess.run(["smbclient", "--version"], capture_output=True)
    return r.returncode == 0


def _cifs_url(cfg, host_key="CIFS_SOURCE_HOST", prefix="test-data"):
    user = cfg.get("CIFS_USER", "administrator")
    passwd = cfg.get("CIFS_PASS", "")
    host = cfg[host_key]
    share = cfg.get("CIFS_SOURCE_SHARE", _PC.SHARE)
    return f"smb://{user}:{passwd}@{host}/{share}/{prefix}"


def _cleanup(a, host, ch_host, cfg):
    share = cfg.get("CIFS_SOURCE_SHARE", _PC.SHARE)
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
            except Exception as e:
                print(f"⚠ cleanup warning: {e}", flush=True)
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env: dict = None) -> dict:
    os.chdir(_PROJECT_ROOT)
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
    setup_sh = _SKILL_DIR.parent / "_shared" / "cifs" / "setup-cifs-test-data.sh"
    if not setup_sh.exists():
        results.append(AssertionResult("setup", False, {}, {},
                                       f"✗ setup: {setup_sh} not found"))
        return build_result(results, start)

    setup_env = os.environ.copy()
    setup_env.update({
        "CIFS_HOST": src_host,
        "CIFS_SHARE": cfg.get("CIFS_SOURCE_SHARE", _PC.SHARE),
        "CIFS_USER": cfg.get("CIFS_USER", "administrator"),
        "CIFS_PASS": cfg.get("CIFS_PASS", ""),
        "CIFS_PORT": cfg.get("CIFS_PORT", "445"),
    })
    proc_setup = subprocess.run(
        ["bash", str(setup_sh)], capture_output=True, text=True, timeout=120, env=setup_env)
    setup_ok = proc_setup.returncode == 0
    msg = f"{'✓' if setup_ok else '✗'} cifs_setup_data"
    if not setup_ok:
        msg += f": {(proc_setup.stderr or proc_setup.stdout).strip()[-300:]}"
    results.append(AssertionResult("setup", setup_ok, {}, {}, msg))
    if not setup_ok:
        return build_result(results, start)

    # 全量扫描
    proc = run_terrasync_timed([binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        results.append(AssertionResult("scan_exit", False, {}, {}, "✗ scan failed"))
        _cleanup(a, src_host, ch_host, cfg)
        return build_result(results, start)

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
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
