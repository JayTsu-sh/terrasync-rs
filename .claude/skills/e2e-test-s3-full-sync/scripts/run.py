#!/usr/bin/env python3
"""
e2e-test-s3-full-sync/scripts/run.py
S3 全量同步 e2e 测试（源端 → 目标端）。
"""

import os
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

SYNC_JOB_ID = "s3-full-sync"
DST_SCAN_JOB_ID = "s3-full-sync-dst"
SANITIZED = "s3_full_sync"
EXPECTED_DIRS, EXPECTED_FILES = 40, 117

_TABLES_PATTERN = "s3_full_sync"


def _s3_url(cfg, ip_key, bucket_key, port_key, prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    return f"s3://{ak}:{sk}@{cfg[bucket_key]}.{cfg[ip_key]}:{cfg.get(port_key, '39000')}/{prefix}"


def _mc_cleanup_remote(a, ip, bucket, cfg, port_key="S3_SOURCE_PORT"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get(port_key, "39000")
    try:
        a.ssh_exec(ip,
                   f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                   f"mc rm --recursive --force ts3/{bucket}/test-data/ 2>/dev/null || true")
    except Exception:
        pass


def _patch_and_run(a, src_ip, sh_path, cfg):
    with open(sh_path) as f:
        content = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    for old, new in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
        ('S3_BUCKET="mbucket-src"', f'S3_BUCKET="{bucket}"'),
    ]:
        content = content.replace(old, new)
    tmp = f"/tmp/s3_sh_{os.getpid()}.sh"
    with open(tmp, "w") as f:
        f.write(content)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-test-data.sh")
        return a.ssh_exec(src_ip, "bash /tmp/setup-s3-test-data.sh", timeout=300)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _cleanup_all(a, src_ip, dest_ip, ch_host, cfg):
    src_bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    dst_bucket = cfg.get("S3_BUCKET_DST", "test-bucket")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(_mc_cleanup_remote, a, src_ip, src_bucket, cfg, "S3_SOURCE_PORT"),
            ex.submit(_mc_cleanup_remote, a, dest_ip, dst_bucket,
                      {**cfg, "S3_SOURCE_PORT": cfg.get("S3_DEST_PORT", "39000")},
                      "S3_SOURCE_PORT"),
            ex.submit(_drop_sync_tables, a, ch_host),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception:
                pass
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def _drop_sync_tables(a, ch_host):
    tables = a.clickhouse_query(
        ch_host,
        f"SELECT name FROM system.tables WHERE database='default' "
        f"AND name LIKE '%{_TABLES_PATTERN}%' FORMAT TabSeparated"
    )
    for t in tables.strip().splitlines():
        t = t.strip()
        if t:
            a.clickhouse_query(ch_host, f"DROP TABLE IF EXISTS default.{t}")


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "S3_SOURCE_IP", "S3_DEST_IP", "CLICKHOUSE_HOST",
                   "S3_ACCESS_KEY", "S3_SECRET_KEY")

    src_ip, dest_ip = cfg["S3_SOURCE_IP"], cfg["S3_DEST_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")

    src_url = _s3_url(cfg, "S3_SOURCE_IP", "S3_BUCKET_SRC", "S3_SOURCE_PORT")
    dst_url = _s3_url(cfg, "S3_DEST_IP", "S3_BUCKET_DST", "S3_DEST_PORT")

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup_all(a, src_ip, dest_ip, ch_host, cfg)

    # 上传源端数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-s3-incremental-scan" / "scripts" / "setup-s3-test-data.sh"
    try:
        out = _patch_and_run(a, src_ip, setup_sh, cfg)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return build_result(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} s3_setup"))
    if not setup_ok:
        return build_result(results, start)

    # 全量 Sync
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync",
         "--id", SYNC_JOB_ID, src_url, dst_url],
        capture_output=True, text=True, timeout=600)
    sync_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("sync_exit", False, {}, {}, "✗ sync failed"))
        _cleanup_all(a, src_ip, dest_ip, ch_host, cfg)
        return build_result(results, start)

    exp = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES}
    results.append(a.check_cli_sync_output(sync_out, exp))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", exp))

    # 目标端扫描验证
    proc2 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan",
         "--id", DST_SCAN_JOB_ID, dst_url],
        capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(proc2.stdout + proc2.stderr, exp))

    # Integrity Check
    proc3 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "integrity-check", src_url, dst_url, "--quick"],
        capture_output=True, text=True, timeout=300)
    ic_out = proc3.stdout + proc3.stderr
    passed = proc3.returncode == 0 and "All Passed" in ic_out
    results.append(AssertionResult("integrity_check", passed, {}, {},
                                   f"{'✓' if passed else '✗'} integrity_check_quick"))

    _cleanup_all(a, src_ip, dest_ip, ch_host, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
