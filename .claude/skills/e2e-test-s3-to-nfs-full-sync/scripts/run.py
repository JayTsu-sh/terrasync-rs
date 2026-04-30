#!/usr/bin/env python3
"""
e2e-test-s3-to-nfs-full-sync/scripts/run.py
跨协议全量同步：S3 (rustfs) 源端 → NFS v3 目标端。
"""

import os
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
from assertions import AssertionResult, TerrasyncAssertions, build_result
from protocol_constants import S3 as _PC, NfsV3 as _NPC

SYNC_JOB_ID = "s3-to-nfs-sync"
DST_SCAN_JOB_ID = "s3-to-nfs-sync-dst"
SANITIZED = "s3_to_nfs_sync"
EXPECTED_DIRS, EXPECTED_FILES = _PC.BASELINE_DIRS, _PC.BASELINE_FILES


def _s3_url(cfg, prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg.get("S3_SOURCE_IP", "192.168.50.173")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_BUCKET_SRC", _PC.BUCKET_SRC)
    return f"s3://{ak}:{sk}@{bucket}.{ip}:{port}/{prefix}"


def _nfs_dst_url(cfg):
    dest_ip = cfg.get("NFS_V3_DEST_IP", "192.168.50.23")
    nfs_export = cfg.get("NFS_V3_EXPORT", _NPC.EXPORT)
    return f"nfs://{dest_ip}{nfs_export}"


def _mc_cleanup_src(a, cfg):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg.get("S3_SOURCE_IP", "192.168.50.173")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_BUCKET_SRC", _PC.BUCKET_SRC)
    try:
        a.ssh_exec(ip, f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                       f"mc rm --recursive --force ts3/{bucket}/test-data/ 2>/dev/null || true")
    except Exception as e:

        print(f"⚠ cleanup warning: {e}", flush=True)


def _cleanup(a, ch_host, cfg):
    dest_ip = cfg.get("NFS_V3_DEST_IP", "192.168.50.23")
    nfs_export = cfg.get("NFS_V3_EXPORT", _NPC.EXPORT)
    tables = a.clickhouse_query(ch_host,
        f"SELECT name FROM system.tables WHERE database='default' AND name LIKE '%{SANITIZED}%' FORMAT TabSeparated")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(_mc_cleanup_src, a, cfg),
            ex.submit(a.ssh_exec, dest_ip,
                      f"sudo find {nfs_export} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} + && echo ok || true"),
            *[ex.submit(a.clickhouse_execute, ch_host, f"DROP TABLE IF EXISTS default.{t.strip()}")
              for t in tables.strip().splitlines() if t.strip()],
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
    envmod.require(cfg, "S3_SOURCE_IP", "CLICKHOUSE_HOST", "S3_ACCESS_KEY", "S3_SECRET_KEY")

    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_ip = cfg.get("S3_SOURCE_IP", "192.168.50.173")
    s3_src_url = _s3_url(cfg)
    nfs_dst_url = _nfs_dst_url(cfg)

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, ch_host, cfg)

    # 上传 S3 源端数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-s3-incremental-scan" / "scripts" / "setup-s3-test-data.sh"
    with open(setup_sh) as f: c = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bucket = cfg.get("S3_BUCKET_SRC", _PC.BUCKET_SRC)
    port = cfg.get("S3_SOURCE_PORT", "39000")
    for o, n in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
        ('S3_BUCKET="mbucket-src"', f'S3_BUCKET="{bucket}"'),
    ]: c = c.replace(o, n)
    tmp = f"/tmp/s3src_{os.getpid()}.sh"
    with open(tmp, "w") as f: f.write(c)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-test-data.sh")
        out = a.ssh_exec(src_ip, "bash /tmp/setup-s3-test-data.sh", timeout=300)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ s3_setup: {e}"))
        return build_result(results, start)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", ok, {}, {}, f"{'✓' if ok else '✗'} s3_setup"))
    if not ok: return build_result(results, start)

    # 跨协议 Sync: S3 → NFS
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync",
         "--id", SYNC_JOB_ID, s3_src_url, nfs_dst_url],
        capture_output=True, text=True, timeout=900)
    sync_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("sync_exit", False, {}, {}, "✗ s3→nfs sync failed"))
        _cleanup(a, ch_host, cfg)
        return build_result(results, start)
    exp = {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES}
    results.append(a.check_cli_sync_output(sync_out, exp))

    # 目标端 NFS 扫描验证
    proc2 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan",
         "--id", DST_SCAN_JOB_ID, nfs_dst_url],
        capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(proc2.stdout + proc2.stderr, exp))

    _cleanup(a, ch_host, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
