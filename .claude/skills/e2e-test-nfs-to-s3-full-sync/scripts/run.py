#!/usr/bin/env python3
"""
e2e-test-nfs-to-s3-full-sync/scripts/run.py
跨协议全量同步：NFS v3 源端 → S3 (rustfs) 目标端。
注意：NFS 有 symlink（79个），S3 不支持 symlink，sync 时 symlink 被跳过。
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
from assertions import AssertionResult, TerrasyncAssertions

SYNC_JOB_ID = "nfs-to-s3-sync"
DST_SCAN_JOB_ID = "nfs-to-s3-sync-dst"
SANITIZED = "nfs_to_s3_sync"
# NFS 源端（含 symlink）
NFS_DIRS, NFS_FILES, NFS_SYMLINKS = 113, 335, 79
# S3 目标端（symlink 被跳过）
S3_DIRS, S3_FILES = 113, 335


def _nfs_url(cfg, prefix=""):
    src_ip = cfg["NFS_V3_SOURCE_IP"]
    nfs_export = cfg.get("NFS_V3_EXPORT", "/export/nfs")
    return f"nfs://{src_ip}{nfs_export}{prefix}"


def _s3_url(cfg, prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg.get("S3_DEST_IP", cfg.get("S3_SOURCE_IP", "192.168.50.23"))
    port = cfg.get("S3_DEST_PORT", "39000")
    bucket = cfg.get("S3_BUCKET_DST", "test-bucket")
    return f"s3://{ak}:{sk}@{bucket}.{ip}:{port}/{prefix}"


def _s3_cleanup_dest(a, cfg):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg.get("S3_DEST_IP", cfg.get("S3_SOURCE_IP", "192.168.50.23"))
    port = cfg.get("S3_DEST_PORT", "39000")
    bucket = cfg.get("S3_BUCKET_DST", "test-bucket")
    try:
        a.ssh_exec(ip, f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                       f"mc rm --recursive --force ts3/{bucket}/test-data/ 2>/dev/null || true")
    except Exception:
        pass


def _cleanup(a, src_ip, ch_host, cfg):
    nfs_export = cfg.get("NFS_V3_EXPORT", "/export/nfs")
    tables = a.clickhouse_query(ch_host,
        f"SELECT name FROM system.tables WHERE database='default' AND name LIKE '%{SANITIZED}%' FORMAT TabSeparated")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"sudo rm -rf {nfs_export}/test-data && echo ok || true"),
            ex.submit(_s3_cleanup_dest, a, cfg),
            *[ex.submit(a.clickhouse_query, ch_host, f"DROP TABLE IF EXISTS default.{t.strip()}")
              for t in tables.strip().splitlines() if t.strip()],
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try: f.result()
            except Exception: pass
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def _patch_s3_script(path, cfg):
    with open(path) as f: c = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bucket = cfg.get("S3_BUCKET_DST", "test-bucket")
    port = cfg.get("S3_DEST_PORT", "39000")
    ip = cfg.get("S3_DEST_IP", cfg.get("S3_SOURCE_IP"))
    for o, n in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
        ('S3_BUCKET="mbucket-src"', f'S3_BUCKET="{bucket}"'),
    ]: c = c.replace(o, n)
    tmp = f"/tmp/s3dst_{os.getpid()}.sh"
    with open(tmp, "w") as f: f.write(c)
    return tmp


def run(env=None):
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "NFS_V3_SOURCE_IP", "CLICKHOUSE_HOST", "S3_ACCESS_KEY", "S3_SECRET_KEY")

    src_ip = cfg["NFS_V3_SOURCE_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    nfs_src_url = _nfs_url(cfg)
    s3_dst_url = _s3_url(cfg)

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, ch_host, cfg)

    # 创建 NFS 测试数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-nfs-v3" / "scripts" / "setup-test-data.sh"
    try:
        a.scp_to(setup_sh, src_ip, "/tmp/setup-test-data.sh")
        out = a.ssh_exec(src_ip, "sudo bash /tmp/setup-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ nfs_setup: {e}"))
        return _build(results, start)
    ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", ok, {}, {}, f"{'✓' if ok else '✗'} nfs_setup"))
    if not ok: return _build(results, start)

    # 跨协议 Sync: NFS → S3
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "sync",
         "--id", SYNC_JOB_ID, nfs_src_url, s3_dst_url],
        capture_output=True, text=True, timeout=900)
    sync_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("sync_exit", False, {}, {}, "✗ nfs→s3 sync failed"))
        _cleanup(a, src_ip, ch_host, cfg)
        return _build(results, start)
    # 验证：S3 不含 symlink
    results.append(a.check_cli_sync_output(sync_out, {"dirs": S3_DIRS, "files": S3_FILES}))

    # 目标端 S3 扫描验证
    proc2 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan",
         "--id", DST_SCAN_JOB_ID, s3_dst_url],
        capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(proc2.stdout + proc2.stderr,
                                           {"dirs": S3_DIRS, "files": S3_FILES}))

    _cleanup(a, src_ip, ch_host, cfg)
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
