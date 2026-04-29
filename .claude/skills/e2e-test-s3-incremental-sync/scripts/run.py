#!/usr/bin/env python3
"""e2e-test-s3-incremental-sync/scripts/run.py — S3 增量同步 e2e 测试。"""

import os, re, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result
from protocol_constants import S3 as _PC

SYNC_JOB_ID = "s3-incr-sync"
DST_SCAN_JOB_ID = "s3-incr-sync-dst"
SANITIZED = "s3_incr_sync"
BASELINE_DIRS, BASELINE_FILES = _PC.BASELINE_DIRS, _PC.BASELINE_FILES
POST_DIRS, POST_FILES = _PC.POST_DIRS, _PC.POST_FILES
_SETUP = _SKILL_DIR.parent / "e2e-test-s3-incremental-scan" / "scripts" / "setup-s3-test-data.sh"
_MUTATE = _SKILL_DIR.parent / "e2e-test-s3-incremental-scan" / "scripts" / "mutate-s3-test-data.sh"


def _url(cfg, ip_key, bkt_key, port_key="S3_SOURCE_PORT", prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    return f"s3://{ak}:{sk}@{cfg[bkt_key]}.{cfg[ip_key]}:{cfg.get(port_key,'39000')}/{prefix}"


def _patch(path, cfg, bkt_key="S3_BUCKET_SRC"):
    with open(path) as f: c = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bkt = cfg.get(bkt_key, "test-bucket"); port = cfg.get("S3_SOURCE_PORT", "39000")
    for o, n in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
        ('S3_BUCKET="mbucket-src"', f'S3_BUCKET="{bkt}"'),
    ]: c = c.replace(o, n)
    tmp = f"/tmp/s3sh_{os.getpid()}.sh"
    with open(tmp, "w") as f: f.write(c)
    return tmp


def _mc_rm(a, ip, bkt, cfg, port_key="S3_SOURCE_PORT"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get(port_key, "39000")
    try:
        a.ssh_exec(ip, f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                       f"mc rm --recursive --force ts3/{bkt}/test-data/ 2>/dev/null || true")
    except Exception: pass


def _cleanup(a, src_ip, dest_ip, ch_host, cfg):
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(_mc_rm, a, src_ip, cfg.get("S3_BUCKET_SRC", _PC.BUCKET_SRC), cfg),
            ex.submit(_mc_rm, a, dest_ip, cfg.get("S3_BUCKET_DST", _PC.BUCKET_DST),
                      {**cfg,"S3_SOURCE_PORT":cfg.get("S3_DEST_PORT","39000")}),
            ex.submit(a.clickhouse_drop_tables, ch_host, SANITIZED),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' -exec rm -rf {{}} +"),
        ]
        for f in as_completed(futs):
            try: f.result()
            except Exception: pass
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env=None):
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "S3_SOURCE_IP", "S3_DEST_IP", "CLICKHOUSE_HOST", "S3_ACCESS_KEY", "S3_SECRET_KEY")
    src_ip, dest_ip = cfg["S3_SOURCE_IP"], cfg["S3_DEST_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    src_url = _url(cfg, "S3_SOURCE_IP", "S3_BUCKET_SRC")
    dst_url = _url(cfg, "S3_DEST_IP", "S3_BUCKET_DST", "S3_DEST_PORT")
    a = TerrasyncAssertions(ssh_user=cfg.get("SSH_USER","root"))
    results = []

    _cleanup(a, src_ip, dest_ip, ch_host, cfg)

    # 上传基线数据
    tmp = _patch(_SETUP, cfg)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-test-data.sh")
        out = a.ssh_exec(src_ip, "bash /tmp/setup-s3-test-data.sh", timeout=300)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}")); return build_result(results, start)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", ok, {}, {}, f"{'✓' if ok else '✗'} s3_setup"))
    if not ok: return build_result(results, start)

    # 全量 Sync
    p = subprocess.run([binary, "-c", config, "-l", "trace", "sync", "--id", SYNC_JOB_ID, src_url, dst_url],
                       capture_output=True, text=True, timeout=600)
    if p.returncode != 0:
        results.append(AssertionResult("full_sync", False, {}, {}, "✗ full_sync failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, cfg); return build_result(results, start)
    bl = {"dirs": BASELINE_DIRS, "files": BASELINE_FILES}
    results.append(a.check_cli_sync_output(p.stdout + p.stderr, bl))

    # 变更数据
    tmp2 = _patch(_MUTATE, cfg)
    try:
        a.scp_to(tmp2, src_ip, "/tmp/mutate-s3-test-data.sh")
        mut = a.ssh_exec(src_ip, "bash /tmp/mutate-s3-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ mutate: {e}"))
        _cleanup(a, src_ip, dest_ip, ch_host, cfg); return build_result(results, start)
    finally:
        if os.path.exists(tmp2): os.unlink(tmp2)
    ok2 = "OK:" in mut or "OK：" in mut
    results.append(AssertionResult("mutate", ok2, {}, {}, f"{'✓' if ok2 else '✗'} s3_mutate"))
    if not ok2: _cleanup(a, src_ip, dest_ip, ch_host, cfg); return build_result(results, start)

    # 增量 Sync
    p2 = subprocess.run([binary, "-c", config, "-l", "trace", "sync", "--id", SYNC_JOB_ID, src_url, dst_url],
                        capture_output=True, text=True, timeout=600)
    incr_out = p2.stdout + p2.stderr
    post = {"dirs": POST_DIRS, "files": POST_FILES}
    results.append(a.check_cli_scan_output(incr_out, post))

    # 验证目标端
    p3 = subprocess.run([binary, "-c", config, "-l", "trace", "scan", "--id", DST_SCAN_JOB_ID, dst_url],
                        capture_output=True, text=True, timeout=300)
    results.append(a.check_cli_scan_output(p3.stdout + p3.stderr, post))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}_dst", post))

    # Integrity Check
    p4 = subprocess.run([binary, "-c", config, "-l", "trace", "integrity-check", src_url, dst_url, "--quick"],
                        capture_output=True, text=True, timeout=300)
    ok4 = p4.returncode == 0 and "All Passed" in (p4.stdout + p4.stderr)
    results.append(AssertionResult("integrity_quick", ok4, {}, {}, f"{'✓' if ok4 else '✗'} integrity_quick"))

    _cleanup(a, src_ip, dest_ip, ch_host, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
