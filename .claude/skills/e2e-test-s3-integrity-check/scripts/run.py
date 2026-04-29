#!/usr/bin/env python3
"""e2e-test-s3-integrity-check/scripts/run.py — S3 完整性校验 e2e 测试（多场景）。"""

import os, re, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result

SYNC_JOB_ID = "s3-ic-sync"
IC_JOB_ID = "s3-ic-test"
SANITIZED = "s3_ic"
EXPECTED_DIRS, EXPECTED_FILES = 40, 117
_SETUP = _SKILL_DIR.parent / "e2e-test-s3-incremental-scan" / "scripts" / "setup-s3-test-data.sh"


def _url(cfg, ip_key, bkt_key, port_key="S3_SOURCE_PORT", prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    return f"s3://{ak}:{sk}@{cfg[bkt_key]}.{cfg[ip_key]}:{cfg.get(port_key,'39000')}/{prefix}"


def _patch(path, cfg):
    with open(path) as f: c = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bkt = cfg.get("S3_BUCKET_SRC","test-bucket"); port = cfg.get("S3_SOURCE_PORT","39000")
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


def _drop_ic_tables(a, ch_host):
    tables = a.clickhouse_query(ch_host,
        f"SELECT name FROM system.tables WHERE database='default' "
        f"AND name LIKE '%{SANITIZED}%' FORMAT TabSeparated")
    for t in tables.strip().splitlines():
        if t.strip(): a.clickhouse_query(ch_host, f"DROP TABLE IF EXISTS default.{t.strip()}")


def _ic(binary, config, src_url, dst_url, *args, timeout=300):
    return subprocess.run([binary, "-c", config, "-l", "trace", "integrity-check",
                          src_url, dst_url, *args], capture_output=True, text=True, timeout=timeout)


def _check_ic(out, label, expect_pass, min_mm=0, min_ms=0):
    all_passed = "All Passed" in out
    if expect_pass:
        return AssertionResult(f"ic_{label}", all_passed, {}, {},
                               f"{'✓' if all_passed else '✗'} ic_{label}")
    mm = int((re.search(r"Mismatch[:\s]+(\d+)", out, re.I) or [0,'0'])[1])
    ms = int((re.search(r"Missing[:\s]+(\d+)", out, re.I) or [0,'0'])[1])
    passed = mm >= min_mm and ms >= min_ms
    return AssertionResult(f"ic_{label}", passed, {"mm>=": min_mm, "ms>=": min_ms},
                           {"mm": mm, "ms": ms}, f"{'✓' if passed else '✗'} ic_{label}: mm={mm}, ms={ms}")


def _cleanup(a, src_ip, dest_ip, ch_host, cfg):
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(_mc_rm, a, src_ip, cfg.get("S3_BUCKET_SRC","test-bucket"), cfg),
            ex.submit(_mc_rm, a, dest_ip, cfg.get("S3_BUCKET_DST","test-bucket"),
                      {**cfg,"S3_SOURCE_PORT":cfg.get("S3_DEST_PORT","39000")}),
            ex.submit(_drop_ic_tables, a, ch_host),
            ex.submit(a.run_shell_quiet, f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
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

    # 上传数据
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

    # Sync 建基线
    p = subprocess.run([binary, "-c", config, "-l", "trace", "sync", "--id", SYNC_JOB_ID, src_url, dst_url],
                       capture_output=True, text=True, timeout=600)
    if p.returncode != 0:
        results.append(AssertionResult("sync", False, {}, {}, "✗ sync failed"))
        _cleanup(a, src_ip, dest_ip, ch_host, cfg); return build_result(results, start)
    results.append(AssertionResult("sync", True, {}, {}, "✓ initial_sync"))

    # 场景1: Full 一致
    r1 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-full")
    results.append(_check_ic(r1.stdout + r1.stderr, "full_consistent", True))

    # 场景2: Quick 一致
    r2 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-quick", "--quick")
    results.append(_check_ic(r2.stdout + r2.stderr, "quick_consistent", True))

    # 制造差异：在目标端 SSH 修改对象内容（需要通过 mc）
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_DEST_PORT", "39000")
    dst_bkt = cfg.get("S3_BUCKET_DST", "test-bucket")
    try:
        a.ssh_exec(dest_ip,
                   f"mc alias set ts3d http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                   f"echo 'tampered-s3-content' | mc pipe ts3d/{dst_bkt}/test-data/d1/d1_1/file1.txt")
    except Exception: pass

    r3 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-mismatch")
    results.append(_check_ic(r3.stdout + r3.stderr, "detects_mismatch", False, min_mm=1))

    # 删除目标端对象
    try:
        a.ssh_exec(dest_ip, f"mc rm ts3d/{dst_bkt}/test-data/d2/file1.txt 2>/dev/null || true")
    except Exception: pass

    r4 = _ic(binary, config, src_url, dst_url, "--id", f"{IC_JOB_ID}-missing")
    results.append(_check_ic(r4.stdout + r4.stderr, "detects_missing", False, min_mm=1, min_ms=1))

    _cleanup(a, src_ip, dest_ip, ch_host, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
