#!/usr/bin/env python3
"""e2e-test-s3-versioned-incremental-scan/scripts/run.py — S3 多版本增量扫描 e2e 测试。"""

import os, subprocess, sys, time
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions, build_result

JOB_ID = "s3-ver-incr-scan"
SANITIZED = "s3_ver_incr_scan"


def _url(cfg, prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg["S3_SOURCE_IP"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    return f"s3://{ak}:{sk}@{bucket}.{ip}:{port}/{prefix}"


def _mc_setup_bucket(a, src_ip, cfg):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    try:
        a.ssh_exec(src_ip,
                   f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                   f"mc rb --force ts3/{bucket} 2>/dev/null || true; "
                   f"mc mb ts3/{bucket}; mc version enable ts3/{bucket}")
        return True
    except Exception:
        return False


def _cleanup(a, src_ip, ch_host, cfg):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    try:
        a.ssh_exec(src_ip, f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                            f"mc rb --force ts3/{bucket} 2>/dev/null || true")
    except Exception: pass
    a.clickhouse_drop_tables(ch_host, SANITIZED)
    a.run_shell_quiet(f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf")
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env=None):
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "S3_SOURCE_IP", "CLICKHOUSE_HOST", "S3_ACCESS_KEY", "S3_SECRET_KEY")
    src_ip, ch_host = cfg["S3_SOURCE_IP"], cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    src_url = _url(cfg)
    a = TerrasyncAssertions(ssh_user=cfg.get("SSH_USER","root"))
    results = []

    _cleanup(a, src_ip, ch_host, cfg)

    # 创建版本化 bucket 并上传测试数据
    if not _mc_setup_bucket(a, src_ip, cfg):
        results.append(AssertionResult("setup_bucket", False, {}, {}, "✗ versioned bucket setup failed"))
        return build_result(results, start)
    results.append(AssertionResult("setup_bucket", True, {}, {}, "✓ versioned bucket ready"))

    # 上传初始版本（使用 versioned setup script）
    ver_setup = _SKILL_DIR.parent / "s3-versioned-full-scan" / "scripts" / "setup-s3-versioned-test-data.sh"
    if not ver_setup.exists():
        results.append(AssertionResult("setup_data", False, {}, {}, f"✗ setup script not found: {ver_setup}"))
        return build_result(results, start)

    with open(ver_setup) as f: c = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bkt = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    for o, n in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
    ]: c = c.replace(o, n)
    c = c.replace('{S3_VERSIONED_BUCKET}', bkt).replace('mbucket-src-versioned', bkt)
    tmp = f"/tmp/s3ver_{os.getpid()}.sh"
    with open(tmp, "w") as f: f.write(c)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-versioned.sh")
        a.ssh_exec(src_ip, "bash /tmp/setup-s3-versioned.sh", timeout=300)
    except Exception as e:
        results.append(AssertionResult("setup_data", False, {}, {}, f"✗ setup: {e}"))
        return build_result(results, start)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    results.append(AssertionResult("setup_data", True, {}, {}, "✓ versioned test data uploaded"))

    # 全量扫描建基线
    p1 = subprocess.run([binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
                        capture_output=True, text=True, timeout=300)
    ok1 = p1.returncode == 0
    results.append(AssertionResult("full_scan", ok1, {}, {}, f"{'✓' if ok1 else '✗'} full_scan"))
    if not ok1: _cleanup(a, src_ip, ch_host, cfg); return build_result(results, start)

    # 上传新版本（模拟变更）
    ver_mutate = _SKILL_DIR.parent / "s3-versioned-incremental-scan" / "scripts" / "mutate-s3-versioned-test-data.sh"
    if ver_mutate.exists():
        with open(ver_mutate) as f: cm = f.read()
        for o, n in [
            ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
            ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
            ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
        ]: cm = cm.replace(o, n)
        cm = cm.replace('{S3_VERSIONED_BUCKET}', bkt).replace('mbucket-src-versioned', bkt)
        tmp2 = f"/tmp/s3vermut_{os.getpid()}.sh"
        with open(tmp2, "w") as f: f.write(cm)
        try:
            a.scp_to(tmp2, src_ip, "/tmp/mutate-s3-versioned.sh")
            a.ssh_exec(src_ip, "bash /tmp/mutate-s3-versioned.sh", timeout=120)
        except Exception: pass
        finally:
            if os.path.exists(tmp2): os.unlink(tmp2)

    # 增量扫描
    p2 = subprocess.run([binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
                        capture_output=True, text=True, timeout=300)
    ok2 = p2.returncode == 0
    results.append(AssertionResult("incr_scan", ok2, {}, {}, f"{'✓' if ok2 else '✗'} incr_scan"))
    if ok2:
        # 验证增量表有记录
        count = a.clickhouse_query(
            ch_host,
            f"SELECT count(*) FROM default.incremental_{SANITIZED} FINAL FORMAT TabSeparated"
        ).strip()
        has = int(count) > 0 if count.isdigit() else False
        results.append(AssertionResult("incr_ch_has_data", has, {"count>": 0}, {"count": count},
                                       f"{'✓' if has else '✗'} incr_ch_has_data: count={count}"))

    _cleanup(a, src_ip, ch_host, cfg)
    return build_result(results, start)




if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
