#!/usr/bin/env python3
"""e2e-test-s3-versioned-full-scan/scripts/run.py — S3 多版本全量扫描 e2e 测试。"""

import os, subprocess, sys, time
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS))
import env as envmod
from assertions import AssertionResult, TerrasyncAssertions

JOB_ID = "s3-ver-full-scan"
SANITIZED = "s3_ver_full_scan"
_VER_SETUP = Path(_SKILL_DIR.parent.parent).parent / ".claude" / "skills" / "s3-versioned-full-scan" / "scripts" / "setup-s3-versioned-test-data.sh"


def _url(cfg, prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg["S3_SOURCE_IP"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    return f"s3://{ak}:{sk}@{bucket}.{ip}:{port}/{prefix}"


def _mc_setup_versioned_bucket(a, src_ip, cfg):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    cmd = (
        f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
        f"mc rb --force ts3/{bucket} 2>/dev/null || true; "
        f"mc mb ts3/{bucket}; "
        f"mc version enable ts3/{bucket}; "
        f"echo 'bucket_created'"
    )
    out = a.ssh_exec(src_ip, cmd, timeout=60)
    return "bucket_created" in out


def _cleanup(a, src_ip, ch_host, cfg):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    try:
        a.ssh_exec(src_ip,
                   f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                   f"mc rb --force ts3/{bucket} 2>/dev/null || true")
    except Exception: pass
    a.clickhouse_drop_tables(ch_host, SANITIZED)
    a.run_shell_quiet(f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf")
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env=None):
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "S3_SOURCE_IP", "CLICKHOUSE_HOST", "S3_ACCESS_KEY", "S3_SECRET_KEY")
    src_ip = cfg["S3_SOURCE_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    src_url = _url(cfg)
    a = TerrasyncAssertions(ssh_user=cfg.get("SSH_USER","root"))
    results = []

    _cleanup(a, src_ip, ch_host, cfg)

    # 创建并启用版本控制的 bucket
    if not _mc_setup_versioned_bucket(a, src_ip, cfg):
        results.append(AssertionResult("setup_bucket", False, {}, {},
                                       "✗ setup_bucket: failed to create versioned bucket"))
        return _b(results, start)
    results.append(AssertionResult("setup_bucket", True, {}, {}, "✓ versioned bucket created"))

    # 上传多版本测试数据
    ver_setup = _SKILL_DIR.parent / "s3-versioned-full-scan" / "scripts" / "setup-s3-versioned-test-data.sh"
    if not ver_setup.exists():
        results.append(AssertionResult("setup_data", False, {}, {},
                                       f"✗ setup_data: {ver_setup} not found"))
        return _b(results, start)

    with open(ver_setup) as f: content = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bucket = cfg.get("S3_VERSIONED_BUCKET", "test-bucket-versioned")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    for o, n in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
    ]: content = content.replace(o, n)
    # Replace versioned bucket name if pattern exists
    content = content.replace('{S3_VERSIONED_BUCKET}', bucket).replace('mbucket-src-versioned', bucket)
    tmp = f"/tmp/s3_ver_{os.getpid()}.sh"
    with open(tmp, "w") as f: f.write(content)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-versioned.sh")
        out = a.ssh_exec(src_ip, "bash /tmp/setup-s3-versioned.sh", timeout=300)
    except Exception as e:
        results.append(AssertionResult("setup_data", False, {}, {}, f"✗ setup_data: {e}"))
        return _b(results, start)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    ok = "OK:" in out or "OK：" in out or "done" in out.lower()
    results.append(AssertionResult("setup_data", ok, {}, {},
                                   f"{'✓' if ok else '✗'} versioned_test_data"))

    # 全量扫描（versioned S3）
    proc = subprocess.run([binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
                          capture_output=True, text=True, timeout=300)
    scan_out = proc.stdout + proc.stderr
    scan_ok = proc.returncode == 0
    results.append(AssertionResult("scan_exit", scan_ok, {"code": 0}, {"code": proc.returncode},
                                   f"{'✓' if scan_ok else '✗'} scan_exit: {proc.returncode}"))
    if scan_ok:
        # 验证 CH 表有记录
        count = a.clickhouse_query(
            ch_host, f"SELECT count(*) FROM default.base_{SANITIZED} FINAL FORMAT TabSeparated"
        ).strip()
        has_data = int(count) > 0 if count.isdigit() else False
        results.append(AssertionResult("ch_has_data", has_data, {"count>": 0}, {"count": count},
                                       f"{'✓' if has_data else '✗'} ch_has_data: count={count}"))

    _cleanup(a, src_ip, ch_host, cfg)
    return _b(results, start)


def _b(results, start):
    elapsed = round(time.monotonic() - start, 1)
    passed = all(r.passed for r in results)
    for r in results: print(r.message)
    print(f"\n{'PASS' if passed else 'FAIL'} ({elapsed}s)")
    return {"passed": passed, "metrics": {"elapsed_sec": elapsed},
            "assertions": [{"name": r.name, "passed": r.passed, "message": r.message} for r in results]}


if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
