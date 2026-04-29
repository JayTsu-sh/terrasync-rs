#!/usr/bin/env python3
"""
e2e-test-s3-incremental-scan/scripts/run.py
S3 增量扫描 e2e 测试：上传基线数据 → 全量扫描 → 变更 → 增量扫描 → 验证。
S3 使用 Path 策略，没有文件句柄，Renamed=0。
"""

import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_SKILL_DIR = Path(__file__).parent.parent
_HARNESS_SCRIPTS = _SKILL_DIR.parent / "harness-run" / "scripts"
sys.path.insert(0, str(_HARNESS_SCRIPTS))

import env as envmod
from assertions import AssertionResult, TerrasyncAssertions

JOB_ID = "s3-incr-scan"
SANITIZED = "s3_incr_scan"
BASELINE_DIRS, BASELINE_FILES = 40, 117
POST_DIRS, POST_FILES = 41, 115
POST_TOTAL = POST_DIRS + POST_FILES  # 156


def _s3_url(cfg, prefix="test-data"):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    ip = cfg["S3_SOURCE_IP"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    return f"s3://{ak}:{sk}@{bucket}.{ip}:{port}/{prefix}"


def _mc_cmd(cfg, cmd):
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    return f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; {cmd.replace('{B}', bucket)}"


def _patch_script(path, cfg, bucket_key="S3_BUCKET_SRC"):
    with open(path) as f:
        content = f.read()
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    bucket = cfg.get(bucket_key, "test-bucket")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    for old, new in [
        ('S3_HOST="http://10.128.137.245:8184"', f'S3_HOST="http://localhost:{port}"'),
        ('S3_AK="H80NKRVS5DYOVE43U2HS"', f'S3_AK="{ak}"'),
        ('S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"', f'S3_SK="{sk}"'),
        ('S3_BUCKET="mbucket-src"', f'S3_BUCKET="{bucket}"'),
    ]:
        content = content.replace(old, new)
    tmp = f"/tmp/s3_script_{os.getpid()}.sh"
    with open(tmp, "w") as f:
        f.write(content)
    return tmp


def _cleanup(a, src_ip, ch_host, cfg):
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    ak, sk = cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [
            ex.submit(a.ssh_exec, src_ip,
                      f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
                      f"mc rm --recursive --force ts3/{bucket}/test-data/ 2>/dev/null || true"),
            ex.submit(a.clickhouse_drop_tables, ch_host, SANITIZED),
            ex.submit(a.run_shell_quiet,
                      f"find jobs -maxdepth 1 -type d -name '*{SANITIZED}*' | xargs rm -rf"),
        ]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception:
                pass
    a.run_shell_quiet("rm -rf target/debug/logs/*")


def run(env: dict = None) -> dict:
    start = time.monotonic()
    cfg = envmod.load(env)
    envmod.require(cfg, "S3_SOURCE_IP", "CLICKHOUSE_HOST", "S3_ACCESS_KEY", "S3_SECRET_KEY")

    src_ip = cfg["S3_SOURCE_IP"]
    ch_host = cfg["CLICKHOUSE_HOST"]
    binary = cfg.get("TERRASYNC_BINARY", "./target/debug/terrasync")
    config = cfg.get("TERRASYNC_CONFIG", "examples/config.toml")
    ssh_user = cfg.get("SSH_USER", "root")
    src_url = _s3_url(cfg)

    a = TerrasyncAssertions(ssh_user=ssh_user)
    results = []

    _cleanup(a, src_ip, ch_host, cfg)

    # 上传基线数据
    setup_sh = _SKILL_DIR / "scripts" / "setup-s3-test-data.sh"
    tmp = _patch_script(setup_sh, cfg)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-test-data.sh")
        out = a.ssh_exec(src_ip, "bash /tmp/setup-s3-test-data.sh", timeout=300)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return _build(results, start)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} s3_setup_data"))
    if not setup_ok:
        return _build(results, start)

    # 全量扫描建基线
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        results.append(AssertionResult("full_scan", False, {}, {}, "✗ full_scan failed"))
        _cleanup(a, src_ip, ch_host, cfg)
        return _build(results, start)
    results.append(a.check_cli_scan_output(
        proc.stdout + proc.stderr, {"dirs": BASELINE_DIRS, "files": BASELINE_FILES}
    ))
    results.append(a.check_clickhouse_counts(
        ch_host, f"base_{SANITIZED}", {"dirs": BASELINE_DIRS, "files": BASELINE_FILES}
    ))

    # 变更数据
    mutate_sh = _SKILL_DIR / "scripts" / "mutate-s3-test-data.sh"
    tmp2 = _patch_script(mutate_sh, cfg)
    try:
        a.scp_to(tmp2, src_ip, "/tmp/mutate-s3-test-data.sh")
        mut_out = a.ssh_exec(src_ip, "bash /tmp/mutate-s3-test-data.sh", timeout=120)
    except Exception as e:
        results.append(AssertionResult("mutate", False, {}, {}, f"✗ mutate: {e}"))
        _cleanup(a, src_ip, ch_host, cfg)
        return _build(results, start)
    finally:
        if os.path.exists(tmp2):
            os.unlink(tmp2)
    mutate_ok = "OK:" in mut_out or "OK：" in mut_out
    results.append(AssertionResult("mutate", mutate_ok, {}, {},
                                   f"{'✓' if mutate_ok else '✗'} s3_mutate_data"))
    if not mutate_ok:
        _cleanup(a, src_ip, ch_host, cfg)
        return _build(results, start)

    # 增量扫描
    proc2 = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    incr_out = proc2.stdout + proc2.stderr
    post_exp = {"dirs": POST_DIRS, "files": POST_FILES}
    results.append(a.check_cli_scan_output(incr_out, post_exp))

    # 增量统计（S3 Path 策略，Renamed=0）
    actual_incr = {}
    for op in ("new", "changed", "renamed", "deleted"):
        m = re.search(rf"{op}[:\s]+(\d+)\s+total", incr_out, re.IGNORECASE)
        if m:
            actual_incr[op] = int(m.group(1))
    exp_incr = {"new": 10, "changed": 2, "renamed": 0, "deleted": 11}
    passed = all(actual_incr.get(k) == v for k, v in exp_incr.items())
    results.append(AssertionResult(
        "incr_statistics", passed, exp_incr, actual_incr,
        f"{'✓' if passed else '✗'} incr_statistics: expected={exp_incr}, actual={actual_incr}"
    ))
    results.append(a.check_clickhouse_counts(ch_host, f"base_{SANITIZED}", post_exp))

    _cleanup(a, src_ip, ch_host, cfg)
    return _build(results, start)


def _build(results, start):
    elapsed = round(time.monotonic() - start, 1)
    passed = all(r.passed for r in results)
    for r in results:
        print(r.message)
    print(f"\n{'PASS' if passed else 'FAIL'} ({elapsed}s)")
    return {"passed": passed, "metrics": {"elapsed_sec": elapsed},
            "assertions": [{"name": r.name, "passed": r.passed, "message": r.message}
                           for r in results]}


if __name__ == "__main__":
    sys.exit(0 if run()["passed"] else 1)
