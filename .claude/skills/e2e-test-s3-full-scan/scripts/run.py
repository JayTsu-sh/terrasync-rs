#!/usr/bin/env python3
"""
e2e-test-s3-full-scan/scripts/run.py
S3 (rustfs) 全量扫描 e2e 测试。
Setup/Cleanup 通过 SSH 在源端使用 mc（mc 已安装在 192.168.50.173 上）。
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
from assertions import AssertionResult, TerrasyncAssertions

JOB_ID = "s3-full-scan"
SANITIZED = "s3_full_scan"
EXPECTED_DIRS = 40
EXPECTED_FILES = 117
EXPECTED_TOTAL = EXPECTED_DIRS + EXPECTED_FILES  # 157，S3 无 symlink


def _s3_url(cfg, ip_key="S3_SOURCE_IP", bucket_key="S3_BUCKET_SRC", prefix="test-data"):
    ak = cfg["S3_ACCESS_KEY"]
    sk = cfg["S3_SECRET_KEY"]
    ip = cfg[ip_key]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    bucket = cfg[bucket_key]
    return f"s3://{ak}:{sk}@{bucket}.{ip}:{port}/{prefix}"


def _mc_cleanup(a, ip, bucket, cfg, prefix="test-data"):
    ak = cfg["S3_ACCESS_KEY"]
    sk = cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    cmd = (f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
           f"mc rm --recursive --force ts3/{bucket}/{prefix}/ 2>/dev/null || true; echo done")
    try:
        a.ssh_exec(ip, cmd)
    except Exception:
        pass


def _s3_setup(a, src_ip, cfg, setup_sh_path):
    """将 setup 脚本中的硬编码值替换为实际值，SCP 到源端运行。"""
    with open(setup_sh_path) as f:
        content = f.read()
    ak = cfg["S3_ACCESS_KEY"]
    sk = cfg["S3_SECRET_KEY"]
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    port = cfg.get("S3_SOURCE_PORT", "39000")
    replacements = {
        'S3_HOST="http://10.128.137.245:8184"': f'S3_HOST="http://localhost:{port}"',
        'S3_AK="H80NKRVS5DYOVE43U2HS"': f'S3_AK="{ak}"',
        'S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"': f'S3_SK="{sk}"',
        'S3_BUCKET="mbucket-src"': f'S3_BUCKET="{bucket}"',
    }
    for old, new in replacements.items():
        content = content.replace(old, new)
    tmp = f"/tmp/setup-s3-{os.getpid()}.sh"
    with open(tmp, "w") as f:
        f.write(content)
    try:
        a.scp_to(tmp, src_ip, "/tmp/setup-s3-test-data.sh")
        return a.ssh_exec(src_ip, "bash /tmp/setup-s3-test-data.sh", timeout=300)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _cleanup(a, src_ip, ch_host, cfg):
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [
            ex.submit(_mc_cleanup, a, src_ip, bucket, cfg),
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

    # 上传测试数据
    setup_sh = _SKILL_DIR.parent / "e2e-test-s3-incremental-scan" / "scripts" / "setup-s3-test-data.sh"
    try:
        out = _s3_setup(a, src_ip, cfg, setup_sh)
    except Exception as e:
        results.append(AssertionResult("setup", False, {}, {}, f"✗ setup: {e}"))
        return _build(results, start)
    setup_ok = "OK:" in out or "OK：" in out
    results.append(AssertionResult("setup", setup_ok, {}, {},
                                   f"{'✓' if setup_ok else '✗'} s3_setup_data"))
    if not setup_ok:
        return _build(results, start)

    # 全量扫描
    proc = subprocess.run(
        [binary, "-c", config, "-l", "trace", "scan", "--id", JOB_ID, src_url],
        capture_output=True, text=True, timeout=300)
    scan_out = proc.stdout + proc.stderr
    if proc.returncode != 0:
        results.append(AssertionResult("scan_exit", False, {}, {}, "✗ scan failed"))
        _cleanup(a, src_ip, ch_host, cfg)
        return _build(results, start)

    results.append(a.check_cli_scan_output(
        scan_out, {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES}
    ))
    results.append(a.check_clickhouse_counts(
        ch_host, f"base_{SANITIZED}", {"dirs": EXPECTED_DIRS, "files": EXPECTED_FILES}
    ))

    # state 表总行数验证
    state_q = f"SELECT scan_state FROM default.state_{SANITIZED} FINAL WHERE id=1 FORMAT TabSeparated"
    state = a.clickhouse_query(ch_host, state_q).strip()
    if state:
        count = a.clickhouse_query(
            ch_host,
            f"SELECT count(*) FROM default.base_{SANITIZED} FINAL "
            f"WHERE current_state={state} FORMAT TabSeparated"
        ).strip()
        passed = count == str(EXPECTED_TOTAL)
        results.append(AssertionResult(
            "state_total", passed, {"total": EXPECTED_TOTAL}, {"total": count},
            f"{'✓' if passed else '✗'} state_total: expected={EXPECTED_TOTAL}, actual={count}"
        ))

    # 独立 S3 对象核查
    bucket = cfg.get("S3_BUCKET_SRC", "test-bucket")
    ak = cfg["S3_ACCESS_KEY"]
    sk = cfg["S3_SECRET_KEY"]
    port = cfg.get("S3_SOURCE_PORT", "39000")
    mc_count_cmd = (
        f"mc alias set ts3 http://localhost:{port} {ak} {sk} --api s3v4 2>/dev/null; "
        f"mc find ts3/{bucket}/test-data/ --type f 2>/dev/null | wc -l"
    )
    try:
        count_out = a.ssh_exec(src_ip, mc_count_cmd).strip()
        count = int(count_out.splitlines()[-1])
        passed = count == EXPECTED_FILES
        results.append(AssertionResult(
            "s3_file_count", passed,
            {"files": EXPECTED_FILES}, {"files": count},
            f"{'✓' if passed else '✗'} s3_file_count: expected={EXPECTED_FILES}, actual={count}"
        ))
    except Exception as e:
        results.append(AssertionResult("s3_file_count", False, {}, {}, f"✗ s3_file_count: {e}"))

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
