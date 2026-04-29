"""
assertions.py — 共享断言库
所有 e2e skill 的 run.py 导入本模块。
"""

import re
import subprocess
import urllib.request
import urllib.parse
from dataclasses import dataclass, field


@dataclass
class AssertionResult:
    name: str
    passed: bool
    expected: dict
    actual: dict
    message: str


class TerrasyncAssertions:
    def __init__(self, ssh_user="root"):
        self.ssh_user = ssh_user
        self._ssh_opts = [
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
        ]

    # ── SSH / SCP ──────────────────────────────────────────────────────────

    def ssh_exec(self, host, cmd, timeout=120) -> str:
        """在远端执行命令，返回 stdout。失败时抛出 RuntimeError。"""
        r = subprocess.run(
            ["ssh", *self._ssh_opts, f"{self.ssh_user}@{host}", cmd],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            raise RuntimeError(f"SSH {host} failed (rc={r.returncode}): {r.stderr.strip()}")
        return r.stdout

    def scp_to(self, local_path, host, remote_path, timeout=60):
        """上传本地文件到远端。失败时抛出 RuntimeError。"""
        r = subprocess.run(
            ["scp", *self._ssh_opts, str(local_path), f"{self.ssh_user}@{host}:{remote_path}"],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            raise RuntimeError(f"SCP to {host}:{remote_path} failed: {r.stderr.strip()}")

    # ── ClickHouse ─────────────────────────────────────────────────────────

    def clickhouse_query(self, host, query, timeout=30) -> str:
        """发送 HTTP 查询到 ClickHouse，返回原始文本。"""
        url = f"http://{host}/?query={urllib.parse.quote(query)}"
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode()

    def clickhouse_drop_tables(self, host, sanitized_job_id):
        """删除 base_*, state_*, incremental_* 表（IF EXISTS，幂等）。"""
        for prefix in ("base", "state", "incremental"):
            self.clickhouse_query(
                host, f"DROP TABLE IF EXISTS default.{prefix}_{sanitized_job_id}"
            )

    # ── 本地命令 ────────────────────────────────────────────────────────────

    def run_local(self, cmd_list, timeout=60) -> AssertionResult:
        """运行本地命令，返回 AssertionResult。"""
        name = cmd_list[0] if cmd_list else "cmd"
        try:
            r = subprocess.run(cmd_list, capture_output=True, text=True, timeout=timeout)
            passed = r.returncode == 0
            msg = f"{'✓' if passed else '✗'} {name}: exit={r.returncode}"
            if not passed and r.stderr:
                msg += f"\n  {r.stderr.strip()[:200]}"
            return AssertionResult(name, passed, {"rc": 0}, {"rc": r.returncode}, msg)
        except Exception as e:
            return AssertionResult(name, False, {}, {}, f"✗ {name}: {e}")

    def run_shell_quiet(self, cmd_str, timeout=30):
        """运行 shell 命令，忽略输出和错误（best-effort，用于清理步骤）。"""
        try:
            subprocess.run(cmd_str, shell=True, capture_output=True, timeout=timeout)
        except Exception:
            pass

    # ── 断言方法 ────────────────────────────────────────────────────────────

    def check_cli_scan_output(self, stdout: str, expected: dict) -> AssertionResult:
        """
        解析 terrasync scan 输出，验证 dirs/files/symlinks 计数。
        支持多种输出格式：
          - "dirs: 113, files: 335, symlinks: 79"
          - "113 dirs, 335 files, 79 symlinks"
          - "Scanned 113 dirs 335 files 79 symlinks"
        """
        actual = {}
        for field_name in ("dirs", "files", "symlinks"):
            patterns = [
                rf"{field_name}[=:\s]+(\d+)",
                rf"(\d+)\s+{field_name}",
            ]
            for pat in patterns:
                m = re.search(pat, stdout, re.IGNORECASE)
                if m:
                    actual[field_name] = int(m.group(1))
                    break

        if not actual:
            return AssertionResult(
                "cli_scan_output", False, expected, {},
                "✗ cli_scan_output: could not parse counts from output"
            )
        passed = all(actual.get(k) == v for k, v in expected.items())
        return AssertionResult(
            "cli_scan_output", passed, expected, actual,
            f"{'✓' if passed else '✗'} cli_scan_output: expected={expected}, actual={actual}"
        )

    def check_cli_sync_output(self, stdout: str, expected: dict) -> AssertionResult:
        """
        解析 terrasync sync 输出，验证 copied/skipped/deleted 计数。
        expected 中值为 None 的 key 跳过验证。
        """
        actual = {}
        for field_name in ("copied", "skipped", "deleted", "renamed"):
            patterns = [
                rf"{field_name}[=:\s]+(\d+)",
                rf"(\d+)\s+{field_name}",
            ]
            for pat in patterns:
                m = re.search(pat, stdout, re.IGNORECASE)
                if m:
                    actual[field_name] = int(m.group(1))
                    break

        check = {k: v for k, v in expected.items() if v is not None}
        passed = all(actual.get(k) == v for k, v in check.items())
        return AssertionResult(
            "cli_sync_output", passed, check, actual,
            f"{'✓' if passed else '✗'} cli_sync_output: expected={check}, actual={actual}"
        )

    def check_clickhouse_counts(
        self, host: str, table: str, expected: dict
    ) -> AssertionResult:
        """
        查询 ClickHouse base 表，验证 dirs/files/symlinks 计数。
        查询：SELECT is_dir,is_symlink,count(*) GROUP BY is_dir,is_symlink
        """
        query = (
            f"SELECT is_dir,is_symlink,count(*) FROM default.{table} "
            f"FINAL GROUP BY is_dir,is_symlink FORMAT TabSeparated"
        )
        text = self.clickhouse_query(host, query)
        actual = {}
        for line in text.strip().splitlines():
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            is_dir, is_sym, count = parts[0].strip(), parts[1].strip(), int(parts[2].strip())
            if is_dir == "true" and is_sym == "false":
                actual["dirs"] = count
            elif is_dir == "false" and is_sym == "true":
                actual["symlinks"] = count
            elif is_dir == "false" and is_sym == "false":
                actual["files"] = count

        passed = all(actual.get(k) == v for k, v in expected.items())
        name = f"clickhouse_{table}"
        return AssertionResult(
            name, passed, expected, actual,
            f"{'✓' if passed else '✗'} {name}: expected={expected}, actual={actual}"
        )

    def check_integrity_output(self, stdout: str) -> AssertionResult:
        """验证 integrity check 输出中 mismatches == 0。"""
        m = re.search(r"[Mm]ismatch(?:es)?[:\s=]+(\d+)", stdout)
        if not m:
            return AssertionResult(
                "integrity_check", False, {"mismatches": 0}, {},
                "✗ integrity_check: could not parse mismatch count from output"
            )
        count = int(m.group(1))
        passed = count == 0
        return AssertionResult(
            "integrity_check", passed,
            {"mismatches": 0}, {"mismatches": count},
            f"{'✓' if passed else '✗'} integrity_check: mismatches={count}"
        )
