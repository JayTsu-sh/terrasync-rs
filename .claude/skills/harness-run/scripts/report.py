"""
report.py — Markdown + JSON 报告生成
"""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path


def output_dir(label: str, base: str = "") -> Path:
    """
    返回报告输出目录路径（不创建）。
    优先用 base（来自 HARNESS_OUTPUT_DIR），其次用 $TMPDIR。
    """
    root = Path(base) if base else Path(tempfile.gettempdir()) / "terrasync-harness-results"
    return root / label


def write(label: str, cases: list[dict], base: str = "") -> Path:
    """
    生成报告文件，返回报告目录路径。

    cases 格式：
    [
      {
        "id": "nfs-v3-full-scan",
        "result": {"passed": True, "metrics": {"elapsed_sec": 52.3}, "assertions": [...]},
        "skipped": False,
        "skip_reason": "",
      },
      ...
    ]
    """
    out = output_dir(label, base)
    out.mkdir(parents=True, exist_ok=True)
    (out / "logs").mkdir(exist_ok=True)

    total = len([c for c in cases if not c.get("skipped")])
    passed = len([c for c in cases if not c.get("skipped") and c["result"].get("passed")])
    skipped = len([c for c in cases if c.get("skipped")])

    # ── Markdown ──────────────────────────────────────────────────────────
    lines = [
        f"# terrasync-rs Harness — {label}",
        f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| Test Case | Status | Duration |",
        "|-----------|--------|----------|",
    ]
    for c in cases:
        cid = c["id"]
        if c.get("skipped"):
            reason = c.get("skip_reason", "no run.py")
            lines.append(f"| {cid} | ⚠ SKIP ({reason}) | — |")
            continue
        r = c["result"]
        status = "✓ PASS" if r.get("passed") else "✗ FAIL"
        elapsed = r.get("metrics", {}).get("elapsed_sec", "—")
        dur = f"{elapsed}s" if isinstance(elapsed, (int, float)) else str(elapsed)
        lines.append(f"| {cid} | {status} | {dur} |")

    lines += [
        "",
        f"**Result: {passed}/{total} PASSED**"
        + (f" ({skipped} skipped)" if skipped else ""),
    ]

    (out / "summary.md").write_text("\n".join(lines))

    # ── JSON ──────────────────────────────────────────────────────────────
    summary = {
        "label": label,
        "generated": datetime.now().isoformat(),
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "skipped": skipped,
        "cases": cases,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))

    return out


def print_summary(label: str, cases: list[dict]):
    """在终端打印简洁摘要。"""
    total = len([c for c in cases if not c.get("skipped")])
    passed = len([c for c in cases if not c.get("skipped") and c["result"].get("passed")])
    skipped = len([c for c in cases if c.get("skipped")])

    print(f"\n{'='*50}")
    print(f"Harness: {label}")
    print(f"{'='*50}")
    for c in cases:
        cid = c["id"]
        if c.get("skipped"):
            print(f"  ⚠ SKIP  {cid}  ({c.get('skip_reason', 'no run.py')})")
            continue
        r = c["result"]
        icon = "✓" if r.get("passed") else "✗"
        elapsed = r.get("metrics", {}).get("elapsed_sec", "?")
        print(f"  {icon} {'PASS' if r.get('passed') else 'FAIL'}  {cid}  ({elapsed}s)")
    print(f"{'='*50}")
    print(f"Result: {passed}/{total} PASSED" + (f" ({skipped} skipped)" if skipped else ""))
