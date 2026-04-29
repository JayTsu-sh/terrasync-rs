#!/usr/bin/env python3
"""
runner.py — terrasync-rs Harness 主编排器

用法：
  python runner.py --smoke              # 冒烟（smoke suite，并发）
  python runner.py --suite nfs-v3       # 单协议套件
  python runner.py --id nfs-v3-full-scan # 单个用例
  python runner.py --all               # 全量（所有 suite，按顺序）
  python runner.py --label <name>      # 自定义报告标签（默认：run-<timestamp>）
"""

import argparse
import importlib.util
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

SCRIPTS_DIR = Path(__file__).parent
SKILLS_DIR = SCRIPTS_DIR.parent.parent  # .claude/skills/

sys.path.insert(0, str(SCRIPTS_DIR))
import env as envmod
import report as reportmod


def _load_matrix() -> dict:
    with open(SCRIPTS_DIR / "matrix.yaml") as f:
        return yaml.safe_load(f)


def _load_run_module(skill_dir_name: str):
    """
    动态加载 skill 的 run.py，返回模块。
    若 run.py 不存在返回 None。
    """
    run_py = SKILLS_DIR / skill_dir_name / "scripts" / "run.py"
    if not run_py.exists():
        return None
    spec = importlib.util.spec_from_file_location(f"run_{skill_dir_name}", run_py)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_case(case: dict, cfg: dict) -> dict:
    """运行单个 case，返回报告用的 dict。"""
    cid = case["id"]
    skill_dir = case["skill_dir"]
    timeout = case.get("timeout", 600)

    mod = _load_run_module(skill_dir)
    if mod is None:
        print(f"  ⚠ SKIP {cid}: no run.py in {skill_dir}/scripts/")
        return {"id": cid, "skipped": True, "skip_reason": "no run.py", "result": {"passed": False}}

    print(f"  → {cid}")
    try:
        result = mod.run(cfg)
    except Exception as e:
        result = {
            "passed": False,
            "metrics": {"elapsed_sec": 0},
            "assertions": [{"name": "run_exception", "passed": False, "message": str(e)}],
        }
        print(f"  ✗ {cid}: exception: {e}")

    return {"id": cid, "skipped": False, "result": result}


def _run_suite(suite_name: str, suite: dict, cfg: dict) -> list[dict]:
    cases = suite["cases"]
    parallel = suite.get("parallel", False)
    print(f"\n[suite: {suite_name}] {'parallel' if parallel else 'serial'}, {len(cases)} cases")

    if parallel:
        results = []
        with ThreadPoolExecutor(max_workers=min(len(cases), 8)) as ex:
            futs = {ex.submit(_run_case, c, cfg): c for c in cases}
            for f in as_completed(futs):
                results.append(f.result())
        # 按 matrix 顺序排
        id_order = {c["id"]: i for i, c in enumerate(cases)}
        results.sort(key=lambda r: id_order.get(r["id"], 999))
        return results
    else:
        results = []
        for c in cases:
            r = _run_case(c, cfg)
            results.append(r)
            if not r.get("skipped") and not r["result"].get("passed"):
                print(f"  ✗ {c['id']} FAILED — stopping suite")
                break
        return results


def main():
    parser = argparse.ArgumentParser(description="terrasync-rs Harness Runner")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--smoke", action="store_true", help="Run smoke suite")
    mode.add_argument("--suite", metavar="SUITE", help="Run named suite from matrix.yaml")
    mode.add_argument("--id", metavar="CASE_ID", help="Run single case by id")
    mode.add_argument("--all", action="store_true", help="Run all suites in order")
    parser.add_argument("--label", default="", help="Report label (default: run-<timestamp>)")
    args = parser.parse_args()

    label = args.label or f"run-{int(time.time())}"
    matrix = _load_matrix()
    cfg = envmod.load()

    all_results = []

    if args.smoke:
        suite = matrix["suites"].get("smoke")
        if not suite:
            sys.exit("ERROR: 'smoke' suite not found in matrix.yaml")
        all_results = _run_suite("smoke", suite, cfg)

    elif args.suite:
        suite = matrix["suites"].get(args.suite)
        if not suite:
            sys.exit(f"ERROR: suite '{args.suite}' not found in matrix.yaml")
        all_results = _run_suite(args.suite, suite, cfg)

    elif args.id:
        # 在所有 suite 中找到对应 case
        found = None
        for suite_name, suite in matrix["suites"].items():
            for c in suite["cases"]:
                if c["id"] == args.id:
                    found = c
                    break
            if found:
                break
        if not found:
            sys.exit(f"ERROR: case id '{args.id}' not found in matrix.yaml")
        all_results = [_run_case(found, cfg)]

    elif args.all:
        for suite_name, suite in matrix["suites"].items():
            results = _run_suite(suite_name, suite, cfg)
            all_results.extend(results)

    # 生成报告
    base = cfg.get("HARNESS_OUTPUT_DIR", "")
    out_dir = reportmod.write(label, all_results, base)
    reportmod.print_summary(label, all_results)
    print(f"\nReport: {out_dir}/summary.md")

    # 所有非 skip case 全部通过才返回 0
    failed = [r for r in all_results if not r.get("skipped") and not r["result"].get("passed")]
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
