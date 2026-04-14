#!/usr/bin/env python3
"""
Terrasync 日志分析工具

从海量 trace 日志中提取最小有用集合，便于问题分析和定位。

支持两种日志格式：
  - JSON 模式（推荐）：使用 app.json.log，通过 span 字段精确关联追踪链
  - 文本模式（回退）：使用 app.log，通过路径字符串文本匹配关联

功能：
  1. 提取前 N 行启动日志（默认 300）
  2. 解析 error.log 中的 ERROR 信息，提取文件路径
  3. 在日志中查找该路径的完整追踪链
  4. 根据追踪链首尾时间戳 ±5 秒提取上下文日志
  5. 按追踪链分组输出，每组仅包含相关日志

用法：
    # JSON 模式（推荐，需 config.toml 中 enable_json = true）
    python log_analyzer.py --app app.json.log --error error.log -o result.log

    # 文本模式
    python log_analyzer.py --app app.log --error error.log -o result.log

    # 自定义参数
    python log_analyzer.py --app app.log --error error.log --head 500 --margin 10
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ========================================================================
# 时间戳解析
# ========================================================================

# 文本日志时间戳正则：2026-04-06T14:37:22.636732Z
TS_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)")


def parse_ts(ts_str: str) -> datetime | None:
    """解析 ISO 8601 时间戳字符串"""
    if not ts_str:
        return None
    # 去掉可能的前后空白
    ts_str = ts_str.strip()
    try:
        dot_pos = ts_str.index(".")
        frac = ts_str[dot_pos + 1 :].rstrip("Z")
        if len(frac) > 6:
            frac = frac[:6]
        normalized = ts_str[: dot_pos + 1] + frac.ljust(6, "0") + "Z"
        return datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, IndexError):
        return None


def parse_ts_from_line(line: str) -> datetime | None:
    """从文本日志行开头解析时间戳"""
    m = TS_PATTERN.match(line)
    return parse_ts(m.group(1)) if m else None


# ========================================================================
# ERROR 日志解析
# ========================================================================

# 从 ERROR 行提取引号内的文件路径
# 例: CopyEntry failed "test-data/d3/d3_1/d3_1_2/file1.txt": Copy error: ...
PATH_IN_ERROR = re.compile(r'(?:CopyEntry failed|failed)\s+"([^"]+)"')


def extract_error_paths(error_file: Path) -> list[dict]:
    """从 error.log 解析每条 ERROR，提取文件路径和时间戳"""
    errors = []
    with open(error_file, "r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line:
                continue
            ts = parse_ts_from_line(line)
            m = PATH_IN_ERROR.search(line)
            path = m.group(1) if m else None
            errors.append(
                {"line_no": line_no, "timestamp": ts, "path": path, "raw": line}
            )
    return errors


# ========================================================================
# JSON 日志模式
# ========================================================================


def is_json_log(file_path: Path) -> bool:
    """检测文件是否为 JSON 格式日志"""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        first_line = f.readline().strip()
        if not first_line:
            return False
        try:
            json.loads(first_line)
            return True
        except json.JSONDecodeError:
            return False


def parse_json_log(file_path: Path) -> list[dict]:
    """
    解析 JSON 日志文件，每行一个 JSON 对象。

    tracing-subscriber JSON 格式示例：
    {
      "timestamp": "2026-04-06T14:37:22.636732Z",
      "level": "ERROR",
      "target": "app::orchestrator",
      "fields": {"message": "..."},
      "spans": [
        {"name": "sync", "job_id": "nfs_v3_full_sync", ...},
        {"name": "receiver_worker", "recv_id": 0},
        {"name": "recv_entry", "path": "test-data/d3/d3_1/d3_1_2/file1.txt"}
      ],
      "span": {"name": "recv_entry", "path": "..."}
    }
    """
    records = []
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                obj["_line_no"] = line_no
                obj["_raw"] = line
                records.append(obj)
            except json.JSONDecodeError:
                # 非 JSON 行跳过
                pass
    return records


def extract_path_from_json_record(record: dict) -> str | None:
    """从 JSON 日志记录中提取文件路径（来自 span 字段）"""
    # 优先从 spans 数组查找 path 字段
    for span in record.get("spans", []):
        if "path" in span:
            return span["path"]
    # 回退：从 current span 查找
    span = record.get("span", {})
    if span and "path" in span:
        return span["path"]
    # 回退：从 fields.message 提取
    msg = record.get("fields", {}).get("message", "")
    m = PATH_IN_ERROR.search(msg)
    return m.group(1) if m else None


def find_json_trace_chain(
    records: list[dict], search_path: str
) -> list[dict]:
    """在 JSON 日志中查找包含指定路径的所有记录"""
    chain = []
    for record in records:
        path = extract_path_from_json_record(record)
        if path and search_path in path:
            chain.append(record)
            continue
        # 也在 message 中搜索
        msg = record.get("fields", {}).get("message", "")
        if search_path in msg:
            chain.append(record)
    return chain


def get_json_time_window(
    records: list[dict], margin_seconds: float
) -> tuple[datetime, datetime] | None:
    """从 JSON 记录集合计算时间窗口"""
    timestamps = []
    for r in records:
        ts = parse_ts(r.get("timestamp", ""))
        if ts:
            timestamps.append(ts)
    if not timestamps:
        return None
    return (
        min(timestamps) - timedelta(seconds=margin_seconds),
        max(timestamps) + timedelta(seconds=margin_seconds),
    )


def extract_json_in_window(
    records: list[dict], t_min: datetime, t_max: datetime
) -> list[dict]:
    """提取时间窗口内的所有 JSON 记录"""
    result = []
    for r in records:
        ts = parse_ts(r.get("timestamp", ""))
        if ts and t_min <= ts <= t_max:
            result.append(r)
    return result


def format_json_record(record: dict, compact: bool = True) -> str:
    """将 JSON 日志记录格式化为可读文本"""
    ts = record.get("timestamp", "?")
    level = record.get("level", "?")
    target = record.get("target", "?")
    msg = record.get("fields", {}).get("message", "")

    # 构建 span 链
    spans = record.get("spans", [])
    span_chain = ":".join(s.get("name", "?") for s in spans) if spans else ""

    # 提取 span 中的关键字段
    span_fields = {}
    for s in spans:
        for k, v in s.items():
            if k != "name":
                span_fields[k] = v

    if compact:
        span_kv = " ".join(f"{k}={v}" for k, v in span_fields.items())
        line = f"{ts} {level:>5} {span_chain}: {target}: {msg}"
        if span_kv:
            line += f" {span_kv}"
        return line
    else:
        return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


# ========================================================================
# 文本日志模式
# ========================================================================


def find_text_trace_lines(app_lines: list[str], search_path: str) -> list[int]:
    """在文本日志中查找包含指定路径的所有行号（0-based）"""
    return [i for i, line in enumerate(app_lines) if search_path in line]


def get_text_time_window(
    app_lines: list[str], line_indices: list[int], margin_seconds: float
) -> tuple[datetime, datetime] | None:
    """根据匹配行的时间戳范围，扩展 margin 秒"""
    timestamps = [
        ts
        for i in line_indices
        if (ts := parse_ts_from_line(app_lines[i])) is not None
    ]
    if not timestamps:
        return None
    return (
        min(timestamps) - timedelta(seconds=margin_seconds),
        max(timestamps) + timedelta(seconds=margin_seconds),
    )


def extract_text_in_window(
    app_lines: list[str], t_min: datetime, t_max: datetime
) -> list[tuple[int, str]]:
    """提取时间窗口内的所有文本日志行"""
    return [
        (i + 1, line)
        for i, line in enumerate(app_lines)
        if (ts := parse_ts_from_line(line)) is not None and t_min <= ts <= t_max
    ]


# ========================================================================
# 通用逻辑
# ========================================================================


def deduplicate_windows(
    windows: list[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    """合并重叠的时间窗口"""
    if not windows:
        return []
    sorted_w = sorted(windows, key=lambda w: w[0])
    merged = [sorted_w[0]]
    for t_min, t_max in sorted_w[1:]:
        if t_min <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], t_max))
        else:
            merged.append((t_min, t_max))
    return merged


def fmt_ts(ts: datetime) -> str:
    return ts.strftime("%H:%M:%S.%f")


# ========================================================================
# 主流程
# ========================================================================


def run_json_mode(
    app_path: Path,
    error_path: Path,
    output_path: Path | None,
    head_count: int,
    margin: float,
):
    """JSON 日志模式"""
    print(f"[JSON 模式] 读取 {app_path}...", file=sys.stderr)
    records = parse_json_log(app_path)
    print(f"  共 {len(records)} 条记录", file=sys.stderr)

    print(f"解析 {error_path}...", file=sys.stderr)
    errors = extract_error_paths(error_path)
    print(f"  共 {len(errors)} 条错误", file=sys.stderr)

    # 提取唯一路径
    unique_paths = {}
    for err in errors:
        if err["path"] and err["path"] not in unique_paths:
            unique_paths[err["path"]] = err
    print(f"  唯一错误路径: {len(unique_paths)} 个", file=sys.stderr)

    # 为每个路径查找追踪链并计算时间窗口
    windows = []
    path_details = []  # (path, trace_count, t_min, t_max, chain_records)

    for path, err in unique_paths.items():
        chain = find_json_trace_chain(records, path)
        if not chain:
            print(f"  警告: 路径 '{path}' 在日志中未找到追踪", file=sys.stderr)
            if err["timestamp"]:
                t_min = err["timestamp"] - timedelta(seconds=margin)
                t_max = err["timestamp"] + timedelta(seconds=margin)
                windows.append((t_min, t_max))
                path_details.append((path, 0, t_min, t_max, []))
            continue

        window = get_json_time_window(chain, margin)
        if window:
            windows.append(window)
            path_details.append((path, len(chain), window[0], window[1], chain))
            print(
                f"  '{path}': {len(chain)} 条追踪, "
                f"窗口 {fmt_ts(window[0])} ~ {fmt_ts(window[1])}",
                file=sys.stderr,
            )

    merged_windows = deduplicate_windows(windows)
    print(f"\n合并后时间窗口: {len(merged_windows)} 个", file=sys.stderr)
    for i, (t_min, t_max) in enumerate(merged_windows):
        duration = (t_max - t_min).total_seconds()
        print(
            f"  窗口 {i + 1}: {fmt_ts(t_min)} ~ {fmt_ts(t_max)} ({duration:.1f}s)",
            file=sys.stderr,
        )

    # 收集输出
    out = []

    # 第一部分：启动日志
    actual_head = min(head_count, len(records))
    out.append(f"{'=' * 100}")
    out.append(f"[启动日志] 前 {actual_head} 条记录")
    out.append(f"{'=' * 100}")
    for r in records[:actual_head]:
        out.append(f"  L{r['_line_no']:>6} | {format_json_record(r)}")

    # 第二部分：错误摘要
    out.append("")
    out.append(f"{'=' * 100}")
    out.append(f"[错误摘要] 共 {len(errors)} 条错误, {len(unique_paths)} 个唯一路径")
    out.append(f"{'=' * 100}")
    for err in errors:
        out.append(f"  L{err['line_no']:>4} | {err['raw']}")

    # 第三部分：每个时间窗口的上下文日志
    total_context = 0
    for i, (t_min, t_max) in enumerate(merged_windows):
        context = extract_json_in_window(records, t_min, t_max)
        total_context += len(context)
        duration = (t_max - t_min).total_seconds()

        out.append("")
        out.append(f"{'=' * 100}")
        out.append(
            f"[时间窗口 {i + 1}/{len(merged_windows)}] "
            f"{fmt_ts(t_min)} ~ {fmt_ts(t_max)} "
            f"({duration:.1f}s, {len(context)} 条)"
        )
        for path, count, wt_min, wt_max, _ in path_details:
            if wt_min >= t_min and wt_max <= t_max:
                out.append(f"  关联路径: {path} ({count} 条追踪)")
        out.append(f"{'=' * 100}")
        for r in context:
            out.append(f"  L{r['_line_no']:>6} | {format_json_record(r)}")

    # 统计
    out.append("")
    out.append(f"{'=' * 100}")
    out.append("[统计]")
    out.append(f"  日志总记录数: {len(records)}")
    out.append(f"  输出启动日志: {actual_head} 条")
    out.append(f"  输出上下文日志: {total_context} 条")
    ratio = (actual_head + total_context) / max(len(records), 1) * 100
    out.append(f"  压缩比: {ratio:.1f}%")
    out.append(f"{'=' * 100}")

    write_output(out, output_path, actual_head, total_context, len(records))


def run_text_mode(
    app_path: Path,
    error_path: Path,
    output_path: Path | None,
    head_count: int,
    margin: float,
):
    """文本日志模式"""
    print(f"[文本模式] 读取 {app_path}...", file=sys.stderr)
    with open(app_path, "r", encoding="utf-8", errors="replace") as f:
        app_lines = [line.rstrip("\n") for line in f]
    print(f"  共 {len(app_lines)} 行", file=sys.stderr)

    print(f"解析 {error_path}...", file=sys.stderr)
    errors = extract_error_paths(error_path)
    print(f"  共 {len(errors)} 条错误", file=sys.stderr)

    unique_paths = {}
    for err in errors:
        if err["path"] and err["path"] not in unique_paths:
            unique_paths[err["path"]] = err
    print(f"  唯一错误路径: {len(unique_paths)} 个", file=sys.stderr)

    windows = []
    path_details = []

    for path, err in unique_paths.items():
        trace_indices = find_text_trace_lines(app_lines, path)
        if not trace_indices:
            print(f"  警告: 路径 '{path}' 在 app.log 中未找到追踪日志", file=sys.stderr)
            if err["timestamp"]:
                t_min = err["timestamp"] - timedelta(seconds=margin)
                t_max = err["timestamp"] + timedelta(seconds=margin)
                windows.append((t_min, t_max))
                path_details.append((path, 0, t_min, t_max))
            continue

        window = get_text_time_window(app_lines, trace_indices, margin)
        if window:
            windows.append(window)
            path_details.append((path, len(trace_indices), window[0], window[1]))
            print(
                f"  '{path}': {len(trace_indices)} 条追踪, "
                f"窗口 {fmt_ts(window[0])} ~ {fmt_ts(window[1])}",
                file=sys.stderr,
            )

    merged_windows = deduplicate_windows(windows)
    print(f"\n合并后时间窗口: {len(merged_windows)} 个", file=sys.stderr)
    for i, (t_min, t_max) in enumerate(merged_windows):
        duration = (t_max - t_min).total_seconds()
        print(
            f"  窗口 {i + 1}: {fmt_ts(t_min)} ~ {fmt_ts(t_max)} ({duration:.1f}s)",
            file=sys.stderr,
        )

    out = []

    actual_head = min(head_count, len(app_lines))
    out.append(f"{'=' * 100}")
    out.append(f"[启动日志] 前 {actual_head} 行")
    out.append(f"{'=' * 100}")
    for i in range(actual_head):
        out.append(f"{i + 1:>6} | {app_lines[i]}")

    out.append("")
    out.append(f"{'=' * 100}")
    out.append(f"[错误摘要] 共 {len(errors)} 条错误, {len(unique_paths)} 个唯一路径")
    out.append(f"{'=' * 100}")
    for err in errors:
        out.append(f"  L{err['line_no']:>4} | {err['raw']}")

    total_context = 0
    for i, (t_min, t_max) in enumerate(merged_windows):
        context = extract_text_in_window(app_lines, t_min, t_max)
        total_context += len(context)
        duration = (t_max - t_min).total_seconds()

        out.append("")
        out.append(f"{'=' * 100}")
        out.append(
            f"[时间窗口 {i + 1}/{len(merged_windows)}] "
            f"{fmt_ts(t_min)} ~ {fmt_ts(t_max)} "
            f"({duration:.1f}s, {len(context)} 行)"
        )
        for path, count, wt_min, wt_max in path_details:
            if wt_min >= t_min and wt_max <= t_max:
                out.append(f"  关联路径: {path} ({count} 条追踪)")
        out.append(f"{'=' * 100}")
        for line_no, line in context:
            out.append(f"{line_no:>6} | {line}")

    out.append("")
    out.append(f"{'=' * 100}")
    out.append("[统计]")
    out.append(f"  app.log 总行数: {len(app_lines)}")
    out.append(f"  输出启动日志: {actual_head} 行")
    out.append(f"  输出上下文日志: {total_context} 行")
    ratio = (actual_head + total_context) / max(len(app_lines), 1) * 100
    out.append(f"  压缩比: {ratio:.1f}%")
    out.append(f"{'=' * 100}")

    write_output(out, output_path, actual_head, total_context, len(app_lines))


def write_output(
    lines: list[str],
    output_path: Path | None,
    head_count: int,
    context_count: int,
    total: int,
):
    """写出分析结果"""
    text = "\n".join(lines) + "\n"
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
        ratio = (head_count + context_count) / max(total, 1) * 100
        print(f"\n输出已写入: {output_path}", file=sys.stderr)
        print(
            f"总输出: {head_count + context_count} / {total} ({ratio:.1f}%)",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(text)


def main():
    parser = argparse.ArgumentParser(
        description="Terrasync 日志分析工具 - 从海量 trace 日志中提取最小有用集合",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # JSON 模式（推荐，配置 enable_json = true 后自动生成 app.json.log）
  python log_analyzer.py --app logs/app.json.log --error logs/error.log -o result.log

  # 文本模式
  python log_analyzer.py --app logs/app.log --error logs/error.log -o result.log

  # 自定义启动日志行数和时间窗口
  python log_analyzer.py --app app.log --error error.log --head 500 --margin 10
""",
    )
    parser.add_argument(
        "--app",
        required=True,
        help="app.log 或 app.json.log 文件路径（自动检测格式）",
    )
    parser.add_argument("--error", required=True, help="error.log 文件路径")
    parser.add_argument(
        "--output", "-o", default=None, help="输出文件路径（默认 stdout）"
    )
    parser.add_argument(
        "--head",
        type=int,
        default=300,
        help="提取日志前 N 行/条启动信息（默认 300）",
    )
    parser.add_argument(
        "--margin",
        type=float,
        default=5.0,
        help="时间窗口扩展秒数（默认 5 秒）",
    )

    args = parser.parse_args()
    app_path = Path(args.app)
    error_path = Path(args.error)

    if not app_path.exists():
        print(f"错误: 日志文件不存在: {app_path}", file=sys.stderr)
        sys.exit(1)
    if not error_path.exists():
        print(f"错误: error.log 文件不存在: {error_path}", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output) if args.output else None

    # 自动检测日志格式
    if is_json_log(app_path):
        run_json_mode(app_path, error_path, output_path, args.head, args.margin)
    else:
        run_text_mode(app_path, error_path, output_path, args.head, args.margin)


if __name__ == "__main__":
    main()
