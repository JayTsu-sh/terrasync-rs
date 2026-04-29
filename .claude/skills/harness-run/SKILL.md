---
name: harness-run
description: >
  运行 terrasync-rs 自动化测试 harness。
  用户说 "跑 harness"、"batch e2e"、"CI 测试"、"smoke test"、
  "harness run nfs-v3"、"全量 e2e"、"跑所有 e2e 用例"、
  "run all e2e tests"、"harness smoke"、"批量测试" 时触发。
---

# Harness Run Skill

## Overview

统一调度器，批量运行各协议 e2e 测试，生成 Markdown + JSON 报告。

- **交互模式**：由 Claude 执行下方步骤
- **自动化模式**：`python scripts/runner.py --smoke`（不需要 Claude）

## 前置条件

```bash
# 1. 编译 terrasync
cargo build -p cli

# 2. 配置环境
cp .claude/skills/harness-run/.env.example .claude/skills/harness-run/.env
# 编辑 .env，填写实际 IP 和凭据
```

## 运行模式

### 冒烟测试（推荐首次验证）
```bash
python .claude/skills/harness-run/scripts/runner.py --smoke --label smoke-$(date +%Y%m%d)
```
运行 NFS v3 full-scan + S3 full-scan（并发），约 90s。

### 单协议套件
```bash
python .claude/skills/harness-run/scripts/runner.py --suite nfs-v3
python .claude/skills/harness-run/scripts/runner.py --suite s3
python .claude/skills/harness-run/scripts/runner.py --suite nfs-v4
python .claude/skills/harness-run/scripts/runner.py --suite cifs
```

### 单个用例
```bash
python .claude/skills/harness-run/scripts/runner.py --id nfs-v3-full-scan
```

### 全量（慎用，时间长）
```bash
python .claude/skills/harness-run/scripts/runner.py --all
```

## 报告格式

报告输出到 `$TMPDIR/terrasync-harness-results/<label>/`：
- `summary.md` — Markdown 表格
- `summary.json` — 机器可读结果
- `logs/<case_id>.log` — 各用例输出

示例 summary.md：
```markdown
# terrasync-rs Harness — smoke-20260429

| Test Case        | Status  | Duration |
|------------------|---------|----------|
| nfs-v3-full-scan | ✓ PASS  | 52.3s    |
| s3-full-scan     | ✓ PASS  | 38.1s    |

**Result: 2/2 PASSED**
```

## 脚本文件说明

| 文件 | 用途 |
|------|------|
| `scripts/runner.py` | 主编排器：解析参数、加载 matrix、调度 cases |
| `scripts/assertions.py` | 共享断言库（所有 e2e skill 的 run.py 导入）|
| `scripts/env.py` | .env 加载、校验、默认值 |
| `scripts/report.py` | Markdown + JSON 报告生成 |
| `scripts/matrix.yaml` | 测试矩阵：suites、cases、并发策略 |

## 为新 skill 添加 run.py

1. 在 skill 目录创建 `scripts/run.py`，实现 `run(env=None) -> dict` 接口
2. 创建 `.env.example`，列出该 skill 需要的变量
3. 在 `scripts/matrix.yaml` 对应 suite 中注册：
   ```yaml
   - {id: <skill-id>, skill_dir: <skill-directory-name>, timeout: 600}
   ```
4. 运行 `python runner.py --id <skill-id>` 验证
