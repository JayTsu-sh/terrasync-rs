---
description: E2E 测试执行协议，接触 e2e skill 文件时自动加载
paths:
  - ".claude/skills/e2e-test-*/**"
  - ".claude/skills/harness-run/**"
---

# E2E 测试执行协议

## 执行顺序

主流程串行（不可并发）：
```
清理（0a-0e）→ 构建 → 准备数据 → 执行 → 验证（3a-3d）→ 清理
```

可以并发的步骤：
- **清理阶段**（0a-0e）：多个清理命令并发执行
- **验证阶段**（3a-3d）：CLI 验证 + ClickHouse 验证并发执行

## 失败处理

- CLI 输出验证失败 → **立即终止**，不继续执行
- ClickHouse 计数验证失败 → **继续收集日志**，teardown 后统一报告
- SSH 连接失败 → 立即终止并提示检查网络/权限

## 产物归位

- **禁止**测试产物落在项目目录（不污染 git working tree）
- 所有产物落 `$TMPDIR/terrasync-harness-results/<label>/`
- 日志文件：`<label>/logs/<case_id>.log`
- 报告文件：`<label>/summary.md`（Markdown）+ `<label>/summary.json`（JSON）

## 清理脚本规范

- 清理脚本必须**幂等**：多次执行不出错（资源不存在时静默跳过）
- ClickHouse 清理：`DROP TABLE IF EXISTS base_<job_id>` / `state_<job_id>` / `incremental_<job_id>`
- NFS/CIFS 清理：通过 SSH 执行，远端资源不存在时用 `|| true` 保证幂等

## run.py 接口约定

所有 e2e skill 的 `scripts/run.py` 必须实现：

```python
def run(env: dict = None) -> dict:
    """
    harness runner 调用入口。
    env: 由 harness runner 注入的环境变量 dict（独立运行时从 .env 加载）
    返回: {
        "passed": bool,
        "metrics": {"elapsed_sec": float},
        "assertions": [{"name": str, "passed": bool, "message": str}],
    }
    """
```

独立运行时从同目录或 `harness-run/` 的 `.env` 加载配置，打印每条断言结果后输出 PASS/FAIL。
