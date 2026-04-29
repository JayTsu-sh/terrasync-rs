---
name: e2e-test-s3-integrity-check
description: >
  This skill should be used when the user asks to "run s3 integrity check test",
  "test integrity check s3", "s3 一致性校验测试",
  "verify s3 source and dest match",
  or mentions running a standalone integrity-check between two S3 endpoints.
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# S3 Integrity Check Test Skill

## Overview

独立一致性校验测试（S3 存储）。
验证 integrity-check 在多种场景下的正确性：完全一致 → Quick 模式 → Mismatch 检测 → Missing 检测。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 S3 兼容存储。
通过 `mc` 在目标端引入差异用于验证检测能力。

**S3 integrity-check 特点**：
- 无 symlink 相关检查
- 无 uid/gid/mode 属性比较（S3 不支持）
- 主要比较：文件 size + 内容 hash（Full 模式）或仅 size（Quick 模式）
- Auto-Fix 在 S3 场景下不适用（无属性可修复）

## Prerequisites

- `mc`（MinIO Client）已安装并可用

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`S3`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `S3_SOURCE_IP` |
| DEST_IP | `S3_DEST_IP` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| S3_ACCESS_KEY | `S3_ACCESS_KEY` |
| S3_SECRET_KEY | `S3_SECRET_KEY` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`S3`）
| Name | Value |
|------|-------|
| S3_BUCKET_SRC | `test-bucket` |
| S3_BUCKET_DST | `test-bucket` |
| SOURCE_URL | `s3://{S3_ACCESS_KEY}:{S3_SECRET_KEY}@{S3_BUCKET_SRC}.{S3_SOURCE_IP}:39000/test-data` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `s3-ic-sync` |
| IC_JOB_ID | `s3-ic-test` |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 配置 mc alias + 清理 S3

```bash
mc alias set ts3 http://{S3_HOST} {S3_AK} {S3_SK} --api S3v4
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/ 2>/dev/null || true
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/ 2>/dev/null || true
echo "S3 cleaned"
```

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ic%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_ic*"
```

Expected: 无输出（空）。

### 0d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 编译本地 Binary + 准备数据

### 1a. 编译

```bash
cargo build
```

### 1b. 上传源端测试数据

```bash
bash .claude/skills/s3-incremental-scan/scripts/setup-s3-test-data.sh
```

Expected: `S3 files: 117`。

---

## Step 2: Sync 到目标桶

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

### 2b. 目标端 mc 验证

```bash
mc find ts3/{DST_BUCKET}/test-data/ --type f | wc -l
```

Expected: `117`。

**Do not proceed until sync succeeds and dest counts match.**

---

## Step 3: 场景 1 — 完全一致（Full 模式）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，Checked > 0，无 Issues。**

---

## Step 4: 场景 2 — Quick 模式

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-quick --quick "{SOURCE_URL}" "{DEST_URL}"
```

Expected: `All Passed ✓`。

**Quick 模式只比 size，不计算 hash。S3 场景下速度优势不如 NFS 明显（仍需 HEAD 请求获取 size）。**

---

## Step 5: 场景 3 — 引入差异

### 5a. 修改目标端文件内容（制造 Mismatch）

```bash
echo "tampered-content-for-s3-integrity-check" | mc pipe ts3/{DST_BUCKET}/test-data/d1/d1_1/file1.txt
```

### 5b. Full 模式校验（应检测到 Mismatch）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-mismatch "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   ├─ Passed:        ...
   └─ Issues:        ...
      ├─ Mismatch:  1  (files: 1, dirs: 0, symlinks: 0)
```

**Verify: Mismatch >= 1。**

### 5c. 删除目标端文件（制造 Missing）

```bash
mc rm ts3/{DST_BUCKET}/test-data/d2/file1.txt
```

### 5d. 校验（应检测到 Missing + Mismatch）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-missing "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   ├─ Passed:        ...
   └─ Issues:        ...
      ├─ Missing:    1  (files: 1, dirs: 0, symlinks: 0)
      ├─ Mismatch:  1  (files: 1, dirs: 0, symlinks: 0)
```

**Verify: Missing >= 1, Mismatch >= 1。**

**If integrity-check fails unexpectedly，按以下步骤排查：**

1. 查看日志：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **AccessDenied（GetObject/HeadObject）**: AK/SK 无目标桶读权限。integrity-check 需要读取源端和目标端内容进行 hash 比较。
   - **NoSuchKey（目标端）**: 目标端文件被外部删除。如果非 Step 5c 引入的删除，检查是否有其他客户端操作了桶。
   - **Hash 不匹配但文件未被修改**: S3 分段上传可能导致 ETag 计算方式不同（multipart ETag != MD5）。integrity-check 应使用流式 hash 而非 ETag。检查日志中的 hash 算法。
   - **Quick 模式误报 Mismatch**: Quick 模式只比 size。如果 `mc pipe` 写入的内容与原文件 size 相同，Quick 模式不会报 Mismatch。改用不同长度的内容。
   - **超时（大桶场景）**: integrity-check 需要遍历所有对象并逐个比较。大量文件时可能超时。调大 timeout 设置。

---

## Step 6: 并发清理

**6a–6d 可并发执行**。

### 6a. 清理源端 S3

```bash
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/
echo "source S3 cleaned"
```

### 6b. 清理目标端 S3

```bash
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/
echo "dest S3 cleaned"
```

### 6c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

### 6d. 清理 jobs 和日志

```bash
find jobs -maxdepth 1 -type d -name "*s3_ic*" | xargs rm -rf
rm -rf target/debug/logs/*
```

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled + source data uploaded (Step 1)
- [ ] Sync to dest completed: 40/117/0 (Step 2)
- [ ] Full mode: All Passed on identical data (Step 3)
- [ ] Quick mode: All Passed (Step 4)
- [ ] Mismatch detected after tampering dest file (Step 5b)
- [ ] Missing detected after deleting dest file (Step 5d)
- [ ] Source S3 cleaned (Step 6a)
- [ ] Dest S3 cleaned (Step 6b)
- [ ] ClickHouse tables cleaned (Step 6c)
- [ ] jobs dir and logs cleaned (Step 6d)
