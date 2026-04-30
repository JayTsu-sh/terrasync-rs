---
name: e2e-test-s3-incremental-sync
description: >
  This skill should be used when the user asks to "run s3 incremental sync test",
  "test incremental sync s3", "s3 增量拷贝测试", "s3 incremental copy e2e",
  or mentions running the full-sync → mutate → incremental-sync → verify workflow for S3.
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# S3 Incremental Sync Test Skill

## Overview

端到端增量拷贝测试（S3 存储）。
验证完整管线：全量 sync 建基线 → 变更源端 → 增量 sync 检测并同步变更 → 目标端扫描 → integrity-check。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 S3 兼容存储。
测试数据通过 `mc`（MinIO Client）上传、变更和验证。

**S3 增量特点**：
- 使用 `JoinStrategy::Path`（无 file_handle）
- rename = New + Deleted（Renamed 始终为 0）
- `jobs/replicate_{SYNC_JOB_ID}/` 已存在时自动进入增量模式

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
| BASELINE_DIRS | 40 |
| BASELINE_FILES | 117 |
| POST_DIRS | 41 |
| POST_FILES | 115 |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `s3-incr-sync` |
| DST_SCAN_JOB_ID | `s3-incr-sync-dst` |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 配置 mc alias

```bash
mc alias set ts3 http://{S3_HOST} {S3_AK} {S3_SK} --api S3v4
```

### 0b. 清理源端 S3

```bash
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/ 2>/dev/null || true
echo "source S3 cleaned"
```

### 0c. 清理目标端 S3

```bash
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/ 2>/dev/null || true
echo "dest S3 cleaned"
```

### 0d. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25s3_incr_sync%25%27)+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25s3_incr_sync%25%27)+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0e. 清理 jobs 目录和日志

```bash
find jobs -maxdepth 1 -type d -name "*s3_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_incr_sync*"
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 均无输出（空）。

---

## Step 1: 编译本地 Binary

```bash
cargo build
```

Expected: 编译成功，生成 `{BINARY}`，无错误输出。

---

## Step 2: 上传源端测试数据

```bash
bash .claude/skills/s3-incremental-scan/scripts/setup-s3-test-data.sh
```

Expected output (last lines):

```
S3 files: 117
OK: S3 file count verified
```

### 2b. mc 验证

```bash
mc find ts3/{SRC_BUCKET}/test-data/ | wc -l
```

Expected: `117`。

**Stop if the script exits non-zero.**

---

## Step 3: Phase 1 — 全量 Sync（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

### 3b. ClickHouse 验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_incr_sync+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行）：

```
false   false   {BASELINE_FILES}      # 普通文件 = 117
true    false   {BASELINE_DIRS}       # 目录 = 40
```

### 3c. 目标端 mc 验证

```bash
mc find ts3/{DST_BUCKET}/test-data/ | wc -l
```

Expected: `117`。

**Do not proceed until full sync succeeds with all counts matching.**

---

## Step 4: 变更源端数据

```bash
bash .claude/skills/s3-incremental-scan/scripts/mutate-s3-test-data.sh
```

Expected output (last lines):

```
S3 files after mutation: 115
OK: S3 mutation verified
```

### 4b. mc 验证变更后文件数

```bash
mc find ts3/{SRC_BUCKET}/test-data/ | wc -l
```

Expected: `115`。

**Stop if the script exits non-zero.**

---

## Step 5: Phase 2 — 增量 Sync（本地执行）

同一 SYNC_JOB_ID（`jobs/replicate_{SYNC_JOB_ID}/` 已存在 → 自动增量模式）。

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

### 5a. 验证 Scanned Statistics

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks=0。

### 5b. 验证 Incremental Statistics

Expected（S3 Path 模式，rename 拆为 New+Deleted，Renamed=0）：

```
   ├─ New:         10 total | dirs      3 | files      7 | symlinks    0
   ├─ Changed:      2 total | dirs      0 | files      2 | symlinks    0
   ├─ Renamed:      0 total | dirs      0 | files      0 | symlinks    0
   └─ Deleted:     11 total | dirs      2 | files      9 | symlinks    0
```

**Verify ERROR STATISTICS 为 0。**

**If incremental sync fails，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NoSuchKey**: 变更脚本删除的对象在增量 sync 时被尝试读取。检查扫描与 sync 之间是否有时间差导致数据不一致。从 Step 0 重新开始。
   - **AccessDenied（写入目标桶）**: 目标桶 ACL 不允许 PUT/DELETE。检查桶 Policy 是否授权了完整的 s3:PutObject 和 s3:DeleteObject 权限。
   - **SlowDown**: 增量 sync 期间 S3 端限流。检查是否有其他客户端同时访问桶，降低 worker 并发数后重试。
   - **Incremental Statistics 不匹配**: S3 Path 模式下 rename 必然表现为 New+Deleted（Renamed=0）。如果 Renamed > 0 说明存在 bug。检查 JoinStrategy 配置。
   - **目标端数据残留**: 如果全量 sync 后目标端有多余文件未被清理，增量 sync 的 delete 操作可能不匹配。清理目标端后从 Step 3 重新开始。

**Do not proceed to Step 6 until the incremental sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 6: 验证目标端数据

### 6a. mc 直接计数（目标桶）

```bash
mc find ts3/{DST_BUCKET}/test-data/ | wc -l
```

Expected: `115`。

### 6b. scan 验证目标端计数（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks=0.**
If any count mismatches, stop. Do not proceed to cleanup.

### 6c. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_incr_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行）：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 115
true    false   {POST_MUTATE_DIRS}       # 目录 = 41
```

---

## Step 7: Integrity Check（增量后一致性校验）

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

**Verify: 退出码为 0，无不一致报告。若有 Missing 或 Mismatch，停止并记录详情，不执行后续清理。**

---

## Step 8: 并发清理（本地执行）

Only proceed after all Step 6 and Step 7 checks pass. **8a–8d 可并发执行**。

### 8a. 清理源端 S3

```bash
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/
echo "source S3 cleaned"
```

验证：`mc ls ts3/{SRC_BUCKET}/test-data/ 2>/dev/null | wc -l` → `0`。

### 8b. 清理目标端 S3

```bash
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/
echo "dest S3 cleaned"
```

验证：`mc ls ts3/{DST_BUCKET}/test-data/ 2>/dev/null | wc -l` → `0`。

### 8c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25s3_incr_sync%25%27)+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25s3_incr_sync%25%27)+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 8d. 清理 jobs 目录和日志

```bash
find jobs -maxdepth 1 -type d -name "*s3_incr_sync*" | xargs rm -rf
rm -rf target/debug/logs/*
```

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source S3, dest S3, ClickHouse, jobs, logs)
- [ ] Binary compiled (Step 1)
- [ ] Source data uploaded: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks=0 (Step 2)
- [ ] Full sync completed: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks=0 (Step 3)
- [ ] ClickHouse base table verified after full sync (Step 3b)
- [ ] Dest mc count match after full sync (Step 3c)
- [ ] Source mutations applied: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks=0 (Step 4)
- [ ] Incremental sync: new=10/changed=2/renamed=0/deleted=11 (Step 5b)
- [ ] Dest mc count match: 115 files (Step 6a)
- [ ] Dest scan counts match (Step 6b)
- [ ] ClickHouse dest base table verified (Step 6c)
- [ ] Integrity check: All Passed (Step 7)
- [ ] Source S3 cleaned (Step 8a)
- [ ] Dest S3 cleaned (Step 8b)
- [ ] ClickHouse tables cleaned (Step 8c)
- [ ] jobs dir and logs cleaned (Step 8d)
