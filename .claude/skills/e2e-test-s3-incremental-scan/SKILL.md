---
name: e2e-test-s3-incremental-scan
description: >
  This skill should be used when the user asks to "run s3 incremental scan test",
  "test incremental scan s3", "s3 增量扫描测试", "s3 incremental scan e2e",
  "test the incremental scan pipeline against S3",
  or mentions running the full-scan → mutate → incremental-scan → verify workflow
  against the S3 test environment ({S3_HOST}).
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# S3 Incremental Scan Test Skill

## Overview

端到端增量扫描测试（S3 存储）：全量扫描建基线 → 变更对象（增删改+rename） → 增量扫描检测变更 → 验证 CLI 输出和 ClickHouse 数据库。

**与 NFS v3 的关键区别**：S3 无 `file_handle` 文件句柄，使用 `JoinStrategy::Path` 模式。rename 操作（copy+delete）被拆为 **New + Deleted**，Renamed 始终为 0。S3 不支持 symlink。

`terrasync` 本地运行（使用 `{CONFIG}`），通过网络访问 S3。
测试数据通过 `mc`（MinIO Client）上传和变更。

## Prerequisites

- `mc`（MinIO Client）已安装并可用

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`S3`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `S3_SOURCE_IP` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| S3_ACCESS_KEY | `S3_ACCESS_KEY` |
| S3_SECRET_KEY | `S3_SECRET_KEY` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`S3`）
| Name | Value |
|------|-------|
| S3_BUCKET_SRC | `test-bucket` |
| SOURCE_URL | `s3://{S3_ACCESS_KEY}:{S3_SECRET_KEY}@{S3_BUCKET_SRC}.{S3_SOURCE_IP}:39000/test-data` |
| BASELINE_DIRS | 40 |
| BASELINE_FILES | 117 |
| POST_DIRS | 41 |
| POST_FILES | 115 |

### Skill 常量
| Name | Value |
|------|-------|
| JOB_ID | `s3-incr-scan` |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 清理 S3 数据

```bash
mc alias set ts3 http://{S3_HOST} {S3_AK} {S3_SK} --api S3v4
mc rm --recursive --force ts3/{S3_BUCKET}/test-data/ 2>/dev/null || true
echo "S3 cleaned"
```

验证：

```bash
mc find ts3/{S3_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_incr_scan*"
```

Expected: 无输出（空）。

### 0d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 编译本地 Binary

```bash
cargo build
```

Expected: 编译成功，生成 `{BINARY}`，无错误输出。

---

## Step 2: 上传测试数据到 S3

Use the Bash tool to run the setup script:

```bash
bash .claude/skills/s3-incremental-scan/scripts/setup-s3-test-data.sh
```

Expected output (last lines):

```
Expected: dirs=40, files=117, symlinks=0
S3 files: 117
OK: S3 file count verified (dirs will be verified by scan)
```

**Stop if the script exits non-zero.**

---

## Step 3: 全量扫描（建立基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{S3_URL}"
```

**3a.** 验证 CLI Scanned Statistics：dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks=0。

If counts do not match, stop and investigate.

### 3b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_incr_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，S3 无 symlink）：

```
false   false   {BASELINE_FILES}      # 普通文件 = 117
true    false   {BASELINE_DIRS}       # 目录 = 40
```

**若任意计数不符，停止并调查。**

---

## Step 4: 执行变更脚本

Use the Bash tool:

```bash
bash .claude/skills/s3-incremental-scan/scripts/mutate-s3-test-data.sh
```

Expected output (last lines):

```
Expected files: 115
S3 files: 115
OK: Post-mutation file count verified (dirs=41 will be verified by scan)
```

**Stop if the script exits non-zero.**

变更摘要：
- **ADD**: 2 dirs（隐式创建）, 3 files
- **MODIFY**: 2 files（覆盖内容改变 size+mtime）
- **RENAME**: 1 file（copy+delete）, 1 dir+3 files（copy+delete）
- **DELETE**: 1 dir+3 files, 2 standalone files

---

## Step 5: 增量扫描 + 全面验证

使用**同一 JOB_ID**（`jobs/` 目录已存在，自动触发增量模式）。

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{S3_URL}"
```

### 5a. 验证 Scanned Statistics

增量扫描仍遍历当前 S3 全部条目。

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks=0。

### 5b. 验证 Incremental Statistics

S3 使用 Path 模式：无 `file_handle` → rename 拆为 New + Deleted → Renamed 始终为 0。

Expected Incremental Statistics:

```
   ├─ New:         10 total | dirs      3 | files      7 | symlinks    0
   ├─ Changed:      2 total | dirs      0 | files      2 | symlinks    0
   ├─ Renamed:      0 total | dirs      0 | files      0 | symlinks    0
   └─ Deleted:     11 total | dirs      2 | files      9 | symlinks    0
```

**计数说明**：
- New 10 = 纯新增 (2 dirs + 3 files) + rename 目标 (1 dir + 1 file + 3 files in dir)
- Changed 2 = d1/d1_1/file1.txt + d2/d2_2/d2_2_1/file3.txt（内容变化）
- Renamed 0 = S3 无 fh3，无法检测 rename
- Deleted 11 = 删除 (1 dir + 3 files + 2 files) + rename 源 (1 dir + 1 file + 3 files in dir)

**若任意计数不符，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -40
```

2. 检查 ClickHouse 增量表原始记录：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,relative_path,is_dir,is_symlink+FROM+default.incremental_s3_incr_scan+FINAL+ORDER+BY+operation_type,relative_path+FORMAT+TabSeparated"
```

3. 根据原始记录定位不符项的具体路径和操作类型。

### 5c. ClickHouse base 表验证（增量后当前状态）

```bash
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.state_s3_incr_scan+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "current scan_state: $STATE"
[[ -z "$STATE" ]] && echo "ERROR: scan_state 为空，请检查 ClickHouse 连接和 state 表" && exit 1
```

Expected: STATE 非空。

```bash
# 分类计数验证
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_incr_scan+FINAL+WHERE+current_state%3D${STATE}+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行）：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 115
true    false   {POST_MUTATE_DIRS}       # 目录 = 41
```

```bash
# 交叉验证：base 表总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_s3_incr_scan+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `156`（{POST_MUTATE_DIRS}+{POST_MUTATE_FILES} = 41+115，S3 无 symlink）。

**若总行数不符，停止并调查。**

### 5d. ClickHouse incremental 表验证（变更记录明细）

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,is_dir,is_symlink,count(*)+FROM+default.incremental_s3_incr_scan+FINAL+GROUP+BY+operation_type,is_dir,is_symlink+ORDER+BY+operation_type,is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（5 行，S3 无 symlink 和 rename）：

```
changed false   false   2
deleted false   false   9
deleted true    false   2
new     false   false   7
new     true    false   3
```

**若任意行不符，停止并调查。不执行后续清理。**

---

## Step 6: 清理环境

Only proceed after all Step 5 checks pass.

**6a、6b、6c、6d 可并发执行**。

### 6a. 清理 S3 数据

```bash
mc rm --recursive --force ts3/{S3_BUCKET}/test-data/ 2>/dev/null || true
```

验证：

```bash
mc find ts3/{S3_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 6b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 6c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_incr_scan*"
```

Expected: 无输出（空）。

### 6d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## NFS vs S3 增量扫描对比

| 方面 | NFS v3 | S3 |
|------|--------|-----|
| 检测策略 | `JoinStrategy::Fh3`（file_handle） | `JoinStrategy::Path`（relative_path） |
| Rename 检测 | 通过 fh3 精确识别 → Renamed | copy+delete → New + Deleted |
| Symlink | 支持 | 不支持（is_symlink=false） |
| 目录 | 真实目录（有 inode） | 隐式前缀（common_prefixes） |
| 变更检测 | size/mtime/fh3 | size/mtime/path |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled (Step 1)
- [ ] S3 baseline data: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks=0 (Step 2)
- [ ] Full scan counts match baseline (Step 3a)
- [ ] ClickHouse base table verified at baseline (Step 3b)
- [ ] Mutations applied, S3 file count verified: {POST_MUTATE_FILES} (Step 4)
- [ ] Incremental scan Scanned Statistics: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks=0 (Step 5a)
- [ ] Incremental Statistics: new=10/changed=2/renamed=0/deleted=11 (Step 5b)
- [ ] ClickHouse base table post-incremental: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES} (Step 5c)
- [ ] ClickHouse incremental table: 5 rows verified (Step 5d)
- [ ] Environment cleaned (Step 6)
