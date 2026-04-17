---
name: e2e-test-s3-full-scan
description: >
  This skill should be used when the user asks to "run s3 full scan test",
  "test full scan s3", "s3 全量扫描测试", "s3 full scan e2e",
  "test the full scan pipeline against S3",
  or mentions running the full-scan → verify workflow against the S3 test environment ({S3_HOST}).
---

# S3 Full Scan Test Skill

## Overview

端到端全量扫描测试（S3 存储）：上传测试数据 → 全量扫描 → 验证 CLI 输出和 ClickHouse base 表。

**与 NFS v3 的区别**：S3 无 `file_handle` 文件句柄，使用 `JoinStrategy::Path` 模式。S3 不支持 symlink。

`terrasync` 本地运行（使用 `{CONFIG}`），通过网络访问 S3。
测试数据通过 `mc`（MinIO Client）上传。

## Prerequisites

- `mc`（MinIO Client）已安装并可用

## Constants

| Name | Value |
|------|-------|
| S3_AK | `H80NKRVS5DYOVE43U2HS` |
| S3_SK | `FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk` |
| S3_BUCKET | `mbucket-src` |
| S3_HOST | `10.128.137.245:8184` |
| S3_URL | `s3://{S3_AK}:{S3_SK}@{S3_BUCKET}.{S3_HOST}/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| JOB_ID | `s3-full-scan` |
| SANITIZED_JOB_ID | `s3_full_scan` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

ClickHouse 表名：
- `base_s3_full_scan`
- `state_s3_full_scan`

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
mc find ts3/{S3_BUCKET}/test-data/ --type f 2>/dev/null | wc -l
```

Expected: `0`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_full_scan*"
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

## Step 3: 全量扫描 + 全面验证

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{S3_URL}"
```

### 3a. 验证 CLI Scanned Statistics

**Verify**: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0。

If counts do not match, stop and investigate.

### 3b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_full_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，S3 无 symlink）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 40
```

**若任意计数不符，停止并调查。**

### 3c. 验证 state 表 + base 表总行数（交叉验证）

```bash
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.state_s3_full_scan+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "scan_state: ${STATE}"
[[ -z "${STATE}" ]] && echo "ERROR: scan_state 为空，state 表写入失败" && exit 1
```

Expected: STATE 非空。

```bash
# 用 scan_state 交叉验证 base 表总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_s3_full_scan+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `157`（{EXPECTED_DIRS}+{EXPECTED_FILES} = 40+117，S3 无 symlink）。

**若总行数不符，停止并调查。**

### 3d. 独立 S3 对象核查（交叉验证 ClickHouse）

直接通过 `mc` 统计 S3 文件对象数量，应与 ClickHouse base 表中文件计数一致：

```bash
mc find ts3/{S3_BUCKET}/test-data/ --type f 2>/dev/null | wc -l
```

Expected: `117`（与 ClickHouse base 表 files={EXPECTED_FILES} 一致）。

**注**：S3 目录（common prefix）无法通过 `mc find` 独立计数，仅文件数参与此项交叉验证。

---

## Step 4: 清理环境

**4a–4d 可并发执行**。

### 4a. 清理 S3 数据

```bash
mc rm --recursive --force ts3/{S3_BUCKET}/test-data/ 2>/dev/null || true
```

验证：

```bash
mc find ts3/{S3_BUCKET}/test-data/ --type f 2>/dev/null | wc -l
```

Expected: `0`。

### 4b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 4c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_full_scan*"
```

Expected: 无输出（空）。

### 4d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled (Step 1)
- [ ] S3 test data uploaded: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 2)
- [ ] Full scan CLI counts match (Step 3a)
- [ ] ClickHouse base table verified (Step 3b)
- [ ] State table verified (Step 3c)
- [ ] Environment cleaned (Step 4)
