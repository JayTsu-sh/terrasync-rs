---
name: e2e-test-s3-versioned-full-scan
description: >
  This skill should be used when the user asks to "run s3 versioned scan test",
  "test versioned s3 full scan", "s3 多版本全量扫描测试",
  "test the scan pipeline against a versioned S3 bucket",
  or mentions running the full-scan workflow against a versioned S3 bucket.
---

# S3 Versioned Full Scan Test Skill

## Overview

端到端全量扫描测试（S3 多版本桶）。
验证完整管线：创建多版本测试数据（同一 key 多个 version + delete marker） → 全量扫描 → CLI 输出验证 → ClickHouse base 表验证 → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 S3 兼容存储。

**S3 多版本特点**：
- 每个对象可有多个 version，由 `version_id` 标识
- 删除产生 delete marker（`is_delete_marker=true`）
- `is_latest` 标识当前版本
- base 表 ORDER BY 包含 `version_id`，同一 key 的不同版本为不同行
- 扫描使用 `ListObjectVersions` API（非 `ListObjects`）

## Prerequisites

- `mc`（MinIO Client）已安装并可用
- 目标桶已开启版本控制（Versioning Enabled）

## Constants

| Name | Value |
|------|-------|
| S3_AK | `H80NKRVS5DYOVE43U2HS` |
| S3_SK | `FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk` |
| S3_HOST | `10.128.137.245:8184` |
| S3_BUCKET | `{S3_VERSIONED_BUCKET}` |
| S3_URL | `s3://{S3_AK}:{S3_SK}@{S3_BUCKET}.{S3_HOST}/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `10.128.133.213:8123` |
| JOB_ID | `s3-ver-full-scan` |
| SANITIZED_JOB_ID | `s3_ver_full_scan` |
| BASE_TABLE | `base_s3_ver_full_scan` |
| STATE_TABLE | `state_s3_ver_full_scan` |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 删除并重建多版本桶

多版本桶必须先删桶再建桶，否则残留的 delete marker 和旧版本无法彻底清除：

```bash
mc alias set ts3 http://{S3_HOST} {S3_AK} {S3_SK} --api S3v4
mc rb --force ts3/{S3_BUCKET} 2>/dev/null || true
echo "bucket removed"
mc mb ts3/{S3_BUCKET}
echo "bucket created"
mc version enable ts3/{S3_BUCKET}
echo "versioning enabled"
```

Expected:

```
bucket removed
bucket created
versioning enabled
```

验证版本控制已启用：

```bash
mc version info ts3/{S3_BUCKET}
```

Expected: 输出包含 `versioning is enabled`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ver_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ver_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_ver_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_ver_full_scan*"
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

## Step 2: 创建多版本测试数据

```bash
bash .claude/skills/s3-versioned-full-scan/scripts/setup-s3-versioned-test-data.sh
```

脚本应完成以下操作：
1. 创建基础 3x3x3 树（40 dirs + 117 files，含中文和特殊字符文件名）
2. 对部分文件上传第 2 版（修改内容，产生新 version_id）
3. 对部分文件执行 `mc rm`（产生 delete marker）
4. 验证并输出版本统计

Expected output (last lines):

```
Total objects (all versions): 14
Delete markers: 2
Latest versions (non-delete): 7
OK: Versioned test data created
```

**脚本需要记录具体的版本数和 delete marker 数，用于后续验证。**

**Stop if the script exits non-zero.**

### 2b. mc 验证多版本

```bash
mc ls --versions ts3/{S3_BUCKET}/test-data/ --recursive | wc -l
```

确认输出行数大于 117（因为有多版本和 delete marker）。

---

## Step 3: 全量扫描 + 全面验证

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{S3_URL}"
```

### 3a. 验证 CLI Scanned Statistics

验证扫描包含所有版本（不仅仅是 latest），总数应大于 117。

### 3b. ClickHouse base 表 — is_latest 和 is_delete_marker 分组

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_latest,is_delete_marker,count(*)+FROM+default.{BASE_TABLE}+FINAL+GROUP+BY+is_latest,is_delete_marker+ORDER+BY+is_latest,is_delete_marker+FORMAT+TabSeparated"
```

Expected（分组验证）：
- `true + false`：当前有效版本
- `true + true`：当前 delete marker
- `false + false`：历史旧版本

**验证各分组计数与 Step 2 脚本输出一致。**

### 3c. 验证 version_id 非空

所有多版本桶记录都应有 version_id：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+version_id%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`。

### 3d. 验证 version_count 字段

对于有多版本的 key，version_count 应 > 1：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(DISTINCT+relative_path)+as+keys_with_multi_version+FROM+default.{BASE_TABLE}+FINAL+WHERE+is_latest%3Dtrue+AND+version_count%3E1+FORMAT+TabSeparated"
```

Expected: 大于 0（有多版本文件存在）。

### 3e. 验证 delete marker 记录

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+is_delete_marker%3Dtrue+FORMAT+TabSeparated"
```

Expected: 与 Step 2 脚本输出的 delete marker 数一致。

**若任意验证不符，停止并调查。**

**If scan fails，按以下步骤排查：**

1. 查看日志：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **ListObjectVersions 不支持**: S3 兼容存储可能不完全支持 `ListObjectVersions` API。检查存储系统文档确认版本控制功能是否可用。
   - **version_id 解析失败**: 某些 S3 实现的 version_id 格式可能与预期不同。检查日志中 version_id 的实际值。
   - **delete marker 未被扫描到**: 扫描逻辑可能过滤了 delete marker。检查 S3 list-versions 的响应中是否包含 DeleteMarker 条目。
   - **桶版本控制未启用**: `mc version info` 确认 versioning 状态。如果桶未开启版本控制，所有对象只有一个版本。

---

## Step 4: 并发清理

**4a–4d 可并发执行**。

### 4a. 删除多版本桶（彻底清理）

```bash
mc rb --force ts3/{S3_BUCKET}
echo "versioned bucket removed"
```

Expected: `versioned bucket removed`。

### 4b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ver_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证表已清除。

### 4c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_ver_full_scan*" | xargs rm -rf
```

### 4d. 清理日志

```bash
rm -rf target/debug/logs/*
```

---

## Completion Criteria

- [ ] Versioned bucket deleted and recreated with versioning enabled (Step 0a)
- [ ] ClickHouse tables cleaned (Step 0b)
- [ ] Binary compiled (Step 1)
- [ ] Multi-version test data created with versions + delete markers (Step 2)
- [ ] Full scan captures all versions (not just latest) (Step 3a)
- [ ] ClickHouse is_latest/is_delete_marker distribution correct (Step 3b)
- [ ] All records have non-empty version_id (Step 3c)
- [ ] Multi-version keys have version_count > 1 (Step 3d)
- [ ] Delete marker count matches expected (Step 3e)
- [ ] Versioned bucket removed (Step 4a)
- [ ] ClickHouse tables cleaned (Step 4b)
- [ ] jobs dir cleaned (Step 4c)
- [ ] Logs cleaned (Step 4d)
