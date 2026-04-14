---
name: e2e-test-s3-versioned-incremental-scan
description: >
  This skill should be used when the user asks to "run s3 versioned incremental scan test",
  "test incremental scan versioned s3", "s3 多版本增量扫描测试",
  or mentions running the full-scan → mutate → incremental-scan workflow against a versioned S3 bucket.
---

# S3 Versioned Incremental Scan Test Skill

## Overview

端到端增量扫描测试（S3 多版本桶）。
验证完整管线：全量扫描建基线 → 执行多版本变更 → 增量扫描检测变更 → ClickHouse 表验证 → 清理。
`datasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 S3 兼容存储。

**多版本增量扫描的关键区别**：
- `version_id` 参与 base 表唯一性判断（Path 策略使用 `relative_path + version_id`）
- 新版本上传 → 新 version_id → 被检测为 **New**（非 Changed）
- delete marker → 被检测为 **New**（delete marker 是一种特殊版本）
- 旧版本本身不变 → 不被检测为 Changed
- 永久删除旧版本（指定 version_id 删除） → 被检测为 **Deleted**
- Renamed 始终为 0（S3 Path 模式）

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
| BINARY | `./target/debug/datasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| JOB_ID | `s3-ver-incr-scan` |
| SANITIZED_JOB_ID | `s3_ver_incr_scan` |
| BASE_TABLE | `base_s3_ver_incr_scan` |
| INCREMENTAL_TABLE | `incremental_s3_ver_incr_scan` |
| STATE_TABLE | `state_s3_ver_incr_scan` |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 删除并重建多版本桶

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

验证：`mc version info ts3/{S3_BUCKET}` 输出包含 `versioning is enabled`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ver_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ver_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_ver_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_ver_incr_scan*"
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

## Step 2: 创建基线多版本测试数据

```bash
bash .claude/skills/s3-versioned-full-scan/scripts/setup-s3-versioned-test-data.sh
```

脚本创建基础数据（含多版本和 delete marker）。

Expected: 脚本输出版本统计并验证通过。

### 2b. mc 验证

```bash
mc ls --versions ts3/{S3_BUCKET}/test-data/ --recursive | wc -l
```

记录基线总版本数。

---

## Step 3: 全量扫描（建立基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{S3_URL}"
```

### 3a. 验证 CLI Scanned Statistics

记录扫描到的总对象数（含所有版本和 delete marker）。

### 3b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_latest,is_delete_marker,count(*)+FROM+default.{BASE_TABLE}+FINAL+GROUP+BY+is_latest,is_delete_marker+ORDER+BY+is_latest,is_delete_marker+FORMAT+TabSeparated"
```

记录各分组计数，作为增量扫描的对照基线。

### 3c. 验证 version_id 非空

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+version_id%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`。

**Do not proceed until full scan succeeds and all counts verified.**

---

## Step 4: 执行多版本变更

```bash
bash .claude/skills/s3-versioned-incremental-scan/scripts/mutate-s3-versioned-test-data.sh
```

变更脚本应执行以下操作：

1. **上传新版本**：对已有 key 上传新内容（产生新 version_id，旧版本保留）
2. **删除 key**：`mc rm`（产生 delete marker）
3. **上传全新 key**：新 key + 新 version_id
4. **永久删除旧版本**：`mc rm --version-id=xxx`（指定版本永久删除）

Expected output (last lines):

```
New versions uploaded: 3
Delete markers created: 2
New keys created: 2
Old versions permanently deleted: 3
OK: Versioned mutations applied
```

**Stop if the script exits non-zero.**

---

## Step 5: 增量扫描 + 全面验证

同一 JOB_ID（`jobs/{JOB_ID}/` 已存在 → 自动增量模式）。

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{S3_URL}"
```

### 5a. 验证 CLI Scanned Statistics

验证扫描遍历的当前文件系统总数（含所有版本和 delete marker）。

### 5b. 验证 CLI Incremental Statistics

多版本增量扫描预期：
- **New**: 新上传的版本 + delete markers + 新 key（每个新 version_id 都算 New）
- **Changed**: 0（version_id 参与唯一性，新版本是 New 而非 Changed）
- **Renamed**: 0（S3 Path 模式无 rename 检测）
- **Deleted**: 被永久删除的旧版本

```
   ├─ New:          ... total | ...
   ├─ Changed:      0 total | dirs      0 | files      0 | symlinks    0
   ├─ Renamed:      0 total | dirs      0 | files      0 | symlinks    0
   └─ Deleted:      ... total | ...
```

**Verify: Changed=0, Renamed=0，New 和 Deleted 数量与变更脚本输出一致。**

### 5c. ClickHouse incremental 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,is_dir,is_symlink,count(*)+FROM+default.{INCREMENTAL_TABLE}+FINAL+GROUP+BY+operation_type,is_dir,is_symlink+ORDER+BY+operation_type,is_dir,is_symlink+FORMAT+TabSeparated"
```

验证各 operation_type 分组计数与预期一致。

### 5d. ClickHouse base 表验证（增量后）

查询增量后 base 表的 is_latest/is_delete_marker 分组：

```bash
# 查 scan_state
SCAN_STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.{STATE_TABLE}+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "current scan_state: $SCAN_STATE"
[[ -z "$SCAN_STATE" ]] && echo "ERROR: scan_state 为空，请检查 ClickHouse 连接和 state 表" && exit 1
```

Expected: SCAN_STATE 非空。

```bash
# 用 scan_state 过滤 base 表
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_latest,is_delete_marker,count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+current_state%3D${SCAN_STATE}+GROUP+BY+is_latest,is_delete_marker+ORDER+BY+is_latest,is_delete_marker+FORMAT+TabSeparated"
```

验证增量后的版本分布反映了变更操作。

**If incremental scan fails，按以下步骤排查：**

1. 查看日志：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **Changed > 0（应为 0）**: 多版本场景下新版本产生新 version_id，应被检测为 New。如果 Changed > 0 说明 version_id 未正确参与唯一性判断，可能是 JoinStrategy 配置问题。
   - **Renamed > 0（应为 0）**: S3 Path 模式不支持 rename 检测。如果出现非零值，检查 JoinStrategy 是否被错误配置为 Fh3。
   - **永久删除的版本未被检测为 Deleted**: 增量扫描比较 base 表（含 version_id）与当前 list-versions 结果，被永久删除的 version_id 应从当前结果中消失。检查 list-versions API 是否正确排除了永久删除的版本。
   - **delete marker 未被检测为 New**: delete marker 有自己的 version_id，全量扫描后创建的 delete marker 是新 version_id，应被检测为 New。检查扫描是否包含了 delete marker。

---

## Step 6: 并发清理

**6a–6d 可并发执行**。

### 6a. 删除多版本桶（彻底清理）

```bash
mc rb --force ts3/{S3_BUCKET}
echo "versioned bucket removed"
```

Expected: `versioned bucket removed`。

### 6b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_ver_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证表已清除。

### 6c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_ver_incr_scan*" | xargs rm -rf
```

### 6d. 清理日志

```bash
rm -rf target/debug/logs/*
```

---

## Completion Criteria

- [ ] Versioned bucket deleted and recreated with versioning enabled (Step 0a)
- [ ] ClickHouse tables cleaned (Step 0b)
- [ ] Binary compiled (Step 1)
- [ ] Versioned baseline created with versions + delete markers (Step 2)
- [ ] Full scan captures all versions (Step 3a)
- [ ] ClickHouse base table baseline recorded (Step 3b)
- [ ] All records have non-empty version_id (Step 3c)
- [ ] Versioned mutations applied (Step 4)
- [ ] Incremental scan Scanned Statistics verified (Step 5a)
- [ ] Incremental Statistics: Changed=0, Renamed=0, New/Deleted match mutations (Step 5b)
- [ ] ClickHouse incremental table verified (Step 5c)
- [ ] ClickHouse base table reflects post-mutation state (Step 5d)
- [ ] Versioned bucket removed (Step 6a)
- [ ] ClickHouse tables cleaned (Step 6b)
- [ ] jobs dir cleaned (Step 6c)
- [ ] Logs cleaned (Step 6d)
