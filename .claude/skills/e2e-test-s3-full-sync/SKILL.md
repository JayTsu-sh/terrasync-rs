---
name: e2e-test-s3-full-sync
description: >
  This skill should be used when the user asks to "run s3 full sync test",
  "test full sync s3", "s3 全量拷贝测试", "s3 full copy e2e",
  "test the full sync pipeline against S3",
  or mentions running the source-scan → full-copy → dest-scan → integrity-check workflow for S3.
---

# S3 Full Sync Test Skill

## Overview

端到端全量拷贝测试（S3 存储）。
验证完整管线：测试数据上传 → 全量同步 → 目标端扫描 → integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 S3 兼容存储。
测试数据通过 `mc`（MinIO Client）上传和验证。

**S3 特点**：
- URL 格式 `s3://ak:sk@bucket.host:port/prefix`
- 产出 S3Entry（无 file_handle，使用 Path 策略）
- **不支持 symlink**（symlink 计数始终为 0）
- 目录为虚拟概念（`/` 结尾的空对象）

## Prerequisites

- `mc`（MinIO Client）已安装并可用

## Constants

| Name | Value |
|------|-------|
| S3_AK | `H80NKRVS5DYOVE43U2HS` |
| S3_SK | `FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk` |
| S3_HOST | `10.128.137.245:8184` |
| SRC_BUCKET | `mbucket-src` |
| DST_BUCKET | `{DST_S3_BUCKET}` |
| SOURCE_URL | `s3://{S3_AK}:{S3_SK}@{SRC_BUCKET}.{S3_HOST}/test-data` |
| DEST_URL | `s3://{S3_AK}:{S3_SK}@{DST_BUCKET}.{S3_HOST}/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `10.128.133.213:8123` |
| SRC_SCAN_JOB_ID | `s3-full-sync-src` |
| SYNC_JOB_ID | `s3-full-sync` |
| DST_SCAN_JOB_ID | `s3-full-sync-dst` |
| IC_JOB_ID | `s3-full-sync-ic` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

ClickHouse 表名：
- `base_s3_full_sync_src`（源端扫描）
- `state_s3_full_sync_src`
- `base_s3_full_sync_dst`（目标端扫描）
- `state_s3_full_sync_dst`

**注意**：S3 无 symlink，所有 symlink 计数始终为 0。

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

验证：

```bash
mc ls ts3/{SRC_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 0c. 清理目标端 S3

```bash
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/ 2>/dev/null || true
echo "dest S3 cleaned"
```

验证：

```bash
mc ls ts3/{DST_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 0d. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0e. 清理 jobs 目录和日志

```bash
find jobs -maxdepth 1 -type d -name "*s3_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_full_sync*"
```

Expected: 无输出（空）。

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

## Step 2: 上传源端测试数据

Use the Bash tool locally:

```bash
bash .claude/skills/s3-incremental-scan/scripts/setup-s3-test-data.sh
```

脚本功能：通过 `mc` 在源桶创建 3x3x3 目录树（40 dirs + 117 files，无 symlink）。

Expected output (last lines):

```
S3 files: 117
OK: S3 file count verified
```

**Stop if the script exits non-zero.**

### 2b. mc 验证源端数据

```bash
mc find ts3/{SRC_BUCKET}/test-data/ --type f | wc -l
```

Expected: `117`。

---

## Step 3: 扫描源端 S3（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {SRC_SCAN_JOB_ID} "{SOURCE_URL}"
```

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0.**
If counts do not match, stop and investigate.

### 3b. ClickHouse 验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_full_sync_src+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，S3 无 symlink）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 40
```

**若任意计数不符，停止并调查。**

---

## Step 4: 同步源端 → 目标端（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for:
- 进度信息（progress / copied files）
- 错误行（`ERROR`、`WARN`）
- 最终完成消息

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

**If sync fails (non-zero exit or ERROR STATISTICS > 0)，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **AccessDenied / InvalidAccessKeyId**: AK/SK 无效或无目标桶写权限。检查 S3 凭证和桶 ACL/Policy 配置。
   - **NoSuchBucket**: 源桶或目标桶不存在。确认桶名正确，必要时通过 `mc mb ts3/{DST_BUCKET}` 创建。
   - **SignatureDoesNotMatch**: SK 错误或 URL 中特殊字符未正确编码。检查 SK 是否正确拷贝。
   - **SlowDown / 503 Service Unavailable**: S3 端限流或过载。降低并发度（调整 config.toml 中的 worker 数量）后重试。
   - **Connection refused / timeout**: S3 服务不可达。检查 `{S3_HOST}` 的网络连通性和端口 8184 是否开放。
   - **RequestTimeTooSkewed**: 本机时钟与 S3 服务端时间偏差过大（>15min）。同步本机 NTP 时间。

3. 根据日志分析根因并修复，从头重试。

**Do not proceed to Step 5 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 5: 验证目标端数据

### 5a. mc 直接计数（目标桶）

```bash
mc find ts3/{DST_BUCKET}/test-data/ --type f | wc -l
```

Expected: `117`。

```bash
mc find ts3/{DST_BUCKET}/test-data/ --type d | wc -l
```

Expected: 目录数与源端一致。

### 5b. integrity-check 一致性验证（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}" --quick
```

Expected:

```
  Integrity Check Results:               Mode: Quick, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，无不一致报告。若有不一致，停止并记录详情，不执行后续清理。**

### 5c. scan 验证目标端计数（本地执行）

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0.**
If any count mismatches, stop. Do not proceed to cleanup.

### 5d. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_full_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 40
```

**Verify 目标端 base 表计数与源端完全一致。**

---

## Step 6: 并发清理（本地执行）

Only proceed after all Step 5 checks pass. **6a–6d 可并发执行**。

### 6a. 清理源端 S3

```bash
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/
echo "source S3 cleaned"
```

验证：

```bash
mc ls ts3/{SRC_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 6b. 清理目标端 S3

```bash
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/
echo "dest S3 cleaned"
```

验证：

```bash
mc ls ts3/{DST_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 6c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 6d. 清理 jobs 目录和日志

```bash
find jobs -maxdepth 1 -type d -name "*s3_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_full_sync*"
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 均无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source S3, dest S3, ClickHouse, jobs, logs)
- [ ] Binary compiled (Step 1)
- [ ] Test data uploaded with exact counts dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 2)
- [ ] Source S3 scan counts match (Step 3)
- [ ] ClickHouse base_s3_full_sync_src counts verified (Step 3b)
- [ ] Sync completed without errors, counts match (Step 4)
- [ ] mc file count on dest verified (Step 5a)
- [ ] integrity-check passed with 0 inconsistencies (Step 5b)
- [ ] Dest S3 scan counts match (Step 5c)
- [ ] ClickHouse base_s3_full_sync_dst counts verified (Step 5d)
- [ ] Source S3 cleaned and verified empty (Step 6a)
- [ ] Dest S3 cleaned and verified empty (Step 6b)
- [ ] ClickHouse tables cleaned (Step 6c)
- [ ] jobs dir and logs cleaned (Step 6d)
