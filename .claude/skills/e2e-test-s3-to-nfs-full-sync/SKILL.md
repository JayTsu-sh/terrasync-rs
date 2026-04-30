---
name: e2e-test-s3-to-nfs-full-sync
description: >
  This skill should be used when the user asks to "run s3 to nfs sync test",
  "test cross-protocol sync s3 to nfs", "s3 到 nfs 全量拷贝测试",
  "test s3 to nfs migration", or mentions running cross-protocol sync from S3 to NFSv3.
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# S3 → NFS Cross-Protocol Full Sync Test Skill

## Overview

跨协议全量拷贝测试：S3 源端 → NFS v3 目标端。
验证完整管线：S3 测试数据上传 → 跨协议 sync → NFS 目标端扫描 → integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），同时访问 S3 和 NFS 两种存储。

**跨协议关键差异**：
- S3 无 symlink，NFS 目标端也不会产生 symlink → 纯文件+目录拷贝
- S3 无 uid/gid/mode 属性 → NFS 目标端文件使用 NFS export 默认 uid/gid
- S3 目录为虚拟目录 → NFS 目标端创建为实体目录

## Prerequisites

- `mc`（MinIO Client）已安装并可用

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`S3` + `NfsV3`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| S3_AK | `S3_ACCESS_KEY` |
| S3_SK | `S3_SECRET_KEY` |
| S3_HOST | `S3_HOST` |
| DEST_IP | `NFS_V3_DEST_IP` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`S3` + `NfsV3`）
| Name | Value |
|------|-------|
| S3_BUCKET_SRC | `test-bucket` |
| SOURCE_URL | `s3://{S3_AK}:{S3_SK}@{SRC_BUCKET}.{S3_HOST}/test-data` |
| NFS_EXPORT | `/export/nfs` |
| DEST_URL | `nfs://{DEST_IP}{NFS_EXPORT}` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `s3-to-nfs-sync` |
| DST_SCAN_JOB_ID | `s3-to-nfs-sync-dst` |

ClickHouse 表名：
- `base_s3_to_nfs_sync_src`（源端 S3 扫描）
- `base_s3_to_nfs_sync_dst`（目标端 NFS 扫描）

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理 S3 源端

```bash
mc alias set ts3 http://{S3_HOST} {S3_AK} {S3_SK} --api S3v4
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/ 2>/dev/null || true
echo "source S3 cleaned"
```

验证：`mc ls ts3/{SRC_BUCKET}/test-data/ 2>/dev/null | wc -l` → `0`。

### 0b. 清理 NFS 目标端（SSH）

```bash
ssh root@{DEST_IP} 'sudo find {DEST_NFS_EXPORT} -mindepth 1 -maxdepth 1 -exec rm -rf {} + && echo "dest NFS cleaned"'
```

Expected: `dest NFS cleaned`。

### 0c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_to_nfs_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_to_nfs_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*s3_to_nfs_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*s3_to_nfs_sync*"
```

Expected: 无输出（空）。

### 0e. 清理日志文件

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

## Step 2: 上传 S3 源端测试数据

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

## Step 3: 扫描源端 S3（验证基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {SRC_SCAN_JOB_ID} "{SOURCE_URL}"
```

**Verify counts: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0.**

---

## Step 4: 跨协议 Sync（S3 → NFS）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for:
- 进度信息
- 错误行
- 最终完成消息

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

**If sync fails，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **AccessDenied / NoSuchKey（S3 读取）**: AK/SK 无读权限或对象不存在。检查 S3 凭证和桶 ACL 配置。
   - **NFS3ERR_ACCES（NFS 写入）**: 目标 NFS export 不允许写入。检查 `/etc/exports` 中是否配置了 `rw` 且 `no_root_squash`（或匹配 uid/gid）。
   - **NFS3ERR_NOSPC**: 目标 NFS export 磁盘空间不足。清理空间后重试。
   - **NFS3ERR_EXIST**: 目标端目录已存在（上次测试残留）。清理目标端后重试。
   - **Connection refused / timeout（NFS 端）**: NFS 服务不可达。检查目标 IP 连通性和 NFS 服务（`rpcinfo -p {DEST_IP}`）。
   - **S3 目录对象转 NFS 目录失败**: S3 虚拟目录（`/` 结尾对象）在 NFS 端创建为实体目录，如果 NFS 端已有同名文件会冲突。确保目标端初始为空。

**Do not proceed to Step 5 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 5: 验证目标端数据

### 5a. find 直接计数（DEST_IP 上执行）

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(find {DEST_NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(find {DEST_NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(find {DEST_NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0`。

### 5b. scan 验证目标端计数

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0.**

### 5c. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_s3_to_nfs_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（NFS 目标端，无 symlink 因为 S3 源端无 symlink）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 40
```

### 5d. integrity-check 一致性验证

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

---

## Step 6: 并发清理

Only proceed after all Step 5 checks pass. **6a–6d 可并发执行**。

### 6a. 清理 S3 源端

```bash
mc rm --recursive --force ts3/{SRC_BUCKET}/test-data/
echo "source S3 cleaned"
```

验证：`mc ls ts3/{SRC_BUCKET}/test-data/ 2>/dev/null | wc -l` → `0`。

### 6b. 清理 NFS 目标端

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id s3-to-nfs-sync-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25s3_to_nfs_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

### 6d. 清理 jobs 和日志

```bash
find jobs -maxdepth 1 -type d -name "*s3_to_nfs_sync*" | xargs rm -rf
rm -rf target/debug/logs/*
```

---

## S3 → NFS 跨协议对比

| 方面 | S3 源端 | NFS 目标端 |
|------|--------|-----------|
| Entry 类型 | S3Entry | NASEntry |
| Symlink | 0（不支持） | 0（无源端 symlink 可拷） |
| 属性 | 无 uid/gid/mode | 使用 NFS export 默认值 |
| 目录 | 虚拟（`/` 结尾对象） | 实体目录 |
| 文件句柄 | 无 | file_handle（目标端产生） |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: S3 source, NFS dest, ClickHouse, jobs, logs)
- [ ] Binary compiled (Step 1)
- [ ] S3 source data: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 2)
- [ ] Source S3 scan verified (Step 3)
- [ ] Cross-protocol sync completed: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 4)
- [ ] Dest find counts match (Step 5a)
- [ ] Dest NFS scan: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 5b)
- [ ] ClickHouse dest base table verified (Step 5c)
- [ ] integrity-check: All Passed (Step 5d)
- [ ] S3 source cleaned (Step 6a)
- [ ] NFS dest cleaned and verified empty (Step 6b)
- [ ] ClickHouse tables cleaned (Step 6c)
- [ ] jobs dir and logs cleaned (Step 6d)
