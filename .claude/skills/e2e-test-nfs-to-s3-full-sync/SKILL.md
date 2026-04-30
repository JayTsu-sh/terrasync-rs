---
name: e2e-test-nfs-to-s3-full-sync
description: >
  This skill should be used when the user asks to "run nfs to s3 sync test",
  "test cross-protocol sync nfs to s3", "nfs 到 s3 全量拷贝测试",
  "test nfs to s3 migration", or mentions running cross-protocol sync from NFSv3 to S3.
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# NFS → S3 Cross-Protocol Full Sync Test Skill

## Overview

跨协议全量拷贝测试：NFS v3 源端 → S3 目标端。
验证完整管线：NFS 测试数据创建 → 跨协议 sync → S3 目标端扫描 → integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），同时访问 NFS 和 S3 两种存储。

**跨协议关键差异**：
- NFS 有 symlink（36），S3 不支持 symlink → sync 时 symlink 被**跳过**
- NFS 有 uid/gid/mode 属性，S3 无这些概念 → 属性不会被保留
- 目标端预期计数：dirs=40, files=117, symlinks=**0**（非 36）
- integrity-check 比较文件内容和 size，跳过属性差异

## Prerequisites

- `mc`（MinIO Client）已安装并可用

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`NfsV3` + `S3`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `NFS_V3_SOURCE_IP` |
| S3_AK | `S3_ACCESS_KEY` |
| S3_SK | `S3_SECRET_KEY` |
| S3_HOST | `S3_HOST` |
| DST_BUCKET | `S3_DEST_BUCKET` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`NfsV3` + `S3`）
| Name | Value |
|------|-------|
| NFS_EXPORT | `/export/nfs` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}` |
| S3_BUCKET_DST | `test-bucket` |
| DEST_URL | `s3://{S3_AK}:{S3_SK}@{DST_BUCKET}.{S3_HOST}/test-data` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `nfs-to-s3-sync` |
| DST_SCAN_JOB_ID | `nfs-to-s3-sync-dst` |

ClickHouse 表名：
- `base_nfs_to_s3_sync_src`（源端 NFS 扫描）
- `base_nfs_to_s3_sync_dst`（目标端 S3 扫描）

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo find {SOURCE_NFS_EXPORT} -mindepth 1 -maxdepth 1 -exec rm -rf {} + && echo "source NFS cleaned"'
```

Expected: `source NFS cleaned`。

### 0b. 清理目标端 S3

```bash
mc alias set ts3 http://{S3_HOST} {S3_AK} {S3_SK} --api S3v4
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/ 2>/dev/null || true
echo "dest S3 cleaned"
```

验证：

```bash
mc ls ts3/{DST_BUCKET}/test-data/ 2>/dev/null | wc -l
```

Expected: `0`。

### 0c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_to_s3_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_to_s3_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_to_s3_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_to_s3_sync*"
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

## Step 2: 创建 NFS 源端测试数据

### 2a. 上传测试脚本

```bash
scp .claude/skills/nfs-v3-e2e/scripts/setup-test-data.sh root@{SOURCE_IP}:/tmp/setup-test-data.sh
```

### 2b. 执行测试脚本

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-test-data.sh'
```

Expected output (last lines):

```
Expected: dirs={SRC_DIRS}, files={SRC_FILES}, symlinks={SRC_SYMLINKS}
Created:  dirs={SRC_DIRS}, files={SRC_FILES}, symlinks={SRC_SYMLINKS}
find:    dirs={SRC_DIRS}, files={SRC_FILES}, symlinks={SRC_SYMLINKS}
OK: 数量校验通过
```

**Stop if the script exits non-zero.**

---

## Step 3: 扫描源端 NFS（验证基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {SRC_SCAN_JOB_ID} "{SOURCE_URL}"
```

**Verify counts: dirs={SRC_DIRS}, files={SRC_FILES}, symlinks={SRC_SYMLINKS}.**

---

## Step 4: 跨协议 Sync（NFS → S3）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for:
- 进度信息
- **symlink 跳过日志**（WARN 级别，symlink 不可拷贝到 S3）
- 最终完成消息

**Verify: dirs={DST_DIRS}, files={DST_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

**注意**：sync 输出中 symlinks 可能显示 skip 或 warn，这是预期行为（S3 不支持 symlink）。只要普通文件和目录计数正确，symlink 跳过不算错误。

**If sync fails，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS3ERR_STALE（源端读取）**: 源端文件句柄过期。NFS 服务端可能重启过。清理源端数据后重建，从 Step 2 重新开始。
   - **AccessDenied（S3 写入）**: AK/SK 无目标桶写权限。检查 S3 桶 Policy 是否授权了 PutObject。
   - **NoSuchBucket**: 目标桶不存在。通过 `mc mb ts3/{DST_BUCKET}` 创建后重试。
   - **Content-Length mismatch**: NFS 读取文件时文件大小在读取过程中发生变化（源端并发修改）。确保测试期间无其他进程修改源端数据。
   - **Symlink 处理错误（非跳过）**: 如果 symlink 不是被跳过而是引发了 ERROR，检查 sync 代码中 symlink → S3 的处理逻辑，可能需要修复为 skip。

**Do not proceed to Step 5 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 5: 验证目标端数据

### 5a. mc 直接计数（目标桶）

```bash
mc find ts3/{DST_BUCKET}/test-data/ | wc -l
```

Expected: `117`（文件数，不含 symlink）。

### 5b. scan 验证目标端计数

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts: dirs={DST_DIRS}, files={DST_FILES}, symlinks=0。**

### 5c. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_to_s3_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，S3 目标端无 symlink）：

```
false   false   {DST_FILES}       # 普通文件 = 117
true    false   {DST_DIRS}        # 目录 = 40
```

### 5d. integrity-check 一致性验证

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}" --quick
```

**注意**：跨协议 integrity-check 只比较文件内容/size，不比较 NFS 特有属性（uid/gid/mode）。symlink 差异为已知差异，不应报告为 Missing（integrity-check 应只检查 regular files 和 dirs）。

Expected:

```
  Integrity Check Results:               Mode: Quick, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**若 integrity-check 报告 symlink 为 Missing，这是跨协议的已知行为。只关注文件和目录的一致性。**

---

## Step 6: 并发清理

Only proceed after all Step 5 checks pass. **6a–6d 可并发执行**。

### 6a. 清理源端 NFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-to-s3-sync-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6b. 清理目标端 S3

```bash
mc rm --recursive --force ts3/{DST_BUCKET}/test-data/
echo "dest S3 cleaned"
```

验证：`mc ls ts3/{DST_BUCKET}/test-data/ 2>/dev/null | wc -l` → `0`。

### 6c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_to_s3_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证表已清除。

### 6d. 清理 jobs 和日志

```bash
find jobs -maxdepth 1 -type d -name "*nfs_to_s3_sync*" | xargs rm -rf
rm -rf target/debug/logs/*
```

---

## NFS → S3 跨协议对比

| 方面 | NFS 源端 | S3 目标端 |
|------|---------|----------|
| Entry 类型 | NASEntry | S3Entry |
| Symlink | 36 | 0（不支持） |
| 属性（uid/gid/mode） | 有 | 无 |
| 目录 | 实体目录 | 虚拟目录（`/` 结尾） |
| 文件句柄 | file_handle | 无 |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: NFS source, S3 dest, ClickHouse, jobs, logs)
- [ ] Binary compiled (Step 1)
- [ ] NFS source data: dirs={SRC_DIRS}/files={SRC_FILES}/symlinks={SRC_SYMLINKS} (Step 2)
- [ ] Source NFS scan verified (Step 3)
- [ ] Cross-protocol sync completed: dirs={DST_DIRS}/files={DST_FILES}/symlinks=0 (Step 4)
- [ ] mc file count on dest: 117 (Step 5a)
- [ ] Dest S3 scan: dirs={DST_DIRS}/files={DST_FILES}/symlinks=0 (Step 5b)
- [ ] ClickHouse dest base table verified (Step 5c)
- [ ] integrity-check passed for files and dirs (Step 5d)
- [ ] Source NFS cleaned and verified empty (Step 6a)
- [ ] Dest S3 cleaned (Step 6b)
- [ ] ClickHouse tables cleaned (Step 6c)
- [ ] jobs dir and logs cleaned (Step 6d)
