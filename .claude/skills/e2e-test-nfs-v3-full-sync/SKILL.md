---
name: e2e-test-nfs-v3-full-sync
description: >
  This skill should be used when the user asks to "run nfs v3 full sync test",
  "test full sync nfs v3", "nfs v3 全量拷贝测试", "nfs v3 full copy e2e",
  "test the full nfs v3 sync pipeline",
  or mentions running the full scan/sync/verify/cleanup workflow
  against the NFSv3 test environment ({SOURCE_IP} → {DEST_IP}).
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# NFS v3 Full Sync Test Skill

## Overview

端到端全量拷贝测试（NFS v3 存储）。
验证完整管线：测试数据创建 → 源端扫描 → 全量同步 → 目标端验证 → integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv3。
测试数据通过 SSH 在远端创建和验证。

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`NfsV3`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `NFS_V3_SOURCE_IP` |
| DEST_IP | `NFS_V3_DEST_IP` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`NfsV3`）
| Name | Value |
|------|-------|
| NFS_EXPORT | `/export/nfs` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}` |
| DEST_URL | `nfs://{DEST_IP}{NFS_EXPORT}` |
| EXPECTED_DIRS | 113 |
| EXPECTED_FILES | 335 |
| EXPECTED_SYMLINKS | 79 |
| EXPECTED_TOTAL | 527 |

### Skill 常量
| Name | Value |
|------|-------|
| JOB_ID | `nfs-v3-full-sync` |

ClickHouse 表名：
- `base_nfs_v3_full_sync`（同步主表）
- `state_nfs_v3_full_sync`
- `tar_manifest_nfs_v3_full_sync`（sync 写入的 tar 清单）

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo find {NFS_EXPORT} -mindepth 1 -maxdepth 1 -exec rm -rf {} + && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理目标端 NFS 数据（SSH）

```bash
ssh root@{DEST_IP} 'sudo find {NFS_EXPORT} -mindepth 1 -maxdepth 1 -exec rm -rf {} + && echo "dest cleaned"'
```

Expected: `dest cleaned`。

### 0c. 清理 ClickHouse 表

**注意：共 7 个表需要清理（包括 scan 可能生成的 verify 表）。**

```bash
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_full_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_full_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.tar_manifest_nfs_v3_full_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_full_sync_verify_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_full_sync_verify_dst"
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_sync*"
```

Expected: 无输出（空）。

### 0e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 上传测试脚本（SOURCE_IP）

```bash
scp .claude/skills/e2e-test-nfs-v3/scripts/setup-test-data.sh root@{SOURCE_IP}:/tmp/setup-test-data.sh
```

Expected: 无错误输出，scp 退出码为 0。

---

## Step 2: 执行测试脚本创建数据（SOURCE_IP）

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-test-data.sh'
```

Expected output (last lines):

```
Counter: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
find:    dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
total entries: {EXPECTED_TOTAL}
OK: 数量校验通过
```

**Stop if the script exits non-zero.**

---

## Step 3: 同步源端 → 目标端（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for:
- 进度信息（progress / copied files）
- 错误行（`ERROR`、`WARN`）
- 最终完成消息

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}, ERROR STATISTICS 为 0。**

**If sync fails (non-zero exit or ERROR STATISTICS > 0)，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS3ERR_STALE**: 文件句柄过期，目标端可能残留旧数据导致缓存命中失效句柄。先清理目标端再重试。
   - **NFS3ERR_EXIST**: 目录已存在（目标端未清理干净），通常可忽略。
   - **Connection refused / timeout**: NFS 服务不可达，检查网络和 NFS 服务状态。
   - **Permission denied**: UID/GID 不匹配，检查 NFS export 权限配置。

3. 根据日志分析根因并修复，从头重试。

**Do not proceed to Step 4 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 4: 验证目标端数据

### 4a. find 直接计数（DEST_IP 上执行）

**注意：必须使用 `sudo`，因为测试数据包含权限受限的目录（mode 0700/0500 等），普通用户无法访问。**

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(sudo find {NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(sudo find {NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(sudo find {NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}`。

**若不使用 sudo，会报 `Permission denied` 导致计数偏少。**

### 4b. integrity-check 一致性验证（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check "{SOURCE_URL}" "{DEST_URL}" --quick
```

Expected:

```
  Integrity Check Results:               Mode: Quick, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，无不一致报告。若有不一致，停止并记录详情，不执行后续清理。**

### 4c. ClickHouse sync 主表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_full_sync+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {EXPECTED_FILES}
true    false   {EXPECTED_DIRS}
false   true    {EXPECTED_SYMLINKS}
```

**Verify sync 主表计数与预期一致。**

### 4d. 元数据校验（mtime/uid/gid/mode 一致性验证）

上传并执行元数据校验脚本，对比 NFS 文件系统实际属性与 ClickHouse 数据库中的记录：

```bash
scp .claude/skills/e2e-test-nfs-v3-full-sync/scripts/verify-metadata.sh root@{DEST_IP}:/tmp/verify-metadata.sh
ssh root@{DEST_IP} 'sudo bash /tmp/verify-metadata.sh'
```

脚本功能：
1. 从 ClickHouse 导出所有条目的 metadata（relative_path, uid, gid, mode, mtime）
2. 对 NFS 文件系统执行 `stat` 获取实际属性
3. 逐条对比，报告不一致的条目

Expected output:
```
=== Metadata Verification ===
Total entries: {EXPECTED_TOTAL}
Matched: {EXPECTED_TOTAL}
Mismatch: 0

✓ All metadata verified successfully
```

**若发现不一致，停止并调查。**

---

## Step 5: 并发清理（本地执行）

Only proceed after all Step 4 checks pass. **5a–5e 可并发执行**。

### 5a. 清理源端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 5b. 清理目标端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 5c. 清理 ClickHouse 表

**注意：以下命令会创建额外的表，必须全部清理：**

| 命令 | 创建的表 |
|------|----------|
| `sync` | `base_nfs_v3_full_sync`, `state_nfs_v3_full_sync`, `tar_manifest_nfs_v3_full_sync` |
| `scan --id nfs-v3-full-sync-verify-src` | `base_nfs_v3_full_sync_verify_src`, `state_nfs_v3_full_sync_verify_src` |
| `scan --id nfs-v3-full-sync-verify-dst` | `base_nfs_v3_full_sync_verify_dst`, `state_nfs_v3_full_sync_verify_dst` |

**共 7 个表需要清理。**

```bash
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_full_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_full_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.tar_manifest_nfs_v3_full_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_full_sync_verify_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_full_sync_verify_dst"
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 5d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_sync*"
```

Expected: 无输出（空）。

### 5e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source NFS, dest NFS, ClickHouse, jobs, logs)
- [ ] Test data created with exact counts dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 2)
- [ ] Sync completed without errors, counts match (Step 3)
- [ ] find counts on dest match (Step 4a)
- [ ] integrity-check passed with 0 inconsistencies (Step 4b)
- [ ] ClickHouse base_nfs_v3_full_sync counts verified (Step 4c)
- [ ] Metadata verified: uid/gid/mode/mtime consistent with ClickHouse (Step 4d)
- [ ] Source NFS cleaned and verified empty (Step 5a)
- [ ] Dest NFS cleaned and verified empty (Step 5b)
- [ ] ClickHouse tables cleaned (Step 5c)
- [ ] jobs dir cleaned (Step 5d)
- [ ] Logs cleaned (Step 5e)
