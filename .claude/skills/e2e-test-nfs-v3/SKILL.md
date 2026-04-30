---
name: e2e-test-nfs-v3
description: >
  This skill should be used when the user asks to "run nfs v3 e2e test",
  "test terrasync nfs v3 sync", "端到端 nfs v3 测试", "nfs v3 同步测试", "nfs v3 e2e",
  "run the nfs v3 e2e workflow", "test the full nfs v3 sync pipeline",
  or mentions running the full scan/sync/verify/cleanup workflow
  against the NFSv3 test environment ({SOURCE_IP} → {DEST_IP}).
---

# NFS v3 E2E Test Skill

## Overview

End-to-end integration test workflow for terrasync against NFSv3 storage.
Validates the full pipeline: test data → scan → sync → integrity-check → verify counts → cleanup.
`terrasync` runs **locally** (using `{CONFIG}`) and accesses NFSv3 over the network directly.
Test data is created and verified on the remote hosts via SSH.

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`NfsV3`）；环境变量从 `harness-run/.env` 加载。
> **注意**：此综合 skill 使用 40 dirs 小数据集，与独立 skill（full-scan 等，113 dirs）不同。

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

### Skill 常量（此 skill 专用小数据集）
| Name | Value |
|------|-------|
| EXPECTED_DIRS | `40` |
| EXPECTED_FILES | `117` |
| EXPECTED_SYMLINKS | `36` |

---

## Step 0: 清理测试环境（确保干净初始状态）

在开始测试前，清理上次运行可能残留的数据。**0a–0e 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

由于 binary 尚未编译，通过 SSH 直接清理源端：

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

删除所有 job_id 含 `nfs_v3_e2e` 的 ClickHouse 表：

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_e2e%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证已清除：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_e2e%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_e2e*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_e2e*"
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

Use the Bash tool to build the binary (in the project root directory):

```bash
cargo build
```

Expected: 编译成功，生成 `{BINARY}`，无错误输出。

---

## Step 2a: 上传测试脚本（SOURCE_IP）

Use the Bash tool to upload the setup script:

```bash
scp .claude/skills/nfs-v3-e2e/scripts/setup-test-data.sh root@{SOURCE_IP}:/tmp/setup-test-data.sh
```

Expected: 无错误输出，scp 退出码为 0。

---

## Step 2b: 执行测试脚本创建数据（SOURCE_IP）

Use the Bash tool:

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-test-data.sh'
```

Expected output (last lines):

```
Expected: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
Created:  dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
find:    dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
OK: 数量校验通过
```

**Stop if the script exits non-zero. Confirmed counts: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}.**

---

## Step 3: 扫描源端 NFS（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v3-e2e-src "nfs://{SOURCE_IP}{NFS_EXPORT}"
```

Look for summary lines in the output containing dirs / files / symlinks counts.

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}.**
If counts do not match, stop and investigate before proceeding.

### 3b. ClickHouse 验证（扫描后查库）

扫描完成后，job_id `nfs-v3-e2e-src` 会写入 ClickHouse `default` 库，表名 `base_nfs_v3_e2e_src`（`-` 自动转 `_`）。

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_e2e_src+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行，顺序不定）：

```
false   false   {EXPECTED_FILES}      # 普通文件
true    false   {EXPECTED_DIRS}       # 目录
false   true    {EXPECTED_SYMLINKS}   # 软链接
```

**若任意计数不符，停止并调查。**

---

## Step 4: 同步源端 → 目标端（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id nfs-v3-e2e-sync "nfs://{SOURCE_IP}{NFS_EXPORT}" "nfs://{DEST_IP}{NFS_EXPORT}" -l trace
```

Monitor output for:
- 进度信息（progress / copied files）
- 错误行（`ERROR`、`WARN`）
- 最终完成消息

**If sync fails (non-zero exit or ERROR STATISTICS > 0),按以下步骤排查：**

1. 查看 trace 日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS3ERR_STALE**: 文件句柄过期，可能是目标端残留旧数据导致缓存命中失效句柄。先清理目标端再重试。
   - **NFS3ERR_EXIST**: 目录已存在（目标端未清理干净），通常可忽略，但若后续 Lookup 也失败则需清理重试。
   - **Connection refused / timeout**: NFS 服务不可达，检查网络和 NFS 服务状态。
   - **Permission denied**: UID/GID 不匹配，检查 NFS export 权限配置。

3. 根据日志和数据库信息分析出确定的根因并进行代码修复，然后从头开始进行单元测试。

**Do not proceed to Step 5 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 5: 验证目标端数据

### 5a. find 直接计数（DEST_IP 上执行）

Use the Bash tool:

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(find {NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(find {NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(find {NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}`。

### 5b. integrity-check 一致性验证（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id nfs-v3-e2e-ic "nfs://{SOURCE_IP}{NFS_EXPORT}" "nfs://{DEST_IP}{NFS_EXPORT}" --quick
```

Monitor for:
- 不一致条目（mismatched / inconsistent）
- 缺失文件（missing files）
- 最终汇总行

**Verify: 退出码为 0，无不一致报告。若有不一致，停止并记录详情，不执行后续 rm 清理。**

### 5c. scan 验证目标端计数（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v3-e2e-dest "nfs://{DEST_IP}{NFS_EXPORT}"
```

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}.**
If any count mismatches, stop. Do not proceed to cleanup.

---

## Step 6: 并发清理（本地执行）

Only proceed after all Step 5 checks pass.

**6a、6b、6c、6d 可并发执行**（互相独立，同时发起）。

### 6a. 清理源端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "nfs://{SOURCE_IP}{NFS_EXPORT}"
```

**Wait for `rm` to exit (exit code 0).** Then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v3-e2e-verify-src "nfs://{SOURCE_IP}{NFS_EXPORT}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6b. 清理目标端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "nfs://{DEST_IP}{NFS_EXPORT}"
```

**Wait for `rm` to exit (exit code 0).** Then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v3-e2e-verify-dest "nfs://{DEST_IP}{NFS_EXPORT}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6c. 清理 ClickHouse 表

删除所有 job_id 含 `nfs_v3_e2e` 的 ClickHouse 表：

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_e2e%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证已清除：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_e2e%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 6d. 清理 jobs 目录

删除 `jobs/` 下所有含 `nfs_v3_e2e` 的目录：

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_e2e*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_e2e*"
```

Expected: 无输出（空）。

### 6e. 清理日志文件

删除 `target/debug/logs/` 下的所有日志文件：

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source NFS, dest NFS, ClickHouse, jobs, logs)
- [ ] Binary compiled (Step 1)
- [ ] Test data created with exact counts dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 2, VM find)
- [ ] Source NFS scan counts match dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 3)
- [ ] ClickHouse base_nfs_v3_e2e_src counts verified (Step 3b)
- [ ] Sync completed without errors (Step 4)
- [ ] find counts on dest match dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 5a, VM find)
- [ ] integrity-check passed with 0 inconsistencies (Step 5b)
- [ ] Dest NFS scan counts match dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 5c)
- [ ] Source NFS cleaned and verified empty (Step 6a)
- [ ] Dest NFS cleaned and verified empty (Step 6b)
- [ ] ClickHouse tables cleaned (Step 6c)
- [ ] jobs dir cleaned (Step 6d)
- [ ] Logs cleaned (Step 6e)
