---
name: e2e-test-cifs-full-sync
description: >
  This skill should be used when the user asks to "run cifs full sync test",
  "test full sync cifs", "cifs 全量拷贝测试", "cifs full copy e2e",
  "test the full cifs sync pipeline",
  or mentions running the full scan/sync/verify/cleanup workflow
  against the CIFS/SMB test environment (192.168.50.173 → 192.168.50.23).
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# CIFS Full Sync Test Skill

## Overview

端到端全量拷贝测试（CIFS/SMB 存储）。
验证完整管线：测试数据创建 → 源端扫描 → 全量同步 → 目标端验证 → integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 CIFS 共享。
测试数据通过 `smbclient` 在远端共享上创建和验证。

**CIFS 特点**：
- URL 格式 `smb://user:pass@host/share/path`，密码中 `@` 需编码为 `%40`，`:` 编码为 `%3A`
- 产出 NASEntry，有 `file_handle` 字段（可用 Fh3 策略检测 rename）
- **不支持 symlink**（is_symlink 始终为 false）
- 默认端口 445

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`Cifs`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `CIFS_SOURCE_HOST` |
| DEST_IP | `CIFS_DEST_HOST` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`Cifs`）
| Name | Value |
|------|-------|
| CIFS_SHARE | `testshare` |
| SOURCE_URL | `smb://{CIFS_USER}:{CIFS_PASSWORD}@{SOURCE_IP}/{CIFS_SOURCE_SHARE}/test-data` |
| DEST_URL | `smb://{CIFS_USER}:{CIFS_PASSWORD}@{DEST_IP}/{CIFS_DEST_SHARE}/test-data` |
| EXPECTED_DIRS | `39` |
| EXPECTED_FILES | `117` |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `cifs-full-sync` |
| DST_SCAN_JOB_ID | `cifs-full-sync-dst` |
|------|-------|
| SRC_CIFS_HOST | `192.168.50.173` |
| SRC_CIFS_USER | `terrasync` |
| SRC_CIFS_PASS | `terrasync123` |
| SRC_CIFS_SHARE | `testshare` |
| DST_CIFS_HOST | `192.168.50.23` |
| DST_CIFS_USER | `terrasync` |
| DST_CIFS_PASS | `terrasync123` |
| DST_CIFS_SHARE | `testshare` |
| SOURCE_URL | `smb://terrasync:terrasync123@192.168.50.173/testshare/test-data` |
| DEST_URL | `smb://terrasync:terrasync123@192.168.50.23/testshare/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| SRC_SCAN_JOB_ID | `cifs-full-sync-src` |
| SYNC_JOB_ID | `cifs-full-sync` |
| DST_SCAN_JOB_ID | `cifs-full-sync-dst` |
| IC_JOB_ID | `cifs-full-sync-ic` |
| EXPECTED_DIRS | 39 |
| EXPECTED_FILES | 117 |

ClickHouse 表名：
- `base_cifs_full_sync_src`（源端扫描）
- `state_cifs_full_sync_src`
- `base_cifs_full_sync_dst`（目标端扫描）
- `state_cifs_full_sync_dst`

**注意**：CIFS 无 symlink，所有 symlink 计数始终为 0。

---

## Step 0: 清理测试环境（确保干净初始状态）

在开始测试前，清理上次运行可能残留的数据。**0a–0e 可并发执行**。

### 0a. 清理源端 CIFS 数据

通过 `smbclient` 删除源端共享中的 test-data 目录：

```bash
smbclient "//192.168.50.173/testshare" -U "terrasync%terrasync123" -c "deltree test-data" 2>/dev/null || true
echo "source CIFS cleaned"
```

Expected: `source CIFS cleaned`。

验证：

```bash
smbclient "//192.168.50.173/testshare" -U "terrasync%terrasync123" -c "ls test-data/*" 2>&1 | grep -c "test-data" || echo "0"
```

Expected: `0`（目录不存在或为空）。

### 0b. 清理目标端 CIFS 数据

```bash
smbclient "//192.168.50.23/testshare" -U "terrasync%terrasync123" -c "deltree test-data" 2>/dev/null || true
echo "dest CIFS cleaned"
```

Expected: `dest CIFS cleaned`。

### 0c. 清理 ClickHouse 表

删除所有 job_id 含 `cifs_full_sync` 的 ClickHouse 表：

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证已清除：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_full_sync*"
```

Expected: 无输出（空）。

### 0e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 创建源端测试数据

Use the Bash tool to run the setup script：

```bash
bash .claude/skills/cifs-full-sync/scripts/setup-cifs-test-data.sh
```

脚本功能：通过 `smbclient` 在源端 CIFS 共享上创建 3x3x3 目录树（无 symlink）。

Expected output (last lines):

```
Expected: dirs=39, files=117, symlinks=0
CIFS files: 117
OK: CIFS file count verified (dirs will be verified by scan)
```

**Stop if the script exits non-zero.**

---

## Step 2: 扫描源端 CIFS（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {SRC_SCAN_JOB_ID} "{SOURCE_URL}"
```

Look for summary lines in the output containing dirs / files / symlinks counts.

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0.**
If counts do not match, stop and investigate before proceeding.

### 3b. ClickHouse 验证（扫描后查库）

扫描完成后，表名 `base_cifs_full_sync_src`。

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_full_sync_src+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，CIFS 无 symlink）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 39
```

**若任意计数不符，停止并调查。**

### 3c. 验证 file_handle 非空（CIFS 特有验证）

确认 CIFS 扫描结果中所有记录的 file_handle 均非空，保证后续增量扫描可使用 Fh3 策略：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_cifs_full_sync_src+FINAL+WHERE+file_handle%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`。

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

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

**If sync fails (non-zero exit or ERROR STATISTICS > 0)，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **STATUS_ACCESS_DENIED**: SMB 权限不足，检查共享权限和用户权限。
   - **STATUS_LOGON_FAILURE**: 用户名密码错误，检查 URL 编码（特殊字符需 percent-encode）。
   - **STATUS_OBJECT_NAME_NOT_FOUND**: 路径不存在，检查共享名和子路径。
   - **Connection refused / timeout**: SMB 服务不可达，检查端口 445 和防火墙。
   - **STATUS_SHARING_VIOLATION**: 文件被其他进程锁定，等待或终止占用进程。

3. 根据日志和数据库信息分析确定的根因并进行代码修复，然后从头开始重试。

**Do not proceed to Step 4 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 4: 验证目标端数据

### 5a. smbclient 直接计数（目标端）

通过 `smbclient` 列出目标端文件数量进行直接验证：

```bash
FILE_COUNT=$(smbclient "//192.168.50.23/testshare" -U "terrasync%terrasync123" -c "recurse ON; ls test-data/*" 2>/dev/null | grep -c "^\s")
echo "dest CIFS files: $FILE_COUNT"
```

Expected: 文件数量与源端一致（117 个普通文件 + 目录条目）。

### 5b. integrity-check 一致性验证（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}" --quick
```

Monitor for:
- 不一致条目（Mismatch / Missing）
- 最终汇总行

Expected:

```
  Integrity Check Results:               Mode: Quick, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，无不一致报告。若有不一致，停止并记录详情，不执行后续清理。**

### 5c. scan 验证目标端计数（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0.**
If any count mismatches, stop. Do not proceed to cleanup.

### 5d. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_full_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 39
```

**Verify 目标端 base 表计数与源端 base 表完全一致。**

---

## Step 5: 并发清理（本地执行）

Only proceed after all Step 4 checks pass.

**6a–6e 可并发执行**（互相独立，同时发起）。

### 6a. 清理源端 CIFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

**Wait for `rm` to exit (exit code 0).** Then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id cifs-full-sync-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6b. 清理目标端 CIFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

**Wait for `rm` to exit (exit code 0).** Then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id cifs-full-sync-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6c. 清理 ClickHouse 表

删除所有 job_id 含 `cifs_full_sync` 的 ClickHouse 表：

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证已清除：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 6d. 清理 jobs 目录

删除 `jobs/` 下所有含 `cifs_full_sync` 的目录：

```bash
find jobs -maxdepth 1 -type d -name "*cifs_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_full_sync*"
```

Expected: 无输出（空）。

### 6e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## CIFS vs NFS v3 对比

| 方面 | NFS v3 | CIFS |
|------|--------|------|
| URL 格式 | `nfs://ip/export` | `smb://user:pass@host/share` |
| 认证方式 | UID/GID（NFS export 配置） | 用户名+密码（SMB 认证） |
| Symlink | 支持（36） | 不支持（0） |
| file_handle | 有（inode 级别） | 有（SMB file ID） |
| 测试数据管理 | SSH + shell 命令 | smbclient 命令 |
| 常见错误 | NFS3ERR_STALE, Permission denied | STATUS_ACCESS_DENIED, STATUS_LOGON_FAILURE |
| 端口 | 2049 | 445 |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source CIFS, dest CIFS, ClickHouse, jobs, logs)
- [ ] Test data created with exact counts dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 1)
- [ ] Source CIFS scan counts match dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 2)
- [ ] ClickHouse base_cifs_full_sync_src counts verified (Step 2b)
- [ ] file_handle non-empty for all records (Step 2c)
- [ ] Sync completed without errors, counts match (Step 3)
- [ ] smbclient file count on dest verified (Step 4a)
- [ ] integrity-check passed with 0 inconsistencies (Step 4b)
- [ ] Dest CIFS scan counts match dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 4c)
- [ ] ClickHouse base_cifs_full_sync_dst counts verified (Step 4d)
- [ ] Source CIFS cleaned and verified empty (Step 5a)
- [ ] Dest CIFS cleaned and verified empty (Step 5b)
- [ ] ClickHouse tables cleaned (Step 5c)
- [ ] jobs dir cleaned (Step 5d)
- [ ] Logs cleaned (Step 5e)
