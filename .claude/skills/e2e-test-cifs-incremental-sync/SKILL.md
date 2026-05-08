---
name: e2e-test-cifs-incremental-sync
description: >
  This skill should be used when the user asks to "run cifs incremental sync test",
  "test incremental sync cifs", "cifs 增量拷贝测试", "cifs incremental copy e2e",
  or mentions running the full-sync → mutate → incremental-sync → verify workflow for CIFS.
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# CIFS Incremental Sync Test Skill

## Overview

端到端增量拷贝测试（CIFS/SMB 存储）。
验证完整管线：全量 sync 建基线 → 变更源端 → 增量 sync 检测并同步变更 → 目标端扫描 → integrity-check。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 CIFS 共享。
测试数据通过 `smbclient` 在远端共享上创建、变更和验证。

**CIFS 增量特点**：
- CIFS 有 `file_handle` 字段（SMB file ID），使用 `JoinStrategy::Fh3`
- **精确 rename 检测**：rename 表现为 Renamed（不是 New+Deleted）
- **不支持 symlink**（所有 symlink 计数为 0）
- `jobs/replicate_{SYNC_JOB_ID}/` 已存在时自动进入增量模式

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
| SOURCE_URL | `smb://{CIFS_USER}:{CIFS_PASSWORD}@{SOURCE_IP}/{CIFS_SHARE}/test-data` |
| DEST_URL | `smb://{CIFS_USER}:{CIFS_PASSWORD}@{DEST_IP}/{CIFS_SHARE}/test-data` |
| BASELINE_DIRS | `39` |
| BASELINE_FILES | `117` |
| POST_DIRS | `40` |
| POST_FILES | `115` |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `cifs-incr-sync` |
| DST_SCAN_JOB_ID | `cifs-incr-sync-dst` |
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
| SYNC_JOB_ID | `cifs-incr-sync` |
| DST_SCAN_JOB_ID | `cifs-incr-sync-dst` |
| IC_JOB_ID | `cifs-incr-sync-ic` |
| BASELINE_DIRS | 39 |
| BASELINE_FILES | 117 |
| POST_MUTATE_DIRS | 40 |
| POST_MUTATE_FILES | 115 |

ClickHouse 表名：
- `base_cifs_incr_sync`（sync 源端扫描）
- `state_cifs_incr_sync`
- `base_cifs_incr_sync_dst`（目标端扫描）
- `state_cifs_incr_sync_dst`

**注意**：CIFS 无 symlink，所有 symlink 计数始终为 0。

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 CIFS 数据

```bash
smbclient "//192.168.50.173/testshare" -U "terrasync%terrasync123" -c "deltree test-data" 2>/dev/null || true
echo "source CIFS cleaned"
```

Expected: `source CIFS cleaned`。

### 0b. 清理目标端 CIFS 数据

```bash
smbclient "//192.168.50.23/testshare" -U "terrasync%terrasync123" -c "deltree test-data" 2>/dev/null || true
echo "dest CIFS cleaned"
```

Expected: `dest CIFS cleaned`。

### 0c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25cifs_incr_sync%25%27)+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25cifs_incr_sync%25%27)+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_incr_sync*"
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

```bash
bash .claude/skills/cifs-full-sync/scripts/setup-cifs-test-data.sh
```

脚本功能：通过 `smbclient` 在源端 CIFS 共享上创建 3x3x3 目录树（无 symlink，含中文和特殊字符文件名）。

Expected output (last lines):

```
Expected: dirs=39, files=117, symlinks=0
CIFS files: 117
OK: CIFS file count verified (dirs will be verified by scan)
```

**Stop if the script exits non-zero.**

---

## Step 2: Phase 1 — 全量 Sync（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

## Step 3: 验证全量 Sync 结果

### 3a. ClickHouse 验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_incr_sync+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，CIFS 无 symlink）：

```
false   false   {BASELINE_FILES}      # 普通文件 = 117
true    false   {BASELINE_DIRS}       # 目录 = 39
```

### 3b. 验证 file_handle 非空

确认所有记录的 file_handle 均非空（增量扫描 Fh3 策略依赖此字段）：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_cifs_incr_sync+FINAL+WHERE+file_handle%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`。

### 3c. 目标端 smbclient 验证

```bash
FILE_COUNT=$(smbclient "//192.168.50.23/testshare" -U "terrasync%terrasync123" -c "recurse ON; ls test-data/*" 2>/dev/null | grep -c "^\s")
echo "dest CIFS files: $FILE_COUNT"
```

**Do not proceed until full sync succeeds with all counts matching.**

---

## Step 4: 变更源端数据

```bash
bash .claude/skills/cifs-incremental-sync/scripts/mutate-cifs-test-data.sh
```

脚本功能：通过 `smbclient` 在源端 CIFS 共享上执行增删改 rename 操作（无 symlink 相关操作）。

变更内容（与 NFS 变更脚本类似，但排除 symlink）：
- **ADD**: 2 dirs + 3 files
- **MODIFY**: 2 files（追加/覆盖内容）
- **RENAME**: 1 dir（含级联文件）+ 1 file
- **DELETE**: 1 dir（含文件）+ 2 files

Expected output (last lines):

```
CIFS files after mutation: 115
OK: CIFS mutation verified
```

**Stop if the script exits non-zero.**

---

## Step 5: Phase 2 — 增量 Sync（本地执行）

同一 SYNC_JOB_ID（`jobs/replicate_{SYNC_JOB_ID}/` 已存在 → 自动增量模式）。

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

### 5a. 验证 Scanned Statistics

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks=0。

### 5b. 验证 Incremental Statistics

Expected（CIFS Fh3 模式，精确 rename 检测，无 symlink）：

```
   ├─ New:          5 total | dirs      2 | files      3 | symlinks    0
   ├─ Changed:      2 total | dirs      0 | files      2 | symlinks    0
   ├─ Renamed:      5 total | dirs      1 | files      4 | symlinks    0
   └─ Deleted:      6 total | dirs      1 | files      5 | symlinks    0
```

**Verify ERROR STATISTICS 为 0。**

**If incremental sync fails，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **STATUS_SHARING_VIOLATION**: 文件被 SMB 服务端其他会话锁定。增量 sync 读源端或写目标端时触发。等待锁释放或终止占用会话后重试。
   - **STATUS_OBJECT_NAME_COLLISION**: 目标端已存在同名文件/目录（增量 sync 尝试创建已存在的对象）。可能是上次部分完成的 sync 残留。清理目标端后从 Step 3 重新开始。
   - **STATUS_OBJECT_NAME_NOT_FOUND**: 变更脚本 rename/delete 的文件在增量 sync 期间被引用但目标端不存在。检查 base 表与实际文件系统状态是否一致。
   - **STATUS_ACCESS_DENIED**: 写入目标共享或修改已有文件时权限不足。检查 SMB 用户对目标共享的写权限（包括子目录的继承权限）。
   - **Connection reset during rename**: SMB 协议 rename 跨目录时某些服务端实现不稳定。检查 SMB 服务端版本，必要时改用同目录 rename 测试。
   - **Incremental Statistics 不匹配**: CIFS 使用 Fh3 策略，rename 应精确检测为 Renamed。如果 Renamed=0 且 New/Deleted 偏高，检查 file_handle 字段是否正确写入 base 表。

**Do not proceed to Step 6 until the incremental sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 6: 验证目标端数据

### 6a. smbclient 直接计数

```bash
FILE_COUNT=$(smbclient "//192.168.50.23/testshare" -U "terrasync%terrasync123" -c "recurse ON; ls test-data/*" 2>/dev/null | grep -c "^\s")
echo "dest CIFS files after incremental: $FILE_COUNT"
```

### 6b. scan 验证目标端计数（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks=0.**
If any count mismatches, stop. Do not proceed to cleanup.

### 6c. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_incr_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行）：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 115
true    false   {POST_MUTATE_DIRS}       # 目录 = 40
```

---

## Step 7: Integrity Check（增量后一致性校验）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，无不一致报告。若有 Missing 或 Mismatch，停止并记录详情，不执行后续清理。**

---

## Step 8: 并发清理（本地执行）

Only proceed after all Step 6 and Step 7 checks pass. **8a–8e 可并发执行**。

### 8a. 清理源端 CIFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id cifs-incr-sync-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 8b. 清理目标端 CIFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id cifs-incr-sync-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 8c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25cifs_incr_sync%25%27)+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+(name+LIKE+%27%25cifs_incr_sync%25%27)+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 8d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_incr_sync*"
```

Expected: 无输出（空）。

### 8e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## CIFS Incremental vs NFS Incremental 对比

| 方面 | NFS v3 增量 | CIFS 增量 |
|------|------------|----------|
| JoinStrategy | Fh3（inode 句柄） | Fh3（SMB file ID） |
| Rename 检测 | 精确（Renamed） | 精确（Renamed） |
| Symlink 变更 | 有（new/del/rename） | 无（CIFS 不支持 symlink） |
| 常见增量错误 | NFS3ERR_STALE | STATUS_SHARING_VIOLATION |
| 数据管理 | SSH + shell | smbclient |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source CIFS, dest CIFS, ClickHouse, jobs, logs)
- [ ] Binary compiled (Step 1)
- [ ] Source data created: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks=0 (Step 2)
- [ ] Full sync completed: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks=0 (Step 3)
- [ ] ClickHouse base table verified after full sync (Step 3b)
- [ ] file_handle non-empty for all records (Step 3c)
- [ ] Dest smbclient count verified after full sync (Step 3d)
- [ ] Source mutations applied: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks=0 (Step 4)
- [ ] Incremental sync: new=5/changed=2/renamed=5/deleted=7 (Step 5b)
- [ ] Dest smbclient count match after incremental (Step 6a)
- [ ] Dest scan counts match (Step 6b)
- [ ] ClickHouse dest base table verified (Step 6c)
- [ ] Integrity check: All Passed (Step 7)
- [ ] Source CIFS cleaned and verified empty (Step 8a)
- [ ] Dest CIFS cleaned and verified empty (Step 8b)
- [ ] ClickHouse tables cleaned (Step 8c)
- [ ] jobs dir cleaned (Step 8d)
- [ ] Logs cleaned (Step 8e)
