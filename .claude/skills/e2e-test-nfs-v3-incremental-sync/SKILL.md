---
name: e2e-test-nfs-v3-incremental-sync
description: >
  This skill should be used when the user asks to "run nfs v3 incremental sync test",
  "test incremental sync nfs v3", "nfs v3 增量拷贝测试", "nfs v3 incremental copy e2e",
  "test the incremental sync pipeline against NFSv3",
  or mentions running the full-sync → mutate → incremental-sync → verify workflow
  against the NFSv3 test environment ({SOURCE_IP} → {DEST_IP}).
---

# NFS v3 Incremental Sync Test Skill

## Overview

端到端增量拷贝测试（NFS v3 存储）。
验证完整管线：全量 sync 建基线 → 变更源端数据 → 增量 sync 检测并同步变更 → 目标端验证 → integrity-check → 清理。
`datasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv3。
测试数据通过 SSH 在远端创建、变更和验证。

**增量 sync 机制**：当 `jobs/replicate_{SYNC_JOB_ID}/` 目录已存在时自动进入增量模式。
NFS v3 使用 `JoinStrategy::Fh3`，通过 file_handle（文件句柄哈希）精确检测 rename。

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | 192.168.50.173 |
| SOURCE_NFS_EXPORT | `/export/nfs` |
| DEST_IP | `192.168.50.23` |
| DEST_NFS_EXPORT | `/export/nfs` |
| SOURCE_URL | `nfs://{SOURCE_IP}{SOURCE_NFS_EXPORT}` |
| DEST_URL | `nfs://{DEST_IP}{DEST_NFS_EXPORT}` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/datasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| SYNC_JOB_ID | `nfs-v3-incr-sync` |
| DST_SCAN_JOB_ID | `nfs-v3-incr-sync-dst` |
| IC_JOB_ID | `nfs-v3-incr-sync-ic` |
| BASELINE_DIRS | 113 |
| BASELINE_FILES | 335 |
| BASELINE_SYMLINKS | 79 |
| POST_MUTATE_DIRS | 114 |
| POST_MUTATE_FILES | 333 |
| POST_MUTATE_SYMLINKS | 79 |

ClickHouse 表名：
- `base_nfs_v3_incr_sync`（sync 源端扫描）
- `state_nfs_v3_incr_sync`
- `base_nfs_v3_incr_sync_dst`（目标端扫描）
- `state_nfs_v3_incr_sync_dst`

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo rm -rf {SOURCE_NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理目标端 NFS 数据（SSH）

```bash
ssh root@{DEST_IP} 'sudo rm -rf {DEST_NFS_EXPORT}/test-data && echo "dest cleaned"'
```

Expected: `dest cleaned`。

### 0c. 清理 ClickHouse 表

**注意：共 4 个表需要清理（包括 scan 可能生成的 verify 表）。**

```bash
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync_verify_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync_verify_dst"
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_incr_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_sync*"
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

复用 e2e-test-nfs-v3 的 setup-test-data.sh：

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
Counter: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}
find:    dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}
OK: 数量校验通过
```

**Stop if the script exits non-zero.**

---

## Step 3: Phase 1 — 全量 Sync（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for:
- 进度信息（progress / copied files）
- 错误行（`ERROR`、`WARN`）
- 最终完成消息

**Verify: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}, ERROR STATISTICS 为 0。**

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

## Step 3b. ClickHouse 验证（全量 sync 后）

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_incr_sync+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {BASELINE_FILES}      # 普通文件 = 117
true    false   {BASELINE_DIRS}       # 目录 = 40
false   true    {BASELINE_SYMLINKS}   # 软链接 = 36
```

### 3c. 目标端 find 计数验证

**注意：必须使用 `sudo`，因为测试数据包含权限受限的目录（mode 0700/0500 等），普通用户无法访问。**

```bash
ssh root@{DEST_IP} 'sudo find {DEST_NFS_EXPORT}/test-data -type d | wc -l; sudo find {DEST_NFS_EXPORT}/test-data -type f | wc -l; sudo find {DEST_NFS_EXPORT}/test-data -type l | wc -l'
```

Expected: `dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}`。

**若不使用 sudo，会报 `Permission denied` 导致计数偏少。**

**Do not proceed until full sync succeeds with all counts matching.**

---

## Step 4: 变更源端数据

### 4a. 上传变更脚本

```bash
scp .claude/skills/e2e-test-nfs-v3-incremental-scan/scripts/mutate-test-data.sh root@{SOURCE_IP}:/tmp/mutate-test-data.sh
```

### 4b. 执行变更脚本

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/mutate-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}
OK: 变更后数量校验通过
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

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}。

### 5b. 验证 Incremental Statistics

Expected（NFS v3 Fh3 模式，精确 rename 检测）：

```
   ├─ New:          7 total | dirs      2 | files      3 | symlinks    2
   ├─ Changed:      2 total | dirs      0 | files      2 | symlinks    0
   ├─ Renamed:      7 total | dirs      1 | files      4 | symlinks    2
   └─ Deleted:      8 total | dirs      1 | files      5 | symlinks    2
```

**Verify ERROR STATISTICS 为 0。**

**If incremental sync fails，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS3ERR_STALE（源端）**: 变更脚本 rename/delete 导致旧文件句柄失效。增量 sync 从 base 表读取的 fh3 可能已过期。检查 moka 缓存是否命中过期句柄，清理缓存后重试。
   - **NFS3ERR_STALE（目标端）**: 目标端在全量 sync 后缓存的目录句柄可能因 NFS 服务端重启而失效。清理目标端 test-data 后从 Step 3 重新开始。
   - **NFS3ERR_NOENT**: 源端文件在扫描和同步间隙被删除，增量同步尝试读取已不存在的文件。检查变更脚本是否在 sync 运行期间执行了。
   - **NFS3ERR_NOSPC**: 目标端 NFS export 空间不足。清理目标端空间后重试。
   - **Incremental Statistics 不匹配**: 检查变更脚本是否执行完整（所有 rename/delete/add 操作都成功）。重新从 Step 0 开始。

**Do not proceed to Step 6 until the incremental sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 6: 验证目标端数据

### 6a. find 直接计数（DEST_IP 上执行）

**注意：必须使用 `sudo`，因为测试数据包含权限受限的目录（mode 0700/0500 等），普通用户无法访问。**

```bash
ssh root@{DEST_IP} 'sudo find {DEST_NFS_EXPORT}/test-data -type d | wc -l; sudo find {DEST_NFS_EXPORT}/test-data -type f | wc -l; sudo find {DEST_NFS_EXPORT}/test-data -type l | wc -l'
```

Expected: `dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}`。

**若不使用 sudo，会报 `Permission denied` 导致计数偏少。**

### 6b. scan 验证目标端计数（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}.**
If any count mismatches, stop. Do not proceed to cleanup.

### 6c. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_incr_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 115
true    false   {POST_MUTATE_DIRS}       # 目录 = 41
false   true    {POST_MUTATE_SYMLINKS}   # 软链接 = 36
```

---

## Step 7: Integrity Check（增量后一致性校验）

### 7a. Quick Integrity Check（本地执行）

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

**Verify: 退出码为 0，无不一致报告。若有不一致，停止并调查，不执行后续清理。**

### 7b. Full Integrity Check（本地执行）

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

### 8a. 清理源端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 8b. 清理目标端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 8c. 清理 ClickHouse 表

**注意：以下命令会创建额外的表，必须全部清理：**

| 命令 | 创建的表 |
|------|----------|
| `sync` | `base_nfs_v3_incr_sync`, `state_nfs_v3_incr_sync` |
| `sync` (incremental) | `base_nfs_v3_incr_sync_dst`, `state_nfs_v3_incr_sync_dst` |
| `scan --id nfs-v3-incr-sync-verify-src` | `base_nfs_v3_incr_sync_verify_src`, `state_nfs_v3_incr_sync_verify_src` |
| `scan --id nfs-v3-incr-sync-verify-dst` | `base_nfs_v3_incr_sync_verify_dst`, `state_nfs_v3_incr_sync_verify_dst` |

**共 8 个表需要清理。**

```bash
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v3_incr_sync_verify_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v3_incr_sync_verify_dst"
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_incr_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 8d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_sync*"
```

Expected: 无输出（空）。

### 8e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source NFS, dest NFS, ClickHouse, jobs, logs)
- [ ] Test data created with exact counts dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks={BASELINE_SYMLINKS} (Step 2)
- [ ] Full sync completed without errors, counts match (Step 3)
- [ ] ClickHouse base table verified after full sync (Step 3b)
- [ ] Dest find counts match after full sync (Step 3c)
- [ ] Source mutations applied: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks={POST_MUTATE_SYMLINKS} (Step 4)
- [ ] Incremental sync: new=7/changed=2/renamed=7/deleted=8 (Step 5b)
- [ ] Dest find counts match: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks={POST_MUTATE_SYMLINKS} (Step 6a)
- [ ] Dest scan counts match (Step 6b)
- [ ] ClickHouse dest base table verified (Step 6c)
- [ ] Quick integrity-check passed with 0 inconsistencies (Step 7a)
- [ ] Full integrity-check passed with 0 inconsistencies (Step 7b)
- [ ] Source NFS cleaned and verified empty (Step 8a)
- [ ] Dest NFS cleaned and verified empty (Step 8b)
- [ ] ClickHouse tables cleaned (Step 8c)
- [ ] jobs dir cleaned (Step 8d)
- [ ] Logs cleaned (Step 8e)
