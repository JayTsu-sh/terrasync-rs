---
name: e2e-test-nfs-v4-incremental-sync
description: >
  This skill should be used when the user asks to "run nfs v4 incremental sync test",
  "test incremental sync nfs v4", "nfs v4 增量拷贝测试", "nfs v4.1 incremental sync",
  "test nfs v4.1 acl incremental sync", "nfs v4.1 增量同步",
  or mentions running the full-sync → mutate → incremental-sync workflow against NFSv4.1 servers.
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# NFS v4.1 Incremental Sync Test Skill

## Overview

端到端增量拷贝测试（NFS v4.1 存储）。
验证完整管线：全量 sync 建基线（含 ACL/xattr）→ 变更源端数据（含 ACL/xattr 变更）→ 增量 sync 检测并同步变更 → 目标端验证（文件数量 + ACL/xattr 传播）→ integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv4.1。

**NFSv4.1 增量 sync 关键特性**：
- URL 加 `?version=4.1` 强制使用 NFSv4.1
- `--enable-acl` 启用 ACL 和 xattr 复制（全量和增量 sync 都需要）
- 增量 sync 基于 `JoinStrategy::Fh3`，精确检测 rename（不会拆为 New+Deleted）
- ACL/xattr 变更会触发 mtime 更新 → 被检测为 Changed，随增量 sync 重新复制 ACL/xattr
- 目标端 ACL/xattr 验证需用 `nfs4_getfacl` 和 `getfattr` 手动核对

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`NfsV4`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `NFS_V4_SOURCE_IP` |
| DEST_IP | `NFS_V4_DEST_IP` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`NfsV4`）
| Name | Value |
|------|-------|
| NFS_EXPORT | `/export/nfsv4` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}?version=4.1` |
| DEST_URL | `nfs://{DEST_IP}{NFS_EXPORT}?version=4.1` |
| BASELINE_DIRS | 113 |
| BASELINE_FILES | 335 |
| BASELINE_SYMLINKS | 79 |
| POST_DIRS | 114 |
| POST_FILES | 333 |
| POST_SYMLINKS | 79 |

### Skill 常量
| Name | Value |
|------|-------|
| SYNC_JOB_ID | `nfs-v4-incr-sync` |
| DST_SCAN_JOB_ID | `nfs-v4-incr-sync-dst` |
| IC_JOB_ID | `nfs-v4-incr-sync-ic` |

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

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_sync*"
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

## Step 2: 创建源端测试数据（含 ACL 和 xattr）

复用 nfs-v4-full-sync 的 setup-nfs4-test-data.sh：

```bash
scp .claude/skills/nfs-v4-full-sync/scripts/setup-nfs4-test-data.sh root@{SOURCE_IP}:/tmp/setup-nfs4-test-data.sh
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-nfs4-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}
ACL set on: 5 files/dirs
xattr set on: 5 files
OK: 数量校验通过，ACL/xattr 设置完成
```

**Stop if the script exits non-zero.**

---

## Step 3: Phase 1 — 全量 Sync（含 ACL/xattr）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for：
- 进度信息（CopyAcl、CopyXattr 日志条目）
- 错误行（`ERROR`、`WARN`）
- 最终完成消息

**Verify: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}, ERROR STATISTICS 为 0。**

### 3b. ClickHouse 验证（全量 sync 后）

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v4_incr_sync+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {BASELINE_FILES}
true    false   {BASELINE_DIRS}
false   true    {BASELINE_SYMLINKS}
```

### 3c. 目标端 find 计数验证

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(find {NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(find {NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(find {NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}`。

### 3d. 验证全量 sync 后 ACL 已正确复制到目标端

```bash
ssh root@{SOURCE_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
```

```bash
ssh root@{DEST_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
```

**Verify: 两端 ACL 中的自定义 ACE 完全一致。**

### 3e. 验证全量 sync 后 xattr 已正确复制到目标端

```bash
ssh root@{SOURCE_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

**Verify: 两端 `user.*` xattr 字段完全一致。**

**Do not proceed until full sync succeeds with all counts matching.**

---

## Step 4: 变更源端数据（含 ACL/xattr 变更）

使用 nfs-v4-incremental-scan 的变更脚本（包含文件系统变更 + ACL/xattr 修改）：

```bash
scp .claude/skills/nfs-v4-incremental-scan/scripts/mutate-nfs4-test-data.sh root@{SOURCE_IP}:/tmp/mutate-nfs4-test-data.sh
ssh root@{SOURCE_IP} 'sudo bash /tmp/mutate-nfs4-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}
ACL modified on: 2 files
xattr modified on: 2 files
OK: 变更后数量校验通过
```

**Stop if the script exits non-zero.**

---

## Step 5: Phase 2 — 增量 Sync（含 ACL/xattr）

同一 SYNC_JOB_ID（`jobs/replicate_{SYNC_JOB_ID}/` 已存在 → 自动增量模式）。

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

### 5a. 验证 Scanned Statistics

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}。

### 5b. 验证 Incremental Statistics

Expected（NFSv4.1 Fh3 模式）：

```
   ├─ New:          7 total | dirs      2 | files      3 | symlinks    2
   ├─ Changed:      ? total | dirs      0 | files      ? | symlinks    0
   ├─ Renamed:      7 total | dirs      1 | files      4 | symlinks    2
   └─ Deleted:      8 total | dirs      1 | files      5 | symlinks    2
```

**注意**：Changed 包含文件系统变更（2 files）+ ACL/xattr 变更触发的 mtime 更新（最多 2 files）。总 Changed 可能为 2–4。
**Verify: New=7, Renamed=7, Deleted=8, Changed>=2, ERROR STATISTICS 为 0。**

**If incremental sync fails，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS4ERR_STALE（源端）**: 变更脚本的 rename/delete 使旧文件句柄失效，增量 sync 从 base 表读取的 fh3 已过期。清除 moka 缓存（重启程序）后重试。
   - **NFS4ERR_STALE（目标端）**: 目标端在全量 sync 后缓存的句柄因服务端重启而失效。清理目标端 test-data 后从 Step 3 重新开始。
   - **NFS4ERR_BAD_STATEID**: OPEN 建立的 stateid 在 WRITE/SETACL 期间失效（lease 过期或网络中断）。检查服务端 lease time（`/proc/fs/nfsd/nfsv4leasetime`，通常 90s），调大 config.toml 中的超时值后重试。
   - **NFS4ERR_DENIED（SETACL）**: 目标端拒绝 ACL 写入。检查目标端 NFS export 配置（`/etc/exports` 中的 `acl` 选项）和 `nfsd_acl` 内核模块是否已加载（`lsmod | grep nfsd`）。
   - **Failed to copy ACL（WARN）**: ACL 复制失败但非致命。统计数量，目标端需额外手动验证 ACL 是否更新。
   - **Changed 数量多于预期**: ACL/xattr 变更触发 setattr → mtime 变化 → 检测为 Changed。这是预期行为。
   - **Incremental Statistics 不匹配（非 Changed 方向）**: 检查变更脚本是否完整执行。重新从 Step 0 开始。

**Do not proceed to Step 6 until the incremental sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 6: 验证目标端数据

### 6a. find 直接计数

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(find {NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(find {NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(find {NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}`。

### 6b. scan 验证目标端计数

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}.**

### 6c. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v4_incr_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {POST_MUTATE_FILES}
true    false   {POST_MUTATE_DIRS}
false   true    {POST_MUTATE_SYMLINKS}
```

### 6d. 验证增量 sync 后 ACL 更新已传播到目标端

对变更脚本中修改过 ACL 的文件，比对源端和目标端：

```bash
ssh root@{SOURCE_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
```

```bash
ssh root@{DEST_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
```

**Verify: 两端 ACL 完全一致（包括增量 sync 中新修改的 ACE）。**

### 6e. 验证增量 sync 后 xattr 更新已传播到目标端

对变更脚本中修改过 xattr 的文件，比对源端和目标端：

```bash
ssh root@{SOURCE_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

**Verify: 两端 `user.*` xattr 字段完全一致（包括增量 sync 中新修改的值）。**

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

## Step 8: 并发清理

Only proceed after all Step 6 and Step 7 checks pass. **8a–8e 可并发执行**。

### 8a. 清理源端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v4-incr-sync-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 8b. 清理目标端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v4-incr-sync-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 8c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_sync%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 8d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_sync*"
```

Expected: 无输出（空）。

### 8e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## NFSv4.1 vs NFSv3 增量 Sync 对比

| 方面 | NFSv3 | NFSv4.1 |
|------|-------|---------|
| URL | `nfs://ip/export` | `nfs://ip/export?version=4.1` |
| sync 命令 | `sync --id ... src dst` | `sync --id ... --enable-acl src dst` |
| ACL 复制 | 不支持 | `--enable-acl` 触发 GETACL/SETACL |
| xattr 复制 | 不支持 | `--enable-acl` 同时触发 xattr 复制 |
| ACL 变更 → Changed | 不适用 | ACL setattr → mtime 更新 → Changed |
| 常见错误 | NFS3ERR_STALE / NOENT | NFS4ERR_BAD_STATEID / DENIED |
| 目标端 ACL 验证 | 不适用 | `nfs4_getfacl` 手动核对 |
| 目标端 xattr 验证 | 不适用 | `getfattr -d` 手动核对 |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled (Step 1)
- [ ] Source data with ACL/xattr: {BASELINE_DIRS}/{BASELINE_FILES}/{BASELINE_SYMLINKS} (Step 2)
- [ ] Full sync with --enable-acl: counts match, ERROR STATISTICS=0 (Step 3)
- [ ] ClickHouse base table verified after full sync (Step 3b)
- [ ] Dest find counts match after full sync (Step 3c)
- [ ] Dest ACL verified after full sync (Step 3d)
- [ ] Dest xattr verified after full sync (Step 3e)
- [ ] Mutations applied including ACL/xattr changes (Step 4)
- [ ] Incremental sync: New=7, Renamed=7, Deleted=8, Changed>=2 (Step 5b)
- [ ] Dest find counts match: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks={POST_MUTATE_SYMLINKS} (Step 6a)
- [ ] Dest scan counts match (Step 6b)
- [ ] ClickHouse dest base table verified (Step 6c)
- [ ] Dest ACL updated after incremental sync (Step 6d)
- [ ] Dest xattr updated after incremental sync (Step 6e)
- [ ] Integrity check: All Passed (Step 7)
- [ ] Source NFS cleaned and verified empty (Step 8a)
- [ ] Dest NFS cleaned and verified empty (Step 8b)
- [ ] ClickHouse tables cleaned (Step 8c)
- [ ] jobs dir cleaned (Step 8d)
- [ ] Logs cleaned (Step 8e)
