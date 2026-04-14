---
name: e2e-test-nfs-v4-integrity-check
description: >
  This skill should be used when the user asks to "run nfs v4 integrity check",
  "test integrity check nfs v4", "nfs v4 一致性校验测试", "nfs v4.1 integrity check",
  "test nfs v4.1 acl mismatch", "test nfs v4.1 xattr verification",
  or mentions running a standalone integrity-check with ACL/xattr verification between two NFSv4.1 endpoints.
---

# NFS v4.1 Integrity Check Test Skill

## Overview

独立一致性校验测试（NFS v4.1 存储）。
验证 integrity-check 在多种场景下的正确性，并专门测试 ACL/xattr 复制的完整性（反向验证代码功能）：
完全一致 → Mismatch 检测 → Missing 检测 → ACL 差异手动检测 → xattr 差异手动检测 → Auto-Fix → Quick 模式。
`datasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv4.1。
通过 SSH 在目标端引入文件内容差异和 ACL/xattr 差异用于验证检测能力。

**测试覆盖场景**：
1. **完全一致**：sync（含 ACL/xattr）后未修改 → All Passed + ACL/xattr 手动验证匹配
2. **Quick 模式**：size-only 比较，无需 hash 计算
3. **文件内容 Mismatch**：修改目标端文件内容 → 检测到 Mismatch
4. **文件 Missing**：删除目标端文件 → 检测到 Missing
5. **ACL 差异**：`nfs4_setfacl` 改变目标端 ACL → integrity-check 内容一致但 ACL 已偏离（手动 `nfs4_getfacl` 验证）
6. **xattr 差异**：`setfattr` 改变目标端 xattr → xattr 已偏离（手动 `getfattr -d` 验证）
7. **Auto-Fix 模式**：修复属性（uid/gid/mode）差异

**ACL/xattr 场景（5/6）的设计目的**：integrity-check 工具本身校验文件内容完整性，ACL/xattr 差异通过手动命令暴露。这两个场景直接验证了 `--enable-acl` 同步的代码路径：若 ACL/xattr 未被正确复制，手动命令即可发现。

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | `{NFSv4_SOURCE_IP}` |
| DEST_IP | `{NFSv4_DEST_IP}` |
| NFS_EXPORT | `/export/nfsv4` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}?version=4.1` |
| DEST_URL | `nfs://{DEST_IP}{NFS_EXPORT}?version=4.1` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/datasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| SYNC_JOB_ID | `nfs-v4-ic-sync` |
| IC_JOB_ID | `nfs-v4-ic-test` |
| IC_QUICK_JOB_ID | `nfs-v4-ic-quick` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |
| EXPECTED_SYMLINKS | 36 |
| ACL_TEST_FILE | `d1/d1_1/file1.txt` |
| XATTR_TEST_FILE | `d2/d2_1/file1.txt` |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理目标端 NFS 数据（SSH）

```bash
ssh root@{DEST_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "dest cleaned"'
```

Expected: `dest cleaned`。

### 0c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_ic%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_ic*"
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

## Step 2: 创建源端测试数据 + Sync 到目标端（含 ACL/xattr）

### 2a. 上传并执行测试脚本

```bash
scp .claude/skills/nfs-v4-full-sync/scripts/setup-nfs4-test-data.sh root@{SOURCE_IP}:/tmp/setup-nfs4-test-data.sh
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-nfs4-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
ACL set on: 5 files/dirs
xattr set on: 5 files
OK: 数量校验通过，ACL/xattr 设置完成
```

**Stop if the script exits non-zero.**

### 2b. 验证源端 ACL 已设置

```bash
ssh root@{SOURCE_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

Expected: 输出包含自定义 ACE（非纯默认 ACL）。记录此输出用于后续 dest 对比。

### 2c. 验证源端 xattr 已设置

```bash
ssh root@{SOURCE_IP} 'getfattr -d {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

Expected: 输出包含 `user.author`、`user.version` 等 xattr 字段。记录此输出用于后续 dest 对比。

### 2d. Sync 到目标端（含 ACL/xattr）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}, ERROR STATISTICS 为 0。**

### 2e. 目标端 find 验证

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(find {NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(find {NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(find {NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}`。

### 2f. 验证目标端 ACL 已正确复制（初始状态记录）

```bash
ssh root@{DEST_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

**Verify: 输出与 2b 记录的源端 ACL 一致（自定义 ACE 均存在）。**

### 2g. 验证目标端 xattr 已正确复制（初始状态记录）

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

**Verify: `user.*` 字段与 2c 记录的源端 xattr 完全一致。**

**Do not proceed until sync succeeds and dest counts + ACL/xattr match.**

---

## Step 3: 场景 1 — 完全一致校验（Full 模式）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

### 3a. 验证结果

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，Checked 数量 > 0，无 Issues。**

---

## Step 4: 场景 2 — Quick 模式校验

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_QUICK_JOB_ID} --quick "{SOURCE_URL}" "{DEST_URL}"
```

### 4a. 验证结果

Expected:

```
  Integrity Check Results:               Mode: Quick, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: Quick 模式只比较 size（不计算 hash），速度应明显快于 Full 模式。**

---

## Step 5: 场景 3 — 文件内容差异检测

### 5a. 修改目标端文件内容（制造内容 Mismatch）

```bash
ssh root@{DEST_IP} 'echo "tampered-nfs4-content-integrity-check" > {NFS_EXPORT}/test-data/d1/d1_1/file2.txt'
```

### 5b. Full 模式校验（应检测到 Mismatch）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-mismatch "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   ├─ Passed:        ...
   └─ Issues:        ...
      ├─ Mismatch:  1  (files: 1, dirs: 0, symlinks: 0)
```

**Verify: 检测到至少 1 个 Mismatch。退出码可能非 0（存在不一致）。**

### 5c. Quick 模式校验（size 相同时可能漏检）

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_QUICK_JOB_ID}-mismatch --quick "{SOURCE_URL}" "{DEST_URL}"
```

**注意**：若 `echo` 后文件 size 与源文件不同，Quick 模式也会检测到 Mismatch；若恰好 size 相同，Quick 模式可能漏检（这是 Quick 模式的已知局限性，说明 Full 模式的必要性）。

### 5d. 删除目标端文件（制造 Missing）

```bash
ssh root@{DEST_IP} 'rm {NFS_EXPORT}/test-data/d2/file1.txt'
```

### 5e. 校验（应检测到 Missing + Mismatch）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-missing "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   ├─ Passed:        ...
   └─ Issues:        ...
      ├─ Missing:    1  (files: 1, dirs: 0, symlinks: 0)
      ├─ Mismatch:  1  (files: 1, dirs: 0, symlinks: 0)
```

**Verify: Missing >= 1, Mismatch >= 1。**

---

## Step 6: 场景 4 — ACL 差异手动检测（NFSv4.1 专属验证）

此场景直接验证 `--enable-acl` 代码路径：修改目标端 ACL 后，通过 `nfs4_getfacl` 手动比对确认差异，验证 ACL 复制的精确性。

### 6a. 恢复目标端到一致状态

先重新 sync 修复上一步的差异：

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID}-fix --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: ERROR STATISTICS 为 0。**

### 6b. 记录目标端当前 ACL（基准值）

```bash
ssh root@{DEST_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

记录此输出，用于后续与篡改后的 ACL 对比。

### 6c. 篡改目标端 ACL（引入 ACL 差异）

```bash
ssh root@{DEST_IP} 'nfs4_setfacl -a "D::EVERYONE@:rw" {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

此操作在目标端 ACL 中新增一条 Deny ACE，源端没有此 ACE。

### 6d. 比对源端与目标端 ACL

```bash
ssh root@{SOURCE_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

```bash
ssh root@{DEST_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

**Verify: 两端 ACL 不一致**，目标端有额外的 Deny ACE `D::EVERYONE@:rw`，源端没有。

### 6e. integrity-check 仍可 All Passed（ACL 差异不影响文件内容校验）

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-acl "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: integrity-check 通过（文件内容一致），但 6d 手动对比显示 ACL 已偏离。**
**这确认了 integrity-check 不检查 ACL，ACL 一致性需通过 `--enable-acl` + 手动验证保证。**

### 6f. 通过增量 sync 修复 ACL 差异

使用增量 sync 恢复目标端 ACL 到与源端一致的状态：

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID}-acl-fix --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

再次比对 ACL：

```bash
ssh root@{DEST_IP} 'nfs4_getfacl {NFS_EXPORT}/test-data/{ACL_TEST_FILE}'
```

**Verify: 目标端 ACL 恢复与源端一致（DENY ACE 已被移除或覆盖）。**

---

## Step 7: 场景 5 — xattr 差异手动检测（NFSv4.1 专属验证）

此场景验证 `--enable-acl` 触发的 xattr 复制代码路径（RFC 8276 named attributes）。

### 7a. 记录目标端当前 xattr（基准值）

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

记录此输出。

### 7b. 篡改目标端 xattr（修改已有 xattr 值）

```bash
ssh root@{DEST_IP} 'setfattr -n user.author -v "tampered-author" {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

### 7c. 比对源端与目标端 xattr

```bash
ssh root@{SOURCE_IP} 'getfattr -d {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

**Verify: `user.author` 值不一致**，目标端为 `tampered-author`，源端为原始值。

### 7d. integrity-check 仍可 All Passed（xattr 差异不影响文件内容校验）

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-xattr "{SOURCE_URL}" "{DEST_URL}"
```

Expected:

```
  Integrity Check Results:               Mode: Full, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: integrity-check 通过（文件内容未变），但 7c 手动对比显示 xattr 已偏离。**
**这确认了 xattr 一致性需通过 `--enable-acl` 同步保证，非 integrity-check 覆盖范围。**

### 7e. 通过增量 sync 修复 xattr 差异

触发增量 sync（SETATTR mtime 未变则可能不触发 xattr 复制，若需要强制可先修改源文件 mtime）：

```bash
# 若 xattr 变更未触发 Changed，需先在源端 touch 文件更新 mtime
ssh root@{SOURCE_IP} 'touch {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
# 然后执行增量 sync
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID}-xattr-fix --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

再次比对 xattr：

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/{XATTR_TEST_FILE}'
```

**Verify: 目标端 `user.author` 恢复与源端一致（不再是 `tampered-author`）。**

---

## Step 8: 场景 6 — Auto-Fix 模式（修复属性差异）

### 8a. 制造属性差异

```bash
ssh root@{DEST_IP} 'chmod 777 {NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
```

### 8b. 确认差异存在

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-attr "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: 检测到属性 Mismatch（mode 差异）。**

### 8c. 执行 Auto-Fix

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-fix --auto-fix "{SOURCE_URL}" "{DEST_URL}"
```

Expected: auto-fix 修复 uid/gid/mode 差异（不创建缺失文件）。

### 8d. 验证修复效果

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-verify "{SOURCE_URL}" "{DEST_URL}"
```

Expected: `All Passed ✓`。

**If any integrity-check step fails unexpectedly，按以下步骤排查：**

1. 查看日志：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS4ERR_STALE（遍历源端或目标端）**: integrity-check 需同时访问两端 NFS；任一端服务端重启或 lease 过期均可触发。重新从 Step 2d sync 开始。
   - **NFS4ERR_ACCESS（读取目标端文件属性）**: 目标端 export 权限拒绝读取。检查 `/etc/exports` 中的 squash 设置（如 `all_squash` 会改变 uid/gid）。
   - **NFS4ERR_DENIED（读取 ACL）**: 目标端 NFS 服务端拒绝 GETACL RPC。检查 `nfsd_acl` 模块是否加载，export 是否有 `acl` 选项。此错误出现在 Steps 6b–6f 的手动 `nfs4_getfacl` 命令中，非 integrity-check 工具本身。
   - **nfs4_getfacl 返回默认 ACL（非自定义 ACE）**: 说明 `--enable-acl` 的 SETACL 步骤未成功。查看全量 sync 日志中的 `CopyAcl` 或 `Failed to copy ACL` 条目。
   - **getfattr 无 user.* 输出**: 说明 xattr 复制未成功。可能原因：目标端 NFS export 挂载未启用 `user_xattr`（`mount -o ...,user_xattr` 或 `/etc/fstab` 配置）；或目标端文件系统不支持 xattr。
   - **All Passed 但预期有差异**: 检查修改命令是否在正确路径执行（NFSv4.1 路径大小写敏感），用 `ssh` 确认文件确实被修改。

---

## Step 9: 并发清理

Only proceed after all Step 8 checks pass. **9a–9e 可并发执行**。

### 9a. 清理源端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v4-ic-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 9b. 清理目标端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v4-ic-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 9c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证表已清除。

### 9d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_ic*"
```

Expected: 无输出（空）。

### 9e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## NFSv4.1 vs NFSv3 Integrity Check 对比

| 方面 | NFSv3 | NFSv4.1 |
|------|-------|---------|
| sync 准备 | `sync --id ...` | `sync --id ... --enable-acl` |
| integrity-check 命令 | `integrity-check --id ...` | `integrity-check --id ...`（相同） |
| 文件内容 Mismatch | 检测 ✓ | 检测 ✓ |
| 文件 Missing | 检测 ✓ | 检测 ✓ |
| ACL 差异检测 | 不适用 | 手动 `nfs4_getfacl` 对比（工具不覆盖） |
| xattr 差异检测 | 不适用 | 手动 `getfattr -d` 对比（工具不覆盖） |
| Auto-Fix | 修复 uid/gid/mode | 修复 uid/gid/mode（ACL/xattr 需重新 sync） |
| 常见错误 | NFS3ERR_STALE / ACCES | NFS4ERR_STALE / DENIED |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled (Step 1)
- [ ] Source data with ACL/xattr created (Step 2a)
- [ ] Source ACL verified via nfs4_getfacl (Step 2b)
- [ ] Source xattr verified via getfattr (Step 2c)
- [ ] Full sync with --enable-acl: counts match, ERROR STATISTICS=0 (Step 2d)
- [ ] Dest find counts match (Step 2e)
- [ ] Dest ACL matches source after initial sync (Step 2f)
- [ ] Dest xattr matches source after initial sync (Step 2g)
- [ ] Full mode: All Passed on identical data (Step 3)
- [ ] Quick mode: All Passed on identical data (Step 4)
- [ ] Mismatch detected after tampering dest file content (Step 5b)
- [ ] Missing detected after deleting dest file (Step 5e)
- [ ] ACL divergence detected via manual nfs4_getfacl comparison (Step 6d)
- [ ] integrity-check still passes despite ACL divergence (Step 6e)
- [ ] ACL repaired via incremental sync with --enable-acl (Step 6f)
- [ ] xattr divergence detected via manual getfattr comparison (Step 7c)
- [ ] integrity-check still passes despite xattr divergence (Step 7d)
- [ ] xattr repaired via incremental sync with --enable-acl (Step 7e)
- [ ] Auto-Fix repairs attribute mismatches (Step 8c)
- [ ] Post-fix verification: All Passed (Step 8d)
- [ ] Source NFS cleaned and verified empty (Step 9a)
- [ ] Dest NFS cleaned and verified empty (Step 9b)
- [ ] ClickHouse tables cleaned (Step 9c)
- [ ] jobs dir cleaned (Step 9d)
- [ ] Logs cleaned (Step 9e)
