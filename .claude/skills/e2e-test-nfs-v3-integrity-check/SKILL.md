---
name: e2e-test-nfs-v3-integrity-check
description: >
  This skill should be used when the user asks to "run nfs v3 integrity check",
  "test integrity check nfs", "nfs v3 一致性校验测试",
  "verify nfs source and dest match",
  or mentions running a standalone integrity-check between two NFS endpoints.
---

# NFS v3 Integrity Check Test Skill

## Overview

独立一致性校验测试（NFS v3 存储）。
验证 integrity-check 在多种场景下的正确性：完全一致 → Mismatch 检测 → Missing 检测 → Quick 模式 → Auto-Fix 模式。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv3。
通过 SSH 在目标端引入差异用于验证检测能力。

**测试覆盖四种场景**：
1. **完全一致**：sync 后未修改 → All Passed
2. **引入 Mismatch**：修改目标端文件内容 → 检测到 Mismatch
3. **引入 Missing**：删除目标端文件 → 检测到 Missing
4. **Quick 模式 vs Full 模式**：对比 size-only 和 hash 校验
5. **Auto-Fix 模式**：修复属性差异

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | 10.131.9.13 |
| SOURCE_NFS_EXPORT | `/export/nfs` |
| DEST_IP | `{DEST_IP}` |
| DEST_NFS_EXPORT | `{DEST_NFS_EXPORT}` |
| SOURCE_URL | `nfs://{SOURCE_IP}{SOURCE_NFS_EXPORT}` |
| DEST_URL | `nfs://{DEST_IP}{DEST_NFS_EXPORT}` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `10.128.133.213:8123` |
| SYNC_JOB_ID | `nfs-v3-ic-sync` |
| IC_JOB_ID | `nfs-v3-ic-test` |
| IC_QUICK_JOB_ID | `nfs-v3-ic-quick` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |
| EXPECTED_SYMLINKS | 36 |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh ubuntu@{SOURCE_IP} 'sudo rm -rf {SOURCE_NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理目标端 NFS 数据（SSH）

```bash
ssh ubuntu@{DEST_IP} 'sudo rm -rf {DEST_NFS_EXPORT}/test-data && echo "dest cleaned"'
```

Expected: `dest cleaned`。

### 0c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_ic%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_ic*"
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

## Step 2: 创建源端测试数据 + Sync 到目标端

### 2a. 上传并执行测试脚本

```bash
scp .claude/skills/nfs-v3-e2e/scripts/setup-test-data.sh ubuntu@{SOURCE_IP}:/tmp/setup-test-data.sh
ssh ubuntu@{SOURCE_IP} 'sudo bash /tmp/setup-test-data.sh'
```

Expected: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}。

### 2b. Sync 到目标端

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}, ERROR STATISTICS 为 0。**

### 2c. 目标端 find 验证

```bash
ssh ubuntu@{DEST_IP} 'FIND_DIRS=$(find {DEST_NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(find {DEST_NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(find {DEST_NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}`。

**Do not proceed until sync succeeds and dest counts match.**

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

## Step 5: 场景 3 — 引入差异后校验

### 5a. 修改目标端文件内容（制造 Mismatch）

通过 SSH 在目标端修改文件内容（size 不变但 hash 不同需要追加不同长度的内容）：

```bash
ssh ubuntu@{DEST_IP} 'echo "tampered-content-for-integrity-check" > {DEST_NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
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

### 5c. 删除目标端文件（制造 Missing）

```bash
ssh ubuntu@{DEST_IP} 'rm {DEST_NFS_EXPORT}/test-data/d2/file1.txt'
```

### 5d. 校验（应检测到 Missing + Mismatch）

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

**If integrity-check fails unexpectedly，按以下步骤排查：**

1. 查看日志：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS3ERR_STALE（遍历源端/目标端）**: 文件句柄缓存过期。integrity-check 需要同时访问源端和目标端 NFS，任一端重启或句柄过期都会触发。清除 moka 缓存或重启程序后重试。
   - **NFS3ERR_ACCES（读取目标端文件属性）**: 目标端 NFS export 权限不允许读取部分文件的属性。检查 export 配置中的 squash 设置。
   - **Hash 计算超时**: 大文件 hash 计算耗时过长触发 NFS 连接超时。调大 config.toml 中的 timeout 设置。
   - **All Passed 但预期有差异**: 检查修改命令是否在正确路径执行（路径大小写敏感）。用 `ssh` 确认文件确实被修改。

---

## Step 6: 场景 4 — Auto-Fix 模式

先恢复目标端到有 Mismatch 状态（不恢复 Missing，因为 auto-fix 不创建文件）：

### 6a. 恢复被删除的文件

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID}-fix "{SOURCE_URL}" "{DEST_URL}"
```

### 6b. 再次修改目标端文件属性（制造属性 Mismatch）

```bash
ssh ubuntu@{DEST_IP} 'chmod 777 {DEST_NFS_EXPORT}/test-data/d1/d1_1/file1.txt'
```

### 6c. 执行 Auto-Fix

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-fix --auto-fix "{SOURCE_URL}" "{DEST_URL}"
```

### 6d. 验证修复

Expected: auto-fix 修复属性差异（uid/gid/mode）。修复后再次校验应 All Passed。

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID}-verify "{SOURCE_URL}" "{DEST_URL}"
```

Expected: `All Passed ✓`。

---

## Step 7: 并发清理

**7a–7e 可并发执行**。

### 7a. 清理源端 NFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v3-ic-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 7b. 清理目标端 NFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v3-ic-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 7c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证表已清除。

### 7d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_ic*"
```

Expected: 无输出（空）。

### 7e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled (Step 1)
- [ ] Source data created + synced to dest (Step 2)
- [ ] Full mode: All Passed on identical data (Step 3)
- [ ] Quick mode: All Passed on identical data (Step 4)
- [ ] Mismatch detected after tampering dest file content (Step 5b)
- [ ] Missing detected after deleting dest file (Step 5d)
- [ ] Auto-Fix repairs attribute mismatches (Step 6c)
- [ ] Post-fix verification: All Passed (Step 6d)
- [ ] Source NFS cleaned and verified empty (Step 7a)
- [ ] Dest NFS cleaned and verified empty (Step 7b)
- [ ] ClickHouse tables cleaned (Step 7c)
- [ ] jobs dir cleaned (Step 7d)
- [ ] Logs cleaned (Step 7e)
