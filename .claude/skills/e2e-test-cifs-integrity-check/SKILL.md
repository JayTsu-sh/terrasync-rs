---
name: e2e-test-cifs-integrity-check
description: >
  This skill should be used when the user asks to "run cifs integrity check test",
  "test integrity check cifs", "cifs 一致性校验测试",
  "verify cifs source and dest match",
  or mentions running a standalone integrity-check between two CIFS endpoints.
---

# CIFS Integrity Check Test Skill

## Overview

独立一致性校验测试（CIFS/SMB 存储）。
验证 integrity-check 在多种场景下的正确性：完全一致 → Mismatch 检测 → Missing 检测。
`datasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 CIFS 共享。
通过 `smbclient` 在目标端引入差异用于验证检测能力。

**CIFS integrity-check 特点**：
- 无 symlink 相关检查（CIFS 不支持 symlink）
- 比较内容：文件 size + 内容 hash（Full 模式）或仅 size（Quick 模式）
- CIFS 有 uid/gid/mode 属性 → 属性差异也可检测
- 制造差异需通过 `smbclient`（无 SSH 访问底层文件系统）

## Constants

| Name | Value |
|------|-------|
| SRC_CIFS_HOST | `{SRC_CIFS_HOST}` |
| SRC_CIFS_USER | `{SRC_CIFS_USER}` |
| SRC_CIFS_PASS | `{SRC_CIFS_PASS}` |
| SRC_CIFS_SHARE | `{SRC_CIFS_SHARE}` |
| DST_CIFS_HOST | `{DST_CIFS_HOST}` |
| DST_CIFS_USER | `{DST_CIFS_USER}` |
| DST_CIFS_PASS | `{DST_CIFS_PASS}` |
| DST_CIFS_SHARE | `{DST_CIFS_SHARE}` |
| SOURCE_URL | `smb://{SRC_CIFS_USER}:{SRC_CIFS_PASS}@{SRC_CIFS_HOST}/{SRC_CIFS_SHARE}/test-data` |
| DEST_URL | `smb://{DST_CIFS_USER}:{DST_CIFS_PASS}@{DST_CIFS_HOST}/{DST_CIFS_SHARE}/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/datasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| SYNC_JOB_ID | `cifs-ic-sync` |
| IC_JOB_ID | `cifs-ic-test` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0e 可并发执行**。

### 0a. 清理源端 CIFS 数据

```bash
smbclient "//{SRC_CIFS_HOST}/{SRC_CIFS_SHARE}" -U "{SRC_CIFS_USER}%{SRC_CIFS_PASS}" -c "deltree test-data" 2>/dev/null || true
echo "source CIFS cleaned"
```

Expected: `source CIFS cleaned`。

### 0b. 清理目标端 CIFS 数据

```bash
smbclient "//{DST_CIFS_HOST}/{DST_CIFS_SHARE}" -U "{DST_CIFS_USER}%{DST_CIFS_PASS}" -c "deltree test-data" 2>/dev/null || true
echo "dest CIFS cleaned"
```

Expected: `dest CIFS cleaned`。

### 0c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_ic%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_ic*"
```

Expected: 无输出（空）。

### 0e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 编译本地 Binary + 创建测试数据

### 1a. 编译

```bash
cargo build
```

### 1b. 创建源端测试数据

```bash
bash .claude/skills/cifs-full-sync/scripts/setup-cifs-test-data.sh
```

Expected: dirs=40, files=117, symlinks=0。

---

## Step 2: Sync 到目标端

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}"
```

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0, ERROR STATISTICS 为 0。**

### 2b. 目标端 smbclient 验证

```bash
FILE_COUNT=$(smbclient "//{DST_CIFS_HOST}/{DST_CIFS_SHARE}" -U "{DST_CIFS_USER}%{DST_CIFS_PASS}" -c "recurse ON; ls test-data/*" 2>/dev/null | grep -c "^\s")
echo "dest CIFS files: $FILE_COUNT"
```

**Do not proceed until sync succeeds and dest counts match.**

---

## Step 3: 场景 1 — 完全一致校验（Full 模式）

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

**Verify: 退出码为 0，Checked > 0，无 Issues。**

---

## Step 4: 场景 2 — 引入差异 + 校验

### 4a. 修改目标端文件内容（制造 Mismatch）

通过 `smbclient` 上传替换文件：

```bash
echo "tampered-content-for-cifs-integrity-check" > /tmp/tampered_file.txt
smbclient "//{DST_CIFS_HOST}/{DST_CIFS_SHARE}" -U "{DST_CIFS_USER}%{DST_CIFS_PASS}" -c "cd test-data/d1/d1_1; put /tmp/tampered_file.txt file1.txt"
rm /tmp/tampered_file.txt
```

### 4b. Full 模式校验（应检测到 Mismatch）

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

**Verify: Mismatch >= 1。**

### 4c. 删除目标端文件（制造 Missing）

```bash
smbclient "//{DST_CIFS_HOST}/{DST_CIFS_SHARE}" -U "{DST_CIFS_USER}%{DST_CIFS_PASS}" -c "cd test-data/d2; del file1.txt"
```

### 4d. 校验（应检测到 Missing + Mismatch）

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
   - **STATUS_ACCESS_DENIED（读取文件内容）**: SMB 用户对目标共享部分文件无读权限。integrity-check 需要读取源端和目标端文件内容计算 hash。检查共享级和文件级 ACL。
   - **STATUS_SHARING_VIOLATION（文件被锁）**: 其他 SMB 会话持有文件的独占锁，integrity-check 无法读取。等待锁释放或终止占用会话。
   - **STATUS_LOGON_FAILURE**: SMB 认证失败。检查 URL 中用户名密码的编码（特殊字符 `@` → `%40`，`:` → `%3A`）。
   - **smbclient 修改未生效**: `smbclient put` 操作可能因权限或路径错误未实际修改文件。用 `smbclient get` 下载目标端文件并检查内容确认修改生效。
   - **All Passed 但预期有差异**: CIFS 的文件路径大小写可能不敏感（取决于 SMB 服务端配置）。检查 `put` 命令的目标路径是否与 scan 中的路径大小写一致。

---

## Step 5: 并发清理

**5a–5e 可并发执行**。

### 5a. 清理源端 CIFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id cifs-ic-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 5b. 清理目标端 CIFS

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id cifs-ic-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 5c. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_ic%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证表已清除。

### 5d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_ic*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_ic*"
```

Expected: 无输出（空）。

### 5e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## CIFS vs NFS vs S3 Integrity Check 对比

| 方面 | NFS v3 | CIFS | S3 |
|------|--------|------|----|
| Symlink 检查 | 有（36） | 无 | 无 |
| 属性比较（uid/gid/mode） | 有 | 有 | 无 |
| Auto-Fix（属性修复） | 有 | 有 | 不适用 |
| 制造差异方式 | SSH + shell | smbclient put/del | mc pipe/rm |
| 常见错误 | NFS3ERR_STALE | STATUS_SHARING_VIOLATION | AccessDenied |
| 路径大小写 | 敏感 | 可配置（取决于服务端） | 敏感 |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled + source data created (Step 1)
- [ ] Sync to dest completed: 40/117/0 (Step 2)
- [ ] Full mode: All Passed on identical data (Step 3)
- [ ] Mismatch detected after tampering dest file (Step 4b)
- [ ] Missing detected after deleting dest file (Step 4d)
- [ ] Source CIFS cleaned and verified empty (Step 5a)
- [ ] Dest CIFS cleaned and verified empty (Step 5b)
- [ ] ClickHouse tables cleaned (Step 5c)
- [ ] jobs dir cleaned (Step 5d)
- [ ] Logs cleaned (Step 5e)
