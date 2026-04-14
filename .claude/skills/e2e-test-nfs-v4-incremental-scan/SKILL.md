---
name: e2e-test-nfs-v4-incremental-scan
description: >
  This skill should be used when the user asks to "run nfs v4 incremental scan test",
  "test incremental scan nfs v4", "nfs v4 增量扫描测试", "nfs v4.1 incremental scan",
  or mentions running the full-scan → mutate → incremental-scan → verify workflow
  against the NFSv4.1 test environment ({SOURCE_IP}).
---

# NFS v4.1 Incremental Scan Test Skill

## Overview

端到端增量扫描测试（NFS v4.1 存储）。
验证完整管线：全量扫描建基线 → 变更（含 ACL/xattr 变更）→ 增量扫描检测变更 → ClickHouse 表验证 → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv4.1。测试数据和变更通过 SSH 在远端执行。

**NFSv4.1 增量扫描特点**：
- 使用 `JoinStrategy::Fh3`（file_handle 字段）— 与 NFSv3 一致
- 精确 rename 检测（Renamed，非 New+Deleted）
- 扫描本身不读取 ACL/xattr（仅扫文件树计数）
- ACL 修改 + 显式 touch → mtime 更新 → 被检测为 Changed
- 纯 xattr 修改（无 touch）→ mtime 不变 → **不影响**增量扫描计数

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | 10.131.9.13 |
| NFS_EXPORT | `/` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}?version=4.1` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `10.128.133.213:8123` |
| JOB_ID | `nfs-v4-incr-scan` |
| SANITIZED_JOB_ID | `nfs_v4_incr_scan` |
| BASE_TABLE | `base_nfs_v4_incr_scan` |
| INCREMENTAL_TABLE | `incremental_nfs_v4_incr_scan` |
| STATE_TABLE | `state_nfs_v4_incr_scan` |
| BASELINE_DIRS | 113 |
| BASELINE_FILES | 335 |
| BASELINE_SYMLINKS | 79 |
| POST_MUTATE_DIRS | 114 |
| POST_MUTATE_FILES | 333 |
| POST_MUTATE_SYMLINKS | 79 |

**注意**：NFS v4.1 使用伪根（pseudo-root）机制，URL 中的路径必须是相对于 `fsid=0` 的 export。
当前配置中 `/export/nfs4` 设置了 `fsid=0`，因此 NFS v4.1 URL 应使用 `/` 作为路径。

---

## Step 0: 清理测试环境

**0a–0d 可并发执行**。

### 0a. 清理源端 NFS

```bash
ssh ubuntu@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_scan*"
```

Expected: 无输出（空）。

### 0d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1a: 上传测试脚本（SOURCE_IP）

复用 e2e-test-nfs-v4-full-scan 的 setup-nfs4-test-data.sh：

```bash
scp .claude/skills/e2e-test-nfs-v4-full-scan/scripts/setup-nfs4-test-data.sh ubuntu@{SOURCE_IP}:/tmp/setup-nfs4-test-data.sh
```

Expected: 无错误输出，scp 退出码为 0。

---

## Step 1b: 执行测试脚本创建基线数据（SOURCE_IP）

```bash
ssh ubuntu@{SOURCE_IP} 'sudo bash /tmp/setup-nfs4-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}
ACL set on: N entries
xattr set on: M entries
OK: 数量校验通过，ACL/xattr 设置完成
```

注意：ACL/xattr 数量可能因工具可用性而变化，不影响基础计数验证。

**Stop if the script exits non-zero.**

---

## Step 2: 全量扫描（建立基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{SOURCE_URL}"
```

### 2a. 验证 CLI Scanned Statistics

**Verify**: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}。

If counts do not match, stop and investigate.

### 2b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.{BASE_TABLE}+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行，顺序不定）：

```
false   false   {BASELINE_FILES}      # 普通文件 = 335
true    false   {BASELINE_DIRS}       # 目录 = 113
false   true    {BASELINE_SYMLINKS}   # 软链接 = 79
```

**Do not proceed until full scan succeeds.**

---

## Step 3: 执行变更脚本

**注意**：使用 NFS v4 专用的变更脚本（路径为 `/export/nfs4/test-data`），不要复用 NFS v3 脚本。

### 3a. 上传变更脚本

```bash
scp .claude/skills/e2e-test-nfs-v4-incremental-scan/scripts/mutate-nfs4-test-data.sh ubuntu@{SOURCE_IP}:/tmp/mutate-nfs4-test-data.sh
```

### 3b. 执行变更

```bash
ssh ubuntu@{SOURCE_IP} 'sudo bash /tmp/mutate-nfs4-test-data.sh'
```

Expected output (last lines):

```
Expected: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}
find:    dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}
OK: 变更后数量校验通过
```

**Stop if the script exits non-zero.**

变更摘要：

**结构性变更**（与 v3 相同）：
- **ADD**: 2 dirs, 3 files, 2 symlinks
- **MODIFY**: 2 files（内容变更 → size+mtime）
- **RENAME**: 1 file, 1 symlink, 1 dir（含级联 3 files + 1 symlink）
- **DELETE**: 1 dir+内容（3 files + 1 symlink）, 2 files, 1 symlink

**属性变更（不改内容/结构）**：
- **chmod**: 5 files + 2 dirs（仅改 mode → Fh3 模式**不检测**）
- **chown**: 5 files + 2 dirs（仅改 uid/gid → Fh3 模式**不检测**）
- **touch**: 5 files + 2 dirs（改 mtime → Fh3 模式**检测为 Changed**）
- **mixed**: 2 files（mode+owner+mtime → **检测为 Changed**）

**NFSv4 特有变更**：
- **ACL 修改 + touch**: 2 files（d1/file1.txt, d2/file1.txt，改 mtime → **检测为 Changed**）
- **xattr 修改（无 touch）**: 2 files（d2/file1.txt, d3/d3_2/file1.txt，mtime 不变 → **不检测**）

---

## Step 4: 增量扫描 + 全面验证

使用**同一 JOB_ID**（`jobs/` 目录已存在，自动触发增量模式）。

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{SOURCE_URL}"
```

### 4a. 验证 Scanned Statistics

增量扫描仍会遍历当前文件系统全部条目。

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}。

### 4b. 验证 Incremental Statistics

NFSv4.1 使用 Fh3 模式检测变更（与 NFSv3 一致），Changed 多出 2 项（ACL 修改 + touch）：

Expected Incremental Statistics:

```
   ├─ New:          7 total | dirs      2 | files      3 | symlinks    2
   ├─ Changed:     13 total | dirs      2 | files     11 | symlinks    0
   ├─ Renamed:      7 total | dirs      1 | files      4 | symlinks    2
   └─ Deleted:      8 total | dirs      1 | files      5 | symlinks    2
```

**计数说明**：
- New 7 = 2 dirs + 3 files + 2 symlinks
- Changed 13 = content 2 + touch 5 + mixed 2 + ACL+touch 2 files；dirs 2（touch）
  - content 2: d1/d1_1/file1.txt, d2/d2_2/d2_2_1/file3.txt
  - touch 3: d4/d4_3/file1.txt, d1/d1_2/file3.txt, d2/d2_3/d2_3_2/file2.txt
  - special mtime 2: file_2020-01-01..., file_2026-01-01...
  - mixed 2: special/mixed/exec_new.sh, special/mixed/readonly_old.txt
  - ACL+touch 2（NFSv4 特有）: d1/file1.txt, d2/file1.txt
  - dirs 2: d4/d4_4, d1/d1_1（touch）
  - 注意：chmod-only / chown-only / xattr-only 不改 mtime，Fh3 模式**不检测**
- Renamed 7 = 1 file + 1 symlink + 1 dir 级联（dir + 3 files + 1 symlink）
- Deleted 8 = 1 dir(d3_3_3) + 5 files + 2 symlinks

**若任意计数不符，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -40
```

2. 检查 ClickHouse 增量表原始记录：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,relative_path,is_dir,is_symlink+FROM+default.{INCREMENTAL_TABLE}+FINAL+ORDER+BY+operation_type,relative_path+FORMAT+TabSeparated"
```

3. 常见 NFSv4.1 特有问题：
   - **stateid 过期（NFS4ERR_STALE_STATEID）**: 增量扫描耗时较长时 stateid 可能过期，检查 `/proc/fs/nfsd/nfsv4leasetime`（通常 90s）。
   - **OPEN 竞争（NFS4ERR_DENIED）**: 降低并发度后重试。
   - **Changed 少于 13**: nfs4_setfacl 不可用时，脚本退化为仅 touch，Changed 仍为 13（touch 已保证 mtime 更新）。
   - **Fh3 策略不匹配**: 确认 NFSv4.1 的 entry.handle 正确写入了 file_handle 字段，查询 ClickHouse 确认 file_handle 非空。

### 4c. ClickHouse base 表验证（增量后当前状态）

```bash
# 先查 scan_state
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.{STATE_TABLE}+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "current scan_state: $STATE"
[[ -z "$STATE" ]] && echo "ERROR: scan_state 为空，请检查 ClickHouse 连接和 state 表" && exit 1
```

Expected: STATE 非空。

```bash
# 用 scan_state 过滤 base 表（分类计数）
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+current_state%3D${STATE}+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行，顺序不定）：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 333
true    false   {POST_MUTATE_DIRS}       # 目录 = 114
false   true    {POST_MUTATE_SYMLINKS}   # 软链接 = 79
```

```bash
# 交叉验证：base 表当前状态总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `526`（{POST_MUTATE_DIRS}+{POST_MUTATE_FILES}+{POST_MUTATE_SYMLINKS} = 114+333+79）。

**若总行数不符，停止并调查。**

### 4d. ClickHouse incremental 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,is_dir,is_symlink,count(*)+FROM+default.{INCREMENTAL_TABLE}+FINAL+GROUP+BY+operation_type,is_dir,is_symlink+ORDER+BY+operation_type,is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（11 行）：

```
changed false   false   11
changed true    false   2
deleted false   false   5
deleted false   true    2
deleted true    false   1
new     false   false   3
new     false   true    2
new     true    false   2
rename  false   false   4
rename  false   true    2
rename  true    false   1
```

**changed files=11 说明**：content 2 + touch 5 + mixed 2 + ACL+touch 2（NFSv4 特有）。

**若任意行不符，停止并调查。不执行后续清理。**

---

## Step 5: 清理环境

Only proceed after all Step 4 checks pass.

**5a–5d 可并发执行**。

### 5a. 清理源端 NFS

```bash
ssh ubuntu@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 5b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 5c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_incr_scan*"
```

Expected: 无输出（空）。

### 5d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Baseline data created with ACL/xattr: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks={BASELINE_SYMLINKS} (Step 1)
- [ ] Full scan counts match baseline (Step 2a)
- [ ] ClickHouse base table verified at baseline (Step 2b)
- [ ] Mutations applied: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks={POST_MUTATE_SYMLINKS} (Step 3)
- [ ] Incremental scan Scanned Statistics: {POST_MUTATE_DIRS}/{POST_MUTATE_FILES}/{POST_MUTATE_SYMLINKS} (Step 4a)
- [ ] Incremental Statistics: New=7/Changed=13/Renamed=7/Deleted=8 (Step 4b)
- [ ] ClickHouse base table post-incremental verified: total=526 (Step 4c)
- [ ] ClickHouse incremental table: 11 rows verified, changed files=11 (Step 4d)
- [ ] Environment cleaned (Step 5)
