---
name: e2e-test-nfs-v3-incremental-scan
description: >
  This skill should be used when the user asks to "run nfs v3 incremental scan test",
  "test incremental scan nfs v3", "nfs v3 增量扫描测试", "incremental scan e2e",
  "test the incremental scan pipeline against NFSv3",
  or mentions running the full-scan → mutate → incremental-scan → verify workflow
  against the NFSv3 test environment ({SOURCE_IP}).
---

# NFS v3 Incremental Scan Test Skill

## Overview

端到端增量扫描测试：全量扫描建基线 → 变更文件系统（增删改+rename） → 增量扫描检测变更 → 验证 CLI 输出和 ClickHouse 数据库。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络访问 NFSv3。测试数据和变更通过 SSH 在远端执行。

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | 192.168.50.173 |
| NFS_EXPORT | `/export/nfs` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| JOB_ID | `nfs-v3-incr-scan` |
| BASELINE_DIRS | 113 |
| BASELINE_FILES | 335 |
| BASELINE_SYMLINKS | 79 |
| POST_MUTATE_DIRS | 114 |
| POST_MUTATE_FILES | 333 |
| POST_MUTATE_SYMLINKS | 79 |

ClickHouse 表名（`-` → `_`）：
- `base_nfs_v3_incr_scan`
- `incremental_nfs_v3_incr_scan`
- `state_nfs_v3_incr_scan`

NFS URL 格式：`nfs://{SOURCE_IP}{NFS_EXPORT}`。

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_scan*"
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

复用 e2e-test-nfs-v3 的 setup-test-data.sh：

```bash
scp .claude/skills/e2e-test-nfs-v3/scripts/setup-test-data.sh root@{SOURCE_IP}:/tmp/setup-test-data.sh
```

Expected: 无错误输出，scp 退出码为 0。

---

## Step 1b: 执行测试脚本创建基线数据（SOURCE_IP）

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}
total entries: 527
OK: 数量校验通过
```

**Stop if the script exits non-zero.**

---

## Step 2: 全量扫描（建立基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "nfs://{SOURCE_IP}{NFS_EXPORT}"
```

**2a.** 验证 CLI Scanned Statistics：dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks={BASELINE_SYMLINKS}。

If counts do not match, stop and investigate.

### 2b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_incr_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行，顺序不定）：

```
false   false   {BASELINE_FILES}      # 普通文件 = 335
true    false   {BASELINE_DIRS}       # 目录 = 113
false   true    {BASELINE_SYMLINKS}   # 软链接 = 79
```

**若任意计数不符，停止并调查。**

---

## Step 3: 执行变更脚本

### 3a. 上传变更脚本

```bash
scp .claude/skills/e2e-test-nfs-v3-incremental-scan/scripts/mutate-test-data.sh root@{SOURCE_IP}:/tmp/mutate-test-data.sh
```

### 3b. 执行变更

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/mutate-test-data.sh'
```

Expected output (last lines):

```
Expected: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}
find:    dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}
OK: Post-mutation counts verified
```

**Stop if the script exits non-zero.**

变更摘要：

**结构性变更**：
- **ADD**: 2 dirs, 3 files, 2 symlinks
- **MODIFY**: 2 files（追加/覆盖内容改变 size+mtime）
- **RENAME**: 1 file, 1 symlink, 1 dir（含级联 3 files + 1 symlink）
- **DELETE**: 1 dir+内容（3 files + 1 symlink）, 2 files, 1 symlink

**属性变更（不改内容/结构）**：
- **chmod**: 5 files + 2 dirs（仅改 mode，不改 size/mtime → Fh3 模式**不检测**）
- **chown**: 5 files + 2 dirs（仅改 uid/gid，不改 size/mtime → Fh3 模式**不检测**）
- **touch**: 5 files（改 mtime → Fh3 模式**检测为 changed**）
- **mixed**: 2 files（同时改 mode+owner+mtime → Fh3 模式**检测为 changed**，因 mtime 变化）

---

## Step 4: 增量扫描 + 全面验证

使用**同一 JOB_ID**（`jobs/` 目录已存在，自动触发增量模式）。

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "nfs://{SOURCE_IP}{NFS_EXPORT}"
```

### 4a. 验证 Scanned Statistics

增量扫描仍会遍历当前文件系统全部条目。

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks={POST_MUTATE_SYMLINKS}。

### 4b. 验证 Incremental Statistics

NFS v3 使用 Fh3 模式检测变更：
- `detect_new_items`: 用 `file_handle NOT IN base` → 仅纯新增项（rename 不计入 New）
- `detect_changed_items`: 用 `file_handle` 匹配 + size/mtime 变化
- `detect_deleted_items`: old-state 记录 + fh3 分组 → 1 条=Deleted, 2 条=Renamed

Expected Incremental Statistics:

```
   ├─ New:          7 total | dirs      2 | files      3 | symlinks    2
   ├─ Changed:      9 total | dirs      0 | files      9 | symlinks    0
   ├─ Renamed:      7 total | dirs      1 | files      4 | symlinks    2
   └─ Deleted:      8 total | dirs      1 | files      5 | symlinks    2
```

**计数说明**：
- New 7 = 2 dirs + 3 files + 2 symlinks（纯新增，fh3 不存在于 base）
- Changed 9 = 内容变更 2 files + mtime 变更 7 files：
  - 内容变更（size+mtime）：d1/d1_1/file1.txt 追加 + d2/d2_2/d2_2_1/file3.txt 覆盖
  - touch mtime（主树）：d4/d4_3/file1.txt + d1/d1_2/file3.txt + d2/d2_3/d2_3_2/file2.txt（3 files）
  - touch mtime（special）：file_2020-01-01... + file_2026-01-01...（2 files）
  - mixed attr（含 mtime）：exec_new.sh + readonly_old.txt（2 files）
  - 注意：chmod-only / chown-only 不改 mtime，Fh3 模式**不检测**；touch dirs 已移除
- Renamed 7 = 1 file + 1 symlink + 1 dir 级联（dir + 3 files + 1 symlink）
- Deleted 8 = 1 dir(d3_3_3) + 5 files(3 in dir + 2 standalone) + 2 symlinks(1 in dir + 1 standalone)

**若任意计数不符，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -40
```

2. 检查 ClickHouse 增量表原始记录：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,relative_path,is_dir,is_symlink+FROM+default.incremental_nfs_v3_incr_scan+FINAL+ORDER+BY+operation_type,relative_path+FORMAT+TabSeparated"
```

3. 根据原始记录定位不符项的具体路径和操作类型。

### 4c. ClickHouse base 表验证（增量后当前状态）

增量扫描后 base 表包含新旧两种 current_state 记录，需过滤查询：

```bash
# 先查 scan_state
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.state_nfs_v3_incr_scan+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "current scan_state: $STATE"
[[ -z "$STATE" ]] && echo "ERROR: scan_state 为空，请检查 ClickHouse 连接和 state 表" && exit 1
```

Expected: STATE 非空。

```bash
# 用 scan_state 过滤 base 表（分类计数）
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_incr_scan+FINAL+WHERE+current_state%3D${STATE}+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行，顺序不定）：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 333
true    false   {POST_MUTATE_DIRS}       # 目录 = 114
false   true    {POST_MUTATE_SYMLINKS}   # 软链接 = 79
```

```bash
# 交叉验证：base 表当前状态总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_nfs_v3_incr_scan+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `526`（{POST_MUTATE_DIRS}+{POST_MUTATE_FILES}+{POST_MUTATE_SYMLINKS} = 114+333+79）。

**若总行数不符，停止并调查。**

### 4d. ClickHouse incremental 表验证（变更记录明细）

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,is_dir,is_symlink,count(*)+FROM+default.incremental_nfs_v3_incr_scan+FINAL+GROUP+BY+operation_type,is_dir,is_symlink+ORDER+BY+operation_type,is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（11 行）：

```
changed false   false   9
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

**changed 计数说明**：files=9（内容变更 2 + mtime 变更 7）, dirs=2（mtime 变更）。chmod/chown 仅改 mode/uid/gid 不改 mtime，Fh3 模式不检测为 changed。

**若任意行不符，停止并调查。不执行后续清理。**

---

## Step 5: 清理环境

Only proceed after all Step 4 checks pass.

**5a、5b、5c、5d 可并发执行**。

### 5a. 清理源端 NFS（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 5b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_incr_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 5c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_incr_scan*"
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
- [ ] Baseline data created: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks={BASELINE_SYMLINKS} (Step 1)
- [ ] Full scan counts match baseline (Step 2a)
- [ ] ClickHouse base table verified at baseline (Step 2b)
- [ ] Mutations applied, find verified: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks={POST_MUTATE_SYMLINKS} (Step 3)
- [ ] Incremental scan Scanned Statistics: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks={POST_MUTATE_SYMLINKS} (Step 4a)
- [ ] Incremental Statistics: new=7/changed=11/renamed=7/deleted=8 (Step 4b)
- [ ] ClickHouse base table post-incremental: total=526 (Step 4c)
- [ ] ClickHouse incremental table: 11 rows verified (Step 4d)
- [ ] Environment cleaned (Step 5)
