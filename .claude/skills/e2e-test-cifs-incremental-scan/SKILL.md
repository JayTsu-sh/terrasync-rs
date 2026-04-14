---
name: e2e-test-cifs-incremental-scan
description: >
  This skill should be used when the user asks to "run cifs incremental scan test",
  "test incremental scan cifs", "cifs 增量扫描测试", "cifs incremental scan e2e",
  "test the incremental scan pipeline against CIFS/SMB",
  or mentions running the full-scan → mutate → incremental-scan → verify workflow
  against the CIFS test environment.
---

# CIFS Incremental Scan Test Skill

## Overview

端到端增量扫描测试（CIFS/SMB 存储）：全量扫描建基线 → 变更（增删改+rename） → 增量扫描检测变更 → 验证 CLI 输出和 ClickHouse 数据库。

**CIFS 特点**：
- 使用 `smb://` URL，产出 NASEntry
- 有 `file_handle` 字段 → 使用 `JoinStrategy::Fh3` 模式（与 NFS v3 一致）
- **不支持 symlink**
- rename 通过 fh3 精确识别 → Renamed（非 New+Deleted）

`terrasync` 本地运行（使用 `{CONFIG}`），通过网络访问 CIFS 共享。
测试数据通过 `smbclient` 或挂载共享后管理。

## Constants

| Name | Value |
|------|-------|
| CIFS_HOST | `{CIFS_HOST}` |
| CIFS_PORT | `{CIFS_PORT}` |
| CIFS_USER | `{CIFS_USER}` |
| CIFS_PASS | `{CIFS_PASS}` |
| CIFS_SHARE | `{CIFS_SHARE}` |
| CIFS_URL | `smb://{CIFS_USER}:{CIFS_PASS}@{CIFS_HOST}/{CIFS_SHARE}/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `10.128.133.213:8123` |
| JOB_ID | `cifs-incr-scan` |
| SANITIZED_JOB_ID | `cifs_incr_scan` |
| BASELINE_DIRS | 40 |
| BASELINE_FILES | 117 |
| POST_MUTATE_DIRS | 41 |
| POST_MUTATE_FILES | 115 |

ClickHouse 表名：
- `base_cifs_incr_scan`
- `incremental_cifs_incr_scan`
- `state_cifs_incr_scan`

**注意**：CIFS 无 symlink，所有 symlink 计数为 0。

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 清理 CIFS 共享数据

```bash
smbclient "//{CIFS_HOST}/{CIFS_SHARE}" -U "{CIFS_USER}%{CIFS_PASS}" -c "deltree test-data" 2>/dev/null || true
echo "CIFS cleaned"
```

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_incr_scan*" | xargs rm -rf
```

### 0d. 清理日志文件

```bash
rm -rf target/debug/logs/*
```

---

## Step 1: 创建测试数据

```bash
bash .claude/skills/cifs-incremental-scan/scripts/setup-cifs-test-data.sh
```

创建 3x3x3 目录树（无 symlink）：40 dirs / 117 files / 0 symlinks。

**Stop if the script exits non-zero.**

---

## Step 2: 全量扫描（建立基线）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{CIFS_URL}"
```

### 2a. 验证 CLI Scanned Statistics

**Verify**: dirs={BASELINE_DIRS}, files={BASELINE_FILES}, symlinks=0。

### 2b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_incr_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected：

```
false   false   {BASELINE_FILES}      # 普通文件 = 117
true    false   {BASELINE_DIRS}       # 目录 = 40
```

**若任意计数不符，停止并调查。**

---

## Step 3: 执行变更脚本

```bash
bash .claude/skills/cifs-incremental-scan/scripts/mutate-cifs-test-data.sh
```

变更摘要（与 NFS v3 类似，但无 symlink 操作）：
- **ADD**: 2 dirs, 3 files
- **MODIFY**: 2 files（覆盖内容改变 size+mtime）
- **RENAME**: 1 file, 1 dir（含级联 3 files）
- **DELETE**: 1 dir+3 files, 2 standalone files

**Stop if the script exits non-zero.**

---

## Step 4: 增量扫描 + 全面验证

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{CIFS_URL}"
```

### 4a. 验证 Scanned Statistics

**Verify**: dirs={POST_MUTATE_DIRS}, files={POST_MUTATE_FILES}, symlinks=0。

### 4b. 验证 Incremental Statistics

CIFS 有 `file_handle` → Fh3 模式 → rename 精确识别为 Renamed。

Expected Incremental Statistics:

```
   ├─ New:          5 total | dirs      2 | files      3 | symlinks    0
   ├─ Changed:      2 total | dirs      0 | files      2 | symlinks    0
   ├─ Renamed:      5 total | dirs      1 | files      4 | symlinks    0
   └─ Deleted:      7 total | dirs      1 | files      6 | symlinks    0
```

**计数说明**（无 symlink 版本）：
- New 5 = 2 dirs + 3 files（纯新增，fh3 不存在于 base）
- Changed 2 = 2 files（内容变化）
- Renamed 5 = 1 file + 1 dir 级联（dir + 3 files）
- Deleted 7 = 1 dir + 3 files (d3_3_3) + 2 standalone files + 1 standalone file (from rename source already counted)

**注意**：具体 Deleted 计数取决于变更脚本的实际操作。请在第一次运行后根据实际输出调整。

### 4c. ClickHouse base 表验证（增量后当前状态）

```bash
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.state_cifs_incr_scan+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "current scan_state: $STATE"
[[ -z "$STATE" ]] && echo "ERROR: scan_state 为空，请检查 ClickHouse 连接和 state 表" && exit 1
```

Expected: STATE 非空。

```bash
# 分类计数验证
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_incr_scan+FINAL+WHERE+current_state%3D${STATE}+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected：

```
false   false   {POST_MUTATE_FILES}      # 普通文件 = 115
true    false   {POST_MUTATE_DIRS}       # 目录 = 41
```

```bash
# 交叉验证：base 表总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_cifs_incr_scan+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `156`（{POST_MUTATE_DIRS}+{POST_MUTATE_FILES} = 41+115，CIFS 无 symlink）。

**若总行数不符，停止并调查。**

### 5d. ClickHouse incremental 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+operation_type,is_dir,is_symlink,count(*)+FROM+default.incremental_cifs_incr_scan+FINAL+GROUP+BY+operation_type,is_dir,is_symlink+ORDER+BY+operation_type,is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（CIFS 无 symlink，有 rename）：

```
changed false   false   2
deleted false   false   6
deleted true    false   1
new     false   false   3
new     true    false   2
rename  false   false   4
rename  true    false   1
```

**若任意行不符，停止并调查。**

---

## Step 6: 清理环境

**6a–6d 可并发执行**。

### 6a. 清理 CIFS 共享数据

```bash
smbclient "//{CIFS_HOST}/{CIFS_SHARE}" -U "{CIFS_USER}%{CIFS_PASS}" -c "deltree test-data" 2>/dev/null || true
```

### 6b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_incr_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

### 6c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_incr_scan*" | xargs rm -rf
```

### 6d. 清理日志文件

```bash
rm -rf target/debug/logs/*
```

---

## NFS v3 vs CIFS 增量扫描对比

| 方面 | NFS v3 | CIFS |
|------|--------|------|
| URL 格式 | `nfs://ip/export` | `smb://user:pass@host/share` |
| 检测策略 | `JoinStrategy::Fh3` | `JoinStrategy::Fh3` |
| Rename 检测 | fh3 精确识别 → Renamed | fh3 精确识别 → Renamed |
| Symlink | 支持（36） | 不支持（0） |
| 目录 | 真实 inode | 真实（SMB 文件属性） |
| 测试数据管理 | SSH + shell | smbclient 或挂载 |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Binary compiled (Step 1)
- [ ] Baseline data: dirs={BASELINE_DIRS}/files={BASELINE_FILES}/symlinks=0 (Step 2)
- [ ] Full scan counts match baseline (Step 3a)
- [ ] ClickHouse base table verified at baseline (Step 3b)
- [ ] Mutations applied (Step 4)
- [ ] Incremental scan Scanned Statistics: dirs={POST_MUTATE_DIRS}/files={POST_MUTATE_FILES}/symlinks=0 (Step 5a)
- [ ] Incremental Statistics verified (Step 5b)
- [ ] ClickHouse base table post-incremental verified (Step 5c)
- [ ] ClickHouse incremental table verified (Step 5d)
- [ ] Environment cleaned (Step 6)
