---
name: e2e-test-cifs-full-scan
description: >
  This skill should be used when the user asks to "run cifs full scan test",
  "test full scan cifs", "cifs 全量扫描测试", "cifs full scan e2e",
  "test the full scan pipeline against CIFS/SMB",
  or mentions running the full-scan → verify workflow against the CIFS test environment.
---

# CIFS Full Scan Test Skill

## Overview

端到端全量扫描测试（CIFS/SMB 存储）：创建测试数据 → 全量扫描 → 验证 CLI 输出和 ClickHouse base 表。

**特点**：CIFS 使用 `smb://` URL，产出 NASEntry，有 `file_handle` 字段（可用 Fh3 策略）。CIFS **不支持 symlink**。

`terrasync` 本地运行（使用 `{CONFIG}`），通过网络访问 CIFS 共享。
测试数据通过 `smbclient` 或挂载共享后创建。

## Prerequisites

- CIFS 共享已配置且可访问
- `smbclient` 已安装（用于测试数据管理）

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
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| JOB_ID | `cifs-full-scan` |
| SANITIZED_JOB_ID | `cifs_full_scan` |
| EXPECTED_DIRS | 40 |
| EXPECTED_FILES | 117 |

ClickHouse 表名：
- `base_cifs_full_scan`
- `state_cifs_full_scan`

**注意**：CIFS 不支持 symlink，symlinks 始终为 0。

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 清理 CIFS 共享数据

```bash
smbclient "//{CIFS_HOST}/{CIFS_SHARE}" -U "{CIFS_USER}%{CIFS_PASS}" -c "deltree test-data" 2>/dev/null || true
echo "CIFS cleaned"
```

验证：

```bash
smbclient "//{CIFS_HOST}/{CIFS_SHARE}" -U "{CIFS_USER}%{CIFS_PASS}" -c "ls test-data/*" 2>/dev/null | wc -l
```

Expected: `0` 或报错（目录不存在）。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*cifs_full_scan*"
```

Expected: 无输出（空）。

### 0d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 创建测试数据

Use the Bash tool to run the setup script:

```bash
bash .claude/skills/cifs-full-scan/scripts/setup-cifs-test-data.sh
```

脚本需创建 3x3x3 目录树（无 symlink）：40 dirs / 117 files / 0 symlinks。

**Stop if the script exits non-zero.**

---

## Step 2: 全量扫描 + 全面验证

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{CIFS_URL}"
```

### 3a. 验证 CLI Scanned Statistics

**Verify**: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks=0。

If counts do not match, stop and investigate.

### 3b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_full_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（两行，CIFS 无 symlink）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 117
true    false   {EXPECTED_DIRS}       # 目录 = 40
```

**若任意计数不符，停止并调查。**

### 3c. 验证 file_handle 字段非空（CIFS 特有）

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_cifs_full_scan+FINAL+WHERE+file_handle%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`（所有记录的 file_handle 均非空，确认可用 Fh3 策略）。

### 3d. 验证 state 表 + base 表总行数（交叉验证）

```bash
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.state_cifs_full_scan+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "scan_state: ${STATE}"
[[ -z "${STATE}" ]] && echo "ERROR: scan_state 为空，state 表写入失败" && exit 1
```

Expected: STATE 非空。

```bash
# 用 scan_state 交叉验证 base 表总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_cifs_full_scan+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `157`（{EXPECTED_DIRS}+{EXPECTED_FILES} = 40+117，CIFS 无 symlink）。

**若总行数不符，停止并调查。**

---

## Step 3: 清理环境

**4a–4d 可并发执行**。

### 4a. 清理 CIFS 共享数据

```bash
smbclient "//{CIFS_HOST}/{CIFS_SHARE}" -U "{CIFS_USER}%{CIFS_PASS}" -c "deltree test-data" 2>/dev/null || true
```

### 4b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

### 4c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_full_scan*" | xargs rm -rf
```

### 4d. 清理日志文件

```bash
rm -rf target/debug/logs/*
```

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Test data created: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks=0 (Step 1)
- [ ] Full scan CLI counts match (Step 2a)
- [ ] ClickHouse base table verified (Step 2b)
- [ ] file_handle non-empty confirmed (Step 2c)
- [ ] Environment cleaned (Step 3)
