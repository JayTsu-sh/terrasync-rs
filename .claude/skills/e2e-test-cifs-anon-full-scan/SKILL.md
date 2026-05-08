---
name: e2e-test-cifs-anon-full-scan
description: >
  This skill should be used when the user asks to "run cifs anonymous full scan test",
  "test anonymous cifs full scan", "cifs 匿名全量扫描测试", "cifs anon full scan e2e",
  "test full scan against anonymous CIFS share",
  or mentions running the full-scan workflow against an anonymous CIFS/SMB share (no credentials).
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# CIFS Anonymous Full Scan Test Skill

## Overview

端到端全量扫描测试（匿名 CIFS/SMB 存储）：创建测试数据 → 全量扫描 → 验证 CLI 输出和 ClickHouse base 表。

**特点**：使用匿名访问（`guest` 用户 + 空密码），SMB URL 格式为 `smb://guest:@host/share/path`。
CIFS 不支持 symlink，`file_handle` 字段非空（可用 Fh3 策略）。

## Prerequisites

- CIFS 共享已配置为允许匿名访问（`guest ok = yes`，`map to guest = Bad User`）
- `smbclient` 已安装（用于测试数据管理）

## Constants

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `CIFS_ANON_SOURCE_HOST` |
| CIFS_SHARE | `CIFS_ANON_SHARE`（default: `share`）|
| CIFS_PORT | `CIFS_ANON_PORT`（default: `445`）|
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### Skill 常量
| Name | Value |
|------|-------|
| JOB_ID | `cifs-anon-full-scan` |
| SANITIZED_JOB_ID | `cifs_anon_full_scan` |
| SOURCE_HOST | `192.168.50.173` |
| CIFS_SHARE | `share` |
| CIFS_URL | `smb://guest:@192.168.50.173/share/test-data` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| EXPECTED_DIRS | 39 |
| EXPECTED_FILES | 117 |

ClickHouse 表名：
- `base_cifs_anon_full_scan`
- `state_cifs_anon_full_scan`

**注意**：CIFS 不支持 symlink，symlinks 始终为 0。

---

## Step 0: 清理测试环境

**0a–0c 可并发执行**。

### 0a. 清理 CIFS 共享数据（匿名）

```bash
smbclient "//192.168.50.173/share" -N -c "deltree test-data" 2>/dev/null || true
```

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25cifs_anon_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
done
```

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*cifs_anon_full_scan*" | xargs rm -rf
```

---

## Step 1: 创建测试数据

```bash
CIFS_HOST=192.168.50.173 CIFS_SHARE=share bash .claude/skills/_shared/cifs-anon/setup-cifs-anon-test-data.sh
```

脚本创建 3x3x3 目录树（无 symlink）：39 dirs / 117 files / 0 symlinks。

---

## Step 2: 全量扫描

```bash
./target/debug/terrasync -c examples/config.toml -l trace scan --id cifs-anon-full-scan "smb://guest:@192.168.50.173/share/test-data"
```

### 3a. 验证 CLI Scanned Statistics

dirs=39, files=117, symlinks=0。

### 3b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_cifs_anon_full_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected：
```
false   false   117    # 普通文件
true    false   39     # 目录
```

### 3c. 验证 file_handle 字段非空

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_cifs_anon_full_scan+FINAL+WHERE+file_handle%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`

---

## Step 3: 清理环境

```bash
smbclient "//192.168.50.173/share" -N -c "deltree test-data" 2>/dev/null || true
find jobs -maxdepth 1 -type d -name "*cifs_anon_full_scan*" | xargs rm -rf
```

---

## Completion Criteria

- [ ] 测试环境已清理（Step 0）
- [ ] 测试数据创建：dirs=39 / files=117 / symlinks=0（Step 1）
- [ ] 全量扫描 CLI 计数匹配（Step 2a）
- [ ] ClickHouse base 表验证通过（Step 2b）
- [ ] file_handle 非空确认（Step 2c）
- [ ] 环境已清理（Step 3）
