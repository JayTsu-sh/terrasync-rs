---
name: e2e-test-nfs-v3-full-scan
description: >
  This skill should be used when the user asks to "run nfs v3 full scan test",
  "test full scan nfs v3", "nfs v3 全量扫描测试", "nfs v3 full scan e2e",
  "test the full scan pipeline against NFSv3",
  or mentions running the full-scan → verify workflow against the NFSv3 test environment ({SOURCE_IP}).
---

> **自动化模式**：直接运行 `python scripts/run.py`（调试时才按下方步骤执行）


# NFS v3 Full Scan Test Skill

## Overview

端到端全量扫描测试（NFS v3 存储）：创建测试数据 → 全量扫描 → 验证 CLI 输出和 ClickHouse base 表。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络访问 NFSv3。
测试数据通过 SSH 在远端创建。

## Constants

> 协议常量来源 `harness-run/scripts/protocol_constants.py`（`NfsV3`）；环境变量从 `harness-run/.env` 加载。

### 环境变量
| Name | Env Key |
|------|---------|
| SOURCE_IP | `NFS_V3_SOURCE_IP` |
| CLICKHOUSE_HOST | `CLICKHOUSE_HOST` |
| BINARY | `TERRASYNC_BINARY`（default: `./target/debug/terrasync`）|
| CONFIG | `TERRASYNC_CONFIG`（default: `examples/config.toml`）|

### 协议常量（`NfsV3`）
| Name | Value |
|------|-------|
| NFS_EXPORT | `/export/nfs` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}` |
| EXPECTED_DIRS | 113 |
| EXPECTED_FILES | 335 |
| EXPECTED_SYMLINKS | 79 |

### Skill 常量
| Name | Value |
|------|-------|
| JOB_ID | `nfs-v3-full-scan` |

ClickHouse 表名：
- `base_nfs_v3_full_scan`
- `state_nfs_v3_full_scan`

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo find {NFS_EXPORT} -mindepth 1 -maxdepth 1 -exec rm -rf {} + && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_scan*"
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

复用 nfs-v3-e2e 的 setup-test-data.sh：

```bash
scp .claude/skills/e2e-test-nfs-v3/scripts/setup-test-data.sh root@{SOURCE_IP}:/tmp/setup-test-data.sh
```

Expected: 无错误输出，scp 退出码为 0。

---

## Step 1b: 执行测试脚本创建测试数据（SOURCE_IP）

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-test-data.sh'
```

Expected output (last lines):

```
find:    dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
total entries: 527
OK: 数量校验通过
```

脚本创建内容：
1. 4x4x3 主目录树 + special 特殊目录（不同 mode/uid/gid/mtime/symlink 组合）
2. exotic_names 目录（特殊字符和中文命名）：空格/制表符、括号/方括号/花括号、`!@#$%^&`、`+,;=~\`` 等标点、单双引号/问号/星号、纯中文目录与文件、中文+特殊字符混合、隐藏文件/目录（`.` 开头）、超长文件名（200字节 ASCII + 中文）、Unicode（emoji/全角/符号）

**Stop if the script exits non-zero.**

---

## Step 2: 全量扫描 + 全面验证

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "nfs://{SOURCE_IP}{NFS_EXPORT}"
```

### 2a. 验证 CLI Scanned Statistics

**Verify**: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}。

If counts do not match, stop and investigate.

### 2b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v3_full_scan+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行，顺序不定）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 335
true    false   {EXPECTED_DIRS}       # 目录 = 113
false   true    {EXPECTED_SYMLINKS}   # 软链接 = 79
```

**若任意计数不符，停止并调查。**

### 2c. 验证 state 表 + base 表总行数（交叉验证）

```bash
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.state_nfs_v3_full_scan+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "scan_state: ${STATE}"
[[ -z "${STATE}" ]] && echo "ERROR: scan_state 为空，state 表写入失败" && exit 1
```

Expected: STATE 非空。

```bash
# 用 scan_state 交叉验证 base 表总行数（独立于分类计数验证）
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.base_nfs_v3_full_scan+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `527`（{EXPECTED_DIRS}+{EXPECTED_FILES}+{EXPECTED_SYMLINKS} = 113+335+79）。

**若总行数不符，停止并调查。**

### 2d. 独立文件系统核查（交叉验证 ClickHouse）

直接对 NFS 服务端执行 `find`（需要 sudo 访问限制性目录），结果应与 ClickHouse base 表一致：

```bash
ssh root@{SOURCE_IP} "sudo find {NFS_EXPORT}/test-data -type d | wc -l; sudo find {NFS_EXPORT}/test-data -type f | wc -l; sudo find {NFS_EXPORT}/test-data -type l | wc -l"
```

Expected:
```
dirs:     113
files:    335
symlinks: 79
```

**若与 ClickHouse base 表（Step 2b）不一致，说明 scan 遗漏或多写了条目，停止并调查。**

### 2e. 元数据校验（mtime/uid/gid/mode 一致性验证）

上传并执行元数据校验脚本，对比 NFS 文件系统实际属性与 ClickHouse 数据库中的记录：

```bash
scp .claude/skills/e2e-test-nfs-v3-full-scan/scripts/verify-metadata.sh root@{SOURCE_IP}:/tmp/verify-metadata.sh
ssh root@{SOURCE_IP} 'sudo bash /tmp/verify-metadata.sh'
```

脚本功能：
1. 从 ClickHouse 导出所有条路的 metadata（relative_path, uid, gid, mode, mtime）
2. 对 NFS 文件系统执行 `stat` 获取实际属性
3. 逐条对比，报告不一致的条目

Expected output:
```
=== Metadata Verification ===
Total entries: 527
Matched: 527
Mismatch: 0

✓ All metadata verified successfully
```

**若发现不一致，停止并调查。**

---

## Step 3: 清理环境

**3a–3d 可并发执行**。

### 3a. 清理源端 NFS（SSH）

```bash
ssh root@{SOURCE_IP} 'sudo find {NFS_EXPORT} -mindepth 1 -maxdepth 1 -exec rm -rf {} + && echo "source cleaned"'
```

Expected: `source cleaned`。

### 3b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v3_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 3c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v3_full_scan*"
```

Expected: 无输出（空）。

### 3d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Baseline data created: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 1)
- [ ] Full scan CLI counts match (Step 2a)
- [ ] ClickHouse base table verified (Step 2b)
- [ ] State table verified (Step 2c)
- [ ] Independent filesystem check passed (Step 2d)
- [ ] Metadata verified: uid/gid/mode/mtime consistent with ClickHouse (Step 2e)
- [ ] Environment cleaned (Step 3)
