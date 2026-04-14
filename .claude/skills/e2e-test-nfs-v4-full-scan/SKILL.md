---
name: e2e-test-nfs-v4-full-scan
description: >
  This skill should be used when the user asks to "run nfs v4 full scan test",
  "test full scan nfs v4", "nfs v4 全量扫描测试", "nfs v4.1 full scan",
  or mentions running the full-scan workflow against an NFSv4.1 server.
---

# NFS v4.1 Full Scan Test Skill

## Overview

端到端全量扫描测试（NFS v4.1 存储）。
验证完整管线：测试数据创建（含 NFSv4 ACL 和 xattr）→ 全量扫描 → CLI 输出验证 → ClickHouse base 表验证 → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv4.1。
测试数据通过 SSH 在远端创建，同时设置 NFSv4 ACL 和 named attributes（xattr）。

**NFSv4.1 vs NFSv3 关键差异**：
- URL 需显式指定 `?version=4.1`（否则默认自动协商，可能 fallback 到 v3）
- 支持 NFSv4 ACL（GETACL/SETACL RPC）
- 支持 named attributes（xattr，RFC 8276）
- OPEN/CLOSE lifecycle（stateid 管理）
- 文件句柄仍通过 `file_handle` 字段传递（增量扫描使用 Fh3 策略）

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | 10.131.9.13 |
| NFS_EXPORT | `` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}?version=4.1` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `10.128.133.213:8123` |
| JOB_ID | `nfs-v4-full-scan` |
| SANITIZED_JOB_ID | `nfs_v4_full_scan` |
| BASE_TABLE | `base_nfs_v4_full_scan` |
| STATE_TABLE | `state_nfs_v4_full_scan` |
| EXPECTED_DIRS | 113 |
| EXPECTED_FILES | 335 |
| EXPECTED_SYMLINKS | 79 |

**注意**：NFS v4.1 使用伪根（pseudo-root）机制，URL 中的路径必须是相对于 `fsid=0` 的 export。
当前配置中 `/export/nfs4` 设置了 `fsid=0`，因此 NFS v4.1 URL 应使用 `/` 作为路径。

ClickHouse 表名：
- `base_nfs_v4_full_scan`
- `state_nfs_v4_full_scan`

---

## Step 0: 清理测试环境（确保干净初始状态）

**0a–0d 可并发执行**。

### 0a. 清理源端 NFS 数据（SSH）

```bash
ssh ubuntu@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

Expected: `source cleaned`。

### 0b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_full_scan%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_scan*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_scan*"
```

Expected: 无输出（空）。

### 0d. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 上传并执行测试数据脚本

### 1a. 上传脚本

```bash
scp .claude/skills/e2e-test-nfs-v4-full-scan/scripts/setup-nfs4-test-data.sh ubuntu@{SOURCE_IP}:/tmp/setup-nfs4-test-data.sh
```

### 1b. 执行脚本

```bash
ssh ubuntu@{SOURCE_IP} 'sudo bash /tmp/setup-nfs4-test-data.sh'
```

脚本功能：
1. 创建 4x4x3 主目录树 + special 特殊目录（不同 mode/uid/gid/mtime 组合）
2. 创建 exotic_names 目录（特殊字符和中文命名）：空格/制表符、括号类、`!@#$%^&` 等特殊符号、标点符号、单双引号/问号/星号、纯中文目录与文件、中文+特殊字符混合、隐藏文件/目录、超长文件名、Unicode（emoji/全角/符号）
3. 对部分文件/目录设置 NFSv4 ACL（`nfs4_setfacl`，~10 条 ACL，含继承/拒绝/叠加）
4. 对部分文件/目录设置 named attributes/xattr（`setfattr`，~15 个 xattr，含多值/长值/中文/JSON）

Expected output (last lines):

```
find:    dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
ACL set on: N entries
xattr set on: M entries
OK: 数量校验通过，ACL/xattr 设置完成
```

注意：ACL/xattr 数量可能因工具可用性而变化（脚本会自动安装），不影响基础计数验证。

**Stop if the script exits non-zero.**

---

## Step 2: 全量扫描 + 全面验证

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {JOB_ID} "{SOURCE_URL}"
```

### 2a. 验证 CLI Scanned Statistics

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}.**
If counts do not match, stop and investigate.

### 2b. ClickHouse base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.{BASE_TABLE}+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {EXPECTED_FILES}      # 普通文件 = 335
true    false   {EXPECTED_DIRS}       # 目录 = 113
false   true    {EXPECTED_SYMLINKS}   # 软链接 = 79
```

### 2c. 验证 file_handle 非空

NFSv4.1 通过 `file_handle` 字段传递文件句柄（增量扫描 Fh3 策略依赖此字段）：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+file_handle%3D%27%27+FORMAT+TabSeparated"
```

Expected: `0`（所有记录都有 file_handle）。

### 2d. 验证 mode/uid/gid 字段

NFSv4.1 扫描应记录文件权限属性：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+mode%3D0+AND+is_dir%3Dfalse+AND+is_symlink%3Dfalse+FORMAT+TabSeparated"
```

Expected: `0`（所有普通文件都有 mode 字段）。

### 2e. 验证 state 表 + base 表总行数（交叉验证）

```bash
STATE=$(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+scan_state+FROM+default.{STATE_TABLE}+FINAL+WHERE+id%3D1+FORMAT+TabSeparated")
echo "scan_state: ${STATE}"
[[ -z "${STATE}" ]] && echo "ERROR: scan_state 为空，state 表写入失败" && exit 1
```

Expected: STATE 非空。

```bash
# 用 scan_state 交叉验证 base 表总行数
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+count(*)+FROM+default.{BASE_TABLE}+FINAL+WHERE+current_state%3D${STATE}+FORMAT+TabSeparated"
```

Expected: `527`（{EXPECTED_DIRS}+{EXPECTED_FILES}+{EXPECTED_SYMLINKS} = 113+335+79）。

**若总行数不符，停止并调查。**

### 2f. 独立文件系统核查（交叉验证 ClickHouse）

```bash
ssh ubuntu@{SOURCE_IP} "echo 'dirs:'; find {NFS_EXPORT}/test-data -mindepth 1 -type d | wc -l; echo 'files:'; find {NFS_EXPORT}/test-data -type f | wc -l; echo 'symlinks:'; find {NFS_EXPORT}/test-data -type l | wc -l"
```

Expected:
```
dirs:     113
files:    335
symlinks: 79
```

**若与 ClickHouse base 表（Step 2b）不一致，停止并调查。**

**若任意验证不符，停止并调查。**

**If scan fails，按以下步骤排查：**

1. 查看日志：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **版本协商失败（fallback 到 v3）**: URL 中 `?version=4.1` 不生效时会 fallback v3。确认 NFS 服务端已开启 NFSv4.1（`/proc/fs/nfsd/versions` 应包含 `+4.1`）。
   - **NFS OPEN 失败（stateid 错误）**: NFSv4.1 的 OPEN RPC 失败。可能是服务端 lease 过期或 session 问题。重新扫描通常可恢复。
   - **readdir 返回空**：NFSv4.1 的 READDIR 行为与 v3 不同。检查 export 配置和服务端日志（`/var/log/syslog` 或 `journalctl -u nfs-kernel-server`）。
   - **xattr/ACL 相关 WARNING**: 扫描本身不读取 ACL/xattr，若出现相关 WARN 可能是服务端配置问题，不影响计数验证。

---

## Step 3: 清理

**3a–3d 可并发执行**。

### 3a. 清理源端 NFS

```bash
ssh ubuntu@{SOURCE_IP} 'sudo rm -rf {NFS_EXPORT}/test-data && echo "source cleaned"'
```

### 3b. 清理 ClickHouse 表

```bash
for table in $(curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_full_scan%25%27+FORMAT+TabSeparated"); do
  curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.${table}"
  echo "Dropped: $table"
done
```

### 3c. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_scan*" | xargs rm -rf
```

### 3d. 清理日志

```bash
rm -rf target/debug/logs/*
```

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0)
- [ ] Test data created with ACL and xattr: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 1)
- [ ] Full scan counts match (Step 2a)
- [ ] ClickHouse base table: 3-row distribution verified (Step 2b)
- [ ] file_handle non-empty for all records (Step 2c)
- [ ] mode/uid/gid fields populated (Step 2d)
- [ ] Environment cleaned (Step 3)
