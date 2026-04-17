---
name: e2e-test-nfs-v4-full-sync
description: >
  This skill should be used when the user asks to "run nfs v4 full sync test",
  "test full sync nfs v4", "nfs v4 全量拷贝测试", "nfs v4.1 full sync",
  "test the full nfs v4 sync pipeline",
  or mentions running the full scan/sync/verify/cleanup workflow
  against the NFSv4.1 test environment ({SOURCE_IP} → {DEST_IP}).
---

# NFS v4.1 Full Sync Test Skill

## Overview

端到端全量拷贝测试（NFS v4.1 存储）。
验证完整管线：测试数据创建（含 NFSv4 ACL 和 xattr）→ 源端扫描 → 全量同步（含 ACL/xattr 复制）→ 目标端验证 → integrity-check → 清理。
`terrasync` 本地运行（使用 `{CONFIG}`），通过网络直接访问 NFSv4.1。
测试数据通过 SSH 在远端创建和验证。

**NFSv4.1 full sync 关键特性**：
- URL 加 `?version=4.1` 强制使用 NFSv4.1
- `--enable-acl` 标志启用 NFSv4 ACL 复制（GETACL → SETACL）
- xattr（named attributes）随 `--enable-acl` 自动复制（若两端都支持）
- OPEN/CLOSE stateid 生命周期管理（开文件建立 stateid，关文件释放）
- 目标端验证需用 `nfs4_getfacl` 和 `getfattr` 验证 ACL/xattr 是否正确复制

## Constants

| Name | Value |
|------|-------|
| SOURCE_IP | 192.168.50.173 |
| DEST_IP |  192.168.50.23 |
| NFS_EXPORT | `/` |
| SOURCE_URL | `nfs://{SOURCE_IP}{NFS_EXPORT}?version=4.1` |
| DEST_URL | `nfs://{DEST_IP}{NFS_EXPORT}?version=4.1` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| CLICKHOUSE_HOST | `192.168.50.173:8123` |
| SRC_SCAN_JOB_ID | `nfs-v4-full-sync-src` |
| SYNC_JOB_ID | `nfs-v4-full-sync` |
| DST_SCAN_JOB_ID | `nfs-v4-full-sync-dst` |
| IC_JOB_ID | `nfs-v4-full-sync-ic` |
| EXPECTED_DIRS | 113 |
| EXPECTED_FILES | 335 |
| EXPECTED_SYMLINKS | 79 |
| EXPECTED_TOTAL | 527 |
| ACL_TEST_FILES | 10 |
| XATTR_TEST_FILES | 15 |

ClickHouse 表名：
- `base_nfs_v4_full_sync_src`（源端扫描主表）
- `state_nfs_v4_full_sync_src`
- `base_nfs_v4_full_sync_dst`（目标端扫描主表）
- `state_nfs_v4_full_sync_dst`
- `base_nfs_v4_full_sync_verify_src`（清理前验证用）
- `state_nfs_v4_full_sync_verify_src`
- `base_nfs_v4_full_sync_verify_dst`
- `state_nfs_v4_full_sync_verify_dst`

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

**注意：共 8 个表需要清理（包括 scan 可能生成的 verify 表）。**

```bash
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_verify_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_verify_dst"
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 0d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_sync*"
```

Expected: 无输出（空）。

### 0e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## Step 1: 上传测试脚本（SOURCE_IP）

```bash
scp .claude/skills/e2e-test-nfs-v4-full-sync/scripts/setup-nfs4-test-data.sh root@{SOURCE_IP}:/tmp/setup-nfs4-test-data.sh
```

Expected: 无错误输出，scp 退出码为 0。

---

## Step 2: 执行测试脚本创建数据（SOURCE_IP）

```bash
ssh root@{SOURCE_IP} 'sudo bash /tmp/setup-nfs4-test-data.sh'
```

脚本功能：
1. 创建 4x4x3 主目录树 + special 特殊目录（不同 mode/uid/gid/mtime 组合）
2. 创建 exotic_names 目录（特殊字符和中文命名）：空格/制表符、括号类、`!@#$%^&` 等特殊符号、标点符号、单双引号/问号/星号、纯中文目录与文件、中文 + 特殊字符混合、隐藏文件/目录、超长文件名、Unicode（emoji/全角/符号）
3. 对部分文件/目录设置 NFSv4 ACL（`nfs4_setfacl`，~10 条 ACL，含继承/拒绝/叠加）
4. 对部分文件/目录设置 named attributes/xattr（`setfattr`，~15 个 xattr，含多值/长值/中文/JSON）

Expected output (last lines):

```
Counter: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
find:    dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}
ACL set on: N entries
xattr set on: M entries
OK: 数量校验通过，ACL/xattr 设置完成
```

注意：ACL/xattr 数量可能因工具可用性而变化（脚本会自动安装），不影响基础计数验证。

**Stop if the script exits non-zero.**

---

## Step 3: 扫描源端 NFSv4.1（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {SRC_SCAN_JOB_ID} "{SOURCE_URL}"
```

### 3a. 验证 CLI Scanned Statistics

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}.**
If counts do not match, stop and investigate.

### 3b. ClickHouse 源端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v4_full_sync_src+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {EXPECTED_FILES}
true    false   {EXPECTED_DIRS}
false   true    {EXPECTED_SYMLINKS}
```

### 3c. 验证源端 ACL 设置

**注意：`nfs4_getfacl` 无法直接读取本地 export 路径（`Operation not supported`），必须通过 NFSv4.1 loopback mount 验证。**

```bash
ssh root@{SOURCE_IP} '
  mkdir -p /mnt/acl_verify
  mount -t nfs4 -o vers=4.1 127.0.0.1:/ /mnt/acl_verify
  nfs4_getfacl /mnt/acl_verify/test-data/d1/d1_1/file1.txt
  umount /mnt/acl_verify'
```

Expected: 输出包含自定义 ACE（非纯默认 ACL，即除 `OWNER@/GROUP@/EVERYONE@` 三条之外还有额外 ACE）。

### 3d. 验证源端 xattr 设置

```bash
ssh root@{SOURCE_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

Expected: 输出包含 `user.author`、`user.version` 等 xattr 字段。

---

## Step 4: 全量 Sync 源端 → 目标端（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace sync --id {SYNC_JOB_ID} --enable-acl "{SOURCE_URL}" "{DEST_URL}"
```

Monitor output for:
- 进度信息（progress / copied files）
- `CopyAcl`、`CopyXattr` 相关日志（INFO 级别，代表 ACL/xattr 正在复制）
- 错误行（`ERROR`、`WARN`）
- 最终完成消息

**Verify: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}, ERROR STATISTICS 为 0。**

**If sync fails (non-zero exit or ERROR STATISTICS > 0)，按以下步骤排查：**

1. 查看日志中的错误：

```bash
grep -E "ERROR|WARN" target/debug/logs/*/app.log | tail -80
```

2. 分析错误原因：
   - **NFS4ERR_STALE（源端）**: 源端文件句柄在 OPEN/READ 期间失效，通常因 NFS 服务端重启。从 Step 0 重新开始。
   - **NFS4ERR_DENIED（ACL 操作）**: 目标端拒绝 SETACL RPC。检查目标端 NFS export 是否支持 ACL（`/etc/exports` 中的 `acl` 选项）且 NFS 服务端已加载 `nfsd_acl` 模块。
   - **Failed to copy ACL（WARN）**: ACL 复制失败但非致命（仅记录 WARN，不影响文件内容复制）。统计 ACL 失败数量，目标端需额外手动验证。
   - **Failed to copy xattr（WARN）**: xattr 复制失败。检查目标端 NFS export 的文件系统是否支持 xattr（ext4/xfs 均支持，但需要 `user_xattr` mount 选项）。
   - **NFS4ERR_NOSPC**: 目标端空间不足。清理后重试。
   - **stateid 管理错误（NFS4ERR_BAD_STATEID）**: OPEN 建立的 stateid 在 WRITE/SETACL 时已失效。检查 lease time 是否过短，或网络中断导致 session 丢失。
   - **NFS4ERR_WRONGSEC**: 安全机制不匹配。检查客户端和服务端的 sec= 配置是否一致（如 sec=sys）。
   - **NFS4ERR_INVAL**: 无效的 RPC 参数。可能是服务端不支持某些 NFSv4.1 特性。
   - **Connection refused / timeout**: NFS 服务不可达，检查网络和 NFS 服务状态。
   - **Permission denied**: UID/GID 不匹配或 export 权限配置问题，检查 NFS export 权限配置（/etc/exports）。

3. 根据日志分析根因并修复，从头重试。

**注意**：URL 中的 `?version=4.1` 会强制使用 NFSv4.1 协议，不会 fallback 到 v3。若连接失败，请检查：
- NFS 服务端是否开启 NFSv4.1 支持（`/proc/fs/nfsd/versions` 应包含 `+4.1`）
- export 配置是否正确（`/etc/exports` 中的 `fsid=0` 设置）
- 防火墙是否允许 NFS 端口（2049）

**Do not proceed to Step 5 until the sync exits with code 0 and ERROR STATISTICS 为 0。**

---

## Step 5: 验证目标端数据

### 5a. find 直接计数（DEST_IP 上执行）

**注意：必须使用 `sudo`，因为测试数据包含权限受限的目录（mode 0700/0500 等），普通用户无法访问。**

```bash
ssh root@{DEST_IP} 'FIND_DIRS=$(sudo find {NFS_EXPORT}/test-data -type d | wc -l); FIND_FILES=$(sudo find {NFS_EXPORT}/test-data -type f | wc -l); FIND_LINKS=$(sudo find {NFS_EXPORT}/test-data -type l | wc -l); echo "dest find: dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"'
```

Expected: `dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}`。

**若不使用 sudo，会报 `Permission denied` 导致计数偏少。**

### 5b. 验证 ACL 已正确复制

**注意：`nfs4_getfacl` 必须通过 NFSv4.1 loopback mount 才能读取，直接在本地 export 路径执行会返回 `Operation not supported`。**

对有 ACL 的文件，通过 loopback mount 比对源端和目标端的 ACL：

```bash
# 获取源端 ACL（loopback mount）
ssh root@{SOURCE_IP} '
  mkdir -p /mnt/acl_verify
  mount -t nfs4 -o vers=4.1 127.0.0.1:/ /mnt/acl_verify
  nfs4_getfacl /mnt/acl_verify/test-data/d1/d1_1/file1.txt
  umount /mnt/acl_verify'
```

```bash
# 获取目标端 ACL（loopback mount）
ssh root@{DEST_IP} '
  mkdir -p /mnt/acl_verify
  mount -t nfs4 -o vers=4.1 127.0.0.1:/ /mnt/acl_verify
  nfs4_getfacl /mnt/acl_verify/test-data/d1/d1_1/file1.txt
  umount /mnt/acl_verify'
```

**Verify: 两端 ACL 输出完全一致（自定义 ACE 逐行匹配）。**

批量验证目标端 ACL（通过 loopback mount）：

```bash
ssh root@{DEST_IP} '
  mkdir -p /mnt/acl_verify
  mount -t nfs4 -o vers=4.1 127.0.0.1:/ /mnt/acl_verify
  ACL_COUNT=0
  for f in $(find /mnt/acl_verify/test-data -type f | head -30); do
    count=$(nfs4_getfacl "$f" 2>/dev/null | grep -c "^A\|^D\|^U\|^L" || true)
    [ "$count" -gt 3 ] && ACL_COUNT=$((ACL_COUNT+1))
  done
  echo "Files with custom ACL: $ACL_COUNT"
  umount /mnt/acl_verify'
```

Expected: `Files with custom ACL: {ACL_TEST_FILES}`（至少等于源端设置的 ACL 文件数）。

### 5c. 验证 xattr（named attributes）

**已知限制：Linux NFSv4 服务端不通过 NFS 协议暴露 `user.*` xattr（通过本地文件系统 `getfattr` 可读，但 NFS 客户端无法访问）。因此 terrasync 无法通过 NFSv4 读取 xattr，xattr 不会被复制到目标端。这不是 terrasync 的 bug。**

验证源端 xattr 存在（通过本地 export 路径可读）：

```bash
ssh root@{SOURCE_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

Expected: 输出包含 `user.author`、`user.checksum`、`user.version` 等字段。

验证目标端 xattr 状态（预期为空，符合已知限制）：

```bash
ssh root@{DEST_IP} 'getfattr -d {NFS_EXPORT}/test-data/d2/d2_1/file1.txt'
```

Expected: 无输出（目标端 xattr 为空，属于正常行为，不应视为测试失败）。

### 5d. integrity-check 一致性验证（本地执行）

Use the Bash tool locally (timeout=120000):

```bash
{BINARY} -c {CONFIG} -l trace integrity-check --id {IC_JOB_ID} "{SOURCE_URL}" "{DEST_URL}" --quick
```

Expected:

```
  Integrity Check Results:               Mode: Quick, Auto-Fix: Off
   ├─ Checked:       ...
   └─ All Passed ✓
```

**Verify: 退出码为 0，无不一致报告。若有不一致，停止并记录详情，不执行后续清理。**

### 5e. scan 验证目标端计数（本地执行）

Use the Bash tool locally (timeout=60000):

```bash
{BINARY} -c {CONFIG} -l trace scan --id {DST_SCAN_JOB_ID} "{DEST_URL}"
```

**Verify counts match: dirs={EXPECTED_DIRS}, files={EXPECTED_FILES}, symlinks={EXPECTED_SYMLINKS}.**

### 5f. ClickHouse 目标端 base 表验证

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+is_dir,is_symlink,count(*)+FROM+default.base_nfs_v4_full_sync_dst+FINAL+GROUP+BY+is_dir,is_symlink+FORMAT+TabSeparated"
```

Expected（三行）：

```
false   false   {EXPECTED_FILES}
true    false   {EXPECTED_DIRS}
false   true    {EXPECTED_SYMLINKS}
```

### 5g. 元数据校验（mtime/uid/gid/mode 一致性验证）

上传并执行元数据校验脚本，对比 NFS 文件系统实际属性与 ClickHouse 数据库中的记录：

```bash
scp .claude/skills/e2e-test-nfs-v4-full-sync/scripts/verify-metadata.sh root@{DEST_IP}:/tmp/verify-metadata.sh
ssh root@{DEST_IP} 'sudo bash /tmp/verify-metadata.sh'
```

脚本功能：
1. 从 ClickHouse 导出所有条目的 metadata（relative_path, uid, gid, mode, mtime）
2. 对 NFS 文件系统执行 `stat` 获取实际属性
3. 逐条对比，报告不一致的条目

Expected output:
```
=== Metadata Verification ===
scan_state: ...
Total entries: {EXPECTED_TOTAL}
Matched: {EXPECTED_TOTAL}
Mismatch: 0

✓ All metadata verified successfully
```

**若发现不一致，停止并调查。**

---

## Step 6: 并发清理（本地执行）

Only proceed after all Step 5 checks pass. **6a–6e 可并发执行**。

### 6a. 清理源端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{SOURCE_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v4-full-sync-verify-src "{SOURCE_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6b. 清理目标端 NFS（timeout=60000）

```bash
{BINARY} -c {CONFIG} -l trace rm "{DEST_URL}"
```

Wait for exit code 0，then verify:

```bash
{BINARY} -c {CONFIG} -l trace scan --id nfs-v4-full-sync-verify-dst "{DEST_URL}"
```

Expected: 0 dirs, 0 files, 0 symlinks.

### 6c. 清理 ClickHouse 表

**注意：以下命令会创建额外的表，必须全部清理：**

| 命令 | 创建的表 |
|------|----------|
| `scan --id nfs-v4-full-sync-src` | `base_nfs_v4_full_sync_src`, `state_nfs_v4_full_sync_src` |
| `sync` | `base_nfs_v4_full_sync_dst`, `state_nfs_v4_full_sync_dst` |
| `scan --id nfs-v4-full-sync-dst` | (复用 dst 表) |
| `scan --id nfs-v4-full-sync-verify-src` | `base_nfs_v4_full_sync_verify_src`, `state_nfs_v4_full_sync_verify_src` |
| `scan --id nfs-v4-full-sync-verify-dst` | `base_nfs_v4_full_sync_verify_dst`, `state_nfs_v4_full_sync_verify_dst` |

**共 8 个表需要清理。**

```bash
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_verify_src"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.base_nfs_v4_full_sync_verify_dst"
curl -s -X POST "http://{CLICKHOUSE_HOST}/" --data "DROP TABLE IF EXISTS default.state_nfs_v4_full_sync_verify_dst"
```

验证：

```bash
curl -s "http://{CLICKHOUSE_HOST}/?query=SELECT+name+FROM+system.tables+WHERE+database%3D%27default%27+AND+name+LIKE+%27%25nfs_v4_full_sync%25%27+FORMAT+TabSeparated"
```

Expected: 无输出（空）。

### 6d. 清理 jobs 目录

```bash
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_sync*" | xargs rm -rf
find jobs -maxdepth 1 -type d -name "*nfs_v4_full_sync*"
```

Expected: 无输出（空）。

### 6e. 清理日志文件

```bash
rm -rf target/debug/logs/*
ls target/debug/logs/
```

Expected: 无输出（空）。

---

## NFSv4.1 vs NFSv3 Sync 对比

| 方面 | NFSv3 | NFSv4.1 |
|------|-------|---------|
| URL | `nfs://ip/export` | `nfs://ip/export?version=4.1` |
| ACL | 不支持（POSIX ACL 通过 setattr 部分支持） | 完整 NFSv4 ACL（GETACL/SETACL） |
| xattr | 不支持 | 支持（RFC 8276 named attributes） |
| OPEN/CLOSE | 无状态（lookup） | 有状态（stateid） |
| --enable-acl | 不生效（NFS→NFS 静默跳过） | 生效（copy ACL + xattr） |
| 常见错误 | NFS3ERR_STALE | NFS4ERR_DENIED / NFS4ERR_BAD_STATEID |

---

## Completion Criteria

- [ ] Test environment cleaned (Step 0: source NFS, dest NFS, ClickHouse, jobs, logs)
- [ ] Test data created with ACL/xattr: dirs={EXPECTED_DIRS}/files={EXPECTED_FILES}/symlinks={EXPECTED_SYMLINKS} (Step 2)
- [ ] Source ACL verified via nfs4_getfacl (Step 3c)
- [ ] Source xattr verified via getfattr (Step 3d)
- [ ] Source NFSv4.1 scan counts match (Step 3a)
- [ ] ClickHouse src base table verified (Step 3b)
- [ ] Full sync with --enable-acl: counts match, ERROR STATISTICS=0 (Step 4)
- [ ] find counts on dest match (Step 5a)
- [ ] ACL correctly copied to dest (Step 5b)
- [ ] xattr correctly copied to dest (Step 5c)
- [ ] integrity-check passed with 0 inconsistencies (Step 5d)
- [ ] Dest scan counts match (Step 5e)
- [ ] ClickHouse dst base table verified (Step 5f)
- [ ] Metadata verified: uid/gid/mode/mtime consistent with ClickHouse (Step 5g)
- [ ] Source NFS cleaned and verified empty (Step 6a)
- [ ] Dest NFS cleaned and verified empty (Step 6b)
- [ ] ClickHouse tables cleaned (Step 6c)
- [ ] jobs dir cleaned (Step 6d)
- [ ] Logs cleaned (Step 6e)
