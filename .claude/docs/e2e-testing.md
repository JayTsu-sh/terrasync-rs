# E2E 测试 — 矩阵、环境与运行方式

## 测试矩阵（25 个注册用例）

### NFS v3（5 个，串行）
| Skill 目录 | 场景 | 超时 |
|-----------|------|-----|
| `e2e-test-nfs-v3-full-scan` | 全量扫描，验证 dirs/files/symlinks 计数 | 600s |
| `e2e-test-nfs-v3-full-sync` | 全量同步，验证 copied/skipped/deleted | 900s |
| `e2e-test-nfs-v3-incremental-scan` | 增量扫描，验证变更记录 | 600s |
| `e2e-test-nfs-v3-incremental-sync` | 增量同步，验证 rename/move/change 场景 | 1200s |
| `e2e-test-nfs-v3-integrity-check` | 完整性校验，mismatches == 0 | 600s |

### NFS v4（5 个，串行）
| Skill 目录 | 场景 | 超时 |
|-----------|------|-----|
| `e2e-test-nfs-v4-full-scan` | 全量扫描 | 600s |
| `e2e-test-nfs-v4-full-sync` | 全量同步 | 900s |
| `e2e-test-nfs-v4-incremental-scan` | 增量扫描 | 600s |
| `e2e-test-nfs-v4-incremental-sync` | 增量同步 | 1200s |
| `e2e-test-nfs-v4-integrity-check` | 完整性校验 | 600s |

### S3 / rustfs（7 个，串行）
| Skill 目录 | 场景 | 超时 |
|-----------|------|-----|
| `e2e-test-s3-full-scan` | 全量扫描 | 600s |
| `e2e-test-s3-full-sync` | 全量同步 | 900s |
| `e2e-test-s3-incremental-scan` | 增量扫描 | 600s |
| `e2e-test-s3-incremental-sync` | 增量同步 | 900s |
| `e2e-test-s3-integrity-check` | 完整性校验 | 600s |
| `e2e-test-s3-versioned-full-scan` | 多版本全量扫描 | 600s |
| `e2e-test-s3-versioned-incremental-scan` | 多版本增量扫描 | 600s |

### CIFS / SMB（5 个，串行）
| Skill 目录 | 场景 | 超时 |
|-----------|------|-----|
| `e2e-test-cifs-full-scan` | 全量扫描 | 600s |
| `e2e-test-cifs-full-sync` | 全量同步 | 900s |
| `e2e-test-cifs-incremental-scan` | 增量扫描 | 600s |
| `e2e-test-cifs-incremental-sync` | 增量同步 | 900s |
| `e2e-test-cifs-integrity-check` | 完整性校验 | 600s |

### 跨协议（4 个）
| Skill 目录 | 场景 | 超时 |
|-----------|------|-----|
| `e2e-test-nfs-to-s3-full-sync` | NFS → S3 全量同步 | 900s |
| `e2e-test-s3-to-nfs-full-sync` | S3 → NFS 全量同步 | 900s |
| `e2e-test-nfs-v3` | NFS v3 综合场景 | 1200s |
| `e2e-test-local-filter` | 本地存储过滤规则验证 | 300s |

### 旧格式 CIFS skill（4 个，遗留）
`cifs-full-scan`, `cifs-full-sync`, `cifs-incremental-scan`, `cifs-incremental-sync` — 可能为早期版本，以 `e2e-test-cifs-*` 为准。

## 环境拓扑

> **IP、端口、凭据的唯一真值在 `.claude/skills/harness-run/.env.example`**，修改环境只需改该文件。

```
源端服务器 ($NFS_V3_SOURCE_IP / $NFS_V4_SOURCE_IP / $S3_SOURCE_IP / $CIFS_SOURCE_HOST)
    → NFS v3 export: $NFS_V3_EXPORT
    → NFS v4.1 export: $NFS_V4_EXPORT
    → S3 (rustfs): $S3_SOURCE_IP:$S3_SOURCE_PORT  bucket=$S3_BUCKET_SRC
    → CIFS (Samba): $CIFS_SOURCE_HOST/$CIFS_SOURCE_SHARE → $CIFS_DEST_HOST/$CIFS_DEST_SHARE
    → ClickHouse: $CLICKHOUSE_HOST

目标服务器 ($NFS_V3_DEST_IP / $NFS_V4_DEST_IP / $S3_DEST_IP / $CIFS_DEST_HOST)
    → NFS / S3 / CIFS 目标端（同结构）
```

## 运行方式

### 方式一：Claude 交互执行（当前）

打开对应 SKILL.md，Claude 按步骤执行：
```
"跑 nfs-v3-full-scan e2e 测试"
→ Claude 读取 .claude/skills/e2e-test-nfs-v3-full-scan/SKILL.md
→ 按步骤清理 → 构建 → 执行 → 验证
```

### 方式二：独立 Python 运行

```bash
# 配置环境（实际 IP 已在 .env.example 中填好，直接复制）
cp .claude/skills/harness-run/.env.example .claude/skills/harness-run/.env

# 独立运行单个 skill
cd .claude/skills/e2e-test-nfs-v3-full-scan
python scripts/run.py

# 期望输出
# ✓ cli_scan_output: {dirs: 113, files: 335, symlinks: 79}
# ✓ clickhouse_counts: {dirs: 113, files: 335, symlinks: 79}
# PASS (52.3s)
```

### 方式三：harness runner（完整实现，推荐 CI 使用）

```bash
python .claude/skills/harness-run/scripts/runner.py --smoke
python .claude/skills/harness-run/scripts/runner.py --suite nfs-v3
python .claude/skills/harness-run/scripts/runner.py --id nfs-v3-full-scan
python .claude/skills/harness-run/scripts/runner.py --all
```

## 常见失败排查

| 症状 | 可能原因 | 排查方式 |
|------|---------|---------|
| SSH 连接超时 | 目标机器不可达 | `ping 192.168.50.173` |
| ClickHouse 计数为 0 | 任务 job_id 不匹配 | 检查 `jobs/` 目录 + CH 表名 |
| dirs 计数不符 | 源端数据有变化 | SSH 到源端 `ls -la` 验证 |
| `unwrap called on None` | 旧版本二进制 | `cargo build` 重新编译 |
| 增量计数翻倍 | 未清理 jobs/ 目录 | 删除 `jobs/<job_id>/` 后重跑 |
