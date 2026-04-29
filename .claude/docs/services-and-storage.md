# 存储服务与驱动

## StorageEnum dispatch 模式

所有存储操作通过 `data_mover::StorageEnum` 枚举 dispatch，**不用** `Box<dyn Storage>`：

```rust
// data_mover 中的枚举（外部 git dep）
pub enum StorageEnum {
    Local(LocalStorage),
    Nfs(NfsStorage),        // v3 和 v4 共用，协议通过 URL 区分
    S3(S3Storage),
    Cifs(CifsStorage),
}
```

`dir_walker` 接受 `StorageEnum`，对所有协议透明迭代。

## URL 格式详解

### 本地存储
```
/path/to/dir
C:\path\to\dir
```

### NFS v3
```
nfs://server:port/export/path:/prefix?uid=1000&gid=1000

示例（测试环境）：
nfs://192.168.50.173:2049/export/nfs:/data?uid=0&gid=0

参数说明：
  server   — NFS 服务器 IP
  port     — 通常 2049
  /export/path — NFS export 路径
  :/prefix — 挂载后的子目录前缀（可选，: 分隔）
  uid/gid  — 挂载用户
```

### NFS v4.1
```
nfs://server/export/path?version=4.1

注意：NFS v4.1 使用伪根（pseudo-root），/export/nfsv4 设置了 fsid=0
示例（测试环境）：
nfs://192.168.50.173/export/nfsv4?version=4.1   # 源端
nfs://192.168.50.23/export/nfsv4?version=4.1    # 目标端
```

### S3 / rustfs
```
# HTTP（rustfs 测试环境）
s3://access_key:secret_key@bucket.host:port/prefix

示例（测试环境 rustfs）：
s3://rustfsadmin:rustfsadmin123@test-bucket.192.168.50.173:39000/test-data
s3://rustfsadmin:rustfsadmin123@test-bucket.192.168.50.23:39000/test-data

# HTTPS（生产 S3 或 rustfs HTTPS 端口）
s3+https://access_key:secret_key@bucket.host:port/prefix
```

### SMB / CIFS
```
smb://user:password@host[:port]/share[/sub/path]

注意：域用户的 \ 必须编码为 %5C
示例（测试环境 Samba）：
smb://terrasync:terrasync123@192.168.50.173/testshare/test-data   # 源端
smb://terrasync:terrasync123@192.168.50.23/testshare/test-data    # 目标端
```

## 测试环境配置（Single Source of Truth）

> **所有 IP、端口、凭据、bucket 名统一在 `.claude/skills/harness-run/.env.example` 中定义。**
> 这是唯一需要修改的地方，各 skill 的 `.env.example` 只是引用指针。

```bash
# 初始化：复制并按实际环境填写
cp .claude/skills/harness-run/.env.example .claude/skills/harness-run/.env
# 编辑 .env（此文件 gitignore，不入库）
```

主要变量：

| 变量 | 说明 |
|------|------|
| `NFS_V3_SOURCE_IP` / `NFS_V3_DEST_IP` | NFS v3 源/目标服务器 |
| `NFS_V3_EXPORT` / `NFS_V4_EXPORT` | NFS export 路径 |
| `NFS_V4_SOURCE_IP` / `NFS_V4_DEST_IP` | NFS v4.1 源/目标服务器 |
| `S3_SOURCE_IP` / `S3_DEST_IP` | S3(rustfs) 源/目标 IP |
| `S3_SOURCE_PORT` / `S3_DEST_PORT` | S3 端口 |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | S3 认证凭据 |
| `S3_BUCKET_SRC` / `S3_BUCKET_DST` | S3 bucket 名 |
| `CIFS_SOURCE_HOST` / `CIFS_DEST_HOST` | Samba 服务器 |
| `CIFS_SHARE` / `CIFS_USER` / `CIFS_PASS` | Samba 凭据 |
| `CLICKHOUSE_HOST` | ClickHouse 连接地址 |
| `TERRASYNC_BINARY` / `TERRASYNC_CONFIG` | 二进制和配置文件路径 |

## data-mover 依赖说明

`data-mover` 是外部 git 依赖（非本 workspace crate）：

```toml
# Cargo.toml
[dependencies]
data-mover = { git = "...", rev = "<pinned-commit>" }
```

升级 `data-mover` 时：
1. 更新 rev（pinned commit）
2. `cargo check` 验证接口兼容
3. 运行相关 e2e 测试验证行为一致
