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

### NFS v4
```
nfs4://server:port/export/path:/prefix?uid=1000&gid=1000

示例（测试环境）：
nfs4://192.168.50.173:2049/export/nfs4:/data?uid=0&gid=0
```

### S3 / MinIO
```
# HTTP（MinIO 开发环境）
s3://access_key:secret_key@bucket.host:port/prefix

# HTTPS（生产 S3）
s3+https://access_key:secret_key@bucket.s3.amazonaws.com/prefix

示例（测试环境 MinIO）：
s3://minioadmin:minioadmin@mbucket-src.10.128.137.245:8184/
s3://minioadmin:minioadmin@mbucket-dst.10.128.137.245:8184/
```

### SMB / CIFS
```
smb://user:password@host[:port]/share[/sub/path]

注意：域用户的 \ 必须编码为 %5C
示例：smb://DOMAIN%5Cadmin:pass@192.168.50.100/testshare

示例（测试环境）：
smb://administrator:@192.168.50.100/testshare
```

## 测试环境配置

| 服务 | 地址 | 用途 |
|------|------|------|
| NFS v3/v4 源端 | `192.168.50.173` | 扫描/同步源，含 ClickHouse |
| NFS v3/v4 目标端 | `192.168.50.23` | 同步目标 |
| NFS v3 export | `/export/nfs` | NFS v3 挂载点 |
| NFS v4 export | `/export/nfs4` | NFS v4 挂载点 |
| S3 / MinIO | `10.128.137.245:8184` | S3 兼容存储 |
| S3 源 bucket | `mbucket-src` | 扫描/同步源 |
| S3 目标 bucket | `mbucket-dst` | 同步目标 |
| CIFS | `192.168.50.100` | SMB 文件服务器 |
| CIFS share | `testshare` | 测试共享目录 |
| ClickHouse | `192.168.50.173:8123` | 元数据数据库 |
| SSH user | `root` | 所有远端操作 |

## 本地配置

所有 IP 通过环境变量管理，**不要**在代码或 SKILL.md 里 hardcode：

```bash
# 复制配置模板
cp .claude/skills/harness-run/.env.example .claude/skills/harness-run/.env
# 编辑 .env 填写实际值（此文件 gitignore，不入库）
```

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
