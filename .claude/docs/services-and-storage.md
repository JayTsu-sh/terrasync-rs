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

## 测试环境配置

| 服务 | 地址 | 用途 |
|------|------|------|
| NFS v3/v4 源端 | `192.168.50.173` | 扫描/同步源，含 ClickHouse |
| NFS v3/v4 目标端 | `192.168.50.23` | 同步目标 |
| NFS v3 export | `/export/nfs` | NFS v3 挂载点 |
| NFS v4 export | `/export/nfsv4` | NFS v4.1 挂载点（fsid=0 伪根）|
| S3 (rustfs) 源端 | `192.168.50.173:39000` | S3 兼容对象存储 |
| S3 (rustfs) 目标端 | `192.168.50.23:39000` | S3 同步目标 |
| S3 AK/SK | `rustfsadmin / rustfsadmin123` | rustfs 认证凭据 |
| S3 bucket | `test-bucket` | 源端和目标端同名 bucket |
| CIFS 源端 | `192.168.50.173` / `testshare` | Samba 服务器（source）|
| CIFS 目标端 | `192.168.50.23` / `testshare` | Samba 服务器（dest）|
| CIFS user | `terrasync / terrasync123` | Samba 测试账户 |
| ClickHouse | `192.168.50.173:8123` | 元数据数据库 |
| SSH user | `root` | 所有远端 SSH 操作 |

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
