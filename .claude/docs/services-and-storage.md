# 存储服务与驱动

## 原生存储角色与产品配置

存储由 data-mover 的 `Storage` typed roles 提供。产品层 `StorageContext` 保存连接、块大小和重连配置；它不是 backend 枚举。backend 能力、错误分类、传输恢复与发布由 data-mover 管理，任务模式与连接配置由 app 选择。

扫描事实为原生 `ObservedEntry`，不可变历史由 `Arc` 共享；产品任务与 wire 分别使用 task-model 和 transport 的契约。重构剩余范围见 [方案](../../docs/specs/2026-11-05-db-native-refactor-design.md)。

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
s3://<access-key>:<secret-key>@bucket.host:port/prefix

示例（测试环境 rustfs）：
s3://<access-key>:<secret-key>@test-bucket.192.168.50.173:39000/test-data
s3://<access-key>:<secret-key>@test-bucket.192.168.50.23:39000/test-data

# HTTPS（生产 S3 或 rustfs HTTPS 端口）
s3+https://<access-key>:<secret-key>@bucket.host:port/prefix
```

### SMB / CIFS
```
smb://<user>:<password>@host[:port]/share[/sub/path]

注意：域用户的 \ 必须编码为 %5C
示例（测试环境 Samba）：
smb://<user>:<password>@192.168.50.173/testshare/test-data   # 源端
smb://<user>:<password>@192.168.50.23/testshare/test-data    # 目标端
```

### HDFS
```
hdfs://[user-info@]nameservice/path
```

HDFS 客户端配置按角色位于 `storage.source.hdfs` 和
`storage.destination.hdfs`，可配置 `config_dir`、Hadoop override，以及
Kerberos `principal` 与 `keytab`/`cache`。单进程按源/目标分别取值；
双进程发送端只需 source，接收端只需 destination。凭据不得写入
日志或提交到仓库。

## 测试环境配置（Single Source of Truth）

共享实验室的非敏感拓扑和默认值定义在 `tests/lab/common.sh`，凭据由
self-hosted GitHub Actions runner 的服务环境注入，禁止提交到仓库。
完整运行与维护说明见 `tests/lab/README.md`。

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
| `CIFS_SOURCE_SHARE` / `CIFS_DEST_SHARE` / `CIFS_USER` / `CIFS_PASS` | Samba 凭据；源和目标 share name 可不同 |
| `CLICKHOUSE_HOST` | ClickHouse 连接地址 |
| `LAB_HDFS_LOCATION` | HDFS HA URL |
| `LAB_HDFS_ADMIN_USER` | 真实门禁使用的 Kerberos principal |
| `LAB_HDFS_CONFIG_DIR` | `core-site.xml`/`hdfs-site.xml` 目录 |
| `LAB_HDFS_KEYTAB` | runner 上预置的 keytab（禁止提交） |
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
2. 同步修改当前调用方，运行 CI 对应检查
3. 对受影响链路执行真实功能、故障与取消验证，按需报告性能／内存证据
