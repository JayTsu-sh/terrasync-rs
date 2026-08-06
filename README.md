# Rust Terrasync

Rust Terrasync 是一个高性能的文件系统同步和扫描工具，使用Rust语言开发。它提供了强大的文件扫描、过滤和同步功能，支持校验和验证和访问控制列表(ACL)同步。

## 功能特性

- **快速文件扫描**：高效地扫描文件系统，支持深度限制和复杂过滤表达式
- **智能文件同步**：安全地在源目录和目标目录之间同步文件
- **增量扫描与同步**：支持增量操作，仅处理上次操作后变化的文件，提高效率
- **校验和验证**：使用Blake3算法进行文件内容校验，确保文件完整性
- **ACL同步**：在Windows系统上支持访问控制列表(ACL)的同步
- **ACE管理**：在Windows系统上支持访问控制条目(ACE)的列表和复制操作
- **强大的过滤能力**：使用灵活的表达式语言过滤和排除文件
- **配置管理**：支持通过配置文件和命令行参数自定义行为
- **多存储协议支持**：支持本地文件系统、NFS v3/v4.1、S3兼容对象存储和SMB/CIFS共享
- **可扩展的Feature支持**：通过 Feature 机制支持 GUI 和性能分析等可选功能

## Feature支持

Rust Terrasync支持通过Feature机制启用可选功能，您可以在构建时选择启用特定的Feature：

### 可用的Feature

| Feature     | 描述                   | 依赖关系                                          |
| ----------- | ---------------------- | ------------------------------------------------- |
| `basic`     | 基本功能（始终包含）   | -                                                 |
| `gui`       | 启用Web GUI管理界面    | `cli/gui` → `web`                                 |
| `profiling` | 启用性能分析支持       | -                                                 |

### 构建时启用Feature

要在构建时启用特定的Feature，请使用`--features`参数：

```bash
# 启用Web GUI管理界面
cargo build --features gui

# 启用所有Feature
cargo build --all-features
```

### 默认Feature

- 运行`cargo build`（不指定任何Feature）时，默认启用`basic` Feature
- 您可以通过修改`Cargo.toml`文件中的`default`字段来自定义默认启用的Feature

### Feature详细说明

#### `gui` Feature

- **功能**：启用 Web GUI 管理界面，提供基于浏览器的可视化操作平台
- **使用场景**：需要通过图形界面管理端点、创建和监控迁移任务，适合不熟悉命令行或需要直观操作的用户
- **依赖**：需要 `web` crate（Axum + SQLite + rust-embed）
- **前端技术栈**：Vue 3 + Naive UI + Tailwind CSS，前端构建产物会嵌入到最终二进制中
- **命令**：使用 `gui` 子命令启动 Web 服务器
- **注意事项**：首次构建时 `build.rs` 会自动执行 `npm install && npm run build` 构建前端，需要 Node.js 环境

### 示例用法

```bash
# 使用gui Feature构建并启动Web管理界面
cargo run --features gui -- gui --port 8080

```

## 存储协议支持

Rust Terrasync 支持多种存储协议，每种协议都提供了特定的功能和优化：

### 本地文件系统

- 完整支持Windows和Linux平台的本地文件系统操作
- 支持文件和目录的完整元数据获取
- 在Windows系统上支持访问控制列表(ACL)的读取和设置
- 自动处理Windows长路径限制

### NFS v3/v4.1 协议

- 支持通过 NFS v3 和 NFS v4.1 协议访问远程文件系统
- 通过 URL 参数 `version=4.1` 切换协议版本，默认使用 v3
- 实现了高效的目录遍历算法，避免栈溢出问题
- 支持通过prefix指定起始扫描路径，限制扫描范围
- 采用连接池管理NFS连接，优化并发性能
- 支持文件和目录的元数据获取
- **NFSv4.1 增强特性**：
  - NFSv4 ACL 读取与同步
  - 扩展属性（xattr，RFC 8276）读取与同步

### SMB/CIFS 共享

- 支持通过 SMB2/3 协议访问 Windows 文件共享（CIFS）
- 支持域用户认证（如 `DOMAIN\username`）
- 支持访问控制列表（ACL）的读取和同步，包括显式 ACE 与继承 ACE 的正确合并
- 实现了高效的并行目录遍历（walkdir_2），采用 Reader 池 + DFS Driver 架构
- 支持文件和目录的完整元数据获取（大小、时间戳、只读属性等）
- 支持大文件的分块流式读写

### S3兼容对象存储

- 支持访问各种S3兼容的对象存储服务
- 实现了高效的并行扫描算法，充分利用并发性能
- 支持通过prefix指定起始扫描路径，限制扫描范围
- 自动处理S3分页查询和错误重试
- 优化了大量小对象的扫描性能
- **对象标签支持**：支持S3对象的标签管理功能，包括：
  - 在扫描结果中包含对象标签（可通过配置禁用）
  - 在同步过程中保留对象标签
- **对象多版本支持**：支持S3存储桶的对象版本控制功能，包括：
  - 自动检测存储桶是否启用版本控制
  - 支持列出对象的所有版本
  - 在同步过程中处理版本化对象

## 安装

### 安装 Rust

在构建项目之前，您需要先安装 Rust 开发环境。请访问以下链接按照官方指引安装 Rust：
[https://rust-lang.org/zh-CN/tools/install/](https://rust-lang.org/zh-CN/tools/install/)

### 从源码构建

1. 确保已安装 Rust 和 Cargo
2. 克隆代码仓库
   ```bash
   # 克隆rust-terrasync仓库
   git clone http://gitlab.ln.ad/lisa/rust-terrasync.git
   cd rust-terrasync
   ```
3. 构建项目
    ```bash
    cargo build --release
    ```
 4. 可执行文件将位于：
    - Windows: `target/release/terrasync.exe`
    - Linux: `target/release/terrasync`

## License 授权

Terrasync 使用离线 license 授权系统。首次使用前需要获取并激活 license 文件。

### 获取 License

从管理员处获取 `license.json` 文件。

### 部署 License

将 `license.json` 放置在以下位置之一（按优先级排序）：

1. **CLI 参数指定**：`terrasync --license /path/to/license.json scan ...`
2. **配置文件指定**：在 TOML 配置文件中设置 `[license] path = "/path/to/license.json"`
3. **可执行文件同目录**：将 `license.json` 放在 terrasync 二进制文件旁边
4. **当前工作目录**：将 `license.json` 放在运行命令的目录下

### 激活 License

首次使用前，必须在目标机器上执行激活：

```bash
terrasync activate --license /path/to/license.json
```

成功输出：
```
License activated successfully.
```

**重要提示**：
- License 有**激活有效期**，签发后必须在规定时间内完成激活。如看到 `Activation window expired` 提示，说明激活有效期已过，需联系管理员重新签发。
- 激活后 `license.json` 文件会被更新（写入机器绑定信息），**请勿手动修改此文件**。

### 正常使用

激活后，正常使用 terrasync 命令即可，程序启动时会自动验证 license。

### 常见问题

| 错误信息 | 原因及解决方法 |
|---------|--------------|
| `License file not found` | License 文件未找到。检查文件路径是否正确。 |
| `License not activated` | License 尚未激活。执行 `terrasync activate --license /path/to/license.json` 完成激活。 |
| `Activation window expired` | 激活有效期已过。需联系管理员重新签发 license。 |
| `License verification failed` | License 验证失败。可能原因：license 已过期、机器硬件更换、文件被篡改、系统时间异常。请联系管理员获取新的 license。 |

## 命令行用法

Rust Terrasync 提供了以下主要子命令：`scan`、`sync`、`integrity-check`、`rm`、`config`、`gui`（需要 gui Feature）和 `ace`（仅Windows可用）。

### 基本语法

```bash
terrasync [OPTIONS] <COMMAND>
```

### 配置优先级

```
CLI 命令行参数 > -c 配置文件 > 内置默认配置
```

部分参数（如 `--qos`、`--enable-integrity-check`、`--depth` 等）既可通过命令行指定，也可在配置文件中设置默认值。命令行参数始终具有最高优先级。

### 全局选项

```
-c, --config <FILE>          自定义配置文件路径（TOML格式），文件中的设置覆盖内置默认值，命令行参数覆盖两者
-l, --log-level <LOG_LEVEL>  日志级别 (trace, debug, info, warn, error)
    --json                   启用 JSON 结构化日志（输出到 logs/app.json.log）
```

### 存储协议路径格式

**本地文件系统**：直接使用本地路径，如 `C:\path\to\dir` 或 `/path/to/dir`

**NFS v3**：使用 `nfs://server:port/export/path:/prefix?uid=1000&gid=1000` 格式，如 `nfs://fileserver:2049/data:/prefix` 或 `nfs://fileserver:2049/data?uid=1000&gid=1000`
- 支持查询参数：`uid`（用户ID）、`gid`（组ID）

**NFS v4.1**：在 NFS URL 基础上添加 `version=4.1` 参数，如 `nfs://fileserver/export/path?version=4.1` 或 `nfs://fileserver/export/path:/prefix?uid=1000&gid=1000&version=4.1`
- 支持 NFSv4 ACL 
- 扩展属性（xattr）同步

*注意*：如果查询中包含&符号，整个路径需要用引号括起来，如 `”nfs://fileserver:2049/data:/prefix?uid=1000&gid=1000&version=4.1”`

**SMB/CIFS 共享**：使用 `smb://user:password@host[:port]/share[/sub/path]` 格式
- 默认端口为 445
- 用户名和密码支持 URL 编码
- **域用户注意**：域名与用户名之间的反斜杠 `\` 必须使用 `%5C` 编码。例如域用户 `DOMAIN\jay.xu` 应写为 `DOMAIN%5Cjay.xu`
- 密码中的特殊字符（如 `@`、`:`、`/`）也需要 URL 编码（分别为 `%40`、`%3A`、`%2F`）
- 示例：`smb://DOMAIN%5Cjay.xu:MyP%40ssword@fileserver/ShareName/sub/path`

**S3兼容对象存储**：使用 `s3://access_key:secret_key@bucket.host:port/prefix` 或 `s3+https://access_key:secret_key@bucket.host:port/prefix` 格式
- `s3://` 默认为HTTP协议
- `s3+http://` 明确使用HTTP协议
- `s3+https://` 明确使用HTTPS协议
- `s3+hcp://` 用于HCP（Hitachi Content Platform）兼容存储
- bucket 是对象桶名，host是主机名，port是端口号，prefix是可选的前缀路径。
- 示例：`s3://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bucket.example.com:9000/data`

### 子命令详解

#### ace - Windows访问控制条目(ACE)操作（仅Windows可用）

在Windows系统上提供访问控制条目(ACE)的管理功能，包括列出和复制ACE。

**用法：**
```bash
terrasync ace [OPTIONS] <SUBCOMMAND>
```

**子命令：**
```
list    列出文件或目录的访问控制条目(ACE)
copy    从源路径复制ACE到目标路径
```

**list子命令选项：**
```
<path>                         要扫描的目录或文件路径

-i, --id <ID>                  作业ID（用于跟踪）
-d, --depth <DEPTH>            扫描深度级别（0表示无限深度）[默认: 0]
-o, --owner <OWNER>            用于过滤ACE的所有者名称
-m, --match <EXPRESSION>       用于匹配文件/目录的过滤表达式
-e, --exclude <EXPRESSION>     用于排除文件/目录的过滤表达式
    --include-inherited        包含继承的ACE（默认为false）
```

**copy子命令选项：**
```
<source_path>                  源文件或目录路径
<target_path>                  目标文件或目录路径

-i, --id <ID>                  作业ID（用于跟踪）
-d, --depth <DEPTH>            扫描深度级别（0表示无限深度）[默认: 0]
-m, --match <EXPRESSION>       用于匹配文件/目录的过滤表达式
-e, --exclude <EXPRESSION>     用于排除文件/目录的过滤表达式
```

#### scan - 扫描文件系统

扫描指定路径下的文件和目录，支持本地文件系统、NFS v3/v4.1、S3兼容对象存储和SMB/CIFS共享。

**用法：**
```bash
terrasync scan [OPTIONS] <PATH>
```

**选项：**
```
<path>                   要扫描的目录路径

-i, --id <ID>            扫描作业的唯一标识符（可选）
-d, --depth <DEPTH>      扫描深度级别（0表示无限深度）[默认: 0]。也可在配置文件 [scan] depth 中设置
-m, --match <EXPRESSION> 过滤表达式，用于匹配文件/目录。也可在配置文件 [scan] match 中设置。示例: 'modified<0.5 and "ntap" in name and type==file'
-e, --exclude <EXPRESSION> 排除表达式，用于排除文件/目录。也可在配置文件 [scan] exclude 中设置。示例: 'name=="target" or name==".git"'
```

**增量扫描功能：**
- 当使用相同的作业ID再次执行扫描时，系统会自动执行增量扫描
- 增量扫描仅处理上次扫描后新增,修改,删除和rename（仅支持nfs客户端扫描）的文件，显著提高扫描效率
- 系统通过检查jobs目录下是否存在对应作业ID的目录来判断是否执行增量扫描

#### sync - 同步文件（支持增量同步、支持分布式同步）

在源目录和目标目录之间同步文件，支持在不同存储协议之间进行同步。

**用法：**
```bash
terrasync sync [OPTIONS] [SRC_PATH] [DEST_PATH]
```

**选项：**
```
[SRC_PATH]                      源目录路径（必须提供）
[DEST_PATH]                     目标目录路径（必须提供）

-i, --id <ID>                   同步作业的唯一标识符（可选）
    --enable-integrity-check    启用文件校验和验证 [默认: false]。也可在配置文件 [sync] enable_integrity_check 中设置
    --enable-acl                启用访问控制列表(ACL)同步（仅在Windows上生效） [默认: false]。也可在配置文件 [sync] enable_acl 中设置
-m, --match <EXPRESSION>        过滤表达式，用于匹配文件/目录。也可在配置文件 [scan] match 中设置
-e, --exclude <EXPRESSION>      排除表达式，用于排除文件/目录。也可在配置文件 [scan] exclude 中设置
    --qos <QOS>                 带宽速率限制，如 1GiB/s, 100MiB/s 等。也可在配置文件 [sync] qos 中设置
    --peak-qos-rate <RATE>      峰值QoS速率乘数 [默认: 2.0]。也可在配置文件 [sync] peak_qos_rate 中设置
    --iops <IOPS>               IOPS（每秒IO操作数）限制，如 1000
    --block-size <SIZE>         块大小，如 2MiB, 16MiB 等。也可在配置文件 [sync] block_size 中设置
```

**增量同步功能：**
- 当使用相同的作业ID再次执行同步时，系统会自动执行增量同步
- 增量同步仅处理上次同步后新增或修改的文件，显著提高同步效率
- 系统通过检查jobs目录下是否存在对应作业ID的目录来判断是否执行增量同步

**跨协议同步支持：**
Rust Terrasync 支持在不同存储协议之间进行文件同步，包括：
- 本地文件系统 ↔ 本地文件系统
- 本地文件系统 ↔ NFS v3/v4.1
- 本地文件系统 ↔ S3兼容对象存储
- 本地文件系统 ↔ SMB/CIFS
- NFS v3/v4.1 ↔ NFS v3/v4.1
- NFS v3/v4.1 ↔ S3兼容对象存储
- NFS v3/v4.1 ↔ SMB/CIFS
- S3兼容对象存储 ↔ S3兼容对象存储
- SMB/CIFS ↔ SMB/CIFS

**QoS流量控制：**
Rust Terrasync提供了灵活的QoS（服务质量）流量控制机制，支持带宽限制和IOPS限制两个维度，可以精细管理同步过程中的资源使用：

- **带宽限制（--qos）**：设置同步过程中的平均带宽速率限制，支持多种单位格式，如 `1GiB/s`、`100MiB/s`、`500KB/s` 等
- **峰值速率乘数（--peak-qos-rate）**：设置允许的峰值速率与平均速率的倍数关系，默认值为2.0
- **IOPS限制（--iops）**：设置每秒IO操作数上限，如 `1000`，适用于需要控制存储系统IO压力的场景。IOPS限流允许10%或至少10个操作的突发
- 带宽限制和IOPS限制可以同时启用，两者并行生效，任一达到上限即触发限流
- **适用场景**：
  - 在共享网络环境中避免占用过多带宽
  - 防止对源或目标存储系统造成过大IO压力
  - 实现不同同步任务之间的带宽和IOPS分配

**注意事项：**
- ACL同步功能支持Windows本地文件系统和SMB/CIFS共享之间的同步
- 某些元数据（如访问时间、创建时间）在不同存储系统之间可能无法完全保留
- 跨协议同步时，建议启用校验和验证以确保文件完整性

#### integrity-check - 完整性检查

对源路径和目标路径之间的文件进行完整性校验，支持多 worker 并发执行。

- **完整模式**（默认）：使用Blake3哈希进行逐文件数据校验，同时验证元数据（mtime、uid、gid、mode）。源端与目标端的哈希计算并发执行，充分利用IO带宽。
- **快速模式**（`--quick`）：跳过数据哈希计算，比较文件元数据：大小（size）、修改时间（mtime）、uid、gid、权限模式（mode），大幅提升校验速度。

并发 worker 数量可通过配置文件 `[integrity_check] concurrency` 设置，默认值为 8。

**用法：**
```bash
terrasync integrity-check [OPTIONS] <SRC_PATH> <DEST_PATH>
```

**选项：**
```
<SRC_PATH>                      源目录路径
<DEST_PATH>                     目标目录路径

-i, --id <ID>                   校验作业的唯一标识符（可选）
    --quick                     快速校验模式：比较文件元数据（size, mtime, uid, gid, mode），不做数据哈希校验 [默认: false]
    --auto-fix                  自动修复模式：对文件的 uid/gid/mode、目录的 mtime/uid/gid/mode、软链接的 mtime/uid/gid 不一致进行自动修复 [默认: false]
```

#### rm - 删除远程存储路径

删除指定路径下的所有文件和目录，支持本地文件系统、NFS、S3、SMB/CIFS 等所有存储协议。删除过程实时显示进度（已删除文件数和目录数）。

**用法：**
```bash
terrasync rm <PATH>
```

**示例：**
```bash
# 删除本地目录
terrasync rm /data/old_backup

# 删除 NFS 路径
terrasync rm "nfs://fileserver/export/data:/old_dir?uid=1000&gid=1000"

# 删除 NFS v4.1 路径
terrasync rm "nfs://fileserver/export/data:/old_dir?version=4.1"

# 删除 S3 前缀下的所有对象
terrasync rm s3://access_key:secret_key@bucket.example.com:9000/old_prefix

# 删除 SMB/CIFS 共享目录
terrasync rm "smb://user:password@fileserver/share/old_dir"
```

#### config - 显示配置

显示当前的应用配置。

**用法：**
```bash
terrasync config
```

#### gui - Web GUI 管理界面（需要 gui Feature）

启动 Web GUI 管理界面，通过浏览器提供可视化的迁移任务管理功能。

**用法：**
```bash
terrasync gui [OPTIONS]
```

**选项：**
```
    --host <HOST>    绑定的主机地址 [默认: 0.0.0.0]
-p, --port <PORT>    监听端口 [默认: 8080]
```

**功能概览：**

- **端点管理**：创建和管理本地文件系统、NFS v3/v4.1、S3 兼容对象存储端点，支持连接测试
- **路径管理**：为每个端点配置子路径，作为迁移任务的源或目标
- **任务管理**：创建扫描、同步、完整性检查任务，支持启动、取消、查看执行历史
- **实时进度**：通过 WebSocket 推送任务执行进度
- **系统配置**：在线查看和修改系统配置项，持久化到 SQLite 数据库

**配置优先级（GUI 模式）：**
```
GUI 页面保存的配置（SQLite） > -c 配置文件 > 内置默认值
```

**示例：**
```bash
# 默认启动（0.0.0.0:8080）
terrasync gui

# 自定义端口
terrasync gui --port 9090

# 仅本地访问
terrasync gui --host 127.0.0.1 --port 8080

# 使用自定义配置文件启动
terrasync -c custom.toml gui
```

启动后在浏览器访问 `http://<host>:<port>` 即可使用管理界面。

**架构说明：**

GUI 采用前后端分离架构，前端构建产物通过 `rust-embed` 嵌入到二进制中，无需额外部署前端服务。

| 层        | 技术                                  |
| --------- | ------------------------------------- |
| 后端框架  | Axum 0.8（REST API + WebSocket）      |
| 数据库    | SQLite（通过 sqlx，WAL 模式）         |
| 前端框架  | Vue 3 + TypeScript                    |
| UI 组件库 | Naive UI                              |
| CSS       | Tailwind CSS                          |
| 嵌入方式  | rust-embed 将前端构建产物嵌入二进制   |

**REST API：**

GUI 后端提供以下 REST API，也可通过 curl 等工具直接调用：

```
# 端点管理
GET    /api/v1/endpoints              列表
POST   /api/v1/endpoints              创建
GET    /api/v1/endpoints/:id          详情
PUT    /api/v1/endpoints/:id          更新
DELETE /api/v1/endpoints/:id          删除
POST   /api/v1/endpoints/:id/test     测试连接

# 存储浏览
POST   /api/v1/fs/list-dirs           浏览本地目录
POST   /api/v1/nfs/exports            查询NFS导出列表
POST   /api/v1/s3/buckets             查询S3存储桶列表

# 路径管理
GET    /api/v1/endpoints/:id/paths    列表
POST   /api/v1/endpoints/:id/paths    创建
DELETE /api/v1/paths/:id              删除

# 任务管理
GET    /api/v1/tasks                  列表
POST   /api/v1/tasks                  创建
GET    /api/v1/tasks/:id              详情
PUT    /api/v1/tasks/:id              更新
DELETE /api/v1/tasks/:id              删除
POST   /api/v1/tasks/:id/start        启动
POST   /api/v1/tasks/:id/cancel       取消
GET    /api/v1/tasks/:id/progress     任务进度

# 任务分析
GET    /api/v1/tasks/:id/analytics    扫描结果分析数据

# 执行历史
GET    /api/v1/tasks/:id/executions   执行历史列表
GET    /api/v1/executions/:id         执行详情
DELETE /api/v1/executions/:id         删除执行记录
GET    /api/v1/executions/:id/logs    执行日志（支持按级别过滤和分页）

# 过滤条件
GET    /api/v1/filter-fields          获取过滤字段定义

# 系统配置
GET    /api/v1/config                 获取配置
PUT    /api/v1/config                 更新配置
POST   /api/v1/config/test-clickhouse 测试ClickHouse连接

# WebSocket
GET    /api/v1/ws                     实时进度推送
```

## 过滤表达式语法

### 过滤表达式类型

- **rmatch**：匹配表达式（白名单），只有匹配的文件/目录才会被处理
- **exclude**：排除表达式（黑名单），匹配的文件/目录会被跳过

### 核心功能

- **智能路径深度匹配**：根据路径深度和通配符类型智能决定是否继续扫描子目录
- **通配符支持**：使用glob模式匹配，支持`*`和`**`通配符
- **逻辑运算符**：支持括号、`and`和`or`组合多个条件，优先级为：括号 > `and` > `or`
- **多种过滤条件**：支持文件名、路径、文件类型、修改时间、文件大小

### 比较运算符

| 运算符 | 描述     | 适用文件属性                   |
| ------ | -------- | ------------------------------ |
| `==`   | 等于     | 除modified之外的所有属性       |
| `!=`   | 不等于   | 除size、modified之外的所有属性 |
| `<`    | 小于     | size, modified                 |
| `>`    | 大于     | size, modified                 |
| `<=`   | 小于等于 | size, modified                 |
| `>=`   | 大于等于 | size, modified                 |

### 逻辑运算符

| 运算符 | 描述   | 优先级 |
| ------ | ------ | ------ |
| `and`  | 逻辑与 | 高     |
| `or`   | 逻辑或 | 低     |

### 文件属性

| 属性名     | 描述                         | 示例值                     |
| ---------- | ---------------------------- | -------------------------- |
| `name`     | 文件名（不含路径）           | `file.txt`                 |
| `path`     | 文件完整路径                 | `/data/documents/file.txt` |
| `type`     | 文件类型                     | `file`, `dir`, `symlink`   |
| `size`     | 文件大小（字节）             | `1024`                     |
| `modified` | 最后修改时间（天，小数表示） | `0.5`（12小时）            |

### 通配符匹配规则

过滤表达式中的通配符遵循 Unix shell 风格的 glob 模式：

| 通配符   | 描述                                                  | 示例                                           |
| -------- | ----------------------------------------------------- | ---------------------------------------------- |
| `?`      | 匹配任意单个字符                                      | `test_?` 匹配 `test_a`，不匹配 `test_ab`       |
| `*`      | 匹配任意（可以为空的）字符序列，不跨越路径分隔符      | `*.txt` 匹配 `file.txt`，不匹配 `dir/file.txt` |
| `**`     | 匹配当前目录及任意深度子目录                          | `**/*.log` 匹配任意深度下的 `.log` 文件        |
| `[...]`  | 匹配括号内的任意一个字符，支持范围（按 Unicode 排序） | `[0-9]` 匹配 0-9，`[abc]` 匹配 a、b 或 c       |
| `[!...]` | 匹配括号内字符以外的任意字符（取反）                  | `[!0-9]` 匹配任意非数字字符                    |

**注意事项：**
- `**` 必须作为独立的路径段使用，`**a` 和 `b**` 均为无效模式
- 连续超过两个 `*` 的序列（如 `***`）也是无效的
- 未闭合的方括号 `[` 是无效的
- 元字符 `?`、`*`、`[`、`]` 可以用方括号转义匹配，如 `[?]` 匹配字面量 `?`
- 在字符集中，`]` 紧跟在 `[` 或 `[!` 后面时被视为字符集的一部分，如 `[]]` 匹配 `]`
- `-` 字符放在字符集开头或末尾表示字面量，如 `[abc-]` 匹配 a、b、c 或 `-`

### 智能路径深度匹配规则

过滤系统根据路径深度和通配符类型智能决定扫描行为，这是提高扫描效率的关键机制：

#### 1. 带有 `**` 通配符的路径模式

- **匹配成功时**：不跳过当前条目，继续扫描子目录，子目录不需要再匹配
- **匹配失败时**：跳过当前条目，继续扫描子目录，子目录需要再匹配

示例：
```bash
# 匹配任意深度下的 subdir 开头的目录
path == "**/subdir*"
# 如果当前目录是 a/b/c，会继续扫描子目录，因为可能存在 a/b/c/subdir
```

#### 2. 普通路径模式（无 `**` 通配符）

- **文件路径深度 < 模式深度**：继续扫描子目录，子目录需要匹配
  - 示例：模式为 `a/b/c`，当前路径为 `a`，会继续扫描，可能存在 `a/b/c`

- **文件路径深度 >= 模式深度**：不继续扫描子目录，子目录不需要匹配
  - 示例：模式为 `a/b/c`，当前路径为 `a/b/c`，不会继续扫描子目录

#### 3. 部分路径匹配 vs 全路径匹配

- **部分路径匹配**：路径的前缀匹配模式，但深度不足
  - 行为：继续扫描子目录，子目录需要匹配
  - 示例：模式为 `a/b/c`，当前路径为 `a/b`

- **全路径匹配**：路径完全匹配模式
  - 行为：不跳过当前条目，如果是目录则继续扫描，子目录不需要再匹配
  - 示例：模式为 `a/b/c`，当前路径为 `a/b/c`

#### 4. 匹配表达式（rmatch）的行为

- **匹配成功**：
  - 如果是全路径匹配：不跳过当前条目，如果是目录则继续扫描，子目录不需要再匹配
  - 如果是部分路径匹配：不跳过当前条目，如果是目录则继续扫描，子目录需要再匹配

- **匹配失败**：
  - 如果是全路径扫描（明确路径深度）：跳过当前条目，不继续扫描，子目录不需要再匹配
  - 如果是部分路径扫描：跳过当前条目，如果是目录则继续扫描，子目录需要再匹配

#### 5. 排除表达式（exclude）的行为

- **匹配成功**：
  - 如果是部分路径匹配：跳过当前条目，继续扫描子目录，子目录需要再匹配
  - 如果是全路径匹配：跳过当前条目，不继续扫描子目录，子目录不需要再匹配

- **匹配失败**：不跳过当前条目，继续正常扫描

### 示例表达式

#### 基本匹配

- `modified<0.5` - 匹配最近12小时内修改的文件  
- `type=="file" and size>1024` - 匹配大于1KB的文件  
- `name=="target" and type=="dir"` - 匹配名为"target"的目录  

#### 路径匹配

- `path == "*/dir1/*"` - 匹配dir1目录下的所有文件和子目录  
- `path == "**/subdir*"` - 匹配任意深度下的subdir开头的目录（会继续扫描子目录）
- `path == "*/*/*/*/202501*"` - 匹配特定深度路径（5层）下的2025年1月文件
- `path == "*/HZ"` - 匹配包含/HZ路径段的目录（部分路径匹配，会继续扫描）

#### 文件名匹配

- `name == "project_*"` - 匹配名称以"project_"开头的文件/目录  
- `name == "*.txt"` - 匹配扩展名为.txt的文件  
- `name == "*.log"` - 匹配所有.log文件  
- `name == "file*.txt"` - 匹配file开头的txt文件


#### 组合条件

- `name == "test.txt" or name == "*.log"` - 匹配test.txt文件或所有.log文件  
- `path == "*dir*" and name == "*.txt" and type == "file"` - 匹配目录中所有txt文件  
- `modified<1 and (name == "*.jpg" or name == "*.png")` - 匹配最近24小时内修改的图片文件  
- `name == "test.txt" or (name == "*.log" and (modified < 1 or size > 2048 and modified > 2)) or name == "*.jpg" and modified > 1` - 匹配test.txt文件+最近24小时内修改或大小超过2KB且修改时间超过2天的log文件+最近24小时内修改的jpg文件  


#### 通配符高级用法

- `name == "*doc*txt"` - 匹配名称中包含doc和txt的文件  
- `path == "/**/*.txt"` - 匹配根目录下任意深度的txt文件  
- `path == "**/subdir2/**"` - 匹配包含subdir2的任意深度路径  
- `name == "report_20250[12]*.docx"` - 匹配2025年1-2月的报告文件 

Note: 具体示例可以参见app\tests\test_scan.rs里的相关测试用例。

## 配置文件

Rust Terrasync 使用TOML格式的配置文件。配置优先级为：**CLI 命令行参数 > `-c` 配置文件 > 内置默认配置**。

**默认配置路径：** 程序内置了默认配置，也可以通过 `-c` / `--config` 选项指定自定义配置文件来覆盖默认值。完整的当前配置可以通过 `terrasync config` 命令查看。

**配置文件示例 _config.toml_：** 包含了所有可配置选项的默认值和详细说明。请注意，配置文件中的信息应根据您的实际环境进行配置，特别是数据库连接相关的设置。

部分参数既可通过命令行指定，也可在配置文件中设置默认值，避免每次执行时重复输入。这些参数包括：
- `[scan]` 下的 `depth`、`match`、`exclude`
- `[sync]` 下的 `enable_integrity_check`、`enable_acl`、`qos`、`peak_qos_rate`、`block_size`

Rust Terrasync 使用 ClickHouse 存储扫描和同步状态。

**ClickHouse 远程访问配置：**

ClickHouse 默认仅监听本地回环地址（`127.0.0.1`），如果 Rust Terrasync 与 ClickHouse 不在同一台机器上，需要修改 ClickHouse Server 配置文件以允许远程访问。

编辑 `/etc/clickhouse-server/config.xml`（或 `config.d/` 下的自定义配置文件），取消注释或添加 `<listen_host>` 配置：

```xml
<!-- 监听所有网络接口 -->
<listen_host>::</listen_host>

<!-- 或仅监听指定 IP -->
<!-- <listen_host>0.0.0.0</listen_host> -->
```

修改后重启 ClickHouse Server 使配置生效：

```bash
sudo systemctl restart clickhouse-server
```

如果启用了数据库功能（`[database].enabled = true`），您可以通过相应的客户端工具查看数据库内的信息：
- ClickHouse CLI 客户端使用指南：[https://clickhouse.com/docs/zh/interfaces/cli](https://clickhouse.com/docs/zh/interfaces/cli)

配置示例：

```toml
[log]
max_size = 100           # Maximum size in megabytes of the log file before it gets rotated (default: 100)
max_backups = 10         # Maximum number of old log files to retain (default: 10)
level = "info"           # Log level: "debug", "info", "warn", "error" (default: "info")
enable_json = false      # Enable JSON structured logging to app.json.log (default: false)

[scan]
concurrency = 8          # Concurrency threads for scan operation (default: 8)
include_tags = true      # Include tags of S3 object in scan results 

[sync]
is_source_reserved = true    # Force overwrite existing files (default: false)
concurrency = 7              # Concurrency level for migration 

[integrity_check]

[database]
enabled = true           # Enable Database integration
type = "clickhouse"      # Only ClickHouse is supported
batch_size = 800000      # Batch size for file entries

[database.clickhouse]
# ClickHouse specific configuration
dsn = "http://10.131.9.20:8123"  # DSN format: http://host:port
dial_timeout = 5
read_timeout = 30
database = "default"
username = "default"
password = ""

```

## 示例用法

### 扫描示例

扫描当前目录，深度限制为3层，并排除git目录：
```bash
terrasync scan --id my_scan --depth 3 . --exclude 'name=="*.git"'
```

扫描D盘，只匹配最近24小时内修改的文本文件：
```bash
terrasync scan --id recent_files D:\ --match 'modified<1 and name == "*.txt"'
```

扫描NFS v3共享目录（带参数）：
```bash
terrasync scan --id nfs_scan --depth 5 nfs://fileserver:/data?uid=1000&gid=1000 --match 'type==file and size>1048576'
```

扫描NFS v4.1共享目录：
```bash
terrasync scan --id nfs4_scan "nfs://fileserver/export/data?version=4.1"
```

扫描S3存储桶中的特定路径（带认证信息）：
```bash
terrasync scan --id s3_scan s3://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG@bucket.example.com:9000/documents --match 'extension=="pdf"'
```

使用HTTPS协议扫描S3存储桶：
```bash
terrasync scan --id s3_https_scan s3+https://bucket.example.com/data
```

### 同步示例

基本同步操作，从源目录同步到目标目录：
```bash
terrasync sync --id my_sync C:\source D:\backup
```

启用校验和验证和ACL同步：
```bash
terrasync sync --id secure_sync --enable-integrity-check --enable-acl C:\source D:\backup
```

仅同步特定文件类型，排除临时文件：
```bash
terrasync sync --id selective_sync --match 'type==file and (name == "*.jpg" or name == "*.png")' --exclude 'name starts_with "~$"' C:\photos D:\backup_photos
```

从NFS v3同步到本地文件系统（带用户权限）：
```bash
terrasync sync --id nfs_to_local --enable-integrity-check nfs://fileserver:/share/data?uid=1000&gid=1000 D:\nfs_backup
```

从NFS v4.1同步到本地文件系统（启用ACL和xattr同步）：
```bash
terrasync sync --id nfs4_to_local --enable-integrity-check "nfs://fileserver/export/data?version=4.1&uid=1000&gid=1000" /local/backup
```

NFS v4.1到NFS v4.1同步（保留ACL和扩展属性）：
```bash
terrasync sync --id nfs4_sync "nfs://source-server/export/data?version=4.1" "nfs://dest-server/export/data?version=4.1"
```

从本地同步到S3存储（带认证信息）：
```bash
terrasync sync --id local_to_s3 --enable-integrity-check C:\documents s3://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG@bucket.example.com:9000/archives
```

从S3存储同步到NFS共享（使用HTTPS协议）：
```bash
terrasync sync --id s3_to_nfs --enable-integrity-check s3+https://bucket.example.com/backups nfs://fileserver:/share/archives
```

使用QoS带宽限制进行同步：
```bash
terrasync sync --id qos_sync --qos 100MiB/s --peak-qos-rate 1.5 --enable-integrity-check C:\large_files D:\backup
```

上述命令将：
- 设置平均同步速率限制为100MiB/s
- 设置峰值速率乘数为1.5，允许最高速率达到150MiB/s
- 启用文件校验和验证确保数据完整性

使用IOPS限制进行同步（适用于控制存储系统IO压力）：
```bash
terrasync sync --id iops_sync --iops 1000 C:\large_files D:\backup
```

同时启用带宽和IOPS限制：
```bash
terrasync sync --id dual_qos_sync --qos 200MiB/s --iops 5000 C:\large_files D:\backup
```

### 完整性检查示例

使用Blake3哈希进行完整数据校验：
```bash
terrasync integrity-check --id my_check /source /destination
```

快速校验模式，仅比较文件大小（适用于大规模数据的快速比对）：
```bash
terrasync integrity-check --id my_check --quick /source /destination
```

自动修复模式，检测到不一致时自动修正目标端元数据：
```bash
terrasync integrity-check --id my_check --auto-fix /source /destination
```

跨协议完整性检查（NFS与本地文件系统）：
```bash
terrasync integrity-check --id nfs_check nfs://fileserver:/share/data?uid=1000&gid=1000 /local/backup
```

## 任务（Job）管理

每个扫描、同步或ACE操作任务执行时，Rust Terrasync 会在程序运行目录下的 `jobs` 文件夹中生成对应的任务目录。任务目录具有以下重要作用：

1. **存储任务状态**：任务目录保存任务运行时状态；扫描结果和同步状态存储在 ClickHouse 中。
2. **增量操作依据**：任务目录作为全量操作和增量操作的评判标准。如果job目录已存在并再次执行相同id的任务，则系统会自动视为增量操作。

任务目录的命名规则如下：

- **指定了Job ID的情况**：如果在命令中使用 `--id` 参数指定了任务标识符（如前面示例中的 `my_scan`、`secure_sync` 等），则任务目录名将直接使用该标识符。
- **未指定Job ID的情况**：如果没有指定任务标识符，程序会自动生成包含任务类型和时间戳信息的任务目录名，格式为`[任务类型]_[YYYYMMDD_HHMMSS]`（例如`scan_20251009_144534`、`replicate_20251009_143744`或`scan_ace_20251009_145000`），确保唯一性。


## 日志与调用链追踪

Rust Terrasync 使用 [tracing](https://docs.rs/tracing) 提供分层日志和调用链追踪能力。

### 日志文件

| 文件 | 格式 | 说明 |
|------|------|------|
| `logs/app.log` | compact 文本 | 全量日志（级别由配置控制），人眼友好 |
| `logs/error.log` | compact 文本 | 仅 ERROR 级别，快速定位问题 |
| `logs/app.json.log` | JSON 结构化 | 可选，启用后供 jq / ELK / Loki 等工具做结构化查询 |

所有日志文件支持按大小和时间自动轮转（配置 `max_size` 和 `max_backups`）。

### 启用 JSON 结构化日志

通过配置文件或命令行启用：

```toml
# config.toml
[log]
enable_json = true
```

```bash
# 或通过命令行参数
terrasync --json sync --id my_sync /src /dest
```

### Span 层级结构

每条日志事件都带有完整的 span 上下文，形成调用链：

```
sync{job_id, src, dest}                        ← 顶层任务
  └─ receiver_worker{recv_id=3}                ← worker 级别
       └─ process_entry{path="data/file.csv"}  ← 条目级别
            └─ ERROR: Failed to copy ...       ← 日志事件
```

### JSON 日志格式

启用 `enable_json` 后，`app.json.log` 中每行一条 JSON 记录：

```json
{
  "timestamp": "2026-04-02T10:15:32.123456Z",
  "level": "ERROR",
  "fields": {
    "message": "Error creating directory"
  },
  "target": "app::sync",
  "filename": "app/src/sync.rs",
  "line_number": 480,
  "spans": [
    {"name": "sync", "job_id": "my_job", "src": "nfs://server/export", "dest": "s3://bucket"},
    {"name": "receiver_worker", "recv_id": 3},
    {"name": "process_entry", "path": "data/reports/2026"}
  ]
}
```

### 使用 jq 分析 JSON 日志

```bash
# 查找某个文件的所有日志（完整调用链）
cat logs/app.json.log | jq 'select(.spans[]? | .path? == "data/report.csv")'

# 查看所有 ERROR 及其调用链
cat logs/app.json.log | jq 'select(.level == "ERROR") | {time: .timestamp, msg: .fields.message, spans: .spans}'

# 统计各 worker 的错误数
cat logs/app.json.log | jq -r 'select(.level == "ERROR") | .spans[] | select(.recv_id?) | .recv_id' | sort | uniq -c

# 按 job_id 过滤
cat logs/app.json.log | jq 'select(.spans[]? | .job_id? == "my_job")'

# 查看某个 worker 处理了哪些文件
cat logs/app.json.log | jq 'select(.spans[]? | .recv_id? == 3) | .spans[] | select(.path?) | .path' | sort -u

# 只看拷贝失败的条目路径
cat logs/app.json.log | jq -r 'select(.level == "ERROR") | .spans[] | select(.path?) | .path' | sort -u

# 按时间范围过滤（示例：10:15 到 10:20 之间的错误）
cat logs/app.json.log | jq 'select(.level == "ERROR" and .timestamp >= "2026-04-02T10:15" and .timestamp < "2026-04-02T10:20")'
```

### 使用 compact 日志快速排查

不启用 JSON 时，也可以用 grep 在 compact 格式日志中追踪：

```bash
# 在 error.log 中找到出错条目
grep "CopyEntry failed" logs/error.log

# 拿到 recv_id 和时间戳，去 app.log 中找同一 worker 的完整上下文
grep "recv_id=3" logs/app.log | grep "10:15:3"
```

### 使用日志分析工具

项目提供了 `tools/log_analyzer.py`，可以从海量 trace 日志中自动提取最小有用集合，便于问题分析和定位。

#### 工作原理

1. 提取 app.log（或 app.json.log）前 N 行启动日志，获取任务配置上下文
2. 解析 error.log 中的 ERROR 信息，提取出错的文件路径
3. 在 app 日志中查找该路径的完整追踪链（JSON 模式通过 span 字段精确匹配，文本模式通过路径字符串匹配）
4. 根据追踪链首尾时间戳前后各扩展 5 秒，提取该时间窗口内的所有上下文日志
5. 合并重叠的时间窗口，输出去重后的最小日志集合

#### 基本用法

```bash
# JSON 模式（推荐，需配置 enable_json = true）
python tools/log_analyzer.py --app logs/app.json.log --error logs/error.log -o result.log

# 文本模式（自动检测格式，无需指定）
python tools/log_analyzer.py --app logs/app.log --error logs/error.log -o result.log

# 输出到 stdout
python tools/log_analyzer.py --app logs/app.log --error logs/error.log
```

#### 可选参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--app` | （必填） | app.log 或 app.json.log 文件路径，自动检测格式 |
| `--error` | （必填） | error.log 文件路径 |
| `--output, -o` | stdout | 输出文件路径 |
| `--head` | 300 | 提取日志前 N 行/条启动信息 |
| `--margin` | 5.0 | 时间窗口前后扩展秒数 |

#### 示例

```bash
# 扩大启动日志范围和时间窗口
python tools/log_analyzer.py --app logs/app.json.log --error logs/error.log --head 500 --margin 10 -o result.log

# 从远程服务器拷贝日志后分析
scp server:/opt/terrasync/logs/{app.json.log,error.log} /tmp/
python tools/log_analyzer.py --app /tmp/app.json.log --error /tmp/error.log -o /tmp/result.log
```

#### 输出格式

分析结果包含四个部分：

1. **启动日志** — 前 N 行，包含任务配置、数据库连接等初始化信息
2. **错误摘要** — error.log 中的所有 ERROR 条目
3. **时间窗口上下文** — 每个合并后的时间窗口内的完整日志，标注关联的错误路径和追踪条数
4. **统计信息** — 总行数、输出行数、压缩比

#### JSON 模式 vs 文本模式

| | JSON 模式 | 文本模式 |
|---|---|---|
| 日志文件 | `app.json.log` | `app.log` |
| 追踪链关联 | 通过 span 字段精确匹配 `path` | 通过路径字符串文本搜索 |
| 精确度 | 高（结构化数据） | 中（可能匹配到无关行） |
| 前置条件 | 配置 `enable_json = true` | 无 |

## 注意事项

- 在Windows系统上使用长路径时，程序会自动处理路径长度限制
- ACL同步和ACE管理功能仅在Windows系统上可用
- 对于大型目录结构，建议适当调整`--depth`参数以平衡扫描范围和性能
- 对于大型目录结构，NFS和S3存储可以通过指定prefix来平衡扫描范围和性能
- 增量操作依赖于任务目录的存在，删除任务目录将导致下次操作默认为全量操作

## 支持

如有问题或建议，请联系项目维护者。
