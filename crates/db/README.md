# 数据库工厂

ClickHouse 数据库工厂及统一访问接口。

## 特性

- **统一接口**：数据库操作使用统一的异步 API
- **类型安全**：每种数据库都有强类型配置
- **Async/Await**：为异步Rust应用程序构建
- **工厂模式**：创建和使用之间的清晰分离

## 支持的数据库

- **ClickHouse**：面向分析型工作负载的列式数据库

## 使用方法

### 1. 初始化工厂

```rust
use db::init;

// 初始化数据库工厂，注册内置类型
init().unwrap();
```

### 2. 创建数据库配置

#### ClickHouse配置

```rust
use db::{DatabaseConfig, ClickHouseConfig};

let config = DatabaseConfig {
    enabled: true,
    db_type: "clickhouse".to_string(),
    batch_size: 1000,
    clickhouse: Some(ClickHouseConfig {
        dsn: "tcp://localhost:9000".to_string(),
        dial_timeout: 10,
        read_timeout: 30,
        database: "default".to_string(),
        username: "default".to_string(),
        password: Some("password".to_string()),
    }),
};
```

### 3. 创建数据库实例

```rust
use db::create_database;

// 需要提供任务ID用于表命名隔离
let job_id = "my_scan_job";
let database = create_database(&config, job_id)?;
```

### 4. 使用数据库

所有数据库操作使用相同的统一接口：

```rust
use serde_json::json;
use db::traits::FileScanRecord;

// 创建表结构
let _ = database.create_scan_base_table().await?;
let _ = database.create_scan_state_table().await?;

// 创建临时表
let _ = database.create_scan_temporary_table().await?;
let temp_table_name = database.get_scan_temp_table_name();

// 批量插入数据到临时表
let records = vec![
    FileScanRecord {
        path: "/test/file1.txt".to_string(),
        size: 1024,
        ext: Some("txt".to_string()),
        ctime: 1620000000000,
        mtime: 1620000000000,
        atime: 1620000000000,
        perm: Some("644".to_string()),
        is_symlink: false,
        is_dir: false,
        is_regular_file: true,
        hard_links: 1,
        current_state: 0,
    },
    // 更多记录...
];
let _ = database.batch_insert_temp_record(records).await?;

// 查询数据
let columns = &["path", "size", "mtime"];
let scan_records = database.query_scan_base_table(columns).await?;

// 查询扫描状态
let scan_state = database.query_scan_state().await?;

// 切换扫描状态
let _ = database.switch_scan_state().await?;

// 查询新增、变更和删除的文件
let (sender_new, receiver_new) = std::sync::mpsc::channel();
let (sender_changed, receiver_changed) = std::sync::mpsc::channel();
let (sender_deleted, receiver_deleted) = std::sync::mpsc::channel();

let _ = database.query_new_items(sender_new).await?;
let _ = database.query_changed_items(sender_changed).await?;
let _ = database.query_deleted_items(scan_state, sender_deleted).await?;

// 处理结果...
// let new_items: Vec<FileScanRecord> = receiver_new.iter().collect();

// 删除临时表
let _ = database.drop_scan_temporary_table().await?;

// 关闭数据库连接
let _ = database.close().await?;
```

## 添加新的数据库类型

要添加新的数据库类型，实现`Database` trait并注册它：

```rust
use async_trait::async_trait;
use db::{Database, DatabaseConfig, DatabaseFactory, Result};
use db::traits::FileScanRecord;

pub struct MyCustomDatabase {
    // 你的实现
}

#[async_trait]
impl Database for MyCustomDatabase {
    async fn ping(&self) -> Result<()> {
        // 实现
    }
    
    // 实现所有必需的方法...
    
    fn database_type(&self) -> &'static str {
        "custom"
    }
}

// 注册新类型
DatabaseFactory::register_database_type("custom", |config, job_id| {
    // 根据配置创建数据库实例
    let db = MyCustomDatabase::new(config, &job_id)?;
    Ok(Arc::new(db) as Arc<dyn Database>)
})?;
```

## 表结构

数据库工厂支持以下表结构：

### 1. 主扫描表 (scan_base_{job_id})

存储文件系统扫描的基础数据，表名包含任务ID作为后缀。

### 2. 扫描状态表 (scan_state_{job_id})

存储扫描任务的状态信息，用于增量扫描。

### 3. 临时扫描表

用于存储最新扫描的文件数据，与主表进行比较以识别变更。

## 错误处理

所有操作返回`db::Result<T>`，它包装了自定义的`DatabaseError`类型：

- `ConnectionError`：数据库连接问题
- `QueryError`：SQL查询执行错误
- `ConfigError`：配置验证错误
- `UnsupportedType`：尝试使用未注册的数据库类型
- `DatabaseNotFound`：请求的数据库不存在

## 配置

ClickHouse 配置包括连接字符串、超时设置和认证信息。

## 注意事项

1. **多任务隔离**：所有表名都包含`{job_id}`后缀，确保多任务同时运行时的数据隔离
2. **资源管理**：使用完毕后记得关闭数据库连接和删除临时表
3. **异步API**：所有操作都是异步的，适合在异步环境中使用
4. **参数匹配**：调用API时，请确保参数类型和数量与接口定义匹配
5. **事务处理**：所有表操作都在事务中执行，确保数据一致性
6. **临时表命名**：临时表命名包含UUID，避免命名冲突
7. **批量处理**：插入操作使用批量处理，提高性能
8. **列名限制**：查询结果中的列名可能因数据库而异，需要适配处理
