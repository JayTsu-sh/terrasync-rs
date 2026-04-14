# ClickHouse表创建功能文档

本文档描述了`ClickHouseDatabase`中的表创建和管理功能，这些功能基于Golang示例实现。

## 表结构

### 1. 主扫描表 (scan_base_{job_id})
存储完整的文件扫描信息，使用`ReplacingMergeTree`引擎处理重复数据。表名包含job_id确保多任务隔离。

**字段定义：**
- `path String` - 文件路径
- `size UInt64` - 文件大小
- `ext Nullable(String)` - 文件扩展名
- `ctime DateTime64(3)` - 创建时间（精确到毫秒）
- `mtime DateTime64(3)` - 修改时间（精确到毫秒）
- `atime DateTime64(3)` - 访问时间（精确到毫秒）
- `perm Nullable(String)` - 权限
- `is_symlink Bool` - 是否为符号链接
- `is_dir Bool` - 是否为目录
- `is_regular_file Bool` - 是否为普通文件
- `hard_links UInt32` - 硬链接数
- `current_state UInt8` - 当前状态

### 2. 临时扫描表 (temp_files_{uuid})
结构与主表相同，但使用`MergeTree`引擎，表名包含UUID确保唯一性。用于存储临时扫描结果。

### 3. 状态表 (scan_state_{job_id})
存储扫描状态信息，使用`ReplacingMergeTree`引擎。表名包含job_id确保多任务隔离。

**字段定义：**
- `id UInt8` - 状态ID
- `scan_state UInt8` - 原始状态值

## 使用方法

### 基本用法

```rust
use db::clickhouse::ClickHouseDatabase;
use db::config::ClickHouseConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClickHouseConfig {
        dsn: "tcp://localhost:9000".to_string(),
        database: "default".to_string(),
        username: "default".to_string(),
        password: None,
    };

    let db = ClickHouseDatabase::new(config, "my_scan_job");
    db.ping().await?;

    // 创建主扫描表
    db.create_scan_base_table().await?;
    
    // 创建状态表
    db.create_scan_state_table().await?;
    
    // 创建临时表
    let mut db_with_temp = ClickHouseDatabase::new(config, "my_scan_job");
    db_with_temp.create_scan_temporary_table().await?;
    if let Some(temp_name) = db_with_temp.get_scan_temp_table_name() {
        println!("临时表名： {}", temp_name);
    }

    // 清理临时表
    db_with_temp.drop_scan_temporary_table().await?;

    Ok(())
}```

### API参考

#### 构造方法

- `new(config: ClickHouseConfig, job_id: &str)` - 创建新的ClickHouse数据库连接实例，job_id用于区分不同扫描任务

#### 表管理方法

- `create_scan_base_table()` - 创建主扫描表
- `create_scan_state_table()` - 创建状态表
- `create_scan_temporary_table()` - 创建临时表（需要可变引用）
- `drop_table_by_name(table_name: &str)` - 根据表名删除指定表
- `drop_tables_with_prefix(prefix: &str)` - 删除所有以指定前缀开头的表
- `drop_scan_temporary_table()` - 删除临时表（需要可变引用）
- `get_scan_temp_table_name()` - 获取当前临时表名
- `table_exists(table_name: &str)` - 检查表是否存在

#### 数据操作方法

- `batch_insert_temp_record(records: Vec<FileScanRecord>)` - 批量插入记录到临时表
- `batch_insert_base_record(records: &Vec<FileScanRecord>)` - 批量插入记录到主表（异步模式）
- `query_scan_base_table(columns: &[&str])` - 查询主扫描表，支持指定列查询，使用FINAL关键字

#### 状态管理方法

- `query_scan_state()` - 查询scan_state表，返回id=1的scan_state值
- `switch_scan_state()` - 切换scan_state表状态（1 - 当前状态）
- `insert_scan_state(scan_state: u8)` - 同步插入scan_state表，id固定为1

#### 文件比较方法

- `query_new_items(item_sender: std::sync::mpsc::Sender<FileScanRecord>)` - 查询在最新扫描中新增的文件
- `query_changed_items(item_sender: std::sync::mpsc::Sender<FileScanRecord>)` - 查询在最新扫描中内容发生变更的文件
- `query_deleted_items(scan_state: u8, item_sender: std::sync::mpsc::Sender<FileScanRecord>)` - 查询在上一次扫描中存在但在最新扫描中缺失的文件

#### 表名常量

- `SCAN_BASE_TABLE_BASE_NAME` - "scan_base"
- `SCAN_STATE_TABLE_BASE_NAME` - "scan_state"
- `SCAN_TEMP_TABLE_BASE_NAME` - "temp_files"

## 注意事项

1. **临时表管理**：临时表需要时单独创建，创建后应在使用完毕时调用`drop_scan_temporary_table()`进行清理。每次调用`create_scan_temporary_table()`都会生成新的UUID，旧的临时表不会自动清理。

2. **错误处理**：所有方法都返回`Result<()>`或`Result<T>`，需要适当的错误处理。

3. **连接管理**：初始化后可以使用`ping()`方法验证连接是否正常。

4. **测试环境**：集成测试需要实际的ClickHouse服务器，默认被忽略。

5. **方法签名差异**：
   - 大多数表管理和查询方法只需要不可变引用 `&self`
   - `create_scan_temporary_table()` 和 `drop_scan_temporary_table()` 需要可变引用 `&mut self`，因为它们会更新内部状态

6. **批量操作**：批量操作（插入、删除）时会使用同步或异步模式，确保数据一致性。

7. **多任务隔离**：通过job_id区分不同的扫描任务，避免表名冲突。

8. **FINAL关键字**：查询主表时使用FINAL关键字确保获取最新合并后的数据。

## 示例代码

查看以下文件获取完整示例：
- `tests/test_clickhouse_tables.rs` - 集成测试