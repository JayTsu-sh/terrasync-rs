# DuckDB 表结构与功能说明

本文档详细描述了 DuckDB 数据库在 terrasync 项目中的表结构、功能实现和使用方法。

## 表结构设计

### 1. 主扫描表 (scan_base_{job_id})

主扫描表用于存储文件系统扫描的基础数据，表名格式为 `scan_base_{job_id}`，其中 `{job_id}` 是任务唯一标识符。

**表结构定义：**
```sql
CREATE TABLE scan_base_{job_id} (
    path TEXT NOT NULL PRIMARY KEY,
    size BIGINT,
    ext TEXT,
    ctime BIGINT,
    mtime BIGINT,
    atime BIGINT,
    perm TEXT,
    is_symlink INTEGER,
    is_dir INTEGER,
    is_regular_file INTEGER,
    hard_links INTEGER,
    current_state INTEGER
);
```

**字段说明：**
- `path`: 文件路径，作为主键
- `size`: 文件大小 (UInt64)
- `ext`: 文件扩展名 (可选)
- `ctime`: 创建时间戳 (Unix时间戳，以毫秒为单位)
- `mtime`: 修改时间戳 (Unix时间戳，以毫秒为单位)
- `atime`: 访问时间戳 (Unix时间戳，以毫秒为单位)
- `perm`: 文件权限字符串 (可选)
- `is_symlink`: 是否为符号链接 (布尔值，存储为整数 0/1)
- `is_dir`: 是否为目录 (布尔值，存储为整数 0/1)
- `is_regular_file`: 是否为普通文件 (布尔值，存储为整数 0/1)
- `hard_links`: 硬链接数量
- `current_state`: 当前状态标识 (0 或 1，用于增量扫描)

### 2. 扫描状态表 (scan_state_{job_id})

状态表用于存储扫描任务的状态信息，表名格式为 `scan_state_{job_id}`。

**表结构定义：**
```sql
CREATE TABLE scan_state_{job_id} (
    id INTEGER PRIMARY KEY,
    scan_state INTEGER
);
```

**字段说明：**
- `id`: 固定为 1，用于标识唯一的状态记录
- `scan_state`: 原点状态值 (0 或 1)，用于确定文件的新增、变更和删除

### 3. 临时扫描表 (temp_files_{job_id}_{uuid})

临时表用于存储最新扫描的文件数据，表名格式为 `temp_files_{job_id}_{uuid}`，其中 `uuid` 是为每次扫描生成的唯一标识符。

**表结构定义：** 与主扫描表相同

## 使用方法

### 1. 创建数据库连接

```rust
// 创建DuckDB连接
let db = DuckDBDatabase::new("path/to/db", "job_id");
```

### 2. 创建表结构

```rust
// 创建主扫描表
db.create_scan_base_table().await?;

// 创建状态表
db.create_scan_state_table().await?;

// 创建临时扫描表
db.create_scan_temporary_table().await?;

// 获取临时表名
let temp_table_name = db.get_scan_temp_table_name();
```

### 3. 插入数据

```rust
// 向临时表批量插入记录
let records = vec![/* FileScanRecord对象 */];
db.batch_insert_temp_record(records).await?;

// 向主表批量插入记录
let records = vec![/* FileScanRecord对象 */];
db.batch_insert_base_record(&records).await?;
```

### 4. 查询数据

```rust
// 查询主表中的指定列记录
let columns = &["path", "size", "mtime"];
let records = db.query_scan_base_table(columns).await?;

// 查询扫描状态
let scan_state = db.query_scan_state().await?;

// 查询新增文件
let (sender, receiver) = std::sync::mpsc::channel();
db.query_new_items(sender).await?;

// 查询变更文件
let (sender, receiver) = std::sync::mpsc::channel();
db.query_changed_items(sender).await?;

// 查询删除文件
let (sender, receiver) = std::sync::mpsc::channel();
db.query_deleted_items(scan_state, sender).await?;
```

### 5. 管理表结构

```rust
// 检查表是否存在
if db.table_exists("table_name").await? {
    // 删除表
db.drop_table("table_name").await?;
}

// 删除临时表
db.drop_scan_temporary_table().await?;

// 关闭数据库连接
db.close().await?;
```

## API 参考

### 构造函数

#### `DuckDBDatabase::new(db_path: &str, job_id: &str) -> Self`
- **描述**: 创建DuckDB数据库连接
- **参数**: 
  - `db_path`: DuckDB数据库文件路径
  - `job_id`: 任务唯一标识符
- **返回值**: DuckDBDatabase实例

### 核心接口

#### `ping(&self) -> Result<()>`
- **描述**: 检查数据库连接是否正常
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `create_table(&self, table_name: &str) -> Result<()>`
- **描述**: 创建表
- **参数**: 
  - `table_name`: 表名
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `drop_table(&self, table_name: &str) -> Result<()>`
- **描述**: 删除指定名称的表
- **参数**: 
  - `table_name`: 要删除的表名
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult>`
- **描述**: 执行不返回结果的查询（INSERT, UPDATE, DELETE）
- **参数**: 
  - `sql`: SQL查询语句
  - `params`: 参数数组
- **返回值**: 包含受影响行数等信息的查询结果

#### `table_exists(&self, table_name: &str) -> Result<bool>`
- **描述**: 检查指定名称的表是否存在
- **参数**: 
  - `table_name`: 要检查的表名
- **返回值**: 表存在时返回true，不存在时返回false，出错时返回错误

#### `close(&self) -> Result<()>`
- **描述**: 关闭数据库连接
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `database_type(&self) -> &'static str`
- **描述**: 获取数据库类型
- **参数**: 无
- **返回值**: 数据库类型字符串

### 表管理函数

#### `create_scan_base_table(&self) -> Result<()>`
- **描述**: 创建主扫描表
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `create_scan_state_table(&self) -> Result<()>`
- **描述**: 创建扫描状态表
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `create_scan_temporary_table(&mut self) -> Result<()>`
- **描述**: 创建临时扫描表
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `drop_scan_temporary_table(&mut self) -> Result<()>`
- **描述**: 删除当前临时表
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `get_scan_temp_table_name(&self) -> Option<&str>`
- **描述**: 获取当前临时表名
- **参数**: 无
- **返回值**: 临时表名（如果存在）

### 数据操作函数

#### `batch_insert_temp_record(&self, records: Vec<FileScanRecord>) -> Result<()>`
- **描述**: 同步批量插入数据到临时表
- **参数**: 
  - `records`: 文件扫描记录向量
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `batch_insert_base_record(&self, records: &Vec<FileScanRecord>) -> Result<()>`
- **描述**: 批量插入数据到base表
- **参数**: 
  - `records`: 文件扫描记录向量引用
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `query_scan_base_table(&self, columns: &[&str]) -> Result<Vec<FileScanRecord>>`
- **描述**: 查询scan_base表，支持指定列查询
- **参数**: 
  - `columns`: 要查询的列名数组
- **返回值**: 成功时返回记录列表，失败时返回错误

### 状态管理函数

#### `query_scan_state(&self) -> Result<u8>`
- **描述**: 查询scan_state表，返回id=1的scan_state值
- **参数**: 无
- **返回值**: 成功时返回scan_state值，失败时返回错误

#### `switch_scan_state(&self) -> Result<()>`
- **描述**: 切换scan_state表状态
- **参数**: 无
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `insert_scan_state(&self, scan_state: u8) -> Result<()>`
- **描述**: 同步插入scan_state表，id固定为1
- **参数**: 
  - `scan_state`: 要设置的scan_state值
- **返回值**: 成功时返回Ok(())，失败时返回错误

### 差异比较函数

#### `query_new_items(&self, item_sender: std::sync::mpsc::Sender<FileScanRecord>) -> Result<()>`
- **描述**: 查询在最新扫描中新增的文件
- **参数**: 
  - `item_sender`: 用于发送查询结果的Sender
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `query_changed_items(&self, item_sender: std::sync::mpsc::Sender<FileScanRecord>) -> Result<()>`
- **描述**: 查询在最新扫描中内容发生变更的文件
- **参数**: 
  - `item_sender`: 用于发送查询结果的Sender
- **返回值**: 成功时返回Ok(())，失败时返回错误

#### `query_deleted_items(&self, scan_state: u8, item_sender: std::sync::mpsc::Sender<FileScanRecord>) -> Result<()>`
- **描述**: 查询在上一次扫描中存在但在最新扫描中缺失的文件
- **参数**: 
  - `scan_state`: 原点状态值
  - `item_sender`: 用于发送查询结果的Sender
- **返回值**: 成功时返回Ok(())，失败时返回错误

## 注意事项

1. **多任务隔离**：所有表名都包含`{job_id}`后缀，确保多任务同时运行时的数据隔离。

2. **事务处理**：数据插入和删除操作都在事务中执行，确保数据一致性。

3. **临时表命名**：临时表名称包含UUID，避免重复创建导致的命名冲突。

4. **批量操作**：删除操作采用批量处理，提高性能。

5. **错误处理**：所有数据库操作都有完善的错误处理机制，确保异常情况下的可靠性。

6. **异步接口**：所有公开API都是异步的，适合在异步环境中使用。

7. **参数匹配**：调用API时，请确保参数类型和数量与接口定义匹配，特别是在处理可变引用和所有权时。