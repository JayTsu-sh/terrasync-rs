# Filter 过滤表达式系统设计文档

## 1. 概述

Filter 模块（`storage_v2/src/filter.rs`）提供文件/目录过滤能力，支持白名单（`--match`）和黑名单（`--exclude`）两种模式。通过表达式语法描述过滤条件，在目录遍历（walkdir）过程中决定每个条目是否应被跳过、是否继续扫描子目录、子项是否需要继续匹配检查。

## 2. 表达式语法

### 2.1 基本格式

```
<field> <operator> <value>
```

### 2.2 支持的字段（Field）

| 字段 | 说明 | 值类型 | 支持操作符 |
|------|------|--------|-----------|
| `name` | 文件/目录名 | glob 模式 | `==`, `!=` |
| `path` | 相对路径 | glob 模式（含 `*`、`**`） | `==`, `!=` |
| `type` | 条目类型 | `file`, `dir`, `symlink` | `==`, `!=` |
| `modified` | 修改时间 | 相对天数 或 绝对日期 | `<`, `>`, `<=`, `>=`, `==`(仅绝对), `!=`(仅绝对) |
| `size` | 文件大小（字节） | 整数 | `==`, `<`, `>`, `<=`, `>=` |
| `extension` | 文件扩展名 | glob 模式 | `==`, `!=` |
| `dir_date` | 目录名中的日期 | 日期值 | `==`, `!=`, `<`, `>`, `<=`, `>=` |

### 2.3 操作符

| 操作符 | 含义 | 内部枚举 |
|--------|------|---------|
| `==` | 等于 | `CompareOp::Eq` |
| `!=` | 不等于 | `CompareOp::Ne` |
| `<` | 小于 | `CompareOp::Lt` |
| `>` | 大于 | `CompareOp::Gt` |
| `<=` | 小于等于 | `CompareOp::Le` |
| `>=` | 大于等于 | `CompareOp::Ge` |

### 2.4 逻辑运算符

| 运算符 | 优先级 | 说明 |
|--------|--------|------|
| `and` | 高 | 逻辑与，短路求值 |
| `or` | 低 | 逻辑或，短路求值 |
| `()` | 最高 | 括号改变优先级 |

### 2.5 值的格式

**Glob 模式**（name, path, extension）：
- `*` — 匹配单层路径内的任意字符（不跨越 `/`）
- `**` — 匹配零或多层路径（仅 path 条件有意义）
- `?` — 匹配单个字符
- 值可用双引号或单引号包裹：`name == "*.txt"` 或 `name == '*.txt'`

**Modified 值**：
- 相对天数：`3d`、`30`、`0.5`（不支持 `==`）
- ISO 日期：`"2025-01-15"`、`"2025-01-15T12:00:00"`
- 紧凑日期：`20250115`（8 位及以上纯数字 ≥ 10000000）

**Dir_date 值**：
- YYMMDD：`240301`（6 位数字，YY 映射为 2000+YY）
- YYYYMMDD：`20240301`（8 位数字）
- YYYY-MM-DD：`"2024-03-01"`（可带引号）

**Size 值**：
- 纯整数字节数：`1024`、`0`

**Type 值**：
- `file`、`dir`、`symlink`（不需引号）

### 2.6 表达式示例

```bash
# 基本条件
name == "*.txt"
path == "src/**"
type == file
size > 1024
modified < 3d
extension == "rs"
dir_date <= "240301"

# 组合条件
name == "*.txt" and type == file
name == "*.rs" or name == "*.toml"
(name == "*.txt" or name == "*.rs") and type == file
dir_date <= "2024-03-01" and path == "project/*"
modified > "2025-01-01" and modified < "2025-03-01"
```

## 3. 架构设计

### 3.1 处理流水线

```
表达式字符串
    ↓
Lexer（词法分析）── tokenize() ──→ Vec<Token>
    ↓
FilterParser（语法分析）── parse_expression() ──→ FilterASTNode (AST)
    ↓
FilterExpression { root: FilterASTNode }
    ↓
evaluate() ── 递归求值 ──→ MatchResult
    ↓
should_skip() ── 白/黑名单逻辑 ──→ (should_skip, continue_scan, check_children)
```

### 3.2 核心数据结构

```
FilterExpression
  └── root: FilterASTNode
        ├── Condition(FilterCondition)    ── 叶节点
        ├── And(Box<AST>, Box<AST>)       ── 与节点
        └── Or(Box<AST>, Box<AST>)        ── 或节点

FilterCondition（枚举）
  ├── Name { operator, pattern }
  ├── Path { operator, raw_value, pattern, pattern_parts, pattern_depth, has_double_wildcard, pattern_after_wildcard }
  ├── Type { operator, value }
  ├── Modified { operator, value: ModifiedValue }
  ├── Size { operator, value }
  ├── Extension { operator, pattern }
  └── DirDate { operator, epoch }

ModifiedValue（枚举）
  ├── RelativeDays(f64)
  └── AbsoluteEpoch(i64)
```

### 3.3 Token 定义

```
Token
  ├── Condition(FilterCondition)   ── 已解析的条件
  ├── And                          ── 逻辑与
  ├── Or                           ── 逻辑或
  ├── LParen                       ── 左括号
  └── RParen                       ── 右括号
```

## 4. MatchResult 匹配结果体系

### 4.1 枚举定义

```
MatchResult
  ├── Match(MatchAddon)
  │     ├── PathMatch        ── 仅含路径条件的匹配（或 dir_date 匹配）
  │     ├── NonPathMatch     ── 不含路径条件的匹配（透明传递）
  │     └── MixMatch         ── 混合条件的匹配
  ├── PartialMatch           ── 目录部分匹配（仅 Path 条件的目录前缀匹配）
  ├── MisMatch(MisMatchAddon)
  │     ├── FullPathNotMatch ── 完整路径不匹配（或 dir_date 日期不匹配）
  │     └── Other            ── 其他原因不匹配
  └── NotSupport             ── 条件不适用（缺少所需字段数据）
```

### 4.2 BitAnd（与运算）优先级

```
NotSupport > MisMatch > PartialMatch > Match
```

规则：
- 任一 `NotSupport` → `NotSupport`
- 任一 `MisMatch` → `MisMatch`（保留 `FullPathNotMatch` 优先）
- 任一 `PartialMatch` → `PartialMatch`
- 两个 `Match` → `Match`（合并 Addon：PathMatch+NonPathMatch=MixMatch）

### 4.3 BitOr（或运算）优先级

```
Match > PartialMatch > MisMatch > NotSupport
```

规则与 And 相反：取"更乐观"的结果。

### 4.4 短路求值

- **And**：左侧为 `MisMatch` 或 `NotSupport` 时直接返回，不评估右侧
- **Or**：左侧为 `Match` 时直接返回，不评估右侧

## 5. should_skip() 三元组语义

### 5.1 返回值定义

```rust
pub fn should_skip(...) -> (bool, bool, bool)
//                          ↑       ↑        ↑
//                    should_skip  continue  check_children
//                                 _scan
```

| 字段 | 含义 |
|------|------|
| `should_skip` | 是否跳过当前条目（不构建 StorageEntry） |
| `continue_scan` | 是否继续扫描子目录（仅目录有效） |
| `check_children` | 子项是否需要执行 should_skip 匹配 |

### 5.2 执行流程

```
                ┌─────────────────┐
                │  排除表达式检查  │  （黑名单优先）
                │  (exclude_expr) │
                └────────┬────────┘
                         │
         ┌───────────────┼───────────────┐──────────────┐
         ↓               ↓               ↓              ↓
   Match(PathMatch)  Match(Other)   PartialMatch    MisMatch/
   Match(MixMatch)                                  NotSupport
         │               │               │              │
         ↓               ↓               ↓              ↓
   (T, F, F)        (T, is_dir, T)  (T, T, T)    流转到白名单
   跳过+停止递归    跳过+继续扫描    跳过+继续      ↓
                                                ┌──────────────────┐
                                                │  匹配表达式检查    │
                                                │  (match_expr)    │
                                                └────────┬─────────┘
                         ┌───────────────┼──────────────┐──────────────┐──────────────┐
                         ↓               ↓              ↓              ↓              ↓
                   Match(PathMatch)  Match(Other)  PartialMatch  MisMatch(Full)  MisMatch(Other)
                         │               │              │              │              │
                         ↓               ↓              ↓              ↓              ↓
                   (F, is_dir, F)  (F, is_dir, T)  (T, is_dir, T)  (T, F, F)    (T, is_dir, T)
                   保留+子项免检    保留+子项检查    跳过+继续检查    跳过+停止     跳过+继续检查
```

### 5.3 各条件的 MatchResult 映射表

| 条件 | 匹配 | 不匹配 | 字段缺失 |
|------|------|--------|---------|
| `name` | `Match(NonPathMatch)` | `MisMatch(Other)` | `NotSupport` |
| `path` | `Match(PathMatch)` / `PartialMatch` | `MisMatch(FullPathNotMatch/Other)` | `NotSupport` |
| `type` | `Match(NonPathMatch)` | `MisMatch(Other)` | `NotSupport` |
| `modified` | `Match(NonPathMatch)` | `MisMatch(Other)` | `NotSupport` |
| `size` | `Match(NonPathMatch)` | `MisMatch(Other)` | `NotSupport` |
| `extension` | `Match(NonPathMatch)` | `MisMatch(Other)` | `NotSupport` |
| `dir_date` | 见下方独立章节 | 见下方独立章节 | N/A |

## 6. Path 条件的特殊匹配逻辑

### 6.1 Glob 选项

```rust
const GLOB_MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,   // `*` 不匹配 `/`
    require_literal_leading_dot: false,
};
```

### 6.2 匹配层次

`match_path_with_pattern()` 按以下顺序尝试：

1. **全路径匹配**：`pattern.matches(file_path)` — 返回 `Match(PathMatch)`
2. **祖先路径匹配**：截取 `file_path` 的祖先路径用 pattern 匹配 — 返回 `Match(PathMatch)`
   - 无 `**`：只检查 `depth == pattern_depth` 的祖先（O(1)）
   - 有 `**`：遍历 `min_depth..file_depth` 范围
3. **目录部分匹配**（仅目录）：截断 pattern 到目录深度后匹配 — 返回 `PartialMatch`
   - 含 `**` 且有后缀 pattern 时，需验证目录名匹配后缀
4. **不匹配**：文件返回 `MisMatch(FullPathNotMatch)`，目录返回 `MisMatch(Other)`

### 6.3 `!=` 操作符反转

| 原始结果 | `!=` 反转后 |
|---------|-----------|
| `Match(PathMatch/MixMatch)` | `MisMatch(FullPathNotMatch)` |
| `Match(_)/PartialMatch` | `MisMatch(Other)` |
| `MisMatch(_)` | `Match(PathMatch)` |

## 7. dir_date 条件设计

### 7.1 功能说明

`dir_date` 从目录名中提取日期信息，与指定日期进行比较。用于按日期命名的目录结构（如数据归档、日志目录等）。

**约束**：仅用于白名单（`--match`），不用于黑名单（`--exclude`）。

### 7.2 条件值格式

| 格式 | 示例 | 解析方式 |
|------|------|---------|
| YYMMDD | `240301` | year=2000+24, month=03, day=01 |
| YYYYMMDD | `20240301` | year=2024, month=03, day=01 |
| YYYY-MM-DD | `"2024-03-01"` | 标准 ISO 日期（可带引号） |

解析后统一转为 UTC 午夜的 Unix epoch seconds 存储在 `FilterCondition::DirDate { operator, epoch }` 中。

### 7.3 目录名日期提取

`extract_date_from_dir_name(name: &str) -> Option<i64>` 函数按优先级扫描目录名：

1. **YYYY-MM-DD**：滑动窗口查找 `4位数字-2位数字-2位数字` 模式
2. **YYYYMMDD**：查找 ≥8 位连续数字串，取前 8 位解析（year ≥ 1900）
3. **YYMMDD**：查找恰好 6 位连续数字串解析（year = 2000+YY）

日期验证：month ∈ [1,12], day ∈ [1,31]。无效日期返回 `None`。

**支持的目录名示例**：

| 目录名 | 提取结果 |
|--------|---------|
| `20240301` | 2024-03-01 |
| `240301` | 2024-03-01 |
| `2024-03-01` | 2024-03-01 |
| `backup_240301` | 2024-03-01（日期在末尾） |
| `20240301_logs` | 2024-03-01（日期在开头） |
| `project_2024-03-01_final` | 2024-03-01（日期在中间） |
| `nodate_folder` | None |
| `20241301` | None（月份 13 无效） |

### 7.4 MatchResult 语义

| 场景 | 返回值 | should_skip 效果 |
|------|--------|-----------------|
| 非目录（file/symlink） | `Match(NonPathMatch)` | 透明通过，由其他条件决定 |
| 目录名不含日期 | `Match(NonPathMatch)` | 透明通过，由其他条件决定 |
| 目录日期匹配 | `Match(PathMatch)` | **保留**，子项**免检**（check_children=false） |
| 目录日期不匹配 | `MisMatch(FullPathNotMatch)` | **跳过**，**停止扫描**子目录 |

### 7.5 与其他条件的组合行为

#### 独立使用

```bash
--match 'dir_date<="240301"'
```

| 遇到的条目 | dir_date 结果 | 三元组 | 说明 |
|-----------|---------------|--------|------|
| `20240101/`（日期目录，匹配） | `Match(PathMatch)` | `(F, T, F)` | 保留，扫描子目录，子项免检 |
| `20240501/`（日期目录，不匹配） | `MisMatch(FullPathNotMatch)` | `(T, F, F)` | 跳过，停止递归 |
| `project/`（非日期目录） | `Match(NonPathMatch)` | `(F, T, T)` | 保留，扫描子目录，子项继续检查 |
| `readme.txt`（文件） | `Match(NonPathMatch)` | `(F, F, T)` | 保留 |

#### AND 组合

```bash
--match 'dir_date<="2024-03-01" and path=="project/*"'
```

| 条目 | dir_date | path | AND 结果 | 三元组 |
|------|----------|------|---------|--------|
| `project/`（非日期目录） | `Match(NonPath)` | `Match(Path)` | `Match(MixMatch)` | `(F, T, T)` |
| `project/20240101/` | `Match(Path)` | `Match(Path)` | `Match(Path)` | `(F, T, F)` |
| `project/20240501/` | `MisMatch(Full)` | `Match(Path)` | `MisMatch(Full)` | `(T, F, F)` |
| `other/`（非日期，路径不匹配） | `Match(NonPath)` | `MisMatch(Other)` | `MisMatch(Other)` | `(T, T, T)` |

### 7.6 使用示例

```bash
# 打包所有 <= 2024-03-01 的日期目录
terrasync sync --match 'dir_date<="240301"' /src /dest

# 打包所有 < 2024年的日期目录
terrasync sync --match 'dir_date<"20240101"' /src /dest

# 结合路径条件
terrasync sync --match 'dir_date<="2024-03-01" and path=="project/*"' /src /dest

# 日期范围
terrasync sync --match 'dir_date>="20240101" and dir_date<="20240331"' /src /dest
```

## 8. Modified 条件详解

### 8.1 相对天数

计算 `(now_epoch - file_epoch) / 86400.0` 得到文件已修改的天数，与条件值比较。

```bash
modified < 3d     # 3天内修改的文件
modified > 30     # 30天前修改的文件
modified < 0.5    # 12小时内修改的文件
```

**注意**：相对天数不支持 `==` 操作符（浮点精度问题），解析时会报错。

### 8.2 绝对日期

直接比较 epoch seconds。`==` 按天粒度（`epoch / 86400`）比较。

```bash
modified > "2025-01-15"           # YYYY-MM-DD
modified > "2025-01-15T12:00:00"  # YYYY-MM-DDTHH:MM:SS
modified > 20250115               # YYYYMMDD（8位及以上数字 ≥ 10000000）
```

### 8.3 值类型判定逻辑

```
带引号且含 '-' → ISO 日期 (parse_date_to_epoch)
带引号且含 'T' → ISO 日期时间 (parse_date_to_epoch)
纯数字 ≥ 10000000 → 紧凑日期 YYYYMMDD (parse_compact_date_to_epoch)
其他 → 相对天数（带 'd' 后缀或纯数字）
```

## 9. 日期工具函数

### 9.1 date_to_epoch

```rust
fn date_to_epoch(year, month, day, hour, minute, second) -> i64
```

基于 Rata Die 算法变体，将年月日时分秒转换为 UTC Unix epoch seconds。无外部依赖。

### 9.2 parse_date_to_epoch

解析 `"YYYY-MM-DD"` 或 `"YYYY-MM-DDTHH:MM:SS"` 格式。

### 9.3 parse_compact_date_to_epoch

解析 `"YYYYMMDD"` 紧凑格式。

### 9.4 extract_date_from_dir_name

从目录名中提取日期，按 YYYY-MM-DD → YYYYMMDD → YYMMDD 优先级扫描，返回 `Option<i64>`。

### 9.5 parse_dir_date_value

解析 `dir_date` 条件值字符串为 epoch seconds，支持三种日期格式（YYMMDD/YYYYMMDD/YYYY-MM-DD）。

### 9.6 is_valid_date

验证月份（1-12）和日期（1-31）是否在合法范围内。

## 10. Lexer（词法分析器）

### 10.1 职责

将表达式字符串分解为 Token 序列。

### 10.2 处理流程

1. 跳过空白字符
2. 识别括号 `(` `)`
3. 识别逻辑运算符 `and` `or`（需前置 Condition/RParen 检查，避免与字段名冲突）
4. 读取条件表达式 `read_condition()` → 调用 `parse_condition()` 解析为 `FilterCondition`

### 10.3 条件解析

`parse_condition()` 按操作符 `["==", "!=", "<=", ">=", "<", ">"]` 顺序查找分隔符，提取 `field` 和 `value`，根据 field 名称分派到各条件构造逻辑。

## 11. Parser（语法分析器）

### 11.1 文法

```
expression  := or_expr
or_expr     := and_expr ("or" and_expr)*
and_expr    := primary ("and" primary)*
primary     := Condition | "(" or_expr ")"
```

### 11.2 优先级

`()` > `and` > `or`

### 11.3 AST 输出

构建 `FilterASTNode` 树，由 `FilterExpression` 封装。

## 12. 存储层集成

### 12.1 调用链

```
CLI (--match, --exclude)
  → app::config::initialize_scan_config()     # parse_filter_expression()
  → app::dir_walker::DirectoryWalker::walk()  # 传递 Option<FilterExpression>
  → storage_v2::StorageEnum::walkdir()         # 分派到具体后端
  → local.rs / nfs.rs / s3.rs                 # 两阶段调用 should_skip()
```

### 12.2 两阶段过滤（local/nfs）

| 阶段 | 可用字段 | 不可用字段 |
|------|---------|-----------|
| 第一阶段（无 metadata） | name, path, type, extension, dir_date | modified, size |
| 第二阶段（有 metadata） | name, path, type, modified, size, dir_date | extension |

两阶段结果合并：
```rust
(
    first_skip || second_skip,              // 任一跳过则跳过
    first_continue && second_continue,      // 都继续才继续
    first_check || second_check,            // 任一需检查则检查
)
```

**dir_date 说明**：`dir_date` 仅依赖 `file_name` 和 `file_type`，在第一阶段即可完成评估，无需等待 metadata。

### 12.3 遍历栈

目录入栈时携带 `need_submatch` 标记（对应 `check_children`），为 `false` 时子项跳过 should_skip 调用。

## 13. 错误处理

所有解析错误通过 `StorageError` 枚举变体传播：

| 错误 | 触发场景 |
|------|---------|
| `InvalidFilterExpression` | 无效操作符、无效 glob 模式、无效日期格式、无效类型值 |
| `MismatchedParentheses` | 括号不匹配 |
| `InvalidToken` | 非法 token |
| `UnexpectedEndOfToken` | 表达式非预期结束 |

## 14. 配置选项

Glob 匹配全局配置（不可由用户修改）：

```rust
const GLOB_MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: true,              // 大小写敏感
    require_literal_separator: true,   // `*` 不跨越 `/`
    require_literal_leading_dot: false, // `*` 可匹配 `.` 开头
};
```
