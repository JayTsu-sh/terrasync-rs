---
name: e2e-test-local-filter
description: >
  This skill should be used when the user asks to "run local filter tests",
  "test filter expressions on local", "local 过滤器e2e测试",
  "测试本地存储 --match/--exclude 参数", "local filter e2e",
  or mentions testing the filter (match/exclude) pipeline against local filesystem storage.
---

# Local Filter Expression E2E Test Skill

## Overview

本地文件系统过滤器端到端测试：创建测试数据 → 分组运行 scan（带各类 filter 表达式） → 验证 CLI 统计输出。

**代码路径**：`storage_v2/src/local.rs`

**无远端依赖**（无 NFS/S3/ClickHouse）——仅需本地 binary 和临时目录。

**Symlink 注**：Linux 有效，Windows 跳过（Windows 需管理员权限创建符号链接，所有 Symlinks 期望值 = 0）。

## Constants

| Name | Value |
|------|-------|
| BASE_DIR | `/tmp/terrasync-filter-test` |
| CONFIG | `examples/config.toml` |
| BINARY | `./target/debug/terrasync` |
| BASIC_URL | `/tmp/terrasync-filter-test/basic` |
| EXT_URL | `/tmp/terrasync-filter-test/extension` |
| SIZE_URL | `/tmp/terrasync-filter-test/size` |

测试数据集（均在 `{BASE_DIR}` 下）：
- `basic/` — 8 文件 + 4 目录 + 2 symlinks（Linux）
- `extension/` — 10 文件（.txt×2, .jpg, .png, .csv, .json, .log, .md, .rs, .py）+ 4 目录
- `size/` — 6 文件（small=1000B, equal=1048576B, large=1048577B × root+subdir）+ 2 目录

CLI 输出中的统计格式（用于验证）：
```
  Scanned Statistics:
   ├─ Dirs:          <N>  (...)
   ├─ Regular Files: <N>  (...)
   ├─ Symlinks:      <N>  (...)
   └─ Total:         <N>  (...)
```

---

## Step 0: 构建 binary 并创建测试数据

**0a–0b 可并发执行。**

### 0a. 构建 debug binary

```bash
cargo build -p cli 2>&1 | tail -5
```

Expected: 无 error，生成 `./target/debug/terrasync`。

### 0b. 创建测试数据

```bash
bash .claude/skills/e2e-test-local-filter/scripts/setup-local.sh /tmp/terrasync-filter-test
```

Expected（末尾输出）：
```
=== 测试数据创建完成 ===
BASE_DIR: /tmp/terrasync-filter-test
  basic/     — 8 文件 + 4 目录 + 2 symlinks(Linux)
  extension/ — 10 文件（.txt/.jpg/.png/.csv/.json/.log/.md/.rs/.py）
  size/      — 6 文件（<1MB / =1MB / >1MB）× 2 层目录
```

### 清理残留 job 目录

```bash
find jobs -maxdepth 1 -type d -name "filter-local-*" | xargs rm -rf 2>/dev/null || true
```

---

## Step 1: 名称匹配测试（NAME MATCHING）

测试数据：`{BASIC_URL}`。

### 1a. 无过滤器基线

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-no-filter {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Dirs=4, Regular Files=8, Symlinks=2, Total=14
- Windows: Dirs=4, Regular Files=8, Symlinks=0, Total=12

### 1b. name == 'file*'（匹配 file 开头）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-name-prefix -m "name == 'file*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=8, Symlinks=0, Dirs=0

### 1c. name == '*3*'（包含数字 3）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-name-contains -m "name == '*3*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Regular Files=1, Symlinks=1, Dirs=0
- Windows: Regular Files=1, Symlinks=0, Dirs=0

### 1d. name != '*.txt'（非 txt 文件）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-name-not-eq -m "name != '*.txt'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Regular Files=0, Symlinks=2, Dirs=4
- Windows: Regular Files=0, Symlinks=0, Dirs=4

---

## Step 2: 路径匹配测试（PATH MATCHING）

测试数据：`{BASIC_URL}`。

### 2a. path == '*dir1*'

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-path-dir1 -m "path == '*dir1*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Regular Files=4, Symlinks=1, Dirs=2
- Windows: Regular Files=4, Symlinks=0, Dirs=2

### 2b. path == '*dir*'（包含 dir 的路径）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-path-dir -m "path == '*dir*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Regular Files=7, Symlinks=1, Dirs=4
- Windows: Regular Files=7, Symlinks=0, Dirs=4

### 2c. path == '**/subdir*'（globstar 匹配 subdir）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-path-globstar -m "path == '**/subdir*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=4, Symlinks=0, Dirs=2

### 2d. path == '*/subdir1/*'（subdir1 下的条目）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-path-subdir1 -m "path == '*/subdir1/*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=2, Symlinks=0, Dirs=0

---

## Step 3: 类型匹配测试（TYPE MATCHING）

测试数据：`{BASIC_URL}`。

### 3a. type == 'file'

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-type-file -m "type == 'file'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=8, Symlinks=0, Dirs=0

### 3b. type == 'symlink'（仅 Linux 有意义）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-type-symlink -m "type == 'symlink'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Regular Files=0, Symlinks=2, Dirs=0
- Windows: Regular Files=0, Symlinks=0, Dirs=0

### 3c. type == 'dir'

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-type-dir -m "type == 'dir'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=0, Symlinks=0, Dirs=4

---

## Step 4: 组合条件测试（COMBINED CONDITIONS）

测试数据：`{BASIC_URL}`。

### 4a. name + type（AND）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-combined-name-type \
  -m "name == '*.txt' and type == 'file'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=8, Symlinks=0, Dirs=0

### 4b. path + name（AND）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-combined-path-name \
  -m "path == '*dir2*/**/*' and name == '*6*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=1（file6.txt）

### 4c. type OR type AND size（运算优先级）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-combined-type-size \
  -m "type == 'dir' or type == 'file' and size >= 15" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=1（file8.txt，15 字节）, Dirs=4

### 4d. 嵌套括号表达式

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-nested-expr \
  -e 'name == "*.txt" and (path == "*dir1*" or path == "*dir2*")' \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=1（只剩 file8.txt）

---

## Step 5: 排除过滤测试（EXCLUDE FILTER）

测试数据：`{BASIC_URL}`。

### 5a. match + exclude 组合（排除特定文件）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-exclude-file \
  -m "name == '*.txt'" -e "name == 'file1.txt'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=7（8 减去 file1.txt）

### 5b. 排除 subdir1 下的文件

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-exclude-subdir \
  -m "name == '*.txt'" -e "path == '*dir*/subdir1*'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=6（去掉 subdir1 下的 file3.txt 和 file4.txt）

### 5c. 排除所有目录

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-exclude-dirs -e "type == 'dir'" \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected:
- Linux: Regular Files=8, Symlinks=2, Dirs=0
- Windows: Regular Files=8, Symlinks=0, Dirs=0

### 5d. OR 逻辑排除

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-exclude-or \
  -e 'name == "file1.txt" or name == "file5.txt"' \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=6（排除 file1.txt 和 file5.txt）

---

## Step 6: 扩展名过滤测试（EXTENSION FILTER）

测试数据：`{EXT_URL}`（10 个文件：2x.txt, .jpg, .png, .csv, .json, .log, .md, .rs, .py）。

### 6a. extension == 'txt'

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-ext-specific -m "extension == 'txt'" \
  {EXT_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=2（file1.txt, file2.txt）

### 6b. extension == 't*'（通配符）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-ext-wildcard -m "extension == 't*'" \
  {EXT_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=2（仅 .txt 文件）

### 6c. extension != 'txt'（非 txt）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-ext-not-eq -m "extension != 'txt'" \
  {EXT_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=8（其余 8 种扩展名的文件）

### 6d. exclude extension == 'txt'

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-ext-exclude -e "extension == 'txt'" \
  {EXT_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=8, Dirs=4

### 6e. 排除多种扩展名（OR 逻辑）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-ext-exclude-multi \
  -e "extension == '*t*' or extension == '*s*'" \
  {EXT_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=5（.jpg, .png, .log, .md, .py 保留；.txt/.csv/.json/.rs 被排除）

---

## Step 7: 大小过滤测试（SIZE FILTER）

测试数据：`{SIZE_URL}`（6 文件：small=1000B, equal=1048576B, large=1048577B × root+subdir）。

### 7a. exclude size < 1048576（排除小于 1MB）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-size-exclude-small -e "size < 1048576" \
  {SIZE_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=4（equal + large × 2 层）

### 7b. match size <= 1048576（保留不超过 1MB）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-size-le -m "size <= 1048576" \
  {SIZE_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=4（small + equal × 2 层）

### 7c. match size > 1048576（仅大文件）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-size-gt -m "size > 1048576" \
  {SIZE_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=2（large_file.txt × 2 层）

---

## Step 8: 深度限制测试（DEPTH LIMIT）

测试数据：`{BASIC_URL}`（2 层子目录）。

### 8a. depth=1（仅根目录直接条目）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-depth-1 --depth 1 \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=1（file8.txt）, Dirs=2（dir1, dir2）

### 8b. depth=2（两层深度）

```bash
{BINARY} -c {CONFIG} scan \
  --id filter-local-depth-2 --depth 2 \
  {BASIC_URL} 2>&1 | grep -A6 "Scanned Statistics"
```

Expected: Regular Files=4（file8.txt + dir1/{file1,file2} + dir2/file5）, Dirs=4

---

## Step 9: 清理

```bash
rm -rf /tmp/terrasync-filter-test
find jobs -maxdepth 1 -type d -name "filter-local-*" | xargs rm -rf 2>/dev/null || true
```

---

## Completion Criteria

- [ ] Binary built (Step 0a)
- [ ] Test data created: basic/extension/size (Step 0b)
- [ ] Name matching tests passed (Step 1)
- [ ] Path matching tests passed (Step 2)
- [ ] Type matching tests passed (Step 3)
- [ ] Combined condition tests passed (Step 4)
- [ ] Exclude filter tests passed (Step 5)
- [ ] Extension filter tests passed (Step 6)
- [ ] Size filter tests passed (Step 7)
- [ ] Depth limit tests passed (Step 8)
- [ ] Environment cleaned (Step 9)

## Notes

- **Platform 差异**：symlink 测试在 Windows 上期望值为 0（Windows 需管理员权限创建符号链接）
- **job_id 冲突**：若重复运行，`jobs/` 目录下的同名 job 会触发增量扫描而非全量扫描，需先清理 `jobs/filter-local-*`
- **config.toml**：需确保 `[database] enabled = false`，避免触发 ClickHouse 写入
