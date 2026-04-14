#!/usr/bin/env bash
# setup-test-data.sh — 创建 filter e2e 测试所需的本地测试数据
# 创建 3 组测试目录：basic / extension / size
# 使用方法：bash setup-test-data.sh [BASE_DIR]
# BASE_DIR 默认为 /tmp/terrasync-filter-test

set -euo pipefail

BASE_DIR="${1:-/tmp/terrasync-filter-test}"

echo "=== 清理旧测试数据 ==="
rm -rf "$BASE_DIR"

echo "=== 创建 basic 测试数据集 ==="
# 结构：8 个普通文件 + 4 个目录 + 2 个符号链接（Linux）
# dir1/file1.txt, dir1/file2.txt
# dir1/subdir1/file3.txt, dir1/subdir1/file4.txt
# dir2/file5.txt
# dir2/subdir2/file6.txt, dir2/subdir2/file7.txt
# file8.txt (根目录文件，内容较大 > 15 字节)

BASIC_DIR="$BASE_DIR/basic"
mkdir -p "$BASIC_DIR/dir1/subdir1"
mkdir -p "$BASIC_DIR/dir2/subdir2"

echo "12345"     > "$BASIC_DIR/dir1/file1.txt"
echo "67890"     > "$BASIC_DIR/dir1/file2.txt"
echo "11223"     > "$BASIC_DIR/dir1/subdir1/file3.txt"
echo "33445"     > "$BASIC_DIR/dir1/subdir1/file4.txt"
echo "55667"     > "$BASIC_DIR/dir2/file5.txt"
echo "77889"     > "$BASIC_DIR/dir2/subdir2/file6.txt"
echo "99001"     > "$BASIC_DIR/dir2/subdir2/file7.txt"
echo "12345678908282" > "$BASIC_DIR/file8.txt"

# 符号链接（仅 Linux 支持）
if [[ "$(uname)" == "Linux" ]]; then
    ln -sf "../subdir1/file3.txt" "$BASIC_DIR/dir1/symlink_to_file3"
    ln -sf "dir1"                 "$BASIC_DIR/symlink1"
fi

echo "basic: $(find "$BASIC_DIR" -type d | wc -l) dirs, $(find "$BASIC_DIR" -type f | wc -l) files"
if [[ "$(uname)" == "Linux" ]]; then
    echo "basic: $(find "$BASIC_DIR" -type l | wc -l) symlinks"
fi

echo ""
echo "=== 创建 extension 测试数据集 ==="
# 10 个不同扩展名的文件
# .txt(2), .jpg, .png, .csv, .json, .log, .md, .rs, .py

EXT_DIR="$BASE_DIR/extension"
mkdir -p "$EXT_DIR/dir1/subdir1"
mkdir -p "$EXT_DIR/dir2/subdir2"

echo "12345"          > "$EXT_DIR/dir1/file1.txt"
echo "67890"          > "$EXT_DIR/dir1/file2.txt"
echo "jpg content"    > "$EXT_DIR/dir1/file1.jpg"
echo "png content"    > "$EXT_DIR/dir1/file2.png"
echo "csv content"    > "$EXT_DIR/dir1/subdir1/file3.csv"
echo "json content"   > "$EXT_DIR/dir1/subdir1/file4.json"
echo "log content"    > "$EXT_DIR/dir2/file5.log"
echo "markdown content" > "$EXT_DIR/dir2/subdir2/file6.md"
echo "rust content"   > "$EXT_DIR/dir2/subdir2/file7.rs"
echo "python content" > "$EXT_DIR/file8.py"

echo "extension: $(find "$EXT_DIR" -type d | wc -l) dirs, $(find "$EXT_DIR" -type f | wc -l) files"

echo ""
echo "=== 创建 size 测试数据集 ==="
# 6 个文件，覆盖 3 种大小区间 (<1MB / =1MB / >1MB) × 根目录 + subdir

SIZE_DIR="$BASE_DIR/size"
mkdir -p "$SIZE_DIR/subdir"

# 1000 字节（小于 1MB）
dd if=/dev/zero bs=1000 count=1 of="$SIZE_DIR/small_file.txt" 2>/dev/null

# 1048576 字节（恰好等于 1MB）
dd if=/dev/zero bs=1048576 count=1 of="$SIZE_DIR/equal_file.txt" 2>/dev/null

# 1048577 字节（大于 1MB）
dd if=/dev/zero bs=1048576 count=1 of="$SIZE_DIR/large_file.txt" 2>/dev/null
echo -n "x" >> "$SIZE_DIR/large_file.txt"

# subdir 下同样的 3 个文件
dd if=/dev/zero bs=1000 count=1 of="$SIZE_DIR/subdir/small_file.txt" 2>/dev/null
dd if=/dev/zero bs=1048576 count=1 of="$SIZE_DIR/subdir/equal_file.txt" 2>/dev/null
dd if=/dev/zero bs=1048576 count=1 of="$SIZE_DIR/subdir/large_file.txt" 2>/dev/null
echo -n "x" >> "$SIZE_DIR/subdir/large_file.txt"

echo "size: $(find "$SIZE_DIR" -type d | wc -l) dirs, $(find "$SIZE_DIR" -type f | wc -l) files"
echo "  small_file.txt:  $(wc -c < "$SIZE_DIR/small_file.txt") bytes (expected: 1000)"
echo "  equal_file.txt:  $(wc -c < "$SIZE_DIR/equal_file.txt") bytes (expected: 1048576)"
echo "  large_file.txt:  $(wc -c < "$SIZE_DIR/large_file.txt") bytes (expected: 1048577)"

echo ""
echo "=== 测试数据创建完成 ==="
echo "BASE_DIR: $BASE_DIR"
echo "  basic/     — 8 文件 + 4 目录 + 2 symlinks(Linux)"
echo "  extension/ — 10 文件（.txt/.jpg/.png/.csv/.json/.log/.md/.rs/.py）"
echo "  size/      — 6 文件（<1MB / =1MB / >1MB）× 2 层目录"
