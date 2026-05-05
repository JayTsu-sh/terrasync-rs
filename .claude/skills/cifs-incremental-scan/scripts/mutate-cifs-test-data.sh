#!/bin/bash
# mutate-cifs-test-data.sh — 对 setup-cifs-test-data.sh 创建的目录树执行增删改+rename
# 基线：40 dirs / 117 files / 0 symlinks
# 变更后：41 dirs / 115 files / 0 symlinks
#
# CIFS Fh3 模式预期增量统计：
#   New:     5 total | dirs 2 | files 3
#   Changed: 2 total | dirs 0 | files 2
#   Renamed: 5 total | dirs 1 | files 4
#   Deleted: 6 total | dirs 1 | files 5
#
# 依赖环境变量（必须提前 export）：
#   CIFS_HOST  — CIFS 服务器 IP 或域名
#   CIFS_SHARE — 共享名称
#   CIFS_USER  — 用户名
#   CIFS_PASS  — 密码
#   CIFS_PORT  — 可选，默认 445
set -e

: "${CIFS_HOST:?需要设置 CIFS_HOST 环境变量}"
: "${CIFS_SHARE:?需要设置 CIFS_SHARE 环境变量}"
: "${CIFS_USER:?需要设置 CIFS_USER 环境变量}"
: "${CIFS_PASS:?需要设置 CIFS_PASS 环境变量}"
CIFS_PORT="${CIFS_PORT:-445}"

SMB_CMD="smbclient //${CIFS_HOST}/${CIFS_SHARE} -U ${CIFS_USER}%${CIFS_PASS} -p ${CIFS_PORT}"

# 校验基线存在
DIR_CHECK=$($SMB_CMD -c "ls test-data\\d1" 2>/dev/null | grep -c 'd1' || true)
if [ "$DIR_CHECK" -eq 0 ]; then
  echo "ERROR: Baseline test-data not found on CIFS share"
  exit 1
fi

echo "=== Starting CIFS mutations ==="

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# ─── ADD: 2 new dirs + files ───
mkdir -p "$TMPDIR/new_dir"
echo "new-file-content-1" > "$TMPDIR/new_dir/new_file1.txt"
mkdir -p "$TMPDIR/new_sub_dir"
echo "new-file-content-sub" > "$TMPDIR/new_sub_dir/.keep"

# 上传新目录到 d1/new_dir（改用 mkdir + mput；tar -Tx 在某些 Samba/SELinux 下会失败）
$SMB_CMD -c "mkdir test-data\\d1\\new_dir" 2>/dev/null || true
(cd "$TMPDIR/new_dir" && $SMB_CMD -D "test-data\\d1\\new_dir" -c "prompt OFF; mput *.txt" >/dev/null 2>&1) || true

# 上传新子目录到 d2/d2_1/new_sub_dir
mkdir -p "$TMPDIR/ns/new_sub_dir"
echo "new-file-content-sub" > "$TMPDIR/ns/new_sub_dir/new_file_sub.txt"
$SMB_CMD -c "mkdir test-data\\d2\\d2_1\\new_sub_dir" 2>/dev/null || true
(cd "$TMPDIR/ns/new_sub_dir" && $SMB_CMD -D "test-data\\d2\\d2_1\\new_sub_dir" -c "prompt OFF; mput *.txt" >/dev/null 2>&1) || true
echo "ADD: 2 new dirs (d1/new_dir with new_file1.txt, d2/d2_1/new_sub_dir)"

# ─── ADD: 1 more new file (standalone) ───
echo "new-file-d2" > "$TMPDIR/new_file_in_d2.txt"
$SMB_CMD -c "put $TMPDIR/new_file_in_d2.txt test-data\\d2\\new_file_in_d2.txt" 2>/dev/null
echo "ADD: 1 file (d2/new_file_in_d2.txt)"

echo "ADD: 3 new files total"

# ─── MODIFY: 2 existing files (overwrite to change size+mtime) ───
echo "appended-content-to-change-size-and-mtime" > "$TMPDIR/file1_modified.txt"
$SMB_CMD -c "put $TMPDIR/file1_modified.txt test-data\\d1\\d1_1\\file1.txt" 2>/dev/null

echo "overwritten-content" > "$TMPDIR/file3_modified.txt"
$SMB_CMD -c "put $TMPDIR/file3_modified.txt test-data\\d2\\d2_2\\d2_2_1\\file3.txt" 2>/dev/null
echo "MODIFY: 2 files (d1/d1_1/file1.txt, d2/d2_2/d2_2_1/file3.txt)"

# ─── RENAME: 1 file (smbclient rename 保留 fh3，CIFS Fh3 策略可检测到 Renamed) ───
$SMB_CMD -c "rename test-data\\d2\\d2_3\\file1.txt test-data\\d2\\d2_3\\file1_renamed.txt" 2>/dev/null
echo "RENAME: 1 file (d2/d2_3/file1.txt -> file1_renamed.txt)"

# ─── RENAME: 1 dir + cascade (3 files inside) ───
# smbclient 支持 rename 目录（服务端 rename，保留文件句柄关联）
$SMB_CMD -c "rename test-data\\d3\\d3_1\\d3_1_3 test-data\\d3\\d3_1\\d3_1_renamed" 2>/dev/null
echo "RENAME: 1 dir (d3/d3_1/d3_1_3 -> d3_1_renamed, cascade: 3 files)"

# ─── DELETE: 1 leaf dir + contents (3 files) ───
$SMB_CMD -c "deltree test-data\\d3\\d3_3\\d3_3_3" 2>/dev/null
echo "DELETE: 1 leaf dir d3/d3_3/d3_3_3 (contained 3 files)"

# ─── DELETE: 2 standalone files ───
$SMB_CMD -c "del test-data\\d1\\file2.txt" 2>/dev/null
$SMB_CMD -c "del test-data\\d2\\d2_1\\file3.txt" 2>/dev/null
echo "DELETE: 2 files (d1/file2.txt, d2/d2_1/file3.txt)"

echo ""
echo "=== Mutations complete ==="
echo ""

# ─── 验证变更后文件数 ───
echo "=== Post-mutation verification ==="
FILE_COUNT=$($SMB_CMD -D "test-data" -c "recurse ON; ls" 2>/dev/null | grep -c '\.txt' || true)
echo "Expected files: 115"
echo "CIFS files: $FILE_COUNT"

if [ "$FILE_COUNT" -ne 115 ]; then
  echo "ERROR: Expected 115 files, got $FILE_COUNT"
  echo "Note: 41 dirs / 115 files / 0 symlinks"
  exit 1
fi
echo "OK: Post-mutation file count verified (dirs=41 will be verified by scan)"
