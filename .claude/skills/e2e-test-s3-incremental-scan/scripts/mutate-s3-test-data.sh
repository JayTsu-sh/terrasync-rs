#!/bin/bash
# mutate-s3-test-data.sh — 对 S3 桶中 3x3x3 目录树执行增删改+rename
# S3 无 symlink，rename = copy + delete（增量扫描视为 New + Deleted）
# 基线：40 dirs / 117 files / 0 symlinks
# 变更后：41 dirs / 115 files / 0 symlinks
# 依赖：mc (MinIO Client)
set -e

S3_HOST="http://10.128.137.245:8184"
S3_AK="H80NKRVS5DYOVE43U2HS"
S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"
S3_BUCKET="mbucket-src"
S3_PREFIX="test-data"
MC_ALIAS="ts3"
S3_BASE="${MC_ALIAS}/${S3_BUCKET}/${S3_PREFIX}"

# 配置 mc alias（幂等）
mc alias set "$MC_ALIAS" "$S3_HOST" "$S3_AK" "$S3_SK" --api S3v4

# 校验基线存在
if ! mc ls "${S3_BASE}/d1/" > /dev/null 2>&1; then
  echo "ERROR: Baseline test-data not found at ${S3_BASE}/"
  exit 1
fi

echo "=== Starting S3 mutations ==="

# ─── ADD: 2 new dirs (created implicitly by uploading files) ───
echo "new-file-content-1" | mc pipe "${S3_BASE}/d1/new_dir/new_file1.txt"
echo "new-file-content-2" | mc pipe "${S3_BASE}/d2/d2_1/new_sub_dir/new_file2.txt"
echo "ADD: 2 new dirs (d1/new_dir, d2/d2_1/new_sub_dir) + 2 files inside"

# ─── ADD: 1 more file in existing dir ───
echo "new-file-content-3" | mc pipe "${S3_BASE}/d3/d3_2/d3_2_1/extra_file.txt"
echo "ADD: 1 file (d3/d3_2/d3_2_1/extra_file.txt)"

# ─── MODIFY: 2 existing files (overwrite to change size+mtime) ───
echo "modified-content-appended-to-change-size-and-mtime" | mc pipe "${S3_BASE}/d1/d1_1/file1.txt"
echo "overwritten-content-for-test" | mc pipe "${S3_BASE}/d2/d2_2/d2_2_1/file3.txt"
echo "MODIFY: 2 files (d1/d1_1/file1.txt, d2/d2_2/d2_2_1/file3.txt)"

# ─── RENAME: 1 file (copy + delete, S3 has no native rename) ───
mc cp "${S3_BASE}/d2/d2_3/file1.txt" "${S3_BASE}/d2/d2_3/file1_renamed.txt"
mc rm "${S3_BASE}/d2/d2_3/file1.txt"
echo "RENAME: 1 file (d2/d2_3/file1.txt -> file1_renamed.txt)"

# ─── RENAME: 1 dir with contents (copy all + delete all) ───
# d3/d3_1/d3_1_3 contains: file1.txt, file2.txt, file3.txt (3 files)
mc cp --recursive "${S3_BASE}/d3/d3_1/d3_1_3/" "${S3_BASE}/d3/d3_1/d3_1_renamed/"
mc rm --recursive --force "${S3_BASE}/d3/d3_1/d3_1_3/"
echo "RENAME: 1 dir (d3/d3_1/d3_1_3 -> d3_1_renamed, cascade: 3 files)"

# ─── DELETE: 1 leaf dir + contents (3 files) ───
mc rm --recursive --force "${S3_BASE}/d3/d3_3/d3_3_3/"
echo "DELETE: 1 leaf dir d3/d3_3/d3_3_3 (contained 3 files)"

# ─── DELETE: 2 standalone files ───
mc rm "${S3_BASE}/d1/file2.txt"
mc rm "${S3_BASE}/d2/d2_1/file3.txt"
echo "DELETE: 2 files (d1/file2.txt, d2/d2_1/file3.txt)"

echo ""
echo "=== Mutations complete ==="
echo ""
echo "=== Expected incremental scan results (S3 Path mode, no fh3) ==="
echo "New:     dirs=3,  files=7, symlinks=0  (total=10)"
echo "Changed: dirs=0,  files=2, symlinks=0  (total=2)"
echo "Renamed: dirs=0,  files=0, symlinks=0  (total=0)  [S3 rename = new+deleted]"
echo "Deleted: dirs=2,  files=9, symlinks=0  (total=11)"
echo ""

# ─── Verify post-mutation file count ───
echo "=== Post-mutation verification ==="
S3_FILES=$(mc find "${S3_BASE}/" --type f 2>/dev/null | wc -l)

EXPECTED_FILES=115  # 117 + 3(add) - 5(del: 3 in d3_3_3 + 2 standalone), rename keeps count

echo "Expected files: $EXPECTED_FILES"
echo "S3 files: $S3_FILES"

if [ "$S3_FILES" -ne "$EXPECTED_FILES" ]; then
  echo "ERROR: Post-mutation file count verification failed"
  exit 1
fi
echo "OK: Post-mutation file count verified (dirs=41 will be verified by scan)"
