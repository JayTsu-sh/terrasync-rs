#!/bin/bash
# setup-s3-test-data.sh — 在 S3 桶中创建 3x3x3 目录树（无 symlink）
# 结果：40 目录前缀 / 117 文件对象 / 0 软链接
# 依赖：mc (MinIO Client)
set -e

S3_HOST="http://10.128.137.245:8184"
S3_AK="H80NKRVS5DYOVE43U2HS"
S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"
S3_BUCKET="mbucket-src"
S3_PREFIX="test-data"
MC_ALIAS="ts3"

# 配置 mc alias
mc alias set "$MC_ALIAS" "$S3_HOST" "$S3_AK" "$S3_SK" --api S3v4

# 清理已有数据
mc rm --recursive --force "${MC_ALIAS}/${S3_BUCKET}/${S3_PREFIX}/" 2>/dev/null || true
echo "Cleaned existing data"

# 创建本地临时目录
TMPDIR=$(mktemp -d)
BASE="$TMPDIR/$S3_PREFIX"
mkdir -p "$BASE"

DIRS=1  # 算上 test-data 自身
FILES=0

for i in 1 2 3; do
  mkdir -p "$BASE/d$i"
  DIRS=$((DIRS+1))
  for f in 1 2 3; do
    echo "level1-content-$i-$f" > "$BASE/d$i/file$f.txt"
    FILES=$((FILES+1))
  done

  for j in 1 2 3; do
    mkdir -p "$BASE/d$i/d${i}_$j"
    DIRS=$((DIRS+1))
    for f in 1 2 3; do
      echo "level2-content-${i}_${j}-$f" > "$BASE/d$i/d${i}_$j/file$f.txt"
      FILES=$((FILES+1))
    done

    for k in 1 2 3; do
      mkdir -p "$BASE/d$i/d${i}_$j/d${i}_${j}_$k"
      DIRS=$((DIRS+1))
      for f in 1 2 3; do
        echo "level3-content-${i}_${j}_${k}-$f" > "$BASE/d$i/d${i}_$j/d${i}_${j}_$k/file$f.txt"
        FILES=$((FILES+1))
      done
    done
  done
done

echo "=== Local tree created ==="
echo "Created: dirs=$DIRS, files=$FILES"

# 上传到 S3
echo "=== Uploading to S3 ==="
mc mirror --overwrite "$BASE/" "${MC_ALIAS}/${S3_BUCKET}/${S3_PREFIX}/"

# 清理本地临时目录
rm -rf "$TMPDIR"

# 验证 S3 上的文件数
S3_FILES=$(mc find "${MC_ALIAS}/${S3_BUCKET}/${S3_PREFIX}/" --type f 2>/dev/null | wc -l)
echo ""
echo "=== S3 verification ==="
echo "Expected: dirs=40, files=117, symlinks=0"
echo "S3 files: $S3_FILES"

if [ "$S3_FILES" -ne 117 ]; then
  echo "ERROR: S3 file count verification failed (expected 117, got $S3_FILES)"
  exit 1
fi
echo "OK: S3 file count verified (dirs will be verified by scan)"
