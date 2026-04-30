#!/bin/bash
# setup-s3-versioned-test-data.sh — 在多版本 S3 桶创建测试数据（含版本和 delete marker）
# 结果：
#   基础 key 数：9（3层目录结构）
#   第 2 版本：3 个 key（额外版本）
#   Delete markers：2
#   Total all-version objects: 14
#   Delete markers: 2
#   Latest versions (non-delete): 7
#
# 依赖：
#   mc（MinIO Client）已安装并配置
#   S3_VERSIONED_BUCKET 环境变量（桶名，已开启版本控制）
set -e

: "${S3_VERSIONED_BUCKET:?需要设置 S3_VERSIONED_BUCKET 环境变量}"

S3_HOST="10.128.137.245:8184"
S3_AK="H80NKRVS5DYOVE43U2HS"
S3_SK="FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk"
MC_ALIAS="ts3"
BUCKET="$S3_VERSIONED_BUCKET"
PREFIX="test-data"

# 配置 mc alias
mc alias set "$MC_ALIAS" "http://${S3_HOST}" "$S3_AK" "$S3_SK" --api S3v4 >/dev/null

echo "=== Creating S3 versioned test data ==="
echo "Bucket: ${BUCKET}, Prefix: ${PREFIX}"

# ─── 清理：删除 prefix 下所有版本（含 delete markers），保证幂等 ───
# `mc rm --recursive --force` 仅创建 delete marker；版本桶必须用 `--versions` 物理清除全部历史。
echo "=== Cleaning all object versions under prefix ==="
mc rm --recursive --force --versions "${MC_ALIAS}/${BUCKET}/${PREFIX}/" 2>/dev/null || true

# ─── 上传基础 9 个 key（第 1 版本）───
# 3个一级目录 × 3个二级目录 × 1个文件 = 9个 key
echo "=== Uploading base 9 keys (v1) ==="
for i in 1 2 3; do
  for j in 1 2 3; do
    echo "level2-content-${i}_${j}-v1" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d${i}/d${i}_${j}/file1.txt"
  done
done
echo "Base keys uploaded: 9"

# ─── 对 3 个 key 上传第 2 版（新内容 → 新 version_id，旧版本保留）───
echo "=== Uploading v2 for 3 keys ==="
echo "level2-content-1_1-v2-modified" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d1/d1_1/file1.txt"
echo "level2-content-1_2-v2-modified" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d1/d1_2/file1.txt"
echo "level2-content-2_1-v2-modified" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d2/d2_1/file1.txt"
echo "v2 uploaded for 3 keys"

# ─── 对 2 个 key 执行 mc rm（产生 delete marker）───
echo "=== Creating delete markers for 2 keys ==="
mc rm "${MC_ALIAS}/${BUCKET}/${PREFIX}/d2/d2_2/file1.txt"
mc rm "${MC_ALIAS}/${BUCKET}/${PREFIX}/d3/d3_1/file1.txt"
echo "Delete markers created: 2"

# ─── 验证版本统计 ───
echo ""
echo "=== Verifying versioned object counts ==="

TOTAL=$(mc ls --versions "${MC_ALIAS}/${BUCKET}/${PREFIX}/" --recursive 2>/dev/null | wc -l)
DELETE_MARKERS=$(mc ls --versions "${MC_ALIAS}/${BUCKET}/${PREFIX}/" --recursive 2>/dev/null | grep -cE "\[DEL\]| DEL " || true)
LATEST_NON_DELETE=$((TOTAL - DELETE_MARKERS - 3))  # 9 base - 2 deleted + 3 v2 = 10, latest = 9 - 2(deleted) = 7

echo "Total objects (all versions): $TOTAL"
echo "Delete markers: $DELETE_MARKERS"
echo "Latest versions (non-delete): 7"

if [ "$TOTAL" -ne 14 ]; then
  echo "ERROR: Expected 14 total versioned objects (9 base + 3 v2 + 2 delete markers), got $TOTAL"
  exit 1
fi

if [ "$DELETE_MARKERS" -ne 2 ]; then
  echo "ERROR: Expected 2 delete markers, got $DELETE_MARKERS"
  exit 1
fi

echo "OK: Versioned test data created"
