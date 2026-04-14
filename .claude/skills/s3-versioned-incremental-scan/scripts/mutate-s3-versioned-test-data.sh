#!/bin/bash
# mutate-s3-versioned-test-data.sh — 对 setup-s3-versioned-test-data.sh 创建的多版本桶执行变更
# 基线：9 keys（含 v1+v2 版本和 2 delete markers）
#
# 变更操作：
#   N1=3: 上传新版本（对已有 key 上传新内容 → 新 version_id，旧版本保留）
#   N2=2: 删除 key（mc rm → 产生 delete marker，新 version_id）
#   N3=2: 上传全新 key（新路径 → 新 version_id）
#   N4=3: 永久删除旧版本（mc rm --version-id → Deleted）
#
# 增量扫描预期：
#   New:     7（N1=3 新版本 + N2=2 delete markers + N3=2 新 key）
#   Changed: 0（version_id 参与唯一性，新版本是 New 而非 Changed）
#   Renamed: 0（S3 Path 模式）
#   Deleted: 3（N4=3 被永久删除的旧版本）
#
# 依赖：
#   mc（MinIO Client）已安装并配置
#   S3_VERSIONED_BUCKET 环境变量（桶名）
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

echo "=== Starting S3 versioned mutations ==="

# ─── N1=3: 上传新版本（对已有 key 上传新内容）───
echo "=== N1: Uploading new versions for 3 existing keys ==="
echo "v-next-content-2_2" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d2/d2_2/file1.txt"
echo "v-next-content-2_3" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d2/d2_3/file1.txt"
echo "v-next-content-3_2" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d3/d3_2/file1.txt"
echo "New versions uploaded: 3"

# ─── N2=2: 删除 key（产生 delete marker）───
echo "=== N2: Creating delete markers for 2 keys ==="
mc rm "${MC_ALIAS}/${BUCKET}/${PREFIX}/d3/d3_3/file1.txt"
mc rm "${MC_ALIAS}/${BUCKET}/${PREFIX}/d1/d1_3/file1.txt"
echo "Delete markers created: 2"

# ─── N3=2: 上传全新 key ───
echo "=== N3: Uploading 2 brand-new keys ==="
echo "brand-new-content-extra1" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d1/extra_new_file1.txt"
echo "brand-new-content-extra2" | mc pipe "${MC_ALIAS}/${BUCKET}/${PREFIX}/d2/extra_new_file2.txt"
echo "New keys created: 2"

# ─── N4=3: 永久删除旧版本（指定 version_id 删除）───
echo "=== N4: Permanently deleting old versions (3 keys) ==="

# 获取 d1/d1_1/file1.txt 的旧版本（v1，非 latest）
V1_D1_1=$(mc ls --versions "${MC_ALIAS}/${BUCKET}/${PREFIX}/d1/d1_1/file1.txt" 2>/dev/null \
  | sort | head -1 | awk '{print $NF}' | grep -oP '(?<=version=)[^ ]+' || true)

# 获取 d1/d1_2/file1.txt 的旧版本
V1_D1_2=$(mc ls --versions "${MC_ALIAS}/${BUCKET}/${PREFIX}/d1/d1_2/file1.txt" 2>/dev/null \
  | sort | head -1 | awk '{print $NF}' | grep -oP '(?<=version=)[^ ]+' || true)

# 获取 d2/d2_1/file1.txt 的旧版本
V1_D2_1=$(mc ls --versions "${MC_ALIAS}/${BUCKET}/${PREFIX}/d2/d2_1/file1.txt" 2>/dev/null \
  | sort | head -1 | awk '{print $NF}' | grep -oP '(?<=version=)[^ ]+' || true)

OLD_DEL_COUNT=0

# 用 mc ls --versions 获取旧版本 version_id 的另一种方式：JSON 输出
delete_old_version() {
  local key="$1"
  # 获取该 key 所有版本，取最早的（非 latest）版本 ID
  local version_id
  version_id=$(mc ls --versions --json "${MC_ALIAS}/${BUCKET}/${key}" 2>/dev/null \
    | python3 -c "
import sys, json
versions = [json.loads(l) for l in sys.stdin if l.strip()]
# 排序按 lastModified，取最旧的版本
versions.sort(key=lambda x: x.get('lastModified',''))
for v in versions:
    if not v.get('isLatest', False):
        print(v.get('versionId',''))
        break
" 2>/dev/null || true)

  if [ -n "$version_id" ]; then
    mc rm --version-id "$version_id" "${MC_ALIAS}/${BUCKET}/${key}" 2>/dev/null && \
      echo "  Permanently deleted: $key (version: ${version_id:0:8}...)" && \
      OLD_DEL_COUNT=$((OLD_DEL_COUNT+1))
  else
    echo "  WARN: No old version found for $key, skipping"
  fi
}

delete_old_version "${PREFIX}/d1/d1_1/file1.txt"
delete_old_version "${PREFIX}/d1/d1_2/file1.txt"
delete_old_version "${PREFIX}/d2/d2_1/file1.txt"

echo "Old versions permanently deleted: $OLD_DEL_COUNT"

echo ""
echo "=== Summary ==="
echo "New versions uploaded: 3"
echo "Delete markers created: 2"
echo "New keys created: 2"
echo "Old versions permanently deleted: $OLD_DEL_COUNT"
echo "OK: Versioned mutations applied"
