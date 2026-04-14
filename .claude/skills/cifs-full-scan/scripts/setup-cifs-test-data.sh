#!/bin/bash
# setup-cifs-test-data.sh — 通过 smbclient 在 CIFS 共享上创建 3x3x3 目录树
# 结果：40 目录（含 test-data 根）/ 117 文件 / 0 软链接（CIFS 不支持）
#
# 依赖环境变量（必须提前 export）：
#   CIFS_HOST  — CIFS 服务器 IP 或域名
#   CIFS_SHARE — 共享名称（不含路径前缀）
#   CIFS_USER  — 用户名（域用户格式：DOMAIN\\user）
#   CIFS_PASS  — 密码
#   CIFS_PORT  — 可选，默认 445
set -e

: "${CIFS_HOST:?需要设置 CIFS_HOST 环境变量}"
: "${CIFS_SHARE:?需要设置 CIFS_SHARE 环境变量}"
: "${CIFS_USER:?需要设置 CIFS_USER 环境变量}"
: "${CIFS_PASS:?需要设置 CIFS_PASS 环境变量}"
CIFS_PORT="${CIFS_PORT:-445}"

SMB_CMD="smbclient //${CIFS_HOST}/${CIFS_SHARE} -U ${CIFS_USER}%${CIFS_PASS} -p ${CIFS_PORT}"

echo "=== Creating CIFS test data ==="
echo "Target: //${CIFS_HOST}/${CIFS_SHARE}/test-data"

# ─── 在本地 tmpdir 创建完整目录树 ───
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

BASE="$TMPDIR/test-data"
mkdir -p "$BASE"

FILES=0

for i in 1 2 3; do
  mkdir -p "$BASE/d$i"
  for f in 1 2 3; do
    echo "level1-content-$i-$f" > "$BASE/d$i/file$f.txt"
    FILES=$((FILES+1))
  done

  for j in 1 2 3; do
    mkdir -p "$BASE/d$i/d${i}_$j"
    for f in 1 2 3; do
      echo "level2-content-${i}_${j}-$f" > "$BASE/d$i/d${i}_$j/file$f.txt"
      FILES=$((FILES+1))
    done

    for k in 1 2 3; do
      mkdir -p "$BASE/d$i/d${i}_$j/d${i}_${j}_$k"
      for f in 1 2 3; do
        echo "level3-content-${i}_${j}_${k}-$f" > "$BASE/d$i/d${i}_$j/d${i}_${j}_$k/file$f.txt"
        FILES=$((FILES+1))
      done
    done
  done
done

echo "Local tree created: files=$FILES"

# ─── 清理 CIFS 上已有数据 ───
echo "=== Cleaning existing data on CIFS ==="
$SMB_CMD -c "deltree test-data" 2>/dev/null || true

# ─── 将本地目录树以 tar 流上传到 CIFS ───
echo "=== Uploading to CIFS via tar stream ==="
cd "$TMPDIR"
tar cf - test-data | $SMB_CMD -Tx -

# ─── 验证：通过 smbclient 列出文件数 ───
echo "=== Verifying upload ==="
FILE_COUNT=$($SMB_CMD -c "recurse ON; ls test-data\\" 2>/dev/null | grep -c '\.txt$' || true)
echo "CIFS files: $FILE_COUNT"

if [ "$FILE_COUNT" -ne 117 ]; then
  echo "ERROR: Expected 117 files, got $FILE_COUNT"
  echo "Note: 40 dirs / 117 files / 0 symlinks (CIFS 不支持软链接)"
  exit 1
fi

echo "Expected: dirs=40, files=117, symlinks=0"
echo "CIFS files: $FILE_COUNT"
echo "OK: CIFS file count verified (dirs will be verified by scan)"
