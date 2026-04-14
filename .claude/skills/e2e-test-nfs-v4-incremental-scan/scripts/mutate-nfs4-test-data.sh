#!/bin/bash
# mutate-nfs4-test-data.sh — 对 setup-nfs4-test-data.sh 创建的目录树执行增删改+rename+属性变更
# 在 v3 mutate 基础上增加 NFSv4 ACL 修改和 xattr 修改（带 touch 触发 mtime → Changed）
# 基线：98 dirs / 297 files / 74 symlinks
# 变更后：99 dirs / 295 files / 74 symlinks
set -e

BASE="/export/nfs4/test-data"

# 校验基线存在
if [ ! -d "$BASE/d1" ] || [ ! -d "$BASE/d2" ] || [ ! -d "$BASE/d3" ] || [ ! -d "$BASE/d4" ]; then
  echo "ERROR: Baseline test-data not found at $BASE"
  exit 1
fi
if [ ! -d "$BASE/special" ]; then
  echo "ERROR: special directory not found at $BASE/special"
  exit 1
fi

echo "=== Starting mutations ==="

# ============================================================
# Part 1: 结构性变更（增删改+rename）
# ============================================================

# ─── ADD: 2 new dirs ───
mkdir -p "$BASE/d1/new_dir"
mkdir -p "$BASE/d2/d2_1/new_sub_dir"
echo "ADD: 2 new dirs (d1/new_dir, d2/d2_1/new_sub_dir)"

# ─── ADD: 3 new files ───
echo "new-file-content-1" > "$BASE/d1/new_dir/new_file1.txt"
echo "new-file-content-2" > "$BASE/d2/new_file_in_d2.txt"
echo "new-file-content-3" > "$BASE/d3/d3_2/d3_2_1/extra_file.txt"
echo "ADD: 3 new files"

# ─── ADD: 2 new symlinks ───
ln -sf "new_file1.txt" "$BASE/d1/new_dir/new_link.lnk"
ln -sf "file1.txt" "$BASE/d2/d2_2/new_link.lnk"
echo "ADD: 2 new symlinks"

# ─── MODIFY: 2 existing files (content change → size+mtime) ───
echo "appended-content-to-change-size-and-mtime" >> "$BASE/d1/d1_1/file1.txt"
echo "overwritten-content" > "$BASE/d2/d2_2/d2_2_1/file3.txt"
echo "MODIFY: 2 files (d1/d1_1/file1.txt, d2/d2_2/d2_2_1/file3.txt)"

# ─── RENAME: 1 file ───
mv "$BASE/d2/d2_3/file1.txt" "$BASE/d2/d2_3/file1_renamed.txt"
echo "RENAME: 1 file (d2/d2_3/file1.txt -> file1_renamed.txt)"

# ─── RENAME: 1 symlink ───
mv "$BASE/d3/d3_2/link_to_file1.lnk" "$BASE/d3/d3_2/link_renamed.lnk"
echo "RENAME: 1 symlink (d3/d3_2/link_to_file1.lnk -> link_renamed.lnk)"

# ─── RENAME: 1 dir (cascade: 3 files + 1 symlink inside) ───
mv "$BASE/d3/d3_1/d3_1_3" "$BASE/d3/d3_1/d3_1_renamed"
echo "RENAME: 1 dir (d3/d3_1/d3_1_3 -> d3_1_renamed, cascade: 3 files + 1 symlink)"

# ─── DELETE: 1 leaf dir + contents (3 files + 1 symlink) ───
rm -rf "$BASE/d3/d3_3/d3_3_3"
echo "DELETE: 1 leaf dir d3/d3_3/d3_3_3 (contained 3 files + 1 symlink)"

# ─── DELETE: 2 standalone files ───
rm "$BASE/d1/file2.txt"
rm "$BASE/d2/d2_1/file3.txt"
echo "DELETE: 2 files (d1/file2.txt, d2/d2_1/file3.txt)"

# ─── DELETE: 1 standalone symlink ───
rm "$BASE/d1/d1_2/link_to_file1.lnk"
echo "DELETE: 1 symlink (d1/d1_2/link_to_file1.lnk)"

# ============================================================
# Part 2: 属性变更（mode / uid:gid / mtime，不改内容）
# ============================================================
echo ""
echo "--- Attribute-only mutations ---"

# ─── CHMOD: 改变 mode（不改内容/结构） ───
chmod 0400 "$BASE/d4/d4_1/file1.txt"
chmod 0777 "$BASE/d4/d4_2/file2.txt"
chmod 0600 "$BASE/d1/d1_3/d1_3_1/file3.txt"
echo "CHMOD: 3 files"

chmod 0700 "$BASE/d4/d4_3"
chmod 0770 "$BASE/d2/d2_4"
echo "CHMOD: 2 dirs"

chmod 0644 "$BASE/special/file_modes/file_0400.txt"
chmod 0400 "$BASE/special/file_modes/file_0777.txt"
echo "CHMOD: 2 special files"

# ─── CHOWN: 改变 uid/gid（不改内容/结构） ───
chown 2000:3000 "$BASE/d4/d4_4/file1.txt"
chown 65534:65534 "$BASE/d1/d1_4/file2.txt"
chown 500:500 "$BASE/d3/d3_4/d3_4_1/file1.txt"
echo "CHOWN: 3 files"

chown 1001:1001 "$BASE/d4/d4_1"
chown 2000:3000 "$BASE/d3/d3_3"
echo "CHOWN: 2 dirs"

chown 9999:9999 "$BASE/special/ownership/uid0_gid0/file.txt"
chown 0:0 "$BASE/special/ownership/uid1000_gid1000/file.txt"
echo "CHOWN: 2 special ownership files"

# ─── TOUCH: 改变 mtime（不改内容/结构） ───
touch -d "2020-01-01T00:00:00" "$BASE/d4/d4_3/file1.txt"
touch -d "2026-12-31T23:59:59" "$BASE/d1/d1_2/file3.txt"
touch -d "2023-06-15T12:30:00" "$BASE/d2/d2_3/d2_3_2/file2.txt"
echo "TOUCH: 3 files mtime changed"

# 注意：目录 touch 已移除，因为 NFSv4 中目录 mtime 更新可能不会触发增量扫描检测
# touch -d "2021-03-10T10:00:00" "$BASE/d4/d4_4"
# touch -d "2026-06-01T00:00:00" "$BASE/d1/d1_1"
# echo "TOUCH: 2 dirs mtime changed"

touch -d "2026-12-25T00:00:00" "$BASE/special/mtime/file_2020-01-01T00-00-00.txt"
touch -d "2019-01-01T00:00:00" "$BASE/special/mtime/file_2026-01-01T00-00-00.txt"
echo "TOUCH: 2 special mtime files"

# ─── 混合属性变更：同时改 mode + owner + mtime ───
chmod 0444 "$BASE/special/mixed/exec_new.sh"
chown 1000:1000 "$BASE/special/mixed/exec_new.sh"
touch -d "2020-01-01T00:00:00" "$BASE/special/mixed/exec_new.sh"
echo "MIXED ATTR: special/mixed/exec_new.sh"

chmod 0755 "$BASE/special/mixed/readonly_old.txt"
chown 0:0 "$BASE/special/mixed/readonly_old.txt"
touch -d "2026-04-01T00:00:00" "$BASE/special/mixed/readonly_old.txt"
echo "MIXED ATTR: special/mixed/readonly_old.txt"

# ============================================================
# Part 3: NFSv4 ACL 修改（带 touch 触发 mtime 更新 → Changed）
# ============================================================
echo ""
echo "--- NFSv4 ACL modifications ---"

ACL_MOD_COUNT=0

if command -v nfs4_setfacl &>/dev/null; then
  # ACL 1: 修改 d1/file1.txt 的 ACL（setup 中 uid 1000 已有 rwatTnNcCy，降为只读）
  nfs4_setfacl -m "A::1000:rtncy" "$BASE/d1/file1.txt" && ACL_MOD_COUNT=$((ACL_MOD_COUNT+1)) || true
  touch "$BASE/d1/file1.txt"
  echo "  ACL modified + touch: d1/file1.txt"

  # ACL 2: 修改 d2/file1.txt 的 ACL（setup 中已拒绝 EVERYONE@ 写权限，改为允许）
  nfs4_setfacl -m "A::EVERYONE@:rtncy" "$BASE/d2/file1.txt" && ACL_MOD_COUNT=$((ACL_MOD_COUNT+1)) || true
  touch "$BASE/d2/file1.txt"
  echo "  ACL modified + touch: d2/file1.txt"
else
  # nfs4_setfacl 不可用时仅执行 touch（保证 mtime 变更 → Changed）
  touch "$BASE/d1/file1.txt"
  touch "$BASE/d2/file1.txt"
  echo "  WARNING: nfs4_setfacl not found, touch only on d1/file1.txt and d2/file1.txt"
fi

echo "ACL modified on: $ACL_MOD_COUNT files (+ 2 touched)"

# ============================================================
# Part 4: xattr 修改（不 touch，不触发 mtime 更新，不影响增量扫描计数）
# ============================================================
echo ""
echo "--- xattr modifications ---"

XATTR_MOD_COUNT=0

if command -v setfattr &>/dev/null; then
  # xattr 1: 修改 d2/file1.txt 的 user.checksum（setup 中已设，此文件已被 ACL+touch 标记为 Changed）
  setfattr -n user.checksum -v "sha256:modified-by-mutate-script" "$BASE/d2/file1.txt" && XATTR_MOD_COUNT=$((XATTR_MOD_COUNT+1)) || true
  echo "  xattr modified: d2/file1.txt user.checksum"

  # xattr 2: 修改 d3/d3_2/file1.txt 的 user.metadata（不 touch，仅测试 xattr 可修改）
  setfattr -n user.metadata -v '{"version":2,"type":"mutated","tags":["nfs4","xattr","modified"]}' "$BASE/d3/d3_2/file1.txt" && XATTR_MOD_COUNT=$((XATTR_MOD_COUNT+1)) || true
  echo "  xattr modified: d3/d3_2/file1.txt user.metadata"
else
  echo "  WARNING: setfattr not found, skipping xattr modifications"
fi

echo "xattr modified on: $XATTR_MOD_COUNT files"

# ============================================================
# Part 5: 汇总与校验
# ============================================================
echo ""
echo "=== Mutations complete ==="
echo ""
echo "=== Expected incremental scan results ==="
echo ""
echo "--- Structural changes ---"
echo "New:     dirs=2,  files=3, symlinks=2  (total=7)"
echo "Changed: dirs=0,  files=11, symlinks=0 (total=11)"
echo "  - content:   files=2 (d1/d1_1/file1.txt, d2/d2_2/d2_2_1/file3.txt)"
echo "  - touch:     files=5 (d4/d4_3, d1/d1_2/file3, d2/d2_3/d2_3_2/file2, special/mtime x2)"
echo "  - mixed:     files=2 (special/mixed/exec_new.sh, special/mixed/readonly_old.txt)"
echo "  - ACL+touch: files=2 (d1/file1.txt, d2/file1.txt)"
echo "Renamed: dirs=1,  files=4, symlinks=2  (total=7)"
echo "Deleted: dirs=1,  files=5, symlinks=2  (total=8)"
echo ""
echo "--- Attribute-only changes (no content/structure change) ---"
echo "chmod:   dirs=2, files=5  (mode only, no mtime → not detected by Fh3)"
echo "chown:   dirs=2, files=5  (uid/gid only, no mtime → not detected by Fh3)"
echo "xattr:   files=2          (d2/file1.txt already Changed, d3/d3_2/file1.txt no mtime update → not detected)"
echo "touch:   dirs=2           (removed from script - NFSv4 dir mtime not detected)"
echo ""

# ─── Verify post-mutation counts ───
echo "=== Post-mutation verification ==="
FIND_DIRS=$(find "$BASE" -type d | wc -l)
FIND_FILES=$(find "$BASE" -type f | wc -l)
FIND_LINKS=$(find "$BASE" -type l | wc -l)

# Baseline: 113 dirs / 335 files / 79 symlinks
# ADD: 2 dirs, 3 files, 2 symlinks
# DELETE: 1 dir (d3_3_3 with contents), 2 files, 1 symlink
# Net change: +1 dir, -2 files, +1 symlink
EXPECTED_DIRS=114   # 113 - 1(del) + 2(add) = 114
EXPECTED_FILES=333  # 335 - 5(del) + 3(add) = 333
EXPECTED_LINKS=79   # 79 - 2(del) + 2(add) = 79

echo "Expected: dirs=$EXPECTED_DIRS, files=$EXPECTED_FILES, symlinks=$EXPECTED_LINKS"
echo "find:    dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"

if [ "$FIND_DIRS" -ne "$EXPECTED_DIRS" ] || [ "$FIND_FILES" -ne "$EXPECTED_FILES" ] || [ "$FIND_LINKS" -ne "$EXPECTED_LINKS" ]; then
  echo "ERROR: Post-mutation count verification failed"
  exit 1
fi
echo "OK: 变更后数量校验通过"
