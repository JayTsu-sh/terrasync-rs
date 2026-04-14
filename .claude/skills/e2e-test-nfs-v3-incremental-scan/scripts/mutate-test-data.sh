#!/bin/bash
# mutate-test-data.sh — 对 setup-test-data.sh 创建的目录树执行增删改+rename+属性变更
# 基线：113 dirs / 335 files / 79 symlinks
# 变更后：114 dirs / 333 files / 79 symlinks
set -e

BASE="/export/nfs/test-data"

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
# 主目录树中的文件
chmod 0400 "$BASE/d4/d4_1/file1.txt"           # 644 → 400（只读）
chmod 0777 "$BASE/d4/d4_2/file2.txt"           # 644 → 777（全权限）
chmod 0600 "$BASE/d1/d1_3/d1_3_1/file3.txt"    # 644 → 600
echo "CHMOD: 3 files (d4/d4_1/file1.txt→0400, d4/d4_2/file2.txt→0777, d1/d1_3/d1_3_1/file3.txt→0600)"

# 主目录树中的目录
chmod 0700 "$BASE/d4/d4_3"                     # 755 → 700
chmod 0770 "$BASE/d2/d2_4"                     # 755 → 770
echo "CHMOD: 2 dirs (d4/d4_3→0700, d2/d2_4→0770)"

# special 区的文件：从一种 mode 改成另一种
chmod 0644 "$BASE/special/file_modes/file_0400.txt"   # 400 → 644
chmod 0400 "$BASE/special/file_modes/file_0777.txt"   # 777 → 400
echo "CHMOD: 2 special files (file_0400→0644, file_0777→0400)"

# ─── CHOWN: 改变 uid/gid（不改内容/结构） ───
# 主目录树中的文件
chown 2000:3000 "$BASE/d4/d4_4/file1.txt"      # default → 2000:3000
chown 65534:65534 "$BASE/d1/d1_4/file2.txt"    # default → nobody:nogroup
chown 500:500 "$BASE/d3/d3_4/d3_4_1/file1.txt" # default → 500:500
echo "CHOWN: 3 files"

# 主目录树中的目录
chown 1001:1001 "$BASE/d4/d4_1"                # default → 1001:1001
chown 2000:3000 "$BASE/d3/d3_3"                # default → 2000:3000
echo "CHOWN: 2 dirs"

# special 区的文件：从一种 owner 改成另一种
chown 9999:9999 "$BASE/special/ownership/uid0_gid0/file.txt"         # root → 9999
chown 0:0 "$BASE/special/ownership/uid1000_gid1000/file.txt"         # 1000 → root
echo "CHOWN: 2 special ownership files"

# ─── TOUCH: 改变 mtime（不改内容/结构） ───
# 主目录树中的文件
touch -d "2020-01-01T00:00:00" "$BASE/d4/d4_3/file1.txt"           # 改为旧时间
touch -d "2026-12-31T23:59:59" "$BASE/d1/d1_2/file3.txt"           # 改为未来时间
touch -d "2023-06-15T12:30:00" "$BASE/d2/d2_3/d2_3_2/file2.txt"    # 改为中间时间
echo "TOUCH: 3 files mtime changed"

# special 区的 mtime 文件：改成不同的时间
touch -d "2026-12-25T00:00:00" "$BASE/special/mtime/file_2020-01-01T00-00-00.txt"   # 旧→新
touch -d "2019-01-01T00:00:00" "$BASE/special/mtime/file_2026-01-01T00-00-00.txt"   # 新→旧
echo "TOUCH: 2 special mtime files"

# ─── 混合属性变更：同时改 mode + owner + mtime ───
chmod 0444 "$BASE/special/mixed/exec_new.sh"
chown 1000:1000 "$BASE/special/mixed/exec_new.sh"
touch -d "2020-01-01T00:00:00" "$BASE/special/mixed/exec_new.sh"
echo "MIXED ATTR: special/mixed/exec_new.sh (mode 0755→0444, owner root→1000, mtime→2020)"

chmod 0755 "$BASE/special/mixed/readonly_old.txt"
chown 0:0 "$BASE/special/mixed/readonly_old.txt"
touch -d "2026-04-01T00:00:00" "$BASE/special/mixed/readonly_old.txt"
echo "MIXED ATTR: special/mixed/readonly_old.txt (mode 0444→0755, owner 1000→root, mtime→2026)"

# ============================================================
# Part 3: 汇总与校验
# ============================================================
echo ""
echo "=== Mutations complete ==="
echo ""
echo "=== Expected incremental scan results ==="
echo ""
echo "--- Structural changes ---"
echo "New:     dirs=2,  files=3, symlinks=2  (total=7)"
echo "Changed: dirs=0,  files=2, symlinks=0  (total=2, content modified)"
echo "Renamed: dirs=1,  files=4, symlinks=2  (total=7)"
echo "Deleted: dirs=1,  files=5, symlinks=2  (total=8)"
echo ""
echo "--- Attribute-only changes (no content/structure change) ---"
echo "chmod:   dirs=2, files=5  (total=7, mode only)"
echo "chown:   dirs=2, files=5  (total=7, uid/gid only)"
echo "touch:   files=5  (total=5, mtime only)"
echo "mixed:   files=2          (total=2, mode+owner+mtime)"
echo ""
echo "Note: attribute changes overlap — some entries have multiple attr changes."
echo "Unique entries with attr-only changes: ~17 (some counted in multiple categories)"
echo ""

# ─── Verify post-mutation counts ───
echo "=== Post-mutation verification ==="
FIND_DIRS=$(find "$BASE" -type d | wc -l)
FIND_FILES=$(find "$BASE" -type f | wc -l)
FIND_LINKS=$(find "$BASE" -type l | wc -l)

EXPECTED_DIRS=114   # 113 - 1(del) + 2(add), rename keeps count
EXPECTED_FILES=333  # 335 - 5(del) + 3(add), rename/modify/attr keep count
EXPECTED_LINKS=79   # 79 - 2(del) + 2(add), rename keeps count

echo "Expected: dirs=$EXPECTED_DIRS, files=$EXPECTED_FILES, symlinks=$EXPECTED_LINKS"
echo "find:    dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"

if [ "$FIND_DIRS" -ne "$EXPECTED_DIRS" ] || [ "$FIND_FILES" -ne "$EXPECTED_FILES" ] || [ "$FIND_LINKS" -ne "$EXPECTED_LINKS" ]; then
  echo "ERROR: Post-mutation count verification failed"
  exit 1
fi
echo "OK: Post-mutation counts verified"
