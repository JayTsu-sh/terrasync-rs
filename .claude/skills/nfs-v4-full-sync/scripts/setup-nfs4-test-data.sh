#!/bin/bash
# setup-nfs4-test-data.sh — 在 /export/nfsv4/test-data 创建丰富的测试目录树
# 包含：多层目录、文件、软链接，以及不同的 mode/uid/gid/mtime 组合
set -e

BASE="/export/nfsv4/test-data"

# 清理已有数据（全量重建）
rm -rf "$BASE"
mkdir -p "$BASE"

DIRS=1  # 算上 $BASE (test-data) 自身
FILES=0
SYMLINKS=0

# ============================================================
# Part 1: 4x4x3 主目录树
# ============================================================
for i in 1 2 3 4; do
  mkdir -p "$BASE/d$i"
  DIRS=$((DIRS+1))
  for f in 1 2 3 4; do
    echo "level1-content-$i-$f" > "$BASE/d$i/file$f.txt"
    FILES=$((FILES+1))
  done
  # level-1 软链接
  ln -sf "file1.txt" "$BASE/d$i/link_to_file1.lnk"
  SYMLINKS=$((SYMLINKS+1))

  for j in 1 2 3 4; do
    mkdir -p "$BASE/d$i/d${i}_$j"
    DIRS=$((DIRS+1))
    for f in 1 2 3; do
      echo "level2-content-${i}_${j}-$f" > "$BASE/d$i/d${i}_$j/file$f.txt"
      FILES=$((FILES+1))
    done
    ln -sf "file1.txt" "$BASE/d$i/d${i}_$j/link_to_file1.lnk"
    SYMLINKS=$((SYMLINKS+1))

    for k in 1 2 3; do
      mkdir -p "$BASE/d$i/d${i}_$j/d${i}_${j}_$k"
      DIRS=$((DIRS+1))
      for f in 1 2 3; do
        echo "level3-content-${i}_${j}_${k}-$f" > "$BASE/d$i/d${i}_$j/d${i}_${j}_$k/file$f.txt"
        FILES=$((FILES+1))
      done
      ln -sf "file2.txt" "$BASE/d$i/d${i}_$j/d${i}_${j}_$k/link_to_file2.lnk"
      SYMLINKS=$((SYMLINKS+1))
    done
  done
done

# ============================================================
# Part 2: 特殊目录 — 不同 mode/uid/gid/mtime 的文件和目录
# ============================================================
SPECIAL="$BASE/special"
mkdir -p "$SPECIAL"
DIRS=$((DIRS+1))

# --- 2a: 不同权限的目录 ---
for mode in 0700 0750 0755 0770 0777 0500; do
  dname="$SPECIAL/dir_mode_${mode}"
  mkdir -p "$dname"
  chmod "$mode" "$dname"
  echo "mode=$mode" > "$dname/info.txt"
  chmod "$mode" "$dname/info.txt"
  DIRS=$((DIRS+1))
  FILES=$((FILES+1))
done

# --- 2b: 不同权限的文件 ---
mkdir -p "$SPECIAL/file_modes"
DIRS=$((DIRS+1))
for mode in 0400 0444 0600 0644 0660 0664 0755 0777; do
  fname="$SPECIAL/file_modes/file_${mode}.txt"
  echo "file with mode=$mode" > "$fname"
  chmod "$mode" "$fname"
  FILES=$((FILES+1))
done

# --- 2c: 不同 uid/gid 的文件和目录 ---
mkdir -p "$SPECIAL/ownership"
DIRS=$((DIRS+1))

# uid/gid 组合：(uid, gid)
OWNERS="0:0 1000:1000 1001:1001 65534:65534 0:1000 1000:0 2000:3000 500:500"
for pair in $OWNERS; do
  uid="${pair%%:*}"
  gid="${pair##*:}"
  dname="$SPECIAL/ownership/uid${uid}_gid${gid}"
  mkdir -p "$dname"
  echo "owned by uid=$uid gid=$gid" > "$dname/file.txt"
  chown "$uid:$gid" "$dname"
  chown "$uid:$gid" "$dname/file.txt"
  DIRS=$((DIRS+1))
  FILES=$((FILES+1))
done

# --- 2d: 不同 mtime 的文件和目录 ---
mkdir -p "$SPECIAL/mtime"
DIRS=$((DIRS+1))

# 不同时间戳（过去和未来）
TIMESTAMPS=(
  "2020-01-01T00:00:00"
  "2022-06-15T12:30:00"
  "2023-12-31T23:59:59"
  "2024-03-15T08:00:00"
  "2025-01-01T00:00:00"
  "2025-06-30T18:45:00"
  "2026-01-01T00:00:00"
)
for ts in "${TIMESTAMPS[@]}"; do
  safe_ts="${ts//:/-}"
  fname="$SPECIAL/mtime/file_${safe_ts}.txt"
  echo "mtime=$ts" > "$fname"
  touch -d "$ts" "$fname"
  FILES=$((FILES+1))
done

# mtime 不同的目录
for ts in "2020-01-01T00:00:00" "2023-06-15T12:00:00" "2025-12-01T00:00:00"; do
  safe_ts="${ts//:/-}"
  dname="$SPECIAL/mtime/dir_${safe_ts}"
  mkdir -p "$dname"
  echo "dir mtime=$ts" > "$dname/marker.txt"
  touch -d "$ts" "$dname"
  touch -d "$ts" "$dname/marker.txt"
  DIRS=$((DIRS+1))
  FILES=$((FILES+1))
done

# --- 2e: 不同类型的软链接 ---
mkdir -p "$SPECIAL/symlinks"
DIRS=$((DIRS+1))

# 指向文件的软链接
echo "symlink target content" > "$SPECIAL/symlinks/target.txt"
FILES=$((FILES+1))
ln -sf "target.txt" "$SPECIAL/symlinks/relative_link.lnk"
SYMLINKS=$((SYMLINKS+1))
ln -sf "$SPECIAL/symlinks/target.txt" "$SPECIAL/symlinks/absolute_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# 指向目录的软链接
mkdir -p "$SPECIAL/symlinks/target_dir"
DIRS=$((DIRS+1))
echo "in target dir" > "$SPECIAL/symlinks/target_dir/inner.txt"
FILES=$((FILES+1))
ln -sf "target_dir" "$SPECIAL/symlinks/dir_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# 悬空软链接（指向不存在的目标）
ln -sf "nonexistent_file.txt" "$SPECIAL/symlinks/dangling_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# 链条软链接（链接指向链接）
ln -sf "relative_link.lnk" "$SPECIAL/symlinks/chain_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# --- 2f: 混合属性 — mode + uid/gid + mtime 组合 ---
mkdir -p "$SPECIAL/mixed"
DIRS=$((DIRS+1))

# 组合1: 只读文件，非root用户，旧 mtime
echo "readonly old file" > "$SPECIAL/mixed/readonly_old.txt"
chmod 0444 "$SPECIAL/mixed/readonly_old.txt"
chown 1000:1000 "$SPECIAL/mixed/readonly_old.txt"
touch -d "2021-03-10T10:00:00" "$SPECIAL/mixed/readonly_old.txt"
FILES=$((FILES+1))

# 组合2: 可执行文件，root用户，新 mtime
echo "#!/bin/bash" > "$SPECIAL/mixed/exec_new.sh"
chmod 0755 "$SPECIAL/mixed/exec_new.sh"
chown 0:0 "$SPECIAL/mixed/exec_new.sh"
touch -d "2026-02-01T00:00:00" "$SPECIAL/mixed/exec_new.sh"
FILES=$((FILES+1))

# 组合3: 组可写目录，特定用户
dname="$SPECIAL/mixed/group_writable"
mkdir -p "$dname"
chmod 0775 "$dname"
chown 2000:3000 "$dname"
touch -d "2024-07-04T12:00:00" "$dname"
echo "group writable content" > "$dname/data.txt"
chmod 0664 "$dname/data.txt"
chown 2000:3000 "$dname/data.txt"
touch -d "2024-07-04T12:00:00" "$dname/data.txt"
DIRS=$((DIRS+1))
FILES=$((FILES+1))

# 组合4: 权限受限目录，root拥有
dname="$SPECIAL/mixed/restricted"
mkdir -p "$dname"
chmod 0700 "$dname"
chown 0:0 "$dname"
touch -d "2023-01-01T00:00:00" "$dname"
echo "restricted content" > "$dname/secret.txt"
chmod 0600 "$dname/secret.txt"
chown 0:0 "$dname/secret.txt"
touch -d "2023-01-01T00:00:00" "$dname/secret.txt"
DIRS=$((DIRS+1))
FILES=$((FILES+1))

# 组合5: 带软链接的混合属性目录
dname="$SPECIAL/mixed/with_links"
mkdir -p "$dname"
chmod 0755 "$dname"
chown 1001:1001 "$dname"
DIRS=$((DIRS+1))
echo "link target" > "$dname/target.txt"
chmod 0644 "$dname/target.txt"
chown 1001:1001 "$dname/target.txt"
touch -d "2025-03-15T09:30:00" "$dname/target.txt"
FILES=$((FILES+1))
ln -sf "target.txt" "$dname/link.lnk"
SYMLINKS=$((SYMLINKS+1))

# --- 2g: 空目录 ---
mkdir -p "$SPECIAL/empty_dir"
DIRS=$((DIRS+1))
chmod 0755 "$SPECIAL/empty_dir"
chown 1000:1000 "$SPECIAL/empty_dir"

# --- 2h: 大量文件的扁平目录（测试批量扫描） ---
mkdir -p "$SPECIAL/many_files"
DIRS=$((DIRS+1))
for i in $(seq 1 50); do
  echo "bulk file content $i" > "$SPECIAL/many_files/bulk_${i}.txt"
  FILES=$((FILES+1))
done
# 其中部分设置不同属性
chown 1000:1000 "$SPECIAL/many_files/bulk_1.txt"
chown 2000:2000 "$SPECIAL/many_files/bulk_25.txt"
chown 65534:65534 "$SPECIAL/many_files/bulk_50.txt"
chmod 0400 "$SPECIAL/many_files/bulk_10.txt"
chmod 0777 "$SPECIAL/many_files/bulk_20.txt"
touch -d "2020-06-01T00:00:00" "$SPECIAL/many_files/bulk_5.txt"
touch -d "2026-01-01T00:00:00" "$SPECIAL/many_files/bulk_45.txt"

# ============================================================
# Part 3: 特殊字符与中文命名
# ============================================================
EXOTIC="$BASE/exotic_names"
mkdir -p "$EXOTIC"
DIRS=$((DIRS+1))

# --- 3a: 空格与常见标点 ---
mkdir -p "$EXOTIC/spaces and tabs"
DIRS=$((DIRS+1))
echo "space in dir" > "$EXOTIC/spaces and tabs/file with spaces.txt"
FILES=$((FILES+1))
echo "tab in name" > "$EXOTIC/spaces and tabs/file	with	tabs.txt"
FILES=$((FILES+1))
ln -sf "file with spaces.txt" "$EXOTIC/spaces and tabs/link to spaces.lnk"
SYMLINKS=$((SYMLINKS+1))

# --- 3b: 括号、方括号、花括号 ---
mkdir -p "$EXOTIC/(parentheses dir)"
DIRS=$((DIRS+1))
echo "paren content" > "$EXOTIC/(parentheses dir)/file(v1).txt"
FILES=$((FILES+1))
mkdir -p "$EXOTIC/[brackets dir]"
DIRS=$((DIRS+1))
echo "bracket content" > "$EXOTIC/[brackets dir]/config[dev].txt"
FILES=$((FILES+1))
mkdir -p "$EXOTIC/{braces dir}"
DIRS=$((DIRS+1))
echo "brace content" > "$EXOTIC/{braces dir}/template{main}.txt"
FILES=$((FILES+1))

# --- 3c: 感叹号、@ # $ % ^ & ---
mkdir -p "$EXOTIC/special!@#"
DIRS=$((DIRS+1))
echo "exclaim" > "$EXOTIC/special!@#/urgent!.txt"
FILES=$((FILES+1))
echo "at sign"  > "$EXOTIC/special!@#/user@host.conf"
FILES=$((FILES+1))
echo "hash"     > "$EXOTIC/special!@#/#comment.txt"
FILES=$((FILES+1))
echo "dollar"   > "$EXOTIC/special!@#/price\$100.txt"
FILES=$((FILES+1))
echo "percent"  > "$EXOTIC/special!@#/ratio50%.txt"
FILES=$((FILES+1))
echo "caret"    > "$EXOTIC/special!@#/pattern^v2.txt"
FILES=$((FILES+1))
echo "ampersand" > "$EXOTIC/special!@#/Tom&Jerry.txt"
FILES=$((FILES+1))

# --- 3d: 加减、逗号、分号、等号、波浪、反引号 ---
mkdir -p "$EXOTIC/math+ops"
DIRS=$((DIRS+1))
echo "plus"      > "$EXOTIC/math+ops/a+b.txt"
FILES=$((FILES+1))
echo "minus"     > "$EXOTIC/math+ops/a-b.txt"
FILES=$((FILES+1))
echo "comma"     > "$EXOTIC/math+ops/a,b.txt"
FILES=$((FILES+1))
echo "semicolon" > "$EXOTIC/math+ops/a;b.txt"
FILES=$((FILES+1))
echo "equals"    > "$EXOTIC/math+ops/key=value.txt"
FILES=$((FILES+1))
echo "tilde"     > "$EXOTIC/math+ops/~backup.txt"
FILES=$((FILES+1))
echo "backtick"  > "$EXOTIC/math+ops/\`code\`.txt"
FILES=$((FILES+1))

# --- 3e: 单引号、双引号、问号、星号（Linux 支持，部分客户端转义） ---
mkdir -p "$EXOTIC/quotes_wild"
DIRS=$((DIRS+1))
echo "single quote" > "$EXOTIC/quotes_wild/it's_a_file.txt"
FILES=$((FILES+1))
echo "double quote" > "$EXOTIC/quotes_wild/say\"hello\".txt"
FILES=$((FILES+1))
echo "question"     > "$EXOTIC/quotes_wild/what?.txt"
FILES=$((FILES+1))
echo "asterisk"     > "$EXOTIC/quotes_wild/glob*.txt"
FILES=$((FILES+1))

# --- 3f: 纯中文目录与文件 ---
mkdir -p "$EXOTIC/数据目录"
DIRS=$((DIRS+1))
echo "中文文件内容" > "$EXOTIC/数据目录/测试文件.txt"
FILES=$((FILES+1))
echo "配置内容" > "$EXOTIC/数据目录/配置文件.conf"
FILES=$((FILES+1))
ln -sf "测试文件.txt" "$EXOTIC/数据目录/测试链接.lnk"
SYMLINKS=$((SYMLINKS+1))

mkdir -p "$EXOTIC/归档/2025年报告"
DIRS=$((DIRS+2))
echo "年度报告" > "$EXOTIC/归档/2025年报告/年度总结.txt"
FILES=$((FILES+1))
echo "季度数据" > "$EXOTIC/归档/2025年报告/第四季度.csv"
FILES=$((FILES+1))

# --- 3g: 中文 + 特殊字符混合 ---
mkdir -p "$EXOTIC/项目(2025)"
DIRS=$((DIRS+1))
echo "混合名称" > "$EXOTIC/项目(2025)/版本v1.0&draft.txt"
FILES=$((FILES+1))
echo "带空格" > "$EXOTIC/项目(2025)/需求 说明书.md"
FILES=$((FILES+1))
ln -sf "需求 说明书.md" "$EXOTIC/项目(2025)/需求文档@latest.lnk"
SYMLINKS=$((SYMLINKS+1))

# --- 3h: 点开头（隐藏文件/目录） ---
mkdir -p "$EXOTIC/.hidden_dir"
DIRS=$((DIRS+1))
echo "hidden file" > "$EXOTIC/.hidden_dir/.hidden_file.txt"
FILES=$((FILES+1))
echo "hidden config" > "$EXOTIC/.hidden_dir/.config"
FILES=$((FILES+1))
echo "dot in middle" > "$EXOTIC/.hidden_dir/no..double.ext.txt"
FILES=$((FILES+1))
ln -sf ".hidden_file.txt" "$EXOTIC/.hidden_dir/.hidden_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# --- 3i: 超长文件名（接近 255 字节限制） ---
mkdir -p "$EXOTIC/long_names"
DIRS=$((DIRS+1))
# 生成 200 字符 ASCII 文件名
LONG_NAME="$(python3 -c "print('a' * 200 + '.txt')" 2>/dev/null || { printf '%0.s-a' {1..200} | head -c 200; echo '.txt'; })"
echo "long name content" > "$EXOTIC/long_names/${LONG_NAME}"
FILES=$((FILES+1))
# 中文长名（每个汉字 3 字节，约 60 个汉字 = 180 字节）
LONG_CN_NAME="$(python3 -c "print('测试文件名' * 12 + '.txt')" 2>/dev/null || echo '超长中文文件名称测试.txt')"
echo "long cn content" > "$EXOTIC/long_names/${LONG_CN_NAME}"
FILES=$((FILES+1))

# --- 3j: Unicode 特殊字符（emoji、全角、箭头等） ---
mkdir -p "$EXOTIC/unicode"
DIRS=$((DIRS+1))
echo "emoji dir content" > "$EXOTIC/unicode/file_🚀.txt"
FILES=$((FILES+1))
echo "star content"      > "$EXOTIC/unicode/★report★.txt"
FILES=$((FILES+1))
echo "arrow content"     > "$EXOTIC/unicode/→flow→.txt"
FILES=$((FILES+1))
echo "fullwidth"         > "$EXOTIC/unicode/ＡＢＣ全角.txt"
FILES=$((FILES+1))
ln -sf "file_🚀.txt" "$EXOTIC/unicode/rocket_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# ============================================================
# Part 4: 统计与校验
# ============================================================
echo ""
echo "=== Setup complete ==="
echo "Counter: dirs=$DIRS, files=$FILES, symlinks=$SYMLINKS"
echo ""
echo "=== find verification ==="
FIND_DIRS=$(find "$BASE" -type d | wc -l)
FIND_FILES=$(find "$BASE" -type f | wc -l)
FIND_LINKS=$(find "$BASE" -type l | wc -l)
echo "find:    dirs=$FIND_DIRS, files=$FIND_FILES, symlinks=$FIND_LINKS"

TOTAL_ENTRIES=$((FIND_DIRS + FIND_FILES + FIND_LINKS))
echo "total entries: $TOTAL_ENTRIES"

if [ "$FIND_DIRS" -ne "$DIRS" ]; then
  echo "WARNING: dir count mismatch: counter=$DIRS, find=$FIND_DIRS"
fi
if [ "$FIND_FILES" -ne "$FILES" ]; then
  echo "WARNING: file count mismatch: counter=$FILES, find=$FIND_FILES"
fi
if [ "$FIND_LINKS" -ne "$SYMLINKS" ]; then
  echo "WARNING: symlink count mismatch: counter=$SYMLINKS, find=$FIND_LINKS"
fi

if [ "$FIND_DIRS" -eq "$DIRS" ] && [ "$FIND_FILES" -eq "$FILES" ] && [ "$FIND_LINKS" -eq "$SYMLINKS" ]; then
  echo "OK: 数量校验通过"
else
  echo "ERROR: 数量校验失败，请检查脚本"
  exit 1
fi

# 显示属性样本
echo ""
echo "=== 属性样本 ==="
echo "--- 不同 mode ---"
ls -la "$SPECIAL/file_modes/" | head -5
echo "--- 不同 owner ---"
ls -lan "$SPECIAL/ownership/" | head -5
echo "--- 不同 mtime ---"
ls -la --time-style=full-iso "$SPECIAL/mtime/" | head -5
echo "--- 软链接类型 ---"
ls -la "$SPECIAL/symlinks/"
echo "--- 特殊/中文名称 ---"
ls -la "$EXOTIC/" | head -20
