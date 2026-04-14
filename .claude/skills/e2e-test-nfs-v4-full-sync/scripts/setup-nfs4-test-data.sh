#!/bin/bash
# setup-nfs4-test-data.sh — 在 /export/nfsv4/test-data 创建丰富的测试目录树
# 包含：多层目录、文件、软链接，以及不同的 mode/uid/gid/mtime 组合
# 同时设置 NFSv4 ACL 和 named attributes（xattr）
set -e

BASE="/export/nfsv4/test-data"

# 清理已有数据（全量重建）
rm -rf "$BASE"
mkdir -p "$BASE"

DIRS=1  # 算上 $BASE (test-data) 自身
FILES=0
SYMLINKS=0
ACL_COUNT=0
XATTR_COUNT=0

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

# 组合 1: 只读文件，非 root 用户，旧 mtime
echo "readonly old file" > "$SPECIAL/mixed/readonly_old.txt"
chmod 0444 "$SPECIAL/mixed/readonly_old.txt"
chown 1000:1000 "$SPECIAL/mixed/readonly_old.txt"
touch -d "2021-03-10T10:00:00" "$SPECIAL/mixed/readonly_old.txt"
FILES=$((FILES+1))

# 组合 2: 可执行文件，root 用户，新 mtime
echo "#!/bin/bash" > "$SPECIAL/mixed/exec_new.sh"
chmod 0755 "$SPECIAL/mixed/exec_new.sh"
chown 0:0 "$SPECIAL/mixed/exec_new.sh"
touch -d "2026-02-01T00:00:00" "$SPECIAL/mixed/exec_new.sh"
FILES=$((FILES+1))

# 组合 3: 组可写目录，特定用户
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

# 组合 4: 权限受限目录，root 拥有
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

# 组合 5: 带软链接的混合属性目录
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

mkdir -p "$EXOTIC/归档/2025 年报告"
DIRS=$((DIRS+2))
echo "年度报告" > "$EXOTIC/归档/2025 年报告/年度总结.txt"
FILES=$((FILES+1))
echo "季度数据" > "$EXOTIC/归档/2025 年报告/第四季度.csv"
FILES=$((FILES+1))

# --- 3g: 中文 + 特殊字符混合 ---
mkdir -p "$EXOTIC/项目 (2025)"
DIRS=$((DIRS+1))
echo "混合名称" > "$EXOTIC/项目 (2025)/版本 v1.0&draft.txt"
FILES=$((FILES+1))
echo "带空格" > "$EXOTIC/项目 (2025)/需求 说明书.md"
FILES=$((FILES+1))
ln -sf "需求 说明书.md" "$EXOTIC/项目 (2025)/需求文档@latest.lnk"
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
echo "fullwidth"         > "$EXOTIC/unicode/ABCD 全角.txt"
FILES=$((FILES+1))
ln -sf "file_🚀.txt" "$EXOTIC/unicode/rocket_link.lnk"
SYMLINKS=$((SYMLINKS+1))

# ============================================================
# Part 4: NFSv4 ACL 设置
# ============================================================
# nfs4_setfacl 依赖 system.nfs4_acl xattr，只能在 NFS 客户端挂载点上使用，
# 无法直接在 NFS 服务器本地文件系统上操作。
# 方案：通过 NFS4.1 回环挂载（127.0.0.1:/）来设置 ACL。

# 安装 nfs4-acl-tools 和 nfs-common（NFS 客户端）
if ! command -v nfs4_setfacl &> /dev/null; then
  apt-get update -qq && apt-get install -y -qq nfs4-acl-tools || {
    echo "WARNING: nfs4_setfacl not available, skipping ACL setup"
    ACL_COUNT=0
  }
fi
if ! command -v mount.nfs4 &> /dev/null; then
  apt-get update -qq && apt-get install -y -qq nfs-common || {
    echo "WARNING: nfs-common not available, skipping ACL setup"
  }
fi

if command -v nfs4_setfacl &> /dev/null && command -v mount.nfs4 &> /dev/null; then
  # 创建本地 NFS4.1 回环挂载点
  NFS_MNT="/mnt/nfs4_acl_setup"
  mkdir -p "$NFS_MNT"
  if mount -t nfs4 -o vers=4.1 127.0.0.1:/ "$NFS_MNT" 2>/dev/null; then
    echo "NFS4.1 loopback mount OK at $NFS_MNT"
    # 计算从 NFS 挂载点到 BASE 的相对路径
    # BASE=/export/nfsv4/test-data, NFS export root=/, 所以 NFS 路径 = $NFS_MNT/export/nfsv4/test-data
    # 但 fsid=0 export 的根是 /export/nfsv4，所以 NFS 路径 = $NFS_MNT/test-data
    NFS_BASE="$NFS_MNT/test-data"
    NFS_EXOTIC="$NFS_BASE/exotic_names"

    # ACL 测试文件 1-5: d1 目录树
    for f in "$NFS_BASE/d1/d1_1/file1.txt" "$NFS_BASE/d1/d1_2/file1.txt" "$NFS_BASE/d1/d1_3/file1.txt" "$NFS_BASE/d1/d1_4/file1.txt" "$NFS_BASE/d1/d1_1/file2.txt"; do
      if [[ -f "$f" ]]; then
        nfs4_setfacl -a "A:g:GROUP@:rw" "$f" 2>/dev/null && ACL_COUNT=$((ACL_COUNT+1))
      fi
    done

    # ACL 测试文件 6-10: special 目录
    for f in "$NFS_BASE/special/mixed/group_writable/data.txt" "$NFS_BASE/special/ownership/uid1000_gid1000/file.txt" "$NFS_BASE/special/file_modes/file_0644.txt" "$NFS_BASE/special/file_modes/file_0755.txt" "$NFS_BASE/special/mtime/file_2024-03-15T08-00-00.txt"; do
      if [[ -f "$f" ]]; then
        nfs4_setfacl -a "A::EVERYONE@:r" "$f" 2>/dev/null && ACL_COUNT=$((ACL_COUNT+1))
      fi
    done

    # ACL 测试目录 1-5
    for d in "$NFS_BASE/d1/d1_1" "$NFS_BASE/d2/d2_1" "$NFS_BASE/special/mixed" "$NFS_BASE/special/ownership" "$NFS_EXOTIC/数据目录"; do
      if [[ -d "$d" ]]; then
        nfs4_setfacl -a "A:g:GROUP@:rx" "$d" 2>/dev/null && ACL_COUNT=$((ACL_COUNT+1))
      fi
    done

    # 卸载回环挂载
    umount "$NFS_MNT" 2>/dev/null
    rmdir "$NFS_MNT" 2>/dev/null
    echo "NFS4.1 loopback unmounted"
  else
    echo "WARNING: Failed to loopback mount NFS4.1, skipping ACL setup"
  fi
fi

# ============================================================
# Part 5: xattr (named attributes) 设置
# ============================================================
# 注意：Linux nfsd 不支持 NFSv4 OPENATTR 操作（named attributes），
# 因此通过 setfattr 在本地文件系统上设置的 user.* xattr 无法通过 NFSv4 协议远程访问。
# 这是 Linux 内核 NFS 服务器的已知限制，非 datasync 的 bug。
# 此处仍然设置 xattr 用于验证本地文件系统层面的正确性。
# 安装 attr 工具（如果尚未安装）
if ! command -v setfattr &> /dev/null; then
  apt-get update -qq && apt-get install -y -qq attr || {
    echo "WARNING: setfattr not available, skipping xattr setup"
    XATTR_COUNT=0
  }
fi

if command -v setfattr &> /dev/null; then
  # xattr 测试文件 1-5: d2 目录树
  for f in "$BASE/d2/d2_1/file1.txt" "$BASE/d2/d2_2/file1.txt" "$BASE/d2/d2_3/file1.txt" "$BASE/d2/d2_4/file1.txt" "$BASE/d2/d2_1/file2.txt"; do
    if [[ -f "$f" ]]; then
      setfattr -n user.author -v "test-user" "$f" 2>/dev/null && XATTR_COUNT=$((XATTR_COUNT+1))
      setfattr -n user.version -v "1.0" "$f" 2>/dev/null
      setfattr -n user.checksum -v "md5:abc123" "$f" 2>/dev/null
    fi
  done

  # xattr 测试文件 6-10: exotic_names 目录
  for f in "$EXOTIC/数据目录/测试文件.txt" "$EXOTIC/归档/2025 年报告/年度总结.txt" "$EXOTIC/项目 (2025)/版本 v1.0&draft.txt" "$EXOTIC/unicode/file_🚀.txt" "$EXOTIC/long_names/file with spaces.txt"; do
    if [[ -f "$f" ]]; then
      setfattr -n user.category -v "文档" "$f" 2>/dev/null && XATTR_COUNT=$((XATTR_COUNT+1))
      setfattr -n user.tags -v "重要，归档，项目" "$f" 2>/dev/null
      setfattr -n user.json -v '{"key":"value","num":123}' "$f" 2>/dev/null
    fi
  done

  # xattr 测试文件 11-15: special 目录
  for f in "$BASE/special/mixed/exec_new.sh" "$BASE/special/mixed/restricted/secret.txt" "$BASE/special/ownership/uid0_gid0/file.txt" "$BASE/special/mtime/file_2026-01-01T00-00-00.txt" "$BASE/special/many_files/bulk_1.txt"; do
    if [[ -f "$f" ]]; then
      setfattr -n user.security -v "high" "$f" 2>/dev/null && XATTR_COUNT=$((XATTR_COUNT+1))
      setfattr -n user.priority -v "1" "$f" 2>/dev/null
    fi
  done
fi

# ============================================================
# Part 6: 统计与校验
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

echo "ACL set on: $ACL_COUNT files/dirs"
echo "xattr set on: $XATTR_COUNT files"

if [ "$FIND_DIRS" -eq "$DIRS" ] && [ "$FIND_FILES" -eq "$FILES" ] && [ "$FIND_LINKS" -eq "$SYMLINKS" ]; then
  echo "OK: 数量校验通过，ACL/xattr 设置完成"
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
if command -v nfs4_getfacl &> /dev/null; then
  echo "--- ACL 样本 ---"
  nfs4_getfacl "$BASE/d1/d1_1/file1.txt" 2>/dev/null | head -10 || echo "ACL not available"
fi
if command -v getfattr &> /dev/null; then
  echo "--- xattr 样本 ---"
  getfattr -d "$BASE/d2/d2_1/file1.txt" 2>/dev/null || echo "xattr not available"
fi
