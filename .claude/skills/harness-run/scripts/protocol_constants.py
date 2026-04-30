"""
单一事实来源：各协议跨 skill 共享的测试常量。
run.py 从此文件 import，SKILL.md 标注来源。改此处即同步所有 skill。
"""


class NfsV3:
    """NFS v3 协议测试数据集常量。测试数据由 setup-nfs3-test-data.sh 创建。"""
    EXPORT = "/export/nfs"
    # 基线（全量 sync/scan 后）
    BASELINE_DIRS     = 113
    BASELINE_FILES    = 335
    BASELINE_SYMLINKS = 79
    BASELINE_TOTAL    = 527   # 113+335+79
    # 变更后（mutate-nfs3-test-data.sh 执行后）
    POST_DIRS         = 114
    POST_FILES        = 333
    POST_SYMLINKS     = 79
    POST_TOTAL        = 526   # 114+333+79


class NfsV4:
    """NFS v4.1 协议测试数据集常量。测试数据由 setup-nfs4-test-data.sh 创建。"""
    EXPORT = "/export/nfsv4"           # 服务端文件系统路径（SSH 命令使用）
    EXPORT_URL_PATH = "/"              # NFSv4 伪根 URL 路径（fsid=0 映射到 EXPORT）
    # 基线
    BASELINE_DIRS     = 113
    BASELINE_FILES    = 335
    BASELINE_SYMLINKS = 79
    BASELINE_TOTAL    = 527
    # 变更后（mutate-nfs4-test-data.sh 执行后）
    POST_DIRS         = 114
    POST_FILES        = 333
    POST_SYMLINKS     = 79


class Cifs:
    """CIFS/SMB 协议测试数据集常量。CIFS 无 symlink。"""
    SHARE = "testshare"
    # 基线
    BASELINE_DIRS  = 40
    BASELINE_FILES = 117
    BASELINE_TOTAL = 157   # 40+117
    # 变更后
    POST_DIRS  = 41
    POST_FILES = 115
    POST_TOTAL = 156       # 41+115


class S3:
    """S3 (rustfs) 协议测试数据集常量。S3 无 symlink，且根 prefix 不作为目录条目计数。
    本地数据生成器创建 40 个目录（含 root），但 S3 scan 只发出 3+9+27=39 个 sub-prefix 条目。
    """
    BUCKET_SRC = "test-bucket"
    BUCKET_DST = "test-bucket"
    # 基线 — S3 不计 root prefix
    BASELINE_DIRS  = 39
    BASELINE_FILES = 117
    BASELINE_TOTAL = 156   # 39+117
    # 变更后 — mutate 脚本新增/删除按相同规则
    POST_DIRS  = 40
    POST_FILES = 115
    POST_TOTAL = 155       # 40+115
