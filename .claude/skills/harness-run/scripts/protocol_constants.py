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
    EXPORT = "/export/nfsv4"
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
    """S3 (rustfs) 协议测试数据集常量。S3 无 symlink。"""
    BUCKET_SRC = "test-bucket"
    BUCKET_DST = "test-bucket"
    # 基线
    BASELINE_DIRS  = 40
    BASELINE_FILES = 117
    BASELINE_TOTAL = 157
    # 变更后
    POST_DIRS  = 41
    POST_FILES = 115
    POST_TOTAL = 156
