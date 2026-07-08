// 标准库
use std::path::PathBuf;
use std::sync::Arc;

// 外部 crate
use bytes::Bytes;
use serde::{Deserialize, Serialize};
// 内部模块
use data_mover::dir_tree::DirPageResult;
use data_mover::{DataChunk, EntryEnum};

// ============================================================
// Sender → Receiver 消息
// ============================================================

/// Sender 发给 Receiver 的消息
#[derive(Debug, Serialize, Deserialize)]
pub enum SenderMsg {
    /// 会话配置（QoS、ACL、完整性校验等参数）
    SessionConfig(SessionConfig),

    // ── File List（双进程模式，复用 walkdir_2 的 DirPageResult） ──
    /// 一页目录内容
    FilePage(DirPageResult),
    /// 目录遍历中的错误
    FileListError { path: String, reason: String },
    /// 文件列表发送完毕
    FileListDone,

    // ── 单进程模式：命令模式（Receiver 有 src+dest storage，自己执行 copy） ──
    /// 复制一个 entry（目录/符号链接/文件，Receiver 根据 entry 类型 dispatch）
    /// Receiver 调用 `process_entry(entry, src_storage, dest_storage, ...)` 完成完整复制
    CopyEntry { entry: Arc<EntryEnum> },

    // ── 双进程模式：数据流模式（Receiver 只有 dest storage，Sender 流式发送数据） ──
    /// 创建目录（Receiver 调用 `create_dir_all` + `set_metadata` + ACL）
    CreateDir { entry: Arc<EntryEnum> },
    /// 创建符号链接（Sender 已读取 target，Receiver 调用 `create_symlink`）
    CreateSymlink { entry: Arc<EntryEnum>, target: PathBuf },
    /// 文件传输开始（Receiver 用于准备写入上下文）
    FileBegin { entry: Arc<EntryEnum> },
    /// 文件数据块
    FileData { entry: Arc<EntryEnum>, chunk: DataChunk },
    /// 文件传输结束
    EndOfFile {
        entry: Arc<EntryEnum>,
        source_hash: Option<String>,
    },

    // ── 增量传输（chunk 级 delta，Phase 5 实现） ──
    /// Delta token: 原始数据（源文件中目标端没有的部分）
    DeltaData { ndx: i32, data: Bytes },
    /// Delta token: 引用目标端已有 block（Receiver 从 basis file 读取）
    DeltaMatch { ndx: i32, block_index: u32 },

    // ── Packaged（tar 打包模式） ──
    /// 打包目录为 tar（Sender 完成打包后发送结果）
    TarPacked {
        tar_entry: Arc<EntryEnum>,
        manifest_entries: Vec<Arc<EntryEnum>>,
    },

    // ── ACL 跨进程传输 ──
    /// 设置 ACL（Sender 读取后发送二进制数据，Receiver 设置）
    SetAcl { entry: Arc<EntryEnum>, acl_data: Bytes },

    // ── 控制消息 ──
    /// Walkdir 中的错误（非致命，继续处理）
    EntryError { path: PathBuf, reason: String },
    /// 所有传输完成
    TransferDone,
}

// ============================================================
// Receiver → Sender 消息
// ============================================================

/// Receiver 发给 Sender 的消息（请求 + ack 合一）
#[derive(Debug, Serialize, Deserialize)]
pub enum ReceiverMsg {
    // ── 双进程模式：按 NDX 请求传输 ──
    /// 请求全量传输（目标文件不存在）
    TransferRequest { ndx: i32 },
    /// 请求增量传输（目标文件存在但数据不匹配，附带 block 签名）
    DeltaTransferRequest {
        ndx: i32,
        block_size: u32,
        signatures: Vec<BlockSignature>,
    },
    /// 仅更新元数据（文件内容一致但 mode/uid/gid/acl/tags 不匹配）
    MetadataUpdateRequest { ndx: i32 },
    /// 不再有新请求（所有 NDX 检查完毕）
    RequestsDone,

    // ── Entry 级别 ack（单进程 + 双进程通用） ──
    /// Entry 写入成功（Receiver 完成写入 + `set_metadata` + ACL 后发送）
    EntrySuccess { entry: Arc<EntryEnum> },
    /// Entry 写入失败
    EntryError { entry: Arc<EntryEnum>, reason: String },
    /// tar 包写入成功
    TarSuccess {
        tar_entry: Arc<EntryEnum>,
        manifest_entries: Vec<Arc<EntryEnum>>,
    },

    // ── NDX 级别 ack（双进程模式） ──
    /// 文件写入成功
    Success { ndx: i32 },
    /// 请求重传（校验失败）
    Redo { ndx: i32 },
    /// 写入失败
    Error { ndx: i32, reason: String },

    // ── 进度 + 控制 ──
    /// 进度快照
    Progress(ProgressSnapshot),
    /// Receiver 完成所有写入
    AllDone,
}

// ============================================================
// 辅助类型
// ============================================================

/// 会话配置，Sender 建立连接后首先发送
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct SessionConfig {
    /// 源端存储路径（Receiver 用于创建 `src_storage`，`CopyEntry` 模式需要）
    pub src_path: String,
    /// `QoS` 带宽限制（例如 "1GiB/s"）
    pub qos: Option<String>,
    /// `QoS` 峰值倍率
    pub peak_qos_rate: f32,
    /// IOPS 限制
    pub iops: Option<u32>,
    /// 是否启用完整性校验（BLAKE3）
    pub enable_integrity_check: bool,
    /// 是否启用 ACL 复制
    pub enable_acl: bool,
    /// 是否保留源端文件（false = 复制后删除源端）
    pub is_source_reserved: bool,
    /// 块大小（例如 "4MiB"）
    pub block_size: Option<String>,
    /// 是否启用 --delete-target（删除目标端多余文件）
    pub delete_target: bool,
}

/// Block 签名（Receiver 计算并发给 Sender，用于 chunk 级 delta）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSignature {
    /// 快速 rolling checksum（Adler32 变体，用于滑动窗口初筛）
    pub rolling: u32,
    /// 强 checksum（BLAKE3 截断为 16 字节）
    pub strong: [u8; 16],
}

/// 进度快照（Receiver → Sender，支持多 Receiver 聚合）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSnapshot {
    /// Receiver 标识（多 Receiver 时用于分桶聚合）
    pub receiver_id: String,
    /// 已传输文件数
    pub files_transferred: u64,
    /// 已创建目录数
    pub dirs_created: u64,
    /// 已传输字节数
    pub bytes_transferred: u64,
    /// 跳过的文件数（mtime+size 匹配）
    pub files_skipped: u64,
    /// 仅元数据更新的文件数
    pub metadata_only: u64,
    /// 错误数
    pub error_count: u64,
    /// 已运行时间（秒）
    pub elapsed_secs: f64,
    /// 传输速率（字节/秒）
    pub speed_bytes_per_sec: f64,
}

/// Receiver 侧比较决策
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDecision {
    /// 目标不存在 → 全量传输
    FullTransfer,
    /// 数据不匹配 → chunk 级增量传输
    DeltaTransfer,
    /// 数据匹配但元数据不匹配 → 仅更新元数据
    MetadataOnly,
    /// 完全匹配 → 跳过
    Skip,
}

// ============================================================
// Disk-commit 内部消息（Receiver 主 task → disk-commit task）
// ============================================================

/// SPSC channel 消息，用于 Receiver 主 task 与 disk-commit task 之间的通信
#[derive(Debug)]
pub enum DiskCommitMsg {
    // ── 目录/符号链接（即时完成，不需要 multi-chunk） ──
    /// 创建目录 + `set_metadata` + ACL
    CreateDir { entry: Arc<EntryEnum> },
    /// 创建符号链接
    CreateSymlink { entry: Arc<EntryEnum>, target: PathBuf },

    // ── 全量文件传输（3 段流式驱动） ──
    /// 文件开始：Receiver 据此 resume_prepare + 起 write_chunk_stream
    FileBegin { entry: Arc<EntryEnum> },
    /// 文件数据块（直接转发 DataChunk 给 write_chunk_stream 的 channel）
    FileChunk { entry: Arc<EntryEnum>, chunk: DataChunk },
    /// 文件传输结束，提交：读回 .part hash 校验 + set_metadata + ACL + 原子 rename
    FileCommit {
        entry: Arc<EntryEnum>,
        source_hash: Option<String>,
    },

    // ── 增量传输 (delta，Phase 5) ──
    /// 开始 delta 重建
    DeltaBegin { entry: Arc<EntryEnum>, block_size: u32 },
    /// Delta: 原始数据
    DeltaData { entry: Arc<EntryEnum>, data: Bytes },
    /// Delta: 引用目标端 basis file 的 block
    DeltaMatch { entry: Arc<EntryEnum>, block_index: u32 },
    /// Delta 完成：校验 + 原子 rename
    DeltaCommit {
        entry: Arc<EntryEnum>,
        source_hash: Option<String>,
    },

    // ── tar 包 ──
    /// tar 打包结果
    TarPacked {
        tar_entry: Arc<EntryEnum>,
        manifest_entries: Vec<Arc<EntryEnum>>,
    },

    // ── 控制 ──
    /// 中止当前正在写入的文件（Sender 读源失败）：丢弃 `.part`，不发 ack
    AbortFile,
    /// 关闭 disk-commit task
    Shutdown,
}

// ============================================================
// NDX 映射表（双进程模式下 Receiver 和 Sender 各持一份）
// ============================================================

/// NDX → `EntryEnum` 的映射表
#[derive(Debug, Default)]
pub struct NdxTable {
    entries: std::collections::HashMap<i32, Arc<EntryEnum>>,
}

impl NdxTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 从 `DirPageResult` 批量插入
    pub fn ingest_page(&mut self, page: &DirPageResult) {
        for nf in &page.files {
            self.entries.insert(nf.ndx, nf.entry.clone());
        }
        for ns in &page.subdirs {
            self.entries.insert(ns.ndx, ns.entry.clone());
        }
    }

    /// 按 NDX 查找 entry
    pub fn get(&self, ndx: i32) -> Option<&Arc<EntryEnum>> {
        self.entries.get(&ndx)
    }

    /// 当前条目数
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================
// DestIndex（双进程模式 Receiver 按页构建的目标端索引）
// ============================================================

/// 目标端目录索引（按页粒度构建，处理完一页后释放）
#[derive(Debug, Default)]
pub struct DestIndex {
    /// name → 目标端 EntryEnum（仅当前目录下的直接子条目）
    entries: std::collections::HashMap<String, Arc<EntryEnum>>,
    /// 已被源端 file list 匹配的条目名
    matched: std::collections::HashSet<String>,
}

impl DestIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// 插入一个目标端条目
    pub fn insert(&mut self, entry: Arc<EntryEnum>) {
        self.entries.insert(entry.get_name().to_string(), entry);
    }

    /// 与源端 entry 比较，返回传输决策
    pub fn check(&mut self, src_entry: &EntryEnum) -> TransferDecision {
        let name = src_entry.get_name();
        match self.entries.get(name) {
            None => TransferDecision::FullTransfer,
            Some(dest_entry) => {
                self.matched.insert(name.to_string());
                let data_match = data_check(src_entry, dest_entry);
                let meta_match = metadata_check(src_entry, dest_entry);
                match (data_match, meta_match) {
                    (true, true) => TransferDecision::Skip,
                    (true, false) => TransferDecision::MetadataOnly,
                    (false, _) => TransferDecision::DeltaTransfer,
                }
            }
        }
    }

    /// 返回目标端存在但源端 file list 中不存在的条目（需删除）
    pub fn orphaned_entries(&self) -> Vec<&Arc<EntryEnum>> {
        self.entries
            .iter()
            .filter(|(name, _)| !self.matched.contains(name.as_str()))
            .map(|(_, entry)| entry)
            .collect()
    }
}

/// 数据比较：mtime + size
fn data_check(src: &EntryEnum, dest: &EntryEnum) -> bool {
    src.get_mtime() == dest.get_mtime() && src.get_size() == dest.get_size()
}

/// 元数据比较：按 `EntryEnum` 类型比较不同字段
fn metadata_check(src: &EntryEnum, dest: &EntryEnum) -> bool {
    match (src, dest) {
        (EntryEnum::NAS(s), EntryEnum::NAS(d)) => s.mode == d.mode && s.uid == d.uid && s.gid == d.gid,
        (EntryEnum::S3(s), EntryEnum::S3(d)) => s.tags == d.tags,
        // 跨类型（NAS→S3 或 S3→NAS）：元数据体系不同，视为不匹配
        _ => false,
    }
}
