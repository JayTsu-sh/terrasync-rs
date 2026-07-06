//! CLI命令枚举模块
//!
//! 该模块定义了CLI应用程序的各种命令枚举，包括：
//! 1. 主命令枚举 Commands
//! 2. ACE子命令枚举 `AceCommands`
//! 3. 校验和类型枚举 `IntegrityCheckType`

// 外部crate
use clap::Subcommand;

// 内部模块
use crate::commands::{
    validate_block_size, validate_iops, validate_peak_qos_rate, validate_qos_format, validate_storage_path,
};

/// ACE子命令枚举
#[derive(Subcommand, Debug)]
pub enum AceCommands {
    /// List ACEs (Access Control Entries) for a file or directory
    List {
        /// Job ID for tracking
        #[arg(short, long)]
        id: Option<String>,

        /// Scan depth level (0 for infinite)
        #[arg(short, long, default_value = "0")]
        depth: u32,

        /// Directory or file path to scan
        #[arg(value_parser = validate_storage_path)]
        path: String,

        /// Owner Name to filter ACEs
        #[arg(short, long, value_name = "OWNER")]
        owner: Option<String>,

        /// Filter expression to match files/directories
        /// Examples: 'modified<0.5 and "ntap" in name and type==file'
        #[arg(short, long, value_name = "EXPRESSION")]
        r#match: Option<String>,

        /// Filter expression to exclude files/directories
        /// Examples: 'name=="target" or name==".git"'
        #[arg(short, long, value_name = "EXPRESSION")]
        exclude: Option<String>,

        /// Include inherited ACEs (default is false)
        #[arg(long, default_value_t = false)]
        include_inherited: bool,
    },

    /// Copy ACEs from source to target
    Copy {
        /// Job ID for tracking
        #[arg(short, long)]
        id: Option<String>,

        /// Scan depth level (0 for infinite)
        #[arg(short, long, default_value = "0")]
        depth: u32,

        /// Source file or directory path
        #[arg(value_parser = validate_storage_path)]
        source_path: String,

        /// Target file or directory path
        #[arg(value_parser = validate_storage_path)]
        target_path: String,

        /// Filter expression to match files/directories
        /// Examples: 'modified<0.5 and "ntap" in name and type==file'
        #[arg(short, long, value_name = "EXPRESSION")]
        r#match: Option<String>,

        /// Filter expression to exclude files/directories
        /// Examples: 'name=="target" or name==".git"'
        #[arg(short, long, value_name = "EXPRESSION")]
        exclude: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the replicate operation
    Sync {
        /// Job ID for tracking
        #[arg(short, long)]
        id: Option<String>,

        /// Source directory path to sync
        #[arg(value_parser = validate_storage_path)]
        src_path: String,

        /// Destination directory path to sync
        #[arg(value_parser = validate_storage_path)]
        dest_path: String,

        /// Enable checksum verification for files.
        /// Also configurable in config file under `[sync]` `enable_integrity_check`
        #[arg(long, default_value_t = false)]
        enable_integrity_check: bool,

        /// Enable ACL (Windows only).
        /// Also configurable in config file under `[sync]` `enable_acl`
        #[arg(long, default_value_t = false)]
        enable_acl: bool,

        /// Filter expression to match files/directories.
        /// Also configurable in config file under [scan] match.
        /// Examples: 'modified<0.5 and "ntap" in name and type==file'
        #[arg(short, long, value_name = "EXPRESSION")]
        r#match: Option<String>,

        /// Filter expression to exclude files/directories.
        /// Also configurable in config file under [scan] exclude.
        /// Examples: 'name=="target" or name==".git"'
        #[arg(short, long, value_name = "EXPRESSION")]
        exclude: Option<String>,

        /// Quality of service rate limit, such as 1GiB/s, 100MiB/s, etc.
        /// Also configurable in config file under [sync] qos
        #[arg(long, value_parser = validate_qos_format)]
        qos: Option<String>,

        /// Peak `QoS` rate multiplier.
        /// Also configurable in config file under `[sync]` `peak_qos_rate`
        #[arg(long, default_value_t = 2.0, value_parser = validate_peak_qos_rate)]
        peak_qos_rate: f32,

        /// IO operations per second limit (e.g. 1000)
        #[arg(long, value_parser = validate_iops)]
        iops: Option<u32>,

        /// The block size to flush the data to destination storage, such as 4MiB, 16MiB, etc.
        /// Also configurable in config file under `[sync]` `block_size`
        #[arg(long, value_parser = validate_block_size)]
        block_size: Option<String>,

        /// File containing list of files to sync
        #[arg(long, value_name = "FILE")]
        file_list: Option<String>,

        /// Package matched directories into .tar files
        #[arg(long, default_value_t = false)]
        packaged: bool,

        /// Depth below dir_date-matched directory to package (requires --packaged)
        #[arg(long, requires = "packaged")]
        package_depth: Option<usize>,

        /// Connect to a remote Receiver daemon for dual-process sync.
        /// Format: HOST:PORT (e.g. 192.168.1.100:9876)
        #[arg(long, value_name = "HOST:PORT")]
        remote: Option<String>,

        /// Server TLS certificate file for verifying the Receiver (防 MITM).
        /// Copy from the Receiver's --tls-cert-out output.
        #[arg(long, value_name = "FILE", requires = "remote")]
        tls_server_cert: Option<String>,

        /// Disable byte-level resume for large files (force whole-file copy).
        /// By default an interrupted large-file transfer resumes from where it stopped.
        #[arg(long, default_value_t = false)]
        no_resume: bool,
    },

    /// Start a Receiver daemon for dual-process sync (destination side).
    /// Listens for incoming QUIC connections from a Sender.
    Serve {
        /// Listen address and port
        #[arg(long, default_value = "0.0.0.0:9876")]
        listen: String,

        /// Destination storage path (supports Local/NFS/S3/CIFS URLs)
        #[arg(value_parser = validate_storage_path)]
        dest_path: String,

        /// File path to write the server TLS certificate (DER format).
        /// Copy this file to the Sender side and use --tls-server-cert to prevent MITM attacks.
        #[arg(long, default_value = "server.crt")]
        tls_cert_out: String,
    },

    /// Run the scan operation
    Scan {
        /// Job ID for tracking
        #[arg(short, long)]
        id: Option<String>,

        /// Scan depth level (0 for infinite).
        /// Also configurable in config file under [scan] depth
        #[arg(short, long, default_value = "0")]
        depth: u32,

        /// Directory path to scan
        #[arg(value_parser = validate_storage_path)]
        path: String,

        /// Filter expression to match files/directories.
        /// Also configurable in config file under [scan] match.
        /// Examples: 'modified<0.5 and "ntap" in name and type==file'
        #[arg(short, long, value_name = "EXPRESSION")]
        r#match: Option<String>,

        /// Filter expression to exclude files/directories.
        /// Also configurable in config file under [scan] exclude.
        /// Examples: 'name=="target" or name==".git"'
        #[arg(short, long, value_name = "EXPRESSION")]
        exclude: Option<String>,
    },
    #[clap(
        name = "config",
        about = "Show Configuration",
        long_about = None,
    )]
    Config,

    /// 删除指定路径下的所有文件和目录
    Rm {
        /// 要删除的目录或文件路径
        #[arg(value_parser = validate_storage_path)]
        path: String,
    },

    /// Run integrity check between source and destination paths
    #[clap(name = "integrity-check")]
    IntegrityCheck {
        /// Job ID for tracking
        #[arg(short, long)]
        id: Option<String>,

        /// 快速校验模式：仅比较文件大小，不做数据哈希校验
        #[arg(long, default_value_t = false)]
        quick: bool,

        /// 自动修复模式：对 uid/gid/mode（文件）、mtime/uid/gid/mode（目录）、mtime/uid/gid（软链接）的不一致进行自动修复
        #[arg(long, default_value_t = false)]
        auto_fix: bool,

        /// Source directory or file path
        #[arg(value_parser = validate_storage_path)]
        src_path: String,

        /// Destination directory or file path
        #[arg(value_parser = validate_storage_path)]
        dest_path: String,
    },

    /// Activate license on this machine
    #[cfg(feature = "license")]
    Activate,

    /// Launch the Web GUI management interface
    #[cfg(feature = "gui")]
    Gui {
        /// Host address to bind (default: 0.0.0.0)
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Port to listen on (default: 9090)
        #[arg(short, long, default_value_t = 9090)]
        port: u16,
    },

    /// Windows ACE (Access Control Entry) operations (Windows only)
    #[cfg(target_os = "windows")]
    Ace {
        #[clap(subcommand)]
        ace_command: AceCommands,
    },
}
