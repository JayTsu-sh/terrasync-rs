// 标准库
use std::path::PathBuf;
use std::process;

// 外部 crate
use clap::{Parser, Subcommand};

// 内部模块
// 无（使用 licensing crate）

#[derive(Parser)]
#[command(name = "license-gen")]
#[command(about = "Terrasync license generation tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 生成 Ed25519 密钥对和 HMAC secret
    Keygen {
        /// 密钥输出目录
        #[arg(short, long, default_value = "./keys")]
        output: PathBuf,
    },

    /// 生成签名的 license 文件
    Generate {
        /// Ed25519 私钥文件路径
        #[arg(short, long)]
        key: PathBuf,

        /// 被授权人姓名
        #[arg(short, long)]
        licensee: String,

        /// 有效天数（与 --permanent 二选一）
        #[arg(short, long, conflicts_with = "permanent")]
        days: Option<u32>,

        /// 永久有效
        #[arg(long, conflicts_with = "days")]
        permanent: bool,

        /// 最大绑定机器数（默认 1，0 = 不限）
        #[arg(short, long, default_value = "1")]
        max_machines: u32,

        /// 激活窗口期（天，默认 7）
        #[arg(short, long, default_value = "7")]
        activation_window: u32,

        /// 输出文件路径
        #[arg(short, long, default_value = "license.json")]
        output: PathBuf,
    },

    /// 显示 license 文件信息
    Info {
        /// License 文件路径
        #[arg(default_value = "license.json")]
        license: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Keygen { output } => licensing::generate::save_keypair(&output),
        Commands::Generate {
            key,
            licensee,
            days,
            permanent,
            max_machines,
            activation_window,
            output,
        } => {
            let effective_days = if permanent { None } else { Some(days.unwrap_or(30)) };
            licensing::generate::generate_license(
                &key,
                &licensee,
                effective_days,
                max_machines,
                activation_window,
                &output,
            )
        }
        Commands::Info { license } => licensing::generate::show_license_info(&license),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        process::exit(1);
    }
}
