#![recursion_limit = "256"]

//! 程序主入口模块
//!
//! 该模块包含程序的主函数，负责初始化运行环境并解析命令行参数执行相应的命令。

// 标准库
use std::process::exit;

// 内部模块
use cli::cli_match;
#[cfg(feature = "profiling")]
use console_subscriber::init;

// 使用 mimalloc 替换 MUSL 默认分配器，改善高并发场景下的内存碎片化问题
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// 程序的主入口函数
///
/// 负责：
/// 1. 初始化性能分析（如果启用了profiling特性）
/// 2. 解析命令行参数并执行相应命令
/// 3. 处理命令执行过程中的错误
#[tokio::main]
async fn main() {
    // 如果启用了profiling特性，初始化console_subscriber用于性能分析
    #[cfg(feature = "profiling")]
    {
        init();
    }

    // 解析命令行参数并执行匹配的命令
    if let Err(e) = cli_match().await {
        eprintln!("Error: {e}");
        exit(1);
    }
}
