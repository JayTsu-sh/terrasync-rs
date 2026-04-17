//! 构建脚本模块
//!
//! 该构建脚本负责在Windows平台上处理 `DuckDB` 库的链接和复制操作，
//! 确保编译过程中能正确找到和使用 `DuckDB` 静态库。

// 标准库
#[cfg(target_os = "windows")]
use std::env;
#[cfg(target_os = "windows")]
use std::fs;
#[cfg(target_os = "windows")]
use std::path::{Path, PathBuf};

/// 构建脚本的主函数
///
/// 在Windows平台上执行以下操作：
/// 1. 获取当前项目根目录
/// 2. 构建 `DuckDB` 库的自定义路径
/// 3. 向Cargo传递链接器搜索路径和库名称
/// 4. 获取目标输出目录
/// 5. 递归复制 `DuckDB` 库文件到目标目录
fn main() {
    #[cfg(target_os = "windows")]
    {
        // 获取当前项目根目录，用于构建DuckDB库的绝对路径
        let current_dir = env::current_dir().expect("Failed to get current directory");

        // 相对于项目根目录的DuckDB库路径
        let custom_lib_relative_path = PathBuf::from("libduckdb");

        // 构建DuckDB库的绝对路径
        let custom_lib_path = current_dir.join(&custom_lib_relative_path);

        // 向Cargo传递链接器搜索路径，使其能找到DuckDB静态库
        println!("cargo:rustc-link-search=native={}", custom_lib_path.display());
        // 向Cargo传递要链接的静态库名称
        println!("cargo:rustc-link-lib=static=duckdb");

        // 获取目标输出目录，用于确定最终的库文件复制位置
        let out_dir = env::var("OUT_DIR").unwrap();
        let target_dir = PathBuf::from(&out_dir)
            .ancestors()
            .nth(3) // 向上导航到目标目录
            .expect("Failed to find target directory")
            .to_path_buf();

        // 递归复制文件和目录的辅助函数
        /// 递归复制整个目录的内容到目标位置
        ///
        /// # 参数
        /// - `src`: 源目录路径
        /// - `dst`: 目标目录路径
        ///
        /// # 返回值
        /// - 成功时返回`Ok(())`
        /// - 失败时返回包含错误信息的`Err`
        fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
            // 如果目标目录不存在，则创建它
            if !dst.exists() {
                fs::create_dir_all(dst)?;
            }

            // 遍历源目录中的所有条目
            for entry in fs::read_dir(src)? {
                let entry = entry?;
                let file_type = entry.file_type()?;

                // 如果是目录，则递归复制
                if file_type.is_dir() {
                    copy_dir_all(&entry.path(), &dst.join(entry.file_name()))?;
                } else {
                    // 如果是文件，则直接复制
                    fs::copy(entry.path(), dst.join(entry.file_name()))?;
                }
            }
            Ok(())
        }

        // 递归复制DuckDB库文件到目标目录，确保运行时能正确找到库
        copy_dir_all(&custom_lib_path, &target_dir).expect("Failed to copy files");
    }
}
