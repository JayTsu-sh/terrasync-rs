fn main() {
    // Windows 下 duckdb feature 需要链接 duckdb.lib
    // 自动将 workspace 根目录的 libduckdb/ 加入链接搜索路径
    if cfg!(windows) {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
        let libduckdb_dir = std::path::Path::new(&manifest_dir).join("..").join("libduckdb");
        if libduckdb_dir.exists() {
            println!(
                "cargo:rustc-link-search=native={}",
                libduckdb_dir.canonicalize().unwrap_or(libduckdb_dir).display()
            );
        }
    }
}
