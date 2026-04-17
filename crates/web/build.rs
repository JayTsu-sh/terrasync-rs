use std::path::Path;
use std::process::Command;
use std::time::SystemTime;

fn main() {
    let web_ui_dir = Path::new("../../web-ui");
    let dist_dir = web_ui_dir.join("dist");
    let dist_index = dist_dir.join("index.html");

    // 声明需要监控的前端源码路径
    println!("cargo:rerun-if-changed=../../web-ui/src");
    println!("cargo:rerun-if-changed=../../web-ui/index.html");
    println!("cargo:rerun-if-changed=../../web-ui/package.json");
    println!("cargo:rerun-if-changed=../../web-ui/vite.config.ts");
    println!("cargo:rerun-if-changed=../../web-ui/tsconfig.json");
    println!("cargo:rerun-if-changed=../../web-ui/tsconfig.app.json");

    // 如果外部（如 make.bat）已在并行构建前端，则跳过
    if std::env::var("SKIP_FRONTEND_BUILD").is_ok() {
        return;
    }

    // 判断是否需要重建前端
    let needs_build = !dist_index.exists() || is_source_newer_than_dist(web_ui_dir, &dist_index);

    if !needs_build {
        return;
    }

    // npm install（仅当 node_modules 不存在时执行）
    let node_modules = web_ui_dir.join("node_modules");
    if !node_modules.exists() {
        let npm_install = Command::new("npm").arg("install").current_dir(web_ui_dir).status();

        match npm_install {
            Ok(s) if s.success() => {}
            _ => {
                create_placeholder(&dist_dir);
                return;
            }
        }
    }

    let npm_build = Command::new("npm")
        .arg("run")
        .arg("build")
        .current_dir(web_ui_dir)
        .status();

    match npm_build {
        Ok(s) if s.success() => {
            println!("cargo:warning=Frontend build completed successfully");
        }
        _ => {
            create_placeholder(&dist_dir);
        }
    }
}

/// 检查前端源码是否比 dist/index.html 更新
fn is_source_newer_than_dist(web_ui_dir: &Path, dist_index: &Path) -> bool {
    let Ok(dist_mtime) = dist_index.metadata().and_then(|m| m.modified()) else {
        return true; // 无法获取 dist 时间，视为需要重建
    };

    let src_dir = web_ui_dir.join("src");
    if let Ok(newest) = newest_mtime_in_dir(&src_dir)
        && newest > dist_mtime
    {
        return true;
    }

    // 检查关键配置文件
    let config_files = [
        "index.html",
        "package.json",
        "vite.config.ts",
        "tsconfig.json",
        "tsconfig.app.json",
    ];
    for name in &config_files {
        let path = web_ui_dir.join(name);
        if let Ok(meta) = path.metadata()
            && let Ok(mtime) = meta.modified()
            && mtime > dist_mtime
        {
            return true;
        }
    }

    false
}

/// 递归获取目录中最新文件的修改时间
fn newest_mtime_in_dir(dir: &Path) -> std::io::Result<SystemTime> {
    let mut newest = SystemTime::UNIX_EPOCH;

    if !dir.exists() {
        return Ok(newest);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Ok(t) = newest_mtime_in_dir(&path)
                && t > newest
            {
                newest = t;
            }
        } else if let Ok(meta) = path.metadata()
            && let Ok(mtime) = meta.modified()
            && mtime > newest
        {
            newest = mtime;
        }
    }

    Ok(newest)
}

/// 创建占位 dist 避免 rust-embed 编译错误
fn create_placeholder(dist_dir: &Path) {
    std::fs::create_dir_all(dist_dir).ok();
    std::fs::write(
        dist_dir.join("index.html"),
        "<html><body>Frontend not built. Run: cd web-ui && npm run build</body></html>",
    )
    .ok();
}
