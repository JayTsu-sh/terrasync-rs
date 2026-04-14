// 标准库
use std::path::{Path, PathBuf};
use std::time::Instant;

use storage_v2::StorageEntryMessage;
use storage_v2::acl::{AceType, PermissionEntry, PermissionMapping, SecurityInfo, copy_acl, get_security_info};
// 外部crate
use tracing::{debug, error, info};
use utils::app_config::AppConfig;

// 内部模块
use crate::config::initialize_scan_config;
use crate::dir_walker;
use crate::error::{AppError, Result};
use crate::scan::ScanType;

/// 检查路径是否存在并返回规范化的路径
fn check_path_exists(path: &str) -> Result<PathBuf> {
    let path_obj = std::fs::canonicalize(Path::new(path))?;
    if !path_obj.exists() {
        eprintln!("Error: Path '{}' does not exist.", path_obj.display());
        return Err(AppError::PathNotExistError("Path does not exist".to_string()));
    }
    Ok(path_obj)
}

/// 复制目录的ACE
async fn copy_directory_aces(
    job_id: &str, source_path: &str, _target_path: &str, source_path_obj: &Path, target_path_obj: &Path, depth: u32,
    r#match: &Option<String>, exclude: &Option<String>,
) -> Result<()> {
    let app_config = AppConfig::fetch()?;
    let concurrency = app_config.sync.concurrency;

    // 初始化扫描配置
    let scan_config = initialize_scan_config(
        job_id,
        source_path,
        depth, // 0表示无限深度
        ScanType::Full,
        r#match,
        exclude,
        concurrency,
        false, // include_tags参数，暂设为false
        &None, // file_list参数，暂设为None
        false, // packaged
        0,     // package_depth
    )?;

    // 直接调用dir_walker::walkdir开始扫描
    let walkdir_iter = match dir_walker::walkdir(scan_config).await {
        Ok(iter) => {
            debug!("Directory walker started successfully");
            iter
        }
        Err(e) => {
            error!("Start directory walker failed: {}", e);
            return Err(AppError::ScanError(format!("Start directory walker failed: {}", e)));
        }
    };

    // 处理遍历结果
    let mut entry_count = 0;
    let mut success_count = 0;

    while let Some(msg) = walkdir_iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => {
                entry_count += 1;

                // 计算相对于源目录的相对路径
                let rel_path = entry.get_relative_path().to_path_buf();

                // 构建源文件和目标文件的完整路径
                let src_full_path = source_path_obj.join(&rel_path);
                let dest_full_path = target_path_obj.join(&rel_path);

                // 检查目标文件是否存在
                if dest_full_path.exists() {
                    // 读取源和目标的安全信息
                    if let Err(err) = copy_acl(&src_full_path, &dest_full_path) {
                        eprintln!(
                            "Failed to copy ACL for {} to {}: {}",
                            src_full_path.display(),
                            dest_full_path.display(),
                            err
                        );
                    } else {
                        success_count += 1;
                    }
                }
            }
            StorageEntryMessage::Error { path, reason, .. } => {
                error!("[ACE] Error for {}: {}", path.display(), reason);
            }
            _ => {}
        }
    }

    println!(
        "Processed {} entries, successfully updated ACEs for {} files/directories",
        entry_count, success_count
    );
    Ok(())
}

pub async fn copy_aces(
    job_id: &str, source_path: &str, target_path: &str, depth: u32, r#match: &Option<String>, exclude: &Option<String>,
) -> Result<()> {
    let start_time = Instant::now();

    // 检查并规范化路径
    let source_path_obj = match check_path_exists(source_path) {
        Ok(path) => path,
        Err(_) => return Ok(()),
    };

    let target_path_obj = match check_path_exists(target_path) {
        Ok(path) => path,
        Err(_) => return Ok(()),
    };

    // 判断源路径和目标路径的类型
    let source_is_dir = source_path_obj.is_dir();
    let target_is_dir = target_path_obj.is_dir();

    // 如果一个是文件一个是目录则报错
    if source_is_dir != target_is_dir {
        eprintln!("Error: Source and target must be of the same type (both files or both directories).");
        return Ok(());
    }

    if !source_is_dir && !target_is_dir {
        // 两个都是文件，直接调用storage中的copy_file_aces函数
        copy_acl(&source_path_obj, &target_path_obj)?;
        println!(
            "Successfully copied ACEs from file '{}' to '{}'",
            source_path, target_path
        );
    } else {
        // 两个都是目录，调用目录处理函数
        copy_directory_aces(
            job_id,
            source_path,
            target_path,
            &source_path_obj,
            &target_path_obj,
            depth,
            r#match,
            exclude,
        )
        .await?;
    }

    let elapsed = start_time.elapsed();
    println!("Time taken: {:?}", elapsed);

    Ok(())
}

pub async fn list_security_info(
    job_id: &str, path: &str, owner: &Option<String>, depth: u32, r#match: &Option<String>, exclude: &Option<String>,
    include_inherited: bool,
) -> Result<()> {
    let start_time = Instant::now();

    // 检查并规范化路径
    let path_obj = check_path_exists(path)?;

    // 检查是否为目录
    if path_obj.is_dir() {
        list_directory_security_info(
            job_id,
            path,
            &path_obj,
            owner,
            depth,
            r#match,
            exclude,
            include_inherited,
        )
        .await?;
    } else {
        // 非目录文件，直接获取并打印安全信息
        if let Ok(security_info) = get_security_info(&path_obj) {
            print_security_info(&security_info, owner, include_inherited);
        }
        let elapsed = start_time.elapsed();
        println!("Time taken: {:?}", elapsed);
    }

    Ok(())
}

/// 列出目录的安全信息
async fn list_directory_security_info(
    job_id: &str, path: &str, _path_obj: &Path, owner: &Option<String>, depth: u32, r#match: &Option<String>,
    exclude: &Option<String>, include_inherited: bool,
) -> Result<()> {
    // 加载应用配置
    let app_config = AppConfig::fetch()?;
    debug!("Loaded application configuration: {:?}", app_config);

    // 初始化ScanConfig
    let scan_config = initialize_scan_config(
        job_id,
        path,
        depth, // 0表示无限深度
        ScanType::Full,
        r#match,
        exclude,
        app_config.scan.concurrency,
        false, // include_tags参数，暂设为false
        &None, // file_list参数，暂设为None
        false, // packaged
        0,     // package_depth
    )?;

    // 直接调用dir_walker::walkdir开始扫描
    let walkdir_iter = match dir_walker::walkdir(scan_config).await {
        Ok(iter) => {
            debug!("Directory walker started successfully");
            iter
        }
        Err(e) => {
            error!("Start directory walker failed: {}", e);
            return Err(AppError::ScanError(format!("Start directory walker failed: {}", e)));
        }
    };

    // 处理遍历结果
    let mut entry_count = 0;

    while let Some(msg) = walkdir_iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => {
                // 合并源目录路径和相对路径，形成完整的绝对路径
                let full_path = Path::new(path).join(entry.get_relative_path());

                match get_security_info(&full_path) {
                    Ok(security_info) => {
                        // 如果有ACE信息，打印
                        if !security_info.aces.is_empty() {
                            if print_security_info(&security_info, owner, include_inherited) {
                                entry_count += 1;
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to get security info for {}: {}", full_path.display(), err);
                    }
                }
            }
            StorageEntryMessage::Error { path, reason, .. } => {
                error!("[ACE] Error for {}: {}", path.display(), reason);
            }
            _ => {}
        }
    }

    println!("\nTotal security entries processed: {}", entry_count);
    info!("\nTotal security entries processed: {}", entry_count);

    let elapsed = Instant::now().elapsed();
    println!("Time taken: {:?}", elapsed);
    info!("Time taken: {:?}", elapsed);

    Ok(())
}

/// 判断是否应该显示安全信息的辅助函数
fn should_display_security_info(security_info: &SecurityInfo, include_inherited: bool) -> bool {
    if include_inherited {
        return true;
    }

    // 当include_inherited为false时，只有当inheritance_enabled为false或包含非继承ACE时才显示
    !security_info.inheritance_enabled || security_info.aces.iter().any(|ace| !ace.inherited)
}

/// 打印安全信息的辅助函数
fn print_security_info(security_info: &SecurityInfo, owner: &Option<String>, include_inherited: bool) -> bool {
    // 检查是否需要显示当前security_info
    if !should_display_security_info(security_info, include_inherited) {
        return false;
    }

    // 如果有指定的owner，只打印匹配的ACE
    let aces_to_print = if let Some(owner_c) = owner.clone() {
        let filtered_aces: Vec<_> = security_info
            .aces
            .iter()
            .filter(|ace| ace.trustee.contains(&owner_c) && (include_inherited || !ace.inherited))
            .collect();
        debug!("Filtered ACE Count: {}", filtered_aces.len());
        filtered_aces
    } else {
        // 没有指定owner时，根据include_inherited参数过滤ACE
        security_info
            .aces
            .iter()
            .filter(|ace| include_inherited || !ace.inherited)
            .collect()
    };

    // 打印基本信息
    println!("\n===== Security Info for: {}", security_info.path.display());
    println!("Path: {}", security_info.path.display());
    println!("Owner: {}", security_info.owner);
    println!("Primary Group: {}", security_info.primary_group);
    println!("Has DACL: {}", security_info.has_dacl);
    println!("Inheritance Enabled: {}", security_info.inheritance_enabled);
    println!("ACE Count: {}", aces_to_print.len());

    println!("\n[ACE Details]");

    info!("===== Security Info for: {}", security_info.path.display());
    info!("Path: {}", security_info.path.display());
    info!("Owner: {}", security_info.owner);
    info!("Primary Group: {}", security_info.primary_group);
    info!("Has DACL: {}", security_info.has_dacl);
    info!("Inheritance Enabled: {}", security_info.inheritance_enabled);
    info!("ACE Count: {}", aces_to_print.len());

    info!("[ACE Details]");

    // 创建权限映射
    let permission_mapping = PermissionMapping::new();

    // 打印ACE列表
    for (i, ace) in aces_to_print.iter().enumerate() {
        println!("ACE #{:02}", i + 1);
        println!("  Trustee: {}", ace.trustee);
        println!("  Trustee Type: {}", ace.trustee_type);
        println!("  Access Mode: {}", ace.access_mode);
        println!("  Inherited: {}", if ace.inherited { "Yes" } else { "No" });

        info!("ACE #{:02}", i + 1);
        info!("  Trustee: {}", ace.trustee);
        info!("  Trustee Type: {}", ace.trustee_type);
        info!("  Access Mode: {}", ace.access_mode);
        info!("  Inherited: {}", if ace.inherited { "Yes" } else { "No" });

        // 查找并打印对应的权限信息
        let permissions = permission_mapping.get_permissions(ace.mask);
        if !permissions.is_empty() {
            // 根据access_mode设置正确的ace_type
            let ace_type = match ace.access_mode.as_str() {
                "Allow" => AceType::Allow,
                "Deny" => AceType::Deny,
                _ => AceType::Other(ace.ace_type),
            };
            let permission_entry = PermissionEntry { ace_type, permissions };
            println!("  Permissions: {}", permission_entry);
            info!("  Permissions: {}", permission_entry);
        } else {
            println!("  Permissions: Not mapped");
            info!("  Permissions: Not mapped");
        }
        println!("  Mask: 0x{:08X}", ace.mask);
        info!("  Mask: 0x{:08X}", ace.mask);

        println!("  Flags: 0x{:02X}", ace.ace_flags);
        info!("  Flags: 0x{:02X}", ace.ace_flags);
    }

    return true;
}
