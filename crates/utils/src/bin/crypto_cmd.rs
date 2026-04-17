use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::process::exit;

use regex::Regex;
use utils::crypto::CryptoUtil;
use utils::error::{Result, UtilsError};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    println!("args: {args:?}");
    // 检查命令行参数
    if args.len() < 2 {
        print_usage();
        exit(1);
    }

    let mut decrypt_mode = false;
    let mut file_path = None;
    let mut password = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-d" | "--decrypt" => {
                decrypt_mode = true;
                i += 1;
            }
            "-f" | "--file" => {
                if i + 1 < args.len() {
                    file_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: Missing file path after -f/--file");
                    print_usage();
                    exit(1);
                }
            }
            _ => {
                if password.is_none() {
                    password = Some(args[i].clone());
                } else {
                    eprintln!("Error: Unexpected argument: {}", args[i]);
                    print_usage();
                    exit(1);
                }
                i += 1;
            }
        }
    }

    // 处理文件模式
    if let Some(path) = file_path {
        process_file(&path, decrypt_mode)?;
    }
    // 处理单个密码模式
    else if let Some(pwd) = password {
        if decrypt_mode {
            let decrypted = CryptoUtil::decrypt_password(&pwd)?;
            println!("{decrypted}");
        } else {
            let encrypted = CryptoUtil::encrypt_password(&pwd)?;
            println!("{encrypted}");
        }
    }
    // 无效参数组合
    else {
        eprintln!("Error: Invalid arguments");
        print_usage();
        exit(1);
    }

    Ok(())
}

/// 打印使用说明
fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  Encrypt a password: encrypt_password <password>");
    eprintln!("  Decrypt a password: encrypt_password -d <encrypted_password>");
    eprintln!("  Encrypt all passwords in file: encrypt_password -f <file_path>");
    eprintln!("  Decrypt all passwords in file: encrypt_password -d -f <file_path>");
    eprintln!("  Decrypt all passwords in file: encrypt_password -f <file_path> -d");
}

/// 处理文件中的密码
fn process_file(file_path: &str, decrypt_mode: bool) -> Result<()> {
    // 读取文件内容
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    // 创建临时文件用于写入结果
    let temp_path = format!("{file_path}.tmp");
    let temp_file = File::create(&temp_path)?;
    let mut writer = BufWriter::new(temp_file);

    // 匹配password字段的正则表达式
    let password_regex =
        Regex::new(r#"password\s*=\s*"([^"]*)""#).map_err(|e| UtilsError::CryptoError(format!("Regex error: {e}")))?;

    // 逐行处理
    for line in reader.lines() {
        let mut line = line?;

        // 查找password字段
        if let Some(captures) = password_regex.captures(&line) {
            // 检查password字段是否被注释
            let password_start = if let Some(m) = captures.get(0) {
                m.start()
            } else {
                writeln!(writer, "{line}")?;
                continue;
            };

            // 提取password字段之前的内容
            let before_password = &line[0..password_start];

            // 检查是否有未被引号包围的#符号
            let mut is_commented = false;
            let mut in_quotes = false;

            for c in before_password.chars() {
                match c {
                    '"' => in_quotes = !in_quotes,
                    '#' if !in_quotes => {
                        is_commented = true;
                        break;
                    }
                    _ => {}
                }
            }

            if is_commented {
                // 被注释的password不处理
                writeln!(writer, "{line}")?;
                continue;
            }

            let current_password = if let Some(m) = captures.get(1) {
                m.as_str().to_string()
            } else {
                writeln!(writer, "{line}")?;
                continue;
            };

            if current_password.is_empty() {
                // 空密码保持不变
                writeln!(writer, "{line}")?;
                continue;
            }

            let processed_password = if decrypt_mode {
                // 解密密码
                CryptoUtil::decrypt_password(&current_password)?
            } else {
                // 加密密码
                CryptoUtil::encrypt_password(&current_password)?
            };

            // 替换密码
            line = password_regex
                .replace(&line, |_: &regex::Captures| {
                    format!("password = \"{processed_password}\"")
                })
                .to_string();
        }

        // 写入处理后的行
        writeln!(writer, "{line}")?;
    }

    // 刷新并关闭文件
    writer.flush()?;
    drop(writer);

    // 替换原文件
    fs::rename(&temp_path, file_path)?;

    println!("Successfully processed passwords in {file_path}");
    Ok(())
}
