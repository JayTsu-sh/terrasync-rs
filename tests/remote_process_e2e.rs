//! 进程级 Sender↔Receiver 握手端到端验证 (#18)
//!
//! 起两个真实 `terrasync` 子进程（Receiver `serve` + Sender `sync --remote`），
//! 通过真实 loopback QUIC 连接完成版本/能力协商握手与全量文件同步 —— 与
//! `crates/transport/tests/quic_roundtrip.rs` 的同进程握手测试不同，这里是两个
//! 独立的 OS 进程。
//!
//! 负路径（版本不兼容拒绝 / 缺失 delta 能力降级）已由该文件中的
//! `test_quic_handshake_incompatible_version_rejected_before_phase1` /
//! `test_quic_handshake_missing_delta_capability_downgrades` 覆盖：协议版本号是
//! 编译期常量，无法让单一真实进程谎报旧版本，因此本测试只做 happy-path
//! （版本兼容 → 握手 Accepted → 全量同步成功）。
//!
//! 运行：`cargo test -p terrasync-rs --test remote_process_e2e -- --nocapture`

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::pedantic)]

use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use assert_cmd::Command as AssertCommand;
use assert_cmd::cargo::cargo_bin;

/// 轮询等待 Receiver 写入 TLS 证书文件的超时时间
const READY_TIMEOUT: Duration = Duration::from_secs(10);
/// 轮询间隔
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// 等待 Receiver 进程在 Sender 完成后自行退出的超时时间
///
/// Receiver 发完 `AllDone` 后会 `Endpoint::wait_idle()` 等 Sender 关闭连接才退出；
/// 正常情况下几十毫秒内就会返回，但 quinn 兜底的默认 `max_idle_timeout` 是 30s
/// （Sender 关闭连接的那一帧理论上也可能因为进程过早退出而丢失，要靠 idle timeout 兜底），
/// 这里留够余量避免偶发的 30s 兜底触发时被误判为异常。
const RECEIVER_EXIT_TIMEOUT: Duration = Duration::from_secs(35);

/// 在 loopback 上找一个当前空闲的端口：bind `127.0.0.1:0` 读端口后立即释放。
fn free_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read local_addr").port()
}

/// 在 `dir` 下写入若干测试文件（含多级子目录），返回相对路径列表（供后续比对）。
fn populate_src_dir(dir: &Path) -> Vec<PathBuf> {
    let files: [(&str, &[u8]); 3] = [
        ("a.txt", b"hello world\n"),
        ("sub/b.txt", b"nested file content\n"),
        ("sub/deeper/c.bin", b"\x00\x01\x02binary\xffdata"),
    ];
    let mut rel_paths = Vec::new();
    for (rel, content) in files {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create src subdir");
        }
        fs::write(&path, content).expect("write src file");
        rel_paths.push(PathBuf::from(rel));
    }
    rel_paths
}

/// 断言 `dest_dir` 下每个文件与 `src_dir` 对应文件内容一致（数量 + 内容逐一比对）。
fn assert_dest_matches_src(src_dir: &Path, dest_dir: &Path, rel_paths: &[PathBuf]) {
    for rel in rel_paths {
        let src_content = fs::read(src_dir.join(rel)).expect("read src file");
        let dest_content = fs::read(dest_dir.join(rel)).unwrap_or_else(|e| panic!("dest 缺少文件 {rel:?}: {e}"));
        assert_eq!(src_content, dest_content, "dest 文件 {rel:?} 内容与 src 不一致");
    }
}

/// 轮询等待 Receiver 就绪：TLS 证书文件写盘完成即视为就绪。
///
/// `serve` 进程 bind QUIC endpoint、生成自签名证书后立即写文件（见
/// `crates/transport/src/quic/receiver.rs::bind` 与 `cli/commands.rs::serve_cmd`），
/// 早于等待任何连接，因此证书文件出现是可靠的就绪信号。
///
/// 注：QUIC 基于 UDP，无法像 TCP 那样用 `TcpStream::connect` 探测端口就绪，
/// 证书文件落盘是这里更直接、更可靠的信号。
fn wait_for_cert_file(cert_path: &Path) {
    let deadline = Instant::now() + READY_TIMEOUT;
    while !cert_path.exists() {
        if Instant::now() >= deadline {
            panic!("等待 Receiver 写入 TLS 证书文件超时: {cert_path:?}");
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// 读取日志文件，断言其中出现握手成功的日志行。
fn assert_handshake_accepted_in_log(log_path: &Path, who: &str) {
    let content = fs::read_to_string(log_path).unwrap_or_else(|e| panic!("读取 {who} 日志失败 {log_path:?}: {e}"));
    assert!(
        content.contains("Handshake accepted, negotiated features"),
        "{who} 日志 {log_path:?} 中未发现握手成功记录，内容:\n{content}"
    );
}

/// 等待子进程退出；超时则强制 kill 并返回 `None`。
fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// 读取子进程已终止后残留的 stdout/stderr（便于失败时打印诊断信息）。
fn drain_output(child: &mut Child) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut out) = child.stdout.take() {
        let _ = out.read_to_string(&mut stdout);
    }
    if let Some(mut err) = child.stderr.take() {
        let _ = err.read_to_string(&mut stderr);
    }
    (stdout, stderr)
}

#[test]
fn test_remote_process_e2e_handshake_and_sync() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    // 绕开 ClickHouse 依赖：双进程 remote 模式本身不落库，只有 orchestrator 顶层为判定
    // ScanType 会在 database.enabled=true 时构造真实 Database 连接，测试环境没有 ClickHouse。
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");

    // 让日志 / jobs 目录落在本次测试专属的 tmp 目录内：
    // utils::logger::setup_logging 在 CARGO_MANIFEST_DIR 存在时，用它的父目录作为日志根目录。
    // 子进程默认会继承本测试进程的 CARGO_MANIFEST_DIR（cargo test 设为 worktree 根），若不覆写，
    // 日志会写到 worktree 之外、和其它并行 agent worktree 共享的目录。这里指向 tmp 目录下一个
    // 不存在的子路径，使其父目录正好落在 tmp_path 内（Path::parent() 是纯字面操作，不要求路径存在）。
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

    // ── 启动 Receiver 进程（目标端） ──
    let mut receiver_child = StdCommand::new(&bin_path)
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("serve")
        .arg("--listen")
        .arg(&listen_addr)
        .arg("--tls-cert-out")
        .arg(&cert_path)
        .arg(&dest_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn receiver process");

    wait_for_cert_file(&cert_path);

    // ── 启动 Sender 进程（源端），指向 Receiver ──
    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_test")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);

    // Sender 退出码必须为 0；失败时 assert_cmd 会自动把 stdout/stderr 打进 panic 信息
    sender_cmd.assert().success();

    // Receiver 完成单次传输后会自行退出（非常驻 daemon），等待其退出
    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (receiver_stdout, receiver_stderr) = drain_output(&mut receiver_child);

    match receiver_status {
        Some(status) => assert!(
            status.success(),
            "Receiver 进程异常退出: {status:?}\nstdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
        ),
        None => panic!(
            "Receiver 进程在 {RECEIVER_EXIT_TIMEOUT:?} 内未自行退出，已强制 kill。\n\
             stdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
        ),
    }

    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    // 握手证据：Sender/Receiver 各自的 app.log 里都应有 "Handshake accepted" 记录
    // （日志路径推导见上面 fake_manifest_dir 的注释：父目录即 tmp_path）
    let sender_log = tmp_path.join("logs").join("remote_e2e_test").join("app.log");
    let receiver_log = tmp_path.join("logs").join("app.log");
    assert_handshake_accepted_in_log(&sender_log, "Sender");
    assert_handshake_accepted_in_log(&receiver_log, "Receiver");
}
