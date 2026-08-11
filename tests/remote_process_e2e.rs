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

use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::net::UdpSocket;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, ExitStatus, Stdio};
use std::sync::{Mutex, OnceLock};
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
/// 等待 Sender 在 Receiver 被杀后检测到连接异常并退出的超时时间。
///
/// 实测：loopback 上 QUIC 不信任 ICMP Port Unreachable（防欺骗），Receiver 进程被
/// SIGKILL 后 Sender 稳定要等满 quinn 默认 `max_idle_timeout`（30s）才判定连接已断
/// （实测约 32s），比 [`RECEIVER_EXIT_TIMEOUT`] 的 35s 余量更紧，故单独给更大余量。
const SENDER_EXIT_TIMEOUT: Duration = Duration::from_secs(45);

/// 进程内已分配过的端口号（跨测试函数共享，避免同一 `cargo test` 进程内并发跑的
/// 多个测试探测到同一个端口——见 `free_loopback_port` 文档）。
static ALLOCATED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

/// 在 loopback 上找一个当前空闲的端口：bind `127.0.0.1:0`（UDP，QUIC 实际使用的协议）
/// 读端口后立即释放，供后续 `serve --listen` 使用。
///
/// 探测与实际 bind 之间存在 TOCTOU 窗口：本文件的测试都在同一个 `cargo test` 进程内
/// 默认并发跑（多个测试各自起 `serve`/`sync` 子进程），如果两个测试恰好在同一时刻探测，
/// OS 有可能把刚释放的端口又分配给下一次探测，导致两个测试都以为自己拿到了独占端口、
/// 实际却撞到同一个号——两个 QUIC server 抢同一个端口会导致连接行为不可预期，
/// 表现为随机的长时间 hang（曾在 `correct_token_succeeds` 与
/// `large_multi_chunk_file_mux` 并发跑时实际触发过）。用进程内共享的
/// `ALLOCATED_PORTS` 集合去重，从根源上避免同一进程内的两个测试拿到相同端口。
fn free_loopback_port() -> u16 {
    let allocated = ALLOCATED_PORTS.get_or_init(|| Mutex::new(HashSet::new()));
    loop {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral UDP port");
        let port = socket.local_addr().expect("read local_addr").port();
        drop(socket);
        let mut guard = allocated.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.insert(port) {
            return port;
        }
        // 撞了进程内已分配过的端口，换一次重试
    }
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

/// 读取目录 mtime（秒）。
fn dir_mtime_secs(p: &Path) -> u64 {
    fs::metadata(p)
        .unwrap_or_else(|e| panic!("stat {p:?}: {e}"))
        .modified()
        .expect("mtime")
        .duration_since(std::time::UNIX_EPOCH)
        .expect("epoch")
        .as_secs()
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

/// 回归：双进程同步应保留目录 mtime。写子文件会把目标端目录 mtime 顶到 ~now，
/// 必须在所有传输完成后回写目录元数据（对齐单进程 orchestrator 的目录 mtime 收尾）。
#[test]
fn test_remote_process_e2e_dir_mtime_preserved() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    populate_src_dir(&src_dir);

    // 写完文件后把源端目录 mtime 设成显著的过去时间，以便与"被文件写入顶到 ~now"清晰区分。
    let dirs = ["sub", "sub/deeper"];
    for d in dirs {
        let status = StdCommand::new("touch")
            .arg("-t")
            .arg("200102030405")
            .arg(src_dir.join(d))
            .status()
            .expect("touch src dir mtime");
        assert!(status.success(), "touch {d} 失败");
    }
    let src_mtimes: Vec<u64> = dirs.iter().map(|d| dir_mtime_secs(&src_dir.join(d))).collect();

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_dir_mtime_test")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    // 目标端目录 mtime 必须与源端一致（修复前：被文件写入顶到 ~now，不等于源端过去时间）。
    for (d, &src_m) in dirs.iter().zip(&src_mtimes) {
        let dest_m = dir_mtime_secs(&dest_dir.join(d));
        assert_eq!(
            dest_m, src_m,
            "目标端目录 {d} mtime={dest_m} 应等于源端 {src_m}（未回写则被子文件写入顶到 ~now）"
        );
    }
}

/// Token 鉴权测试（进程级）：Sender 携带与 Receiver 一致的 `--token` → 鉴权通过 → 全量同步成功
#[test]
fn test_remote_process_e2e_correct_token_succeeds() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

    const TOKEN: &str = "correct-token-e2e";

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
        .arg("--token")
        .arg(TOKEN)
        .arg(&dest_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn receiver process");

    wait_for_cert_file(&cert_path);

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_token_ok")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--token")
        .arg(TOKEN)
        .arg(&src_dir)
        .arg(&dest_dir);

    sender_cmd.assert().success();

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
}

/// Token 鉴权测试（进程级）：Sender 携带错误 `--token` → Receiver 拒绝连接 → 未写入任何文件
#[test]
fn test_remote_process_e2e_wrong_token_rejected() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    populate_src_dir(&src_dir);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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
        .arg("--token")
        .arg("right-token")
        .arg(&dest_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn receiver process");

    wait_for_cert_file(&cert_path);

    // Sender 携带错误 token，预期非零退出（鉴权失败），不进入文件列表/数据阶段
    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_token_bad")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--token")
        .arg("wrong-token")
        .arg(&src_dir)
        .arg(&dest_dir);

    sender_cmd.assert().failure();

    // Receiver 收到非法 token 后也会以非零码退出（鉴权失败向上传播为进程错误）
    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (receiver_stdout, receiver_stderr) = drain_output(&mut receiver_child);
    match receiver_status {
        Some(status) => assert!(
            !status.success(),
            "Receiver 进程本应因鉴权失败以非零码退出，实际: {status:?}\nstdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
        ),
        None => panic!(
            "Receiver 进程在 {RECEIVER_EXIT_TIMEOUT:?} 内未自行退出，已强制 kill。\n\
             stdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
        ),
    }

    // 鉴权在 SessionConfig / 文件列表阶段之前被拒绝，目标端不应出现任何同步文件
    let dest_entries: Vec<_> = fs::read_dir(&dest_dir).expect("read dest dir").collect();
    assert!(
        dest_entries.is_empty(),
        "鉴权失败后目标端不应有任何写入，实际发现: {dest_entries:?}"
    );
}

/// 生成 `len` 字节、与位置相关的确定性内容（避免 all-zero/重复模式掩盖 chunk 顺序或
/// offset 错位问题——多路复用改造后大文件数据全部走独立的 `Data` stream，需要确认
/// 跨多个 4MiB chunk 的大文件拼接结果与源文件完全一致）。
fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// 多路复用主路径端到端验证（issue #20）：source 目录内含一个跨多个 4MiB chunk 的
/// 大文件（连同若干小文件），验证全量同步后 dest 与 src 逐字节一致——证明 QUIC
/// 多路复用改造后，大文件数据在独立的 `Data` stream 上分片发送/重组不会丢字节、
/// 不会跨 chunk 错位，同时 file list / 请求 / progress / ack 等控制消息（走
/// `Control`/`FileList`/`AckProgress` 其余三条 stream）不受影响，仍能与既有握手/
/// 鉴权测试共用同一套子进程 harness 正常完成整个同步流程。
#[test]
fn test_remote_process_e2e_large_multi_chunk_file_mux() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let mut rel_paths = populate_src_dir(&src_dir);

    // 追加一个 10MiB 大文件：FILE_CHUNK_SIZE 为 4MiB（见 crates/app/src/remote_sync.rs），
    // 10MiB 确保跨越至少 3 个 FileData chunk。
    const LARGE_FILE_SIZE: usize = 10 * 1024 * 1024;
    let large_rel = PathBuf::from("large_multi_chunk.bin");
    fs::write(src_dir.join(&large_rel), deterministic_bytes(LARGE_FILE_SIZE)).expect("write large src file");
    rel_paths.push(large_rel);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_mux_large_file")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);

    sender_cmd.assert().success();

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
}

/// 应用层字节 credit 流控端到端验证（issue #59）：源目录含一个 80MiB 大文件，超过
/// Sender 侧默认 credit 窗口（`DEFAULT_CREDIT_WINDOW_BYTES` = 64MiB），全量同步必须至少
/// 经历一次「credit 耗尽 → 等待 Receiver `CreditGrant` → 解除阻塞继续发送」的真实
/// pending→grant 周期才能完成——用真实双进程（而非同进程 mock）验证这条链路不会死锁、
/// 不会超时，且开启 `--enable-integrity-check`（BLAKE3 端到端校验）确保数据经过 credit
/// 流控后仍然完整无损（逐字节比对 + hash 双重验证）。
#[test]
fn test_remote_process_e2e_credit_window_large_file_no_deadlock() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let mut rel_paths = populate_src_dir(&src_dir);

    // 80MiB > 64MiB 默认 credit 窗口：全量传输过程中 Sender 必然至少耗尽一次窗口，
    // 依赖 Receiver 半窗批量 CreditGrant 才能继续，是本测试要验证的核心路径。
    const LARGE_FILE_SIZE: usize = 80 * 1024 * 1024;
    let large_rel = PathBuf::from("credit_window_large.bin");
    fs::write(src_dir.join(&large_rel), deterministic_bytes(LARGE_FILE_SIZE)).expect("write large src file");
    rel_paths.push(large_rel);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_credit_window_large_file")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--enable-integrity-check")
        .arg(&src_dir)
        .arg(&dest_dir);

    // 退出码必须为 0：若 credit 记账有死锁/泄漏 bug，Sender 会挂在 send() 里直到测试
    // 超时（assert_cmd 默认无超时，但外层 CI/harness 超时会让本用例明确失败，而不是
    // 静默通过），不会得到 success() 断言通过的结果。
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (receiver_stdout, receiver_stderr) = drain_output(&mut receiver_child);
    match receiver_status {
        Some(status) => assert!(
            status.success(),
            "Receiver 进程异常退出: {status:?}\nstdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
        ),
        None => panic!(
            "Receiver 进程在 {RECEIVER_EXIT_TIMEOUT:?} 内未自行退出（credit 记账若死锁会卡在这里），已强制 kill。\n\
             stdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
        ),
    }

    // 逐字节比对（含 80MiB 大文件）：credit 流控挂起/恢复的过程中数据没有丢失/错位/重复
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);
}

/// `--delete-target`（issue #23）默认关闭：目标端存在但源端已不存在的文件应保留。
#[test]
fn test_remote_process_e2e_without_delete_target_keeps_orphan() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);
    // 目标端预置一个源端不存在的孤儿文件
    let orphan_path = dest_dir.join("orphan.txt");
    fs::write(&orphan_path, b"stale file, should survive without --delete-target").expect("write orphan file");

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    // Sender 不带 --delete-target（默认 false）
    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_no_delete_target")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);
    assert!(orphan_path.exists(), "未传 --delete-target 时，目标端孤儿文件应保留");
}

/// `--delete-target`（issue #23）传入后：目标端存在但源端已不存在的文件应被清理，
/// 主路径覆盖 CLI flag → `SyncJobConfig.delete_target` → `SessionConfig` →
/// Receiver orphan-delete → `Classified{Deleted}` 全链路。
#[test]
fn test_remote_process_e2e_delete_target_removes_orphan() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);
    let orphan_path = dest_dir.join("orphan.txt");
    fs::write(&orphan_path, b"stale file, should be removed with --delete-target").expect("write orphan file");

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_delete_target")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--delete-target")
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);
    assert!(!orphan_path.exists(), "传入 --delete-target 后，目标端孤儿文件应被清理");
}

/// `--delete-target` 回归：目标端**已存在的未变更子目录**不是孤儿，不得被整树删除后
/// 重传（`page.subdirs` 若不在 `DestIndex` 登记 matched，预存子目录会被误判孤儿 →
/// `delete_dir_all` 整树误删，存在中断即数据丢失的窗口）。
///
/// 两轮真实双进程同步：第一轮（无 flag）播种 dest；第二轮 dest 加孤儿文件后带
/// `--delete-target`。用 inode 断言"未被删除重建"——若发生误删重传，子目录内
/// 文件的 inode 必然改变。
#[test]
fn test_remote_process_e2e_delete_target_preserves_existing_subdir() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");
    let bin_path = cargo_bin("terrasync");

    // ── 第一轮：无 --delete-target，把含子目录的数据集播种到 dest ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_preserve_subdir_seed")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "第一轮 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    // dest 加孤儿文件；用 dest **之外**的硬链接钉住 sub/b.txt 的 inode：
    // 若子目录被整树删除重传，旧 inode 被硬链接占住不会释放，新建文件必然分配
    // 不同 inode（直接比较删除前后的 inode 号会被 ext4 的 inode 立即复用糊掉）
    let orphan_path = dest_dir.join("orphan.txt");
    fs::write(&orphan_path, b"stale file, should be removed").expect("write orphan file");
    let keeper_path = tmp_path.join("keeper.link");
    fs::hard_link(dest_dir.join("sub/b.txt"), &keeper_path).expect("hardlink sub/b.txt");
    let ino_before = fs::metadata(&keeper_path).expect("stat keeper.link").ino();

    // ── 第二轮：带 --delete-target，预存子目录必须原样保留 ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    fs::remove_file(&cert_path).expect("remove old cert");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_e2e_preserve_subdir")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--delete-target")
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "第二轮 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);
    assert!(!orphan_path.exists(), "孤儿文件应被 --delete-target 清理");
    let ino_after = fs::metadata(dest_dir.join("sub/b.txt")).expect("stat sub/b.txt").ino();
    assert_eq!(
        ino_before, ino_after,
        "预存子目录被整树删除后重传（inode 改变）——子目录不是孤儿，不得删除"
    );
}

// ============================================================
// issue #26：补充 rsync-like 远端同步端到端测试矩阵
// ============================================================

/// 断言日志中出现 delta 能力协商成功的记录（`FeatureFlags { delta: true, ... }` 的
/// Debug 输出片段），作为"走 `DeltaTransferRequest` 而非降级全量"的非侵入式证据之一
/// （另一半证据见 `parse_changed_total`，两者结合见调用处注释）。
fn assert_delta_negotiated_in_log(log_path: &Path, who: &str) {
    let content = fs::read_to_string(log_path).unwrap_or_else(|e| panic!("读取 {who} 日志失败 {log_path:?}: {e}"));
    assert!(
        content.contains("delta: true"),
        "{who} 日志 {log_path:?} 中未见 delta 能力协商成功记录（FeatureFlags.delta=true），内容:\n{content}"
    );
}

/// 从 Sender stdout 打印的最终统计报表中解析 "Changed:" 行的 total 计数
/// （`StatisticConsumer::finalize()` 用 `println!("{}", self.stats)` 打印
/// `IncrementalStats::fmt`，格式形如 `"   ├─ Changed:        1 total | ..."`）。
fn parse_changed_total(stdout: &str) -> u64 {
    stdout
        .lines()
        .find(|l| l.contains("Changed:"))
        .and_then(|l| l.split_whitespace().nth(2))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// 真实 2 进程 delta sync e2e：第一轮播种 dest，第二轮改 src 文件中间一段字节
/// （保留首尾不变，让 delta 匹配有实际意义）后重跑，断言走 `DeltaTransferRequest`
/// 而非全量重传，且 dest 最终内容与 src 一致。
///
/// 不直接窥探协议内部状态（不侵入协议），改用两条独立、只读的非侵入式证据组合：
/// 1. Sender 日志握手行含 `delta: true`——证明本次连接的 delta 能力协商成功，
///    `receiver.rs::recv_file_list_and_data_phase` 在 `DestIndex::check()` 判定
///    `DeltaTransfer` 时必然发送真实 `DeltaTransferRequest`（协商失败才会降级为
///    `TransferRequest`，见该函数文档）。
/// 2. Sender stdout 最终报表 "Changed:" 行 total ≥ 1——证明确有一个条目被判定为
///    `TransferDecision::DeltaTransfer`（`DestIndex::check()` 对已存在但内容不同的
///    条目恒返回 `DeltaTransfer`，与文件大小是否变化无关，见 `message.rs::check`）。
/// 二者同时成立时，唯一自洽的解释就是该条目走了真实的 `DeltaTransferRequest` 消息。
#[test]
fn test_remote_process_e2e_delta_sync_transfers_changed_content() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let mut rel_paths = populate_src_dir(&src_dir);

    // 专用 delta 目标文件：8000 字节，块大小算法（sqrt(size).clamp(700, 128KiB)）会
    // 切出多个 block，足够体现"部分匹配 + 部分不匹配"的真实 delta 场景。
    const DELTA_FILE_SIZE: usize = 8000;
    let delta_rel = PathBuf::from("delta_target.bin");
    let original_content = deterministic_bytes(DELTA_FILE_SIZE);
    fs::write(src_dir.join(&delta_rel), &original_content).expect("write delta src file");
    rel_paths.push(delta_rel.clone());

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");
    let bin_path = cargo_bin("terrasync");

    // ── 第一轮：全量播种 dest ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_delta_seed")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "第一轮 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    // 修改 delta_target.bin 中间一段字节（保留首尾 3000/4800 字节不变），
    // 内容变化会连带更新 mtime，触发 DestIndex::check() 的 data_check 判定为不匹配。
    let mut modified_content = original_content.clone();
    for byte in &mut modified_content[3000..3200] {
        *byte = 0xAA;
    }
    fs::write(src_dir.join("delta_target.bin"), &modified_content).expect("modify delta src file");

    // ── 第二轮：重跑同步，delta_target.bin 应走 delta 传输 ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    fs::remove_file(&cert_path).expect("remove old cert");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_delta_verify")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    let assert = sender_cmd.assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).into_owned();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "第二轮 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    // 内容一致性：delta 重建结果必须与源端修改后的内容逐字节相同
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    // 非侵入式证据 1：delta 能力协商成功
    let sender_log = tmp_path.join("logs").join("remote_delta_verify").join("app.log");
    assert_delta_negotiated_in_log(&sender_log, "Sender");

    // 非侵入式证据 2：Sender 报表记录了一次 Changed（即 delta_target.bin 走了
    // DeltaTransfer 分类，而非 Skip）
    let changed_total = parse_changed_total(&stdout);
    assert!(
        changed_total >= 1,
        "Sender 报表 Changed 计数应 ≥1（delta_target.bin 内容已变更），实际 stdout:\n{stdout}"
    );
}

/// 断言 Receiver 日志中出现 size 门槛降级记录（`receiver.rs::recv_file_list_and_data_phase`
/// 新增的 `exceeds delta_size_threshold` info 行），作为"该文件被降级为全量传输而非真实
/// `DeltaTransferRequest`"的非侵入式证据（issue #54 阶段 0）。
fn assert_delta_size_threshold_downgrade_in_log(log_path: &Path) {
    let content = fs::read_to_string(log_path).unwrap_or_else(|e| panic!("读取 Receiver 日志失败 {log_path:?}: {e}"));
    assert!(
        content.contains("exceeds delta_size_threshold"),
        "Receiver 日志 {log_path:?} 中未见 size 门槛降级记录，内容:\n{content}"
    );
}

/// 真实 2 进程 `--delta-size-threshold` e2e：与
/// `test_remote_process_e2e_delta_sync_transfers_changed_content` 同样的两轮播种+改内容
/// 场景，但第二轮 Sender 传入 `--delta-size-threshold`（小于被修改文件的大小），断言该文件
/// 走全量降级而非真实 delta 传输，且 dest 最终内容仍与 src 一致（阶段 0 主路径：门槛降级
/// 不影响传输正确性）。
///
/// 非侵入式证据：Receiver 日志出现 size 门槛降级记录（`assert_delta_size_threshold_downgrade_in_log`），
/// 证明该文件确实因 size 超阈值被降级为全量传输，而不是走 `DeltaTransferRequest`。
#[test]
fn test_remote_process_e2e_delta_size_threshold_downgrades_to_full() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let mut rel_paths = populate_src_dir(&src_dir);

    // 8000 字节的目标文件，配合 4KiB 阈值必然超阈值触发降级
    const DELTA_FILE_SIZE: usize = 8000;
    let delta_rel = PathBuf::from("delta_target.bin");
    let original_content = deterministic_bytes(DELTA_FILE_SIZE);
    fs::write(src_dir.join(&delta_rel), &original_content).expect("write delta src file");
    rel_paths.push(delta_rel.clone());

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");
    let bin_path = cargo_bin("terrasync");

    // ── 第一轮：全量播种 dest（不带 --delta-size-threshold，与阈值降级测试无关） ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_delta_threshold_seed")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "第一轮 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    // 修改 delta_target.bin 中间一段字节，触发 DestIndex::check() 判定为 DeltaTransfer
    let mut modified_content = original_content.clone();
    for byte in &mut modified_content[3000..3200] {
        *byte = 0xAA;
    }
    fs::write(src_dir.join("delta_target.bin"), &modified_content).expect("modify delta src file");

    // ── 第二轮：带 --delta-size-threshold 4KiB（< 8000 字节文件）重跑，应降级为全量 ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    fs::remove_file(&cert_path).expect("remove old cert");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_delta_threshold_verify")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--delta-size-threshold")
        .arg("4KiB")
        .arg(&src_dir)
        .arg(&dest_dir);
    let assert = sender_cmd.assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).into_owned();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "第二轮 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    // 内容一致性：即使降级为全量传输，重建结果也必须与源端修改后的内容逐字节相同
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    // 非侵入式证据 1：delta 能力本身仍协商成功（证明降级是 size 门槛而非能力协商失败）
    let sender_log = tmp_path
        .join("logs")
        .join("remote_delta_threshold_verify")
        .join("app.log");
    assert_delta_negotiated_in_log(&sender_log, "Sender");

    // 非侵入式证据 2：Sender 报表记录了一次 Changed（该条目仍按 DeltaTransfer 分类统计）
    let changed_total = parse_changed_total(&stdout);
    assert!(
        changed_total >= 1,
        "Sender 报表 Changed 计数应 ≥1（delta_target.bin 内容已变更），实际 stdout:\n{stdout}"
    );

    // 非侵入式证据 3：Receiver 日志出现 size 门槛降级记录，证明确实走了全量降级而非真实 delta
    let receiver_log = tmp_path.join("logs").join("app.log");
    assert_delta_size_threshold_downgrade_in_log(&receiver_log);
}

/// symlink 真实 2 进程全量同步 e2e：`populate_src_dir` 从不建 symlink（8 个既有 e2e
/// 均未覆盖该路径，见 issue #26 triage），单独构造专属数据集验证符号链接经真实
/// QUIC 连接走 `CreateSymlink` 完整落地——链接类型 + 目标路径字符串 + 可解引用内容。
#[test]
fn test_remote_process_e2e_symlink_synced() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");

    fs::write(src_dir.join("target.txt"), b"symlink target content\n").expect("write target file");
    symlink("target.txt", src_dir.join("link.txt")).expect("create src symlink");

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_symlink_e2e")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    // 目标文件内容一致
    assert_eq!(
        fs::read(src_dir.join("target.txt")).expect("read src target"),
        fs::read(dest_dir.join("target.txt")).expect("read dest target"),
    );

    // dest 侧确实是符号链接（而非被解引用复制成普通文件）
    let dest_link_meta = fs::symlink_metadata(dest_dir.join("link.txt")).expect("stat dest link.txt");
    assert!(dest_link_meta.file_type().is_symlink(), "dest link.txt 应为符号链接");

    // 链接目标字符串与 src 一致
    assert_eq!(
        fs::read_link(src_dir.join("link.txt")).expect("read src link target"),
        fs::read_link(dest_dir.join("link.txt")).expect("read dest link target"),
    );

    // 解引用读取 dest 的 symlink 也能拿到正确内容
    assert_eq!(
        fs::read(dest_dir.join("link.txt")).expect("read dest link.txt (deref)"),
        b"symlink target content\n"
    );
}

/// resume e2e：同步中途 kill Sender，验证被中断的大文件确实未完整落盘（避免"其实第一轮
/// 就传完了"的假阳性），随后用相同 src/dest 重跑，断言最终 dest 与 src 完全一致。
///
/// 用 `--qos` 限速制造足够宽的中断窗口（内容参考
/// `crates/cli/src/commands_enum.rs` 的 `--qos` 文档 + `parse_bandwidth_string`
/// 支持到 KiB/s），避免 sleep+kill 的时序竞争导致测试 flaky。**必须同时用
/// `--block-size` 把单次读取粒度调小**：data-mover 的
/// `QosManager::acquire_bandwidth` 在单次请求的 cell 数超过 burst 容量时，
/// `governor::until_n_ready` 会立即返回 `InsufficientCapacity` 且被静默忽略
/// （见 `qos.rs::acquire_bandwidth` 的 `let _ = limiter.until_n_ready(n).await;`），
/// 等效于该次请求完全不限速；默认 `block_size=2MiB` 换算成的 cell 数远超小带宽下的
/// burst 容量，会导致限速形同虚设、大文件几乎瞬间传完，因此显式收窄
/// `--block-size` 让单次 acquire 的 cell 数落在 burst 容量以内。
///
/// 双进程模式的字节级续传当前未接线（`disk_commit.rs::FileBegin` 硬编码
/// `resume_prepare(..., false)`，归 #25 处置），故重跑是整文件重传而非"断点续传"，
/// 但这正是本 issue 验收标准要求的范围："中断重跑后最终收敛"。
#[test]
fn test_remote_process_e2e_resume_after_sender_kill() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let mut rel_paths = populate_src_dir(&src_dir);

    // 6MiB，配合 64KiB/s 限速 + 64KiB block-size（峰值倍率默认 2.0，burst=128KiB），
    // 1s 内最多传约 128KiB，远不足以传完整个文件，中断窗口足够宽，不依赖精确时序。
    const LARGE_FILE_SIZE: usize = 6 * 1024 * 1024;
    let large_rel = PathBuf::from("resume_large.bin");
    fs::write(src_dir.join(&large_rel), deterministic_bytes(LARGE_FILE_SIZE)).expect("write large src file");
    rel_paths.push(large_rel.clone());

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");
    let bin_path = cargo_bin("terrasync");

    // ── 第一轮：限速 + 中途 kill Sender ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
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

    let mut sender_child = StdCommand::new(&bin_path)
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_resume_interrupted")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--qos")
        .arg("64KiB/s")
        .arg("--block-size")
        .arg("64KiB")
        .arg(&src_dir)
        .arg(&dest_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sender process");

    std::thread::sleep(Duration::from_secs(1));

    // 强制中断双方：不等待优雅关闭（那是 test_remote_process_e2e_receiver_killed_* 系列
    // 关注的场景），这里只需要制造"中断后残留不完整状态"的前置条件。
    let _ = sender_child.kill();
    let _ = sender_child.wait();
    let _ = receiver_child.kill();
    let _ = receiver_child.wait();

    // 确认真的被中断了：大文件走 .terrasync-part 临时文件流式写入，只有完整收到并通过
    // hash 校验后才会原子 rename 成最终文件名（见 disk_commit.rs::finalize_file），
    // 限速下 1s 远不够传完 6MiB，最终文件名此时必然不存在。
    assert!(
        !dest_dir.join(&large_rel).exists(),
        "大文件在被中断的第一轮不应完整落盘——测试前提不成立（可能是限速/时序假设有误）"
    );

    // ── 第二轮：相同 src/dest 重跑（不限速，加快测试），应收敛到与 src 完全一致 ──
    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    fs::remove_file(&cert_path).expect("remove old cert");
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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_resume_rerun")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "重跑 Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );

    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);
}

/// transport 故障注入 e2e：同步中途 kill Receiver，Sender 必须在有限时间内检测到连接
/// 异常并以非零码退出，不能无限期 hang（`quic::mux::reader_loop` 读流出错/EOF 时各自
/// drop 手上的 `tx` clone，四条都 drop 后 `sender.recv()` 返回 `None`；Sender 主循环
/// 据此判定连接已断，返回 Err 而非死等）。
///
/// 沿用 resume 测试同样的限速手法制造中断窗口，避免"其实传完了才杀"的假阳性。
#[test]
fn test_remote_process_e2e_receiver_killed_sender_exits_nonzero() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    populate_src_dir(&src_dir);

    const LARGE_FILE_SIZE: usize = 6 * 1024 * 1024;
    fs::write(src_dir.join("large.bin"), deterministic_bytes(LARGE_FILE_SIZE)).expect("write large src file");

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");
    let bin_path = cargo_bin("terrasync");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
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

    let mut sender_child = StdCommand::new(&bin_path)
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_receiver_killed")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg("--qos")
        .arg("64KiB/s")
        .arg("--block-size")
        .arg("64KiB")
        .arg(&src_dir)
        .arg(&dest_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sender process");

    std::thread::sleep(Duration::from_secs(1));

    // 中途强杀 Receiver：不给它优雅关闭连接的机会
    let _ = receiver_child.kill();
    let _ = receiver_child.wait();

    let sender_status = wait_for_child_exit(&mut sender_child, SENDER_EXIT_TIMEOUT);
    let (sout, serr) = drain_output(&mut sender_child);
    match sender_status {
        Some(status) => assert!(
            !status.success(),
            "Receiver 被杀后 Sender 应以非零码退出，实际: {status:?}\nstdout:\n{sout}\nstderr:\n{serr}"
        ),
        None => panic!(
            "Sender 在 Receiver 被杀后 {SENDER_EXIT_TIMEOUT:?} 内未自行退出（疑似 hang），已强制 kill。\n\
             stdout:\n{sout}\nstderr:\n{serr}"
        ),
    }
}

/// uid/gid 保留 e2e：全量同步后 dest 各文件的 uid/gid 应与 src 一致。
///
/// 非 root 开发环境下无法 `chown` 出一个与当前运行用户不同的 uid/gid 去验证"跨用户
/// 也能正确复制"，因此本测试断言退化为"dest uid/gid == src uid/gid"（两者都等于运行
/// 该测试进程的 euid/egid）。即便如此，仍是有意义的真实断言：它验证的是
/// `disk_commit.rs::finalize_file`／`DiskCommitMsg::CreateDir` 里
/// `dest.set_entry_metadata(&entry)` 用的 `entry.uid`/`entry.gid` 确实来自源端 scan、
/// 经 wire 传输后被正确应用到目标端（而不是被链路上的某一跳意外置零/丢弃/固定为其它
/// 值）——这条链路一旦回归就会在此处失败。真正的"跨用户 chown"需要 root/CAP_CHOWN
/// 权限的专用环境，超出本地开发环境可执行范围。
#[test]
fn test_remote_process_e2e_uid_gid_preserved() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = AssertCommand::new(&bin_path);
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_uid_gid_e2e")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);
    sender_cmd.assert().success();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (rout, rerr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "Receiver 异常退出: {receiver_status:?}\nstdout:\n{rout}\nstderr:\n{rerr}"
    );
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);

    for rel in &rel_paths {
        let src_meta = fs::metadata(src_dir.join(rel)).unwrap_or_else(|e| panic!("stat src {rel:?}: {e}"));
        let dest_meta = fs::metadata(dest_dir.join(rel)).unwrap_or_else(|e| panic!("stat dest {rel:?}: {e}"));
        assert_eq!(
            src_meta.uid(),
            dest_meta.uid(),
            "dest 文件 {rel:?} uid 应与 src 一致（非 root 环境下两者都应等于运行进程 uid）"
        );
        assert_eq!(
            src_meta.gid(),
            dest_meta.gid(),
            "dest 文件 {rel:?} gid 应与 src 一致（非 root 环境下两者都应等于运行进程 gid）"
        );
    }
}

// ============================================================
// issue #57：双进程退出码改报表驱动（entry 级失败 exit 0）+ 报表错误统计补齐
// ============================================================

/// 从 Sender stdout 打印的最终统计报表 `ERROR STATISTICS` 表格中解析 total 行计数
/// （`fmt_error_stats` 用 `"    │ {:^12} │ {:>8} │"` 打印 total 行，是全文件唯一同时
/// 出现 `│` 与字面量 "total" 的行；`ErrorStats::is_empty()` 为 true 时整个表格不打印，
/// 此时返回 0）。
fn parse_error_stats_total(stdout: &str) -> u64 {
    stdout
        .lines()
        .find(|l| l.contains('│') && l.contains("total"))
        .and_then(|l| {
            l.split_whitespace()
                .filter(|s| s.chars().all(|c| c.is_ascii_digit()))
                .next_back()
        })
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// 真实 2 进程部分失败 e2e：源目录一个文件内容不可读（`chmod 0o000`，非 root 环境下
/// 确定性触发 Sender 自检读失败——`walkdir` 仅需目录读权限即可枚举到该文件的元数据，
/// 真正失败的是后续的内容读取），其余文件正常。断言：
/// 1. Sender 进程 exit 0（issue #57：entry 级失败不再影响退出码，报表驱动）；
/// 2. Sender stdout 终态报表 `ERROR STATISTICS` total 行为 1（报表如实反映该失败，
///    不再是此前的恒 0/仅退出码可见）；
/// 3. 其余文件正常同步、内容一致；不可读文件未出现在 dest（该 entry 确实失败了，
///    不是被静默跳过又假装成功）。
#[test]
fn test_remote_process_e2e_partial_failure_exit_zero_with_nonzero_report() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let tmp_path = tmp.path();

    let src_dir = tmp_path.join("src");
    let dest_dir = tmp_path.join("dest");
    fs::create_dir_all(&src_dir).expect("create src dir");
    fs::create_dir_all(&dest_dir).expect("create dest dir");
    let rel_paths = populate_src_dir(&src_dir);

    let unreadable_path = src_dir.join("unreadable.bin");
    fs::write(&unreadable_path, b"content Sender will fail to read").expect("write unreadable file");
    fs::set_permissions(&unreadable_path, fs::Permissions::from_mode(0o000)).expect("chmod 0o000");

    let cert_path = tmp_path.join("server.crt");
    let config_path = tmp_path.join("config.toml");
    fs::write(&config_path, "[database]\nenabled = false\n").expect("write config");
    let fake_manifest_dir = tmp_path.join("fake_manifest");

    let port = free_loopback_port();
    let listen_addr = format!("127.0.0.1:{port}");
    let bin_path = cargo_bin("terrasync");

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

    let mut sender_cmd = if fs::metadata(tmp_path).expect("stat temporary root").uid() == 0 {
        let mut command = AssertCommand::new("setpriv");
        command
            .arg("--bounding-set=-dac_override,-dac_read_search")
            .arg(&bin_path);
        command
    } else {
        AssertCommand::new(&bin_path)
    };
    sender_cmd
        .env("CARGO_MANIFEST_DIR", &fake_manifest_dir)
        .current_dir(tmp_path)
        .arg("-c")
        .arg(&config_path)
        .arg("sync")
        .arg("--id")
        .arg("remote_partial_failure_e2e")
        .arg("--remote")
        .arg(&listen_addr)
        .arg("--tls-server-cert")
        .arg(&cert_path)
        .arg(&src_dir)
        .arg(&dest_dir);

    // 关键断言 1：即便有 entry 级失败，Sender 进程仍应 exit 0（报表驱动，issue #57）
    let assert = sender_cmd.assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).into_owned();

    let receiver_status = wait_for_child_exit(&mut receiver_child, RECEIVER_EXIT_TIMEOUT);
    let (receiver_stdout, receiver_stderr) = drain_output(&mut receiver_child);
    assert!(
        matches!(receiver_status, Some(s) if s.success()),
        "Receiver 进程异常退出: {receiver_status:?}\nstdout:\n{receiver_stdout}\nstderr:\n{receiver_stderr}"
    );

    // 关键断言 2：其余文件正常同步
    assert_dest_matches_src(&src_dir, &dest_dir, &rel_paths);
    // 不可读文件确实失败了（不是被静默跳过又假装成功）
    assert!(
        !dest_dir.join("unreadable.bin").exists(),
        "读取失败的文件不应出现在 dest"
    );

    // 关键断言 3：终态报表如实反映该失败（此前 Sender 从不构造 StorageEntryMessage::Error，
    // ERROR STATISTICS 与 HTTP 回调 error_count 恒为 0，唯一可见出口只有退出码；
    // 本次改动后报表才是失败的机器可见来源）
    let error_total = parse_error_stats_total(&stdout);
    assert_eq!(
        error_total, 1,
        "Sender 报表 ERROR STATISTICS total 应为 1（1 个文件读取失败），实际 stdout:\n{stdout}"
    );
}
