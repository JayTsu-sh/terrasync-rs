//! QUIC transport roundtrip 集成测试
//!
//! 运行方式：cargo test -p transport --features quic

#![cfg(feature = "quic")]
// 集成测试代码：豁免 unwrap/expect 与所有 pedantic 风格 lint
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::pedantic)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use data_mover::{DataChunk, EntryEnum, NASEntry};
use tokio::sync::oneshot;
use transport::message::{
    FeatureFlags, HandshakeResult, HashAlgorithm, ProgressSnapshot, ProtocolHandshake, ReceiverMsg, SenderMsg,
    SessionConfig, TransferDecision,
};
use transport::quic;
use transport::traits::SenderTransport;

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn create_server_endpoint() -> (quinn::Endpoint, SocketAddr) {
    let (certs, key) = quic::cert::generate_self_signed(&[]).unwrap();
    let server_config = quic::cert::build_server_config(certs, key).unwrap();
    let endpoint = quinn::Endpoint::server(server_config, "[::1]:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    (endpoint, addr)
}

/// 测试基本的 TransferDone / AllDone roundtrip
#[tokio::test]
async fn test_quic_transfer_done_roundtrip() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();

    // 用 oneshot channel 让 receiver 等 sender 读完回复后才退出
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        assert!(
            matches!(msg, Some(SenderMsg::TransferDone)),
            "Expected TransferDone, got {:?}",
            msg
        );

        quic::framing::write_msg(&mut send, &ReceiverMsg::AllDone)
            .await
            .unwrap();

        // 等 sender 读完回复后再退出（保持 conn 活着）
        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender.send(SenderMsg::TransferDone).await.unwrap();

    let reply = sender.recv().await;
    assert!(
        matches!(reply, Some(ReceiverMsg::AllDone)),
        "Expected AllDone, got {:?}",
        reply
    );

    // 通知 receiver 可以退出了
    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 测试 TLS 证书验证：connect() 传入服务端证书，覆盖 build_client_config_with_ca 路径
///
/// 服务端手动生成证书并保留 cert_der，Sender 侧通过 Some(cert) 触发验证逻辑。
/// 若证书不匹配或 CN/SAN 不符，TLS 握手将失败（连接建立阶段即报错）。
#[tokio::test]
async fn test_quic_tls_cert_verified_roundtrip() {
    install_crypto_provider();

    // 手动生成证书以便捕获 cert_der（与 quic::accept 内部逻辑相同）
    let (certs, key) = quic::cert::generate_self_signed(&[]).unwrap();
    let cert_der = certs[0].as_ref().to_vec();
    let server_config = quic::cert::build_server_config(certs, key).unwrap();
    let endpoint = quinn::Endpoint::server(server_config, "[::1]:0".parse().unwrap()).unwrap();
    let server_addr = endpoint.local_addr().unwrap();

    let (done_tx, done_rx) = oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        let incoming = endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        assert!(
            matches!(msg, Some(SenderMsg::TransferDone)),
            "Expected TransferDone, got {:?}",
            msg
        );

        quic::framing::write_msg(&mut send, &ReceiverMsg::AllDone)
            .await
            .unwrap();
        let _ = done_rx.await;
    });

    // 关键：传入 server_cert → 触发 build_client_config_with_ca，验证服务端身份
    let server_cert = quic::CertificateDer::from(cert_der);
    let sender = quic::connect(server_addr, "localhost", Some(server_cert))
        .await
        .unwrap();
    sender.send(SenderMsg::TransferDone).await.unwrap();

    let reply = sender.recv().await;
    assert!(
        matches!(reply, Some(ReceiverMsg::AllDone)),
        "Expected AllDone, got {:?}",
        reply
    );

    let _ = done_tx.send(());
    server_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 测试 SessionConfig 序列化/反序列化
#[tokio::test]
async fn test_quic_session_config_roundtrip() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        if let Some(SenderMsg::SessionConfig(config)) = msg {
            assert_eq!(config.qos.as_deref(), Some("1GiB/s"));
            assert!(config.enable_integrity_check);
            assert!(!config.enable_acl);
            assert!(config.delete_target);
        } else {
            panic!("Expected SessionConfig, got {:?}", msg);
        }

        quic::framing::write_msg(&mut send, &ReceiverMsg::AllDone)
            .await
            .unwrap();
        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender
        .send(SenderMsg::SessionConfig(SessionConfig {
            src_path: "/tmp/test".to_string(),
            qos: Some("1GiB/s".to_string()),
            peak_qos_rate: 1.5,
            iops: Some(10000),
            enable_integrity_check: true,
            enable_acl: false,
            is_source_reserved: true,
            block_size: Some("4MiB".to_string()),
            delete_target: true,
            delta_size_threshold: None,
        }))
        .await
        .unwrap();

    let reply = sender.recv().await;
    assert!(
        matches!(reply, Some(ReceiverMsg::AllDone)),
        "Expected AllDone, got {:?}",
        reply
    );

    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 握手兼容性测试：相同版本 → 协商成功，后续 SessionConfig 正常收发
#[tokio::test]
async fn test_quic_handshake_compatible_versions_roundtrip() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        // 阶段 -1：握手
        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        let remote_handshake = match msg {
            Some(SenderMsg::Handshake(h)) => h,
            other => panic!("Expected Handshake, got {:?}", other),
        };
        let result = ProtocolHandshake::current().negotiate(&remote_handshake);
        assert!(
            matches!(result, HandshakeResult::Accepted { .. }),
            "Expected Accepted, got {:?}",
            result
        );
        quic::framing::write_msg(&mut send, &ReceiverMsg::HandshakeAck(result))
            .await
            .unwrap();

        // 阶段 0：握手通过后才应收到 SessionConfig
        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        assert!(
            matches!(msg, Some(SenderMsg::SessionConfig(_))),
            "Expected SessionConfig, got {:?}",
            msg
        );

        quic::framing::write_msg(&mut send, &ReceiverMsg::AllDone)
            .await
            .unwrap();
        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender
        .send(SenderMsg::Handshake(ProtocolHandshake::current()))
        .await
        .unwrap();

    match sender.recv().await {
        Some(ReceiverMsg::HandshakeAck(HandshakeResult::Accepted { features })) => {
            assert!(features.delta, "双方版本一致，delta 能力应协商成功");
        }
        other => panic!("Expected HandshakeAck(Accepted), got {:?}", other),
    }

    // 握手通过后 Sender 才继续发送 SessionConfig（模拟真实 Sender 流程）
    sender
        .send(SenderMsg::SessionConfig(SessionConfig {
            src_path: "/tmp/test".to_string(),
            qos: None,
            peak_qos_rate: 1.0,
            iops: None,
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            block_size: None,
            delete_target: false,
            delta_size_threshold: None,
        }))
        .await
        .unwrap();

    let reply = sender.recv().await;
    assert!(
        matches!(reply, Some(ReceiverMsg::AllDone)),
        "Expected AllDone, got {:?}",
        reply
    );

    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 握手兼容性测试：版本不兼容 → Receiver 回 Rejected，Sender 不得发送任何 Phase 1 消息
/// （`FilePage` / `CopyEntry` / `SessionConfig` 等），拒绝必须发生在 Phase 1 之前。
#[tokio::test]
async fn test_quic_handshake_incompatible_version_rejected_before_phase1() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        let remote_handshake = match msg {
            Some(SenderMsg::Handshake(h)) => h,
            other => panic!("Expected Handshake, got {:?}", other),
        };
        let result = ProtocolHandshake::current().negotiate(&remote_handshake);
        assert!(
            matches!(result, HandshakeResult::Rejected { .. }),
            "Expected Rejected, got {:?}",
            result
        );
        quic::framing::write_msg(&mut send, &ReceiverMsg::HandshakeAck(result))
            .await
            .unwrap();

        // 遵守协议的 Sender 收到 Rejected 后不会再发送任何消息；
        // 带超时读取，确认 Phase 1（SessionConfig/FilePage 等）未被触发。
        let next = tokio::time::timeout(
            Duration::from_millis(200),
            quic::framing::read_msg::<SenderMsg>(&mut recv),
        )
        .await;
        match next {
            Err(_) => {}       // 超时：符合预期，Sender 未再发送任何消息
            Ok(Ok(None)) => {} // stream 已结束：符合预期
            Ok(other) => panic!("Sender 不应在 Rejected 后发送任何 Phase 1 消息，收到: {:?}", other),
        }

        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();

    // 构造一个版本过旧的握手信息（低于 Receiver 的 min_supported_version），触发拒绝
    let incompatible_handshake = ProtocolHandshake {
        protocol_version: 0,
        min_supported_version: 0,
        features: FeatureFlags::current(),
        hash_algorithm: HashAlgorithm::Blake3,
    };
    sender.send(SenderMsg::Handshake(incompatible_handshake)).await.unwrap();

    match sender.recv().await {
        Some(ReceiverMsg::HandshakeAck(HandshakeResult::Rejected { reason })) => {
            assert!(!reason.is_empty(), "拒绝原因不应为空");
        }
        other => panic!("Expected HandshakeAck(Rejected), got {:?}", other),
    }

    // Sender 遵守协议：收到 Rejected 后不再发送 SessionConfig / FilePage，直接结束会话
    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// Token 鉴权测试：正确 token → `AuthResult{ok:true}` → 继续收发 `SessionConfig`
#[tokio::test]
async fn test_quic_auth_success_then_session_config_roundtrip() {
    install_crypto_provider();

    const EXPECTED_TOKEN: &str = "s3cr3t-token";

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        // 阶段 -0.5：Auth
        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        match msg {
            Some(SenderMsg::Auth { token }) => {
                assert_eq!(token, EXPECTED_TOKEN, "token 应与 Sender 发送的一致");
                quic::framing::write_msg(&mut send, &ReceiverMsg::AuthResult { ok: true, reason: None })
                    .await
                    .unwrap();
            }
            other => panic!("Expected Auth, got {:?}", other),
        }

        // 鉴权通过后才应收到 SessionConfig
        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        assert!(
            matches!(msg, Some(SenderMsg::SessionConfig(_))),
            "Expected SessionConfig, got {:?}",
            msg
        );

        quic::framing::write_msg(&mut send, &ReceiverMsg::AllDone)
            .await
            .unwrap();
        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender
        .send(SenderMsg::Auth {
            token: EXPECTED_TOKEN.to_string(),
        })
        .await
        .unwrap();

    match sender.recv().await {
        Some(ReceiverMsg::AuthResult { ok: true, .. }) => {}
        other => panic!("Expected AuthResult(ok:true), got {:?}", other),
    }

    sender
        .send(SenderMsg::SessionConfig(SessionConfig {
            src_path: "/tmp/test".to_string(),
            qos: None,
            peak_qos_rate: 1.0,
            iops: None,
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            block_size: None,
            delete_target: false,
            delta_size_threshold: None,
        }))
        .await
        .unwrap();

    let reply = sender.recv().await;
    assert!(
        matches!(reply, Some(ReceiverMsg::AllDone)),
        "Expected AllDone, got {:?}",
        reply
    );

    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// Token 鉴权测试：错误 token → `AuthResult{ok:false}` → Sender 不得发送 `SessionConfig`
#[tokio::test]
async fn test_quic_auth_failure_rejected_before_session_config() {
    install_crypto_provider();

    const EXPECTED_TOKEN: &str = "s3cr3t-token";

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        match msg {
            Some(SenderMsg::Auth { token }) => {
                assert_ne!(token, EXPECTED_TOKEN, "本测试模拟 token 不匹配的场景");
                quic::framing::write_msg(
                    &mut send,
                    &ReceiverMsg::AuthResult {
                        ok: false,
                        reason: Some("invalid token".to_string()),
                    },
                )
                .await
                .unwrap();
            }
            other => panic!("Expected Auth, got {:?}", other),
        }

        // 遵守协议的 Sender 收到失败结果后不会再发送任何消息（含 SessionConfig）
        let next = tokio::time::timeout(
            Duration::from_millis(200),
            quic::framing::read_msg::<SenderMsg>(&mut recv),
        )
        .await;
        match next {
            Err(_) => {}       // 超时：符合预期，Sender 未再发送任何消息
            Ok(Ok(None)) => {} // stream 已结束：符合预期
            Ok(other) => panic!("Sender 不应在鉴权失败后发送任何消息，收到: {:?}", other),
        }

        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender
        .send(SenderMsg::Auth {
            token: "wrong-token".to_string(),
        })
        .await
        .unwrap();

    match sender.recv().await {
        Some(ReceiverMsg::AuthResult { ok: false, reason }) => {
            assert!(reason.is_some(), "拒绝原因不应为空");
        }
        other => panic!("Expected AuthResult(ok:false), got {:?}", other),
    }

    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 握手兼容性测试：能力缺失 → 协商结果降级
///
/// Receiver 本地不支持 delta（如旧版本部署）时，即使 Sender 支持，
/// 协商后的能力交集也应为 `delta: false`，Receiver 侧据此走全量传输而非 delta。
#[tokio::test]
async fn test_quic_handshake_missing_delta_capability_downgrades() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        let remote_handshake = match msg {
            Some(SenderMsg::Handshake(h)) => h,
            other => panic!("Expected Handshake, got {:?}", other),
        };

        // Receiver 本地能力集：不支持 delta
        let local_handshake = ProtocolHandshake {
            features: FeatureFlags {
                delta: false,
                ..FeatureFlags::current()
            },
            ..ProtocolHandshake::current()
        };
        let result = local_handshake.negotiate(&remote_handshake);
        quic::framing::write_msg(&mut send, &ReceiverMsg::HandshakeAck(result))
            .await
            .unwrap();

        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    // Sender 声明支持完整能力集（含 delta）
    sender
        .send(SenderMsg::Handshake(ProtocolHandshake::current()))
        .await
        .unwrap();

    match sender.recv().await {
        Some(ReceiverMsg::HandshakeAck(HandshakeResult::Accepted { features })) => {
            assert!(!features.delta, "Receiver 不支持 delta 时协商结果应降级为 false");
            assert!(features.acl, "acl 双方均支持，协商结果应保持 true");
        }
        other => panic!("Expected HandshakeAck(Accepted), got {:?}", other),
    }

    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 生成 `len` 字节的不可压缩数据（xorshift64 伪随机），避免 `framing::write_msg` 的 zstd
/// 压缩把测试数据压没了导致实际上线字节数远小于预期，测试前提不成立
fn incompressible_bytes(len: usize, seed: u64) -> Bytes {
    let mut buf = Vec::with_capacity(len);
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        buf.push((state & 0xFF) as u8);
    }
    Bytes::from(buf)
}

/// 构造一个仅用于测试的最小 `NASEntry`（字段值无实际语义，只满足 `FileData` 的类型要求）
fn dummy_file_entry(name: &str) -> Arc<EntryEnum> {
    Arc::new(EntryEnum::NAS(NASEntry {
        name: name.to_string(),
        relative_path: PathBuf::from(name),
        extension: None,
        is_dir: false,
        size: 0,
        atime: 0,
        ctime: 0,
        mtime: 0,
        mode: 0o644,
        is_symlink: false,
        hard_links: None,
        uid: None,
        gid: None,
        ino: None,
        file_handle: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    }))
}

/// #23 协议扩展序列化往返：`ReceiverMsg::TransferRequest{ndx, decision}` 与新增的
/// `Classified{entry, decision}`（含新 variant `TransferDecision::Deleted`）能在真实
/// QUIC 连接上完整往返（编码 → 传输 → 解码），验证 `TransferDecision` 新增的
/// `Serialize`/`Deserialize` 派生与 `ReceiverMsg` 新字段/新消息的 bincode wire 兼容性。
///
/// 仿 `test_quic_transfer_done_roundtrip`：QUIC 流只有发起方实际写入数据后对端才能
/// `accept_bi()` 到，先由 Sender 发一条 `Handshake`（路由到 `Control`）触发该 stream
/// 被对端感知；测试用的手工 harness 在同一条物理 stream 上写回本测试关心的两条
/// `ReceiverMsg`（`sender.recv()` 走完整 mux fan-in，按物理 stream 转发、不校验消息
/// 与 stream 语义分类是否匹配，序列化正确性与路由分类是两件独立的事，此处只验证前者）。
#[tokio::test]
async fn test_quic_classification_messages_roundtrip() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let msg: Option<SenderMsg> = quic::framing::read_msg(&mut recv).await.ok().flatten();
        assert!(
            matches!(msg, Some(SenderMsg::Handshake(_))),
            "Expected Handshake, got {:?}",
            msg
        );

        // TransferRequest{ndx, decision}：New/Changed 复用的既有请求消息新增字段
        quic::framing::write_msg(
            &mut send,
            &ReceiverMsg::TransferRequest {
                ndx: 42,
                decision: TransferDecision::DeltaTransfer,
            },
        )
        .await
        .unwrap();

        // Classified{entry, decision: Deleted}：新增消息 + TransferDecision 新增 variant
        let entry = dummy_file_entry("orphan.bin");
        quic::framing::write_msg(
            &mut send,
            &ReceiverMsg::Classified {
                entry,
                decision: TransferDecision::Deleted,
            },
        )
        .await
        .unwrap();

        let _ = done_rx.await;
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender
        .send(SenderMsg::Handshake(ProtocolHandshake::current()))
        .await
        .unwrap();

    match sender.recv().await {
        Some(ReceiverMsg::TransferRequest {
            ndx: 42,
            decision: TransferDecision::DeltaTransfer,
        }) => {}
        other => panic!(
            "Expected TransferRequest{{ndx:42, decision:DeltaTransfer}}, got {:?}",
            other
        ),
    }

    match sender.recv().await {
        Some(ReceiverMsg::Classified {
            entry,
            decision: TransferDecision::Deleted,
        }) => {
            assert_eq!(entry.get_relative_path(), PathBuf::from("orphan.bin"));
        }
        other => panic!("Expected Classified{{decision:Deleted}}, got {:?}", other),
    }

    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 多路复用专项测试：大文件 `Data` stream 传输期间对端未读取（模拟大文件占满该 stream），
/// `AckProgress` stream 的消息仍应能被及时 `recv()` 到，不受 `Data` stream 阻塞影响。
///
/// 验证方式：
/// 1. Sender 持续往 `Data` stream 写 `FileData`，接收端（手工搭建，模拟"忙于写盘、来不及
///    读取"）故意不读取该 stream —— quinn-proto 0.11.14 `TransportConfig::default()` 的
///    per-stream 接收窗口是固定算法常量 `STREAM_RWND ≈ 1.19 MiB`，写够 8 MiB 后必然阻塞在
///    flow control 上；用 `handle.is_finished() == false` 断言这个阻塞确实发生，证明测试
///    前提成立（不是"因为根本没怎么发数据所以顺带没超时"的假阳性）。
/// 2. 与此同时接收端在 `AckProgress` stream 上回一条 `Progress` 消息，Sender 侧
///    `sender.recv()` 应能在很短的超时内读到它，不被 `Data` stream 的阻塞拖累
///    ——这正是本 issue 要解决的问题：改造前只有一条 stream，`Data` 占满会直接
///    拖住 progress/ack 的读取。
///
/// issue #59 核实：本测试用 `quic::connect()`（默认 credit 窗口 `DEFAULT_CREDIT_WINDOW_
/// BYTES` = 64MiB），flood 总量 8MiB 远小于该窗口，`sender.send()` 内部的 credit 扣减
/// 不会被触发——测试观测到的阻塞仍然精确来自第 1 点描述的 QUIC per-stream 接收窗口
/// （~1.19MiB），语义未因 credit 机制引入而变质。若未来这里的 flood 总量改大到接近或
/// 超过 64MiB，则需要注意阻塞点会从"QUIC 窗口"变成"credit 窗口"，两者是不同的验证目标。
#[tokio::test]
async fn test_quic_mux_large_data_stream_does_not_block_ack_progress_stream() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();
    let (done_tx, done_rx) = oneshot::channel::<()>();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();

        // Control / FileList / Data 由 Sender 发起，这里 accept_bi() 依次接住；
        // AckProgress 反过来由 Receiver 自己 open_bi()（详见 `quic::mux` 模块文档：
        // 没有任何 SenderMsg 会写 AckProgress，若也让 Sender 发起，这条 stream
        // 永远不会被 reveal，accept_bi() 会永远阻塞）。
        let (_ctrl_send, _ctrl_recv) = conn.accept_bi().await.unwrap();
        let (_fl_send, _fl_recv) = conn.accept_bi().await.unwrap();
        let (_data_send, data_recv) = conn.accept_bi().await.unwrap();
        let (mut ack_send, _ack_recv) = conn.open_bi().await.unwrap();

        // 关键：Data stream 的 recv 半部分故意持有但从不读取，模拟"大文件占满该 stream，
        // 对端来不及消费"；不能 drop（drop 会触发 STOP_SENDING，人为解除背压，前提就不成立了）。
        let _keep_data_recv_alive = data_recv;

        // 在 AckProgress stream 上回一条 Progress，验证不受 Data stream 影响
        let snapshot = ProgressSnapshot {
            receiver_id: "mux-test".to_string(),
            files_transferred: 1,
            dirs_created: 0,
            bytes_transferred: 1024,
            files_skipped: 0,
            metadata_only: 0,
            error_count: 0,
            elapsed_secs: 0.1,
            speed_bytes_per_sec: 10240.0,
        };
        quic::framing::write_msg(&mut ack_send, &ReceiverMsg::Progress(snapshot))
            .await
            .unwrap();

        let _ = done_rx.await;
    });

    let sender = Arc::new(quic::connect(server_addr, "localhost", None).await.unwrap());

    // 后台持续往 Data stream 灌数据（对端不读），8MiB = 64 * 128KiB 远超 quinn 默认
    // ~1.19MiB 的 per-stream 接收窗口，足量触发阻塞。数据必须是不可压缩的随机字节——
    // `framing::write_msg` 对 >=1KiB 的 payload 会尝试 zstd 压缩，全 0 数据会被压缩到
    // 几乎为 0 字节，实际上不了线，永远填不满接收窗口。
    let entry = dummy_file_entry("big.bin");
    let flood_sender = sender.clone();
    let flood_handle = tokio::spawn(async move {
        for i in 0..64u64 {
            let chunk = DataChunk {
                offset: i * 128 * 1024,
                data: incompressible_bytes(128 * 1024, i),
            };
            if flood_sender
                .send(SenderMsg::FileData {
                    entry: entry.clone(),
                    chunk,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // 给 flood task 一点时间把窗口写满并阻塞住
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !flood_handle.is_finished(),
        "Data stream flood 任务应因对端未读取而阻塞在 flow control 上（前提不成立，测试无意义）"
    );

    // 核心断言：AckProgress stream 的消息应能很快读到，不受 Data stream 阻塞影响
    let reply = tokio::time::timeout(Duration::from_secs(2), sender.recv())
        .await
        .expect("Progress 消息应在 Data stream 阻塞期间被及时 recv() 到，不应超时");
    match reply {
        Some(ReceiverMsg::Progress(snapshot)) => {
            assert_eq!(snapshot.receiver_id, "mux-test");
        }
        other => panic!("Expected Progress, got {:?}", other),
    }

    flood_handle.abort();
    let _ = done_tx.send(());
    receiver_handle.await.unwrap();
    sender.close().await.unwrap();
}

/// 协议层连接异常测试（issue #26 可选项）：Receiver 在 Handshake 之后、任何回复之前
/// 就主动关闭连接（模拟对端崩溃），Sender 的 `recv()` 应在有限时间内返回 `None`
/// （连接结束），而不是 panic 或永久 hang。
///
/// `sender.recv()` 从统一 fan-in channel 读取（`QuicSenderTransport::recv`），该
/// channel 由 4 条物理 stream 各自的 `reader_loop` 转发；连接关闭后每条
/// `reader_loop` 各自在 `read_msg` 出错/EOF 时退出并 drop 手上的 `tx` clone，全部
/// drop 完后 `UnboundedReceiver::recv()` 返回 `None`（见 `crates/transport/src/quic/mux.rs`）。
#[tokio::test]
async fn test_quic_sender_recv_returns_none_after_connection_dropped() {
    install_crypto_provider();

    let (server_endpoint, server_addr) = create_server_endpoint();

    let receiver_handle = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (_send, _recv) = conn.accept_bi().await.unwrap();

        // 收到 Handshake 后不回复任何消息，直接主动关闭连接，模拟对端异常终止。
        conn.close(1u32.into(), b"simulated receiver crash");
    });

    let sender = quic::connect(server_addr, "localhost", None).await.unwrap();
    sender
        .send(SenderMsg::Handshake(ProtocolHandshake::current()))
        .await
        .unwrap();

    receiver_handle.await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), sender.recv())
        .await
        .expect("连接被 Receiver 侧关闭后 sender.recv() 应快速返回而不是 hang");
    assert!(
        reply.is_none(),
        "连接被 Receiver 侧关闭后应返回 None，实际: {:?}",
        reply
    );
}
