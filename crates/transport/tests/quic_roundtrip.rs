//! QUIC transport roundtrip 集成测试
//!
//! 运行方式：cargo test -p transport --features quic

#![cfg(feature = "quic")]
// 集成测试代码：豁免 unwrap/expect 与所有 pedantic 风格 lint
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::pedantic)]

use std::net::SocketAddr;

use tokio::sync::oneshot;
use transport::message::{ReceiverMsg, SenderMsg, SessionConfig};
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
