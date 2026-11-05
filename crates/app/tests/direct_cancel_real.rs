//! 由 lab runner 启动真实 Receiver；本测试通过真实 QUIC 在指定阶段主动关闭连接。
#![allow(clippy::unwrap_used)]
use std::{env, error::Error, path::Path, sync::Arc, time::Duration};

use bytes::Bytes;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::CertificateDer;
use storage_adapter::dir_tree::{DirPageResult, NdxEntry};
use storage_adapter::{CreateStorageOptions, DataChunk, TransferMode, create_storage};
use tokio::{
    fs,
    io::AsyncReadExt,
    time::{sleep, timeout},
};
use transport::{
    message::{HandshakeResult, ProtocolHandshake, ReceiverMsg, SenderMsg, SessionConfig},
    quic,
    traits::SenderTransport,
};

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
#[ignore = "requires a lab Receiver and phase-triggered real connection closure"]
async fn close_real_quic_during_direct_transfer() -> TestResult {
    timeout(Duration::from_secs(180), run()).await?
}

async fn run() -> TestResult {
    let _ = default_provider().install_default();
    let address = env::var("TERRASYNC_CANCEL_ADDRESS")?.parse()?;
    let cert = fs::read(env::var("TERRASYNC_CANCEL_CERT")?).await?;
    let token = fs::read_to_string(env::var("TERRASYNC_CANCEL_TOKEN_FILE")?).await?;
    let source_path = env::var("TERRASYNC_CANCEL_SOURCE")?;
    let trigger = env::var("TERRASYNC_CANCEL_TRIGGER")?;
    let source = Path::new(&source_path);
    let storage = create_storage(
        source
            .parent()
            .ok_or("source has no parent")?
            .to_str()
            .ok_or("invalid source path")?,
        CreateStorageOptions::new(None, false),
    )
    .await?;
    let entry = Arc::new(storage.get_metadata(Path::new("file")).await?);
    let transport = Arc::new(quic::connect(address, "localhost", Some(CertificateDer::from(cert))).await?);
    let mut handshake = ProtocolHandshake::current();
    handshake.features.delta = false;
    transport.send(SenderMsg::Handshake(handshake)).await?;
    assert!(matches!(
        transport.recv().await,
        Some(ReceiverMsg::HandshakeAck(HandshakeResult::Accepted { .. }))
    ));
    transport.send(SenderMsg::Auth { token }).await?;
    assert!(matches!(
        transport.recv().await,
        Some(ReceiverMsg::AuthResult { ok: true, .. })
    ));
    transport
        .send(SenderMsg::SessionConfig(SessionConfig {
            transfer_mode: TransferMode::Direct,
            src_path: source_path.clone(),
            qos: None,
            peak_qos_rate: 1.0,
            iops: None,
            enable_integrity_check: true,
            enable_acl: false,
            is_source_reserved: true,
            block_size: None,
            delete_target: false,
            delta_size_threshold: None,
        }))
        .await?;
    transport
        .send(SenderMsg::FilePage(DirPageResult {
            dir_path: String::new(),
            ndx_start: 0,
            files: vec![NdxEntry {
                ndx: 0,
                entry: entry.clone().into(),
            }],
            subdirs: Vec::new(),
            gap_ndx: 1,
        }))
        .await?;
    transport.send(SenderMsg::FileListDone).await?;
    loop {
        match transport.recv().await {
            Some(ReceiverMsg::TransferRequest { ndx: 0, .. }) => break,
            Some(ReceiverMsg::RequestsDone | ReceiverMsg::Progress(_)) => {}
            other => return Err(format!("unexpected classification: {other:?}").into()),
        }
    }
    let closing = transport.clone();
    let closer = tokio::spawn(async move {
        while !Path::new(&trigger).exists() {
            sleep(Duration::from_millis(2)).await;
        }
        closing.close().await
    });
    let payload = async {
        transport.send(SenderMsg::FileBegin { ndx: 0 }).await?;
        let mut file = fs::File::open(source).await?;
        let mut offset = 0;
        let mut hash = blake3::Hasher::new();
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            transport
                .send(SenderMsg::FileData {
                    ndx: 0,
                    chunk: DataChunk {
                        offset,
                        data: Bytes::copy_from_slice(&buffer[..read]),
                    },
                })
                .await?;
            offset += read as u64;
        }
        transport
            .send(SenderMsg::EndOfFile {
                ndx: 0,
                source_hash: Some(hash.finalize().to_hex().to_string()),
            })
            .await?;
        transport.send(SenderMsg::TransferDone).await?;
        while let Some(message) = transport.recv().await {
            assert!(
                !matches!(message, ReceiverMsg::Success { .. } | ReceiverMsg::AllDone),
                "故障注入不应完成发布"
            );
        }
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    closer.await??;
    // 连接关闭可能中断 send，也可能中断 recv；业务结果由 runner 独立检查。
    println!(
        "explicit_quic_close_completed; payload_interrupted={}",
        payload.is_err()
    );
    Ok(())
}
