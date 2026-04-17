//! TLS 证书管理
//!
//! 提供自签名证书生成和 quinn ServerConfig/ClientConfig 构建。
//! 开发/测试用自签名，生产模式在 Phase 7 加 mTLS。

use std::sync::Arc;

use quinn::{ClientConfig, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::error::{QuicTransportError, Result};

/// 生成自签名证书（用于开发/测试）
///
/// SAN 包含 "localhost" 和传入的额外 hostname。
pub fn generate_self_signed(extra_sans: &[&str]) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let mut sans: Vec<String> = vec!["localhost".to_string()];
    for s in extra_sans {
        if *s != "localhost" {
            sans.push(s.to_string());
        }
    }

    let rcgen::CertifiedKey { cert, key_pair } = rcgen::generate_simple_self_signed(sans)
        .map_err(|e| QuicTransportError::TlsError(format!("generate cert: {e}")))?;

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())
        .map_err(|e| QuicTransportError::TlsError(format!("serialize key: {e}")))?;

    Ok((vec![cert_der], key_der))
}

/// 构建 quinn ServerConfig（使用自签名证书）
pub fn build_server_config(certs: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Result<ServerConfig> {
    let server_config = ServerConfig::with_single_cert(certs, key)
        .map_err(|e| QuicTransportError::TlsError(format!("server config: {e}")))?;
    Ok(server_config)
}

/// 构建 quinn ClientConfig（跳过证书验证，用于开发/测试）
///
/// 生产模式应使用 `build_client_config_with_ca()` 验证服务端证书。
pub fn build_client_config_insecure() -> Result<ClientConfig> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicTransportError::TlsError(format!("client config: {e}")))?,
    ));
    Ok(client_config)
}

/// 构建 quinn `ClientConfig` — 验证服务端证书（单向 TLS，无客户端证书）
///
/// - `server_cert`: 服务端的自签名证书 DER（用于验证服务端身份，防止 MITM）
pub fn build_client_config_with_ca(server_cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(server_cert)
        .map_err(|e| QuicTransportError::TlsError(format!("add server cert: {e}")))?;

    let crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicTransportError::TlsError(format!("client config: {e}")))?,
    ));
    Ok(client_config)
}

/// 跳过服务端证书验证（仅用于开发/测试）
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self, _end_entity: &CertificateDer<'_>, _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>, _ocsp_response: &[u8], _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self, _message: &[u8], _cert: &CertificateDer<'_>, _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self, _message: &[u8], _cert: &CertificateDer<'_>, _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        // 优先使用已安装的全局 provider（避免每次握手构造新 provider）
        rustls::crypto::CryptoProvider::get_default().map_or_else(
            || {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            },
            |p| p.signature_verification_algorithms.supported_schemes(),
        )
    }
}

// ============================================================
// mTLS：生产模式（CA 验证 + 客户端证书）
// ============================================================

/// 构建 quinn `ClientConfig` — 验证服务端证书（生产模式）
///
/// - `ca_cert`: CA 证书（PEM 或 DER）用于验证服务端
/// - `client_cert`, `client_key`: 客户端证书和私钥（用于双向认证）
pub fn build_client_config_mtls(
    ca_cert: CertificateDer<'static>, client_certs: Vec<CertificateDer<'static>>, client_key: PrivateKeyDer<'static>,
) -> Result<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(ca_cert)
        .map_err(|e| QuicTransportError::TlsError(format!("add CA cert: {e}")))?;

    let crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(|e| QuicTransportError::TlsError(format!("client auth cert: {e}")))?;

    let client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicTransportError::TlsError(format!("quic client config: {e}")))?,
    ));
    Ok(client_config)
}

/// 构建 quinn `ServerConfig` — 要求客户端证书（mTLS 模式）
///
/// - `server_certs`, `server_key`: 服务端证书和私钥
/// - `ca_cert`: CA 证书用于验证客户端
pub fn build_server_config_mtls(
    server_certs: Vec<CertificateDer<'static>>, server_key: PrivateKeyDer<'static>, ca_cert: CertificateDer<'static>,
) -> Result<ServerConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(ca_cert)
        .map_err(|e| QuicTransportError::TlsError(format!("add CA cert: {e}")))?;

    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| QuicTransportError::TlsError(format!("client verifier: {e}")))?;

    let server_crypto = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(server_certs, server_key)
        .map_err(|e| QuicTransportError::TlsError(format!("server config: {e}")))?;

    let server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .map_err(|e| QuicTransportError::TlsError(format!("quic server config: {e}")))?,
    ));
    Ok(server_config)
}
