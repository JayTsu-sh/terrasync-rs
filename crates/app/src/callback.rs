//! Callback 地址的解析、校验和安全诊断；不参与存储 URL 的协议解析。
use std::fmt;

use thiserror::Error;
use url::{Host, ParseError, Url};

#[derive(Debug, Error)]
pub enum CallbackEndpointError {
    #[error("invalid callback URL: {0}")]
    Malformed(#[source] ParseError),
    #[error("callback URL must use HTTP or HTTPS")]
    UnsupportedScheme,
    #[error("callback URL must have a host")]
    MissingHost,
    #[error("callback URL must not contain a fragment")]
    FragmentNotAllowed,
}

/// 保留发送所需的完整地址；格式化永远不输出认证、路径或 query。
#[derive(Clone)]
pub struct CallbackEndpoint {
    url: Url,
}

impl CallbackEndpoint {
    pub fn parse(value: &str) -> Result<Self, CallbackEndpointError> {
        let url = Url::parse(value).map_err(CallbackEndpointError::Malformed)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(CallbackEndpointError::UnsupportedScheme);
        }
        if !url.has_host() {
            return Err(CallbackEndpointError::MissingHost);
        }
        if url.fragment().is_some() {
            return Err(CallbackEndpointError::FragmentNotAllowed);
        }
        Ok(Self { url })
    }

    /// 只用于实际 HTTP 请求，不用于格式化日志或通知错误。
    pub(crate) fn request_url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn is_loopback(&self) -> bool {
        match self.url.host() {
            Some(Host::Domain(host)) => host.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => {
                address.is_loopback() || address.to_ipv4_mapped().is_some_and(|address| address.is_loopback())
            }
            None => false,
        }
    }
}

impl fmt::Debug for CallbackEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallbackEndpoint")
            .field("scheme", &self.url.scheme())
            .field("host", &self.url.host_str())
            .field("port", &self.url.port_or_known_default())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CallbackEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}://{}",
            self.url.scheme(),
            self.url.host_str().unwrap_or("[missing-host]")
        )?;
        if let Some(port) = self.url.port_or_known_default() {
            write!(formatter, ":{port}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn request_preserves_credentials_and_query_but_diagnostics_do_not_expose_them() {
        let raw = "https://user-canary:password-canary@callback.example:8443/path-canary?token=query-canary";
        let endpoint = CallbackEndpoint::parse(raw).unwrap();
        assert_eq!(endpoint.request_url().as_str(), raw);
        for output in [endpoint.to_string(), format!("{endpoint:?}")] {
            assert!(output.contains("callback.example"));
            for secret in ["user-canary", "password-canary", "path-canary", "query-canary"] {
                assert!(!output.contains(secret), "{output}");
            }
        }
    }

    #[test]
    fn validates_before_use_and_recognizes_only_explicit_loopback_hosts() {
        for raw in [
            "http://localhost/cb",
            "http://LOCALHOST./cb",
            "http://127.1.2.3/cb",
            "http://[::1]/cb",
            "http://[::ffff:127.0.0.1]/cb",
        ] {
            assert!(CallbackEndpoint::parse(raw).unwrap().is_loopback(), "{raw}");
        }
        for raw in [
            "https://callback.example/cb",
            "http://localhost.example/cb",
            "http://10.0.0.1/cb",
        ] {
            assert!(!CallbackEndpoint::parse(raw).unwrap().is_loopback(), "{raw}");
        }
        for raw in [
            "relative/path",
            "ftp://host/path",
            "file:///tmp/cb",
            "http://",
            "https://host/cb#fragment",
        ] {
            assert!(CallbackEndpoint::parse(raw).is_err(), "{raw}");
        }
    }
}
