use serde::{Deserialize, Serialize};

/// 端点类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EndpointType {
    Local,
    NfsV3,
    S3,
    Cifs,
}

impl EndpointType {
    /// 用于前端展示的友好名称
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Local => "Local",
            Self::NfsV3 => "NFS v3",
            Self::S3 => "S3",
            Self::Cifs => "CIFS/SMB",
        }
    }
}

impl std::fmt::Display for EndpointType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "local"),
            Self::NfsV3 => write!(f, "nfs_v3"),
            Self::S3 => write!(f, "s3"),
            Self::Cifs => write!(f, "cifs"),
        }
    }
}

impl std::str::FromStr for EndpointType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(Self::Local),
            "nfs" | "nfs_v3" => Ok(Self::NfsV3),
            "s3" => Ok(Self::S3),
            "cifs" | "smb" => Ok(Self::Cifs),
            other => Err(format!("unknown endpoint type: {other}")),
        }
    }
}

/// S3 协议类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum S3Protocol {
    Http,
    Https,
}

impl S3Protocol {
    pub fn url_scheme(&self) -> &str {
        match self {
            Self::Http => "s3://",
            Self::Https => "s3+https://",
        }
    }
}

/// 端点配置（值对象）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EndpointConfig {
    Local {
        path: String,
    },
    Nfs {
        server: String,
        port: u16,
        export_path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        uid: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gid: Option<u32>,
    },
    S3 {
        access_key: String,
        secret_key: String,
        bucket: String,
        host: String,
        port: u16,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        protocol: S3Protocol,
    },
    Cifs {
        server: String,
        port: u16,
        share: String,
        username: String,
        password: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
}

/// Endpoint 聚合根
#[derive(Debug, Clone, Deserialize)]
pub struct Endpoint {
    pub id: String,
    pub name: String,
    pub endpoint_type: EndpointType,
    pub config: EndpointConfig,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// 自定义序列化：自动附加 `endpoint_type_display` 字段
impl Serialize for Endpoint {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Endpoint", 7)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("endpoint_type", &self.endpoint_type)?;
        state.serialize_field("endpoint_type_display", self.endpoint_type.display_name())?;
        state.serialize_field("config", &self.config)?;
        state.serialize_field("created_at", &self.created_at)?;
        state.serialize_field("updated_at", &self.updated_at)?;
        state.end()
    }
}

impl Endpoint {
    /// 将端点配置转换为 storage URL 格式
    ///
    /// 生成与 `data_mover::storage_enum::detect_storage_type()` 兼容的 URL 字符串。
    /// 可选 `sub_path` 会追加到端点基础路径后。
    pub fn to_storage_url(&self, sub_path: Option<&str>) -> String {
        match &self.config {
            EndpointConfig::Local { path } => {
                let base = path.clone();
                match sub_path {
                    Some(sp) => format!("{}/{}", base.trim_end_matches('/'), sp.trim_start_matches('/')),
                    None => base,
                }
            }
            EndpointConfig::Nfs {
                server,
                port,
                export_path,
                prefix,
                uid,
                gid,
            } => {
                let mut url = format!("nfs://{}:{}/{}", server, port, export_path.trim_start_matches('/'));

                // 合并 prefix 和 sub_path
                let effective_prefix = match (prefix.as_deref(), sub_path) {
                    (Some(p), Some(sp)) => Some(format!("{}/{}", p.trim_end_matches('/'), sp.trim_start_matches('/'))),
                    (Some(p), None) => Some(p.to_string()),
                    (None, Some(sp)) => Some(sp.to_string()),
                    (None, None) => None,
                };

                if let Some(ref pfx) = effective_prefix {
                    url.push_str(":/");
                    url.push_str(pfx.trim_start_matches('/'));
                }

                let mut params = Vec::new();
                if let Some(u) = uid {
                    params.push(format!("uid={u}"));
                }
                if let Some(g) = gid {
                    params.push(format!("gid={g}"));
                }
                if !params.is_empty() {
                    url.push('?');
                    url.push_str(&params.join("&"));
                }
                url
            }
            EndpointConfig::S3 {
                access_key,
                secret_key,
                bucket,
                host,
                port,
                prefix,
                protocol,
            } => {
                let scheme = protocol.url_scheme();
                let effective_prefix = match (prefix.as_deref(), sub_path) {
                    (Some(p), Some(sp)) => Some(format!("{}/{}", p.trim_end_matches('/'), sp.trim_start_matches('/'))),
                    (Some(p), None) => Some(p.to_string()),
                    (None, Some(sp)) => Some(sp.to_string()),
                    (None, None) => None,
                };

                let mut url = format!("{scheme}{access_key}:{secret_key}@{bucket}.{host}:{port}");
                if let Some(ref pfx) = effective_prefix {
                    url.push('/');
                    url.push_str(pfx.trim_start_matches('/'));
                }
                url
            }
            EndpointConfig::Cifs {
                server,
                port,
                share,
                username,
                password,
                prefix,
            } => {
                let effective_prefix = match (prefix.as_deref(), sub_path) {
                    (Some(p), Some(sp)) => Some(format!("{}/{}", p.trim_end_matches('/'), sp.trim_start_matches('/'))),
                    (Some(p), None) => Some(p.to_string()),
                    (None, Some(sp)) => Some(sp.to_string()),
                    (None, None) => None,
                };

                let mut url = format!("smb://{username}:{password}@{server}:{port}/{share}");
                if let Some(ref pfx) = effective_prefix {
                    url.push('/');
                    url.push_str(pfx.trim_start_matches('/'));
                }
                url
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_storage_url() {
        let ep = Endpoint {
            id: "1".to_string(),
            name: "test".to_string(),
            endpoint_type: EndpointType::Local,
            config: EndpointConfig::Local {
                path: "/data".to_string(),
            },
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        assert_eq!(ep.to_storage_url(Some("project1")), "/data/project1");
        assert_eq!(ep.to_storage_url(None), "/data");
    }

    #[test]
    fn test_nfs_storage_url() {
        let ep = Endpoint {
            id: "2".to_string(),
            name: "nfs-prod".to_string(),
            endpoint_type: EndpointType::NfsV3,
            config: EndpointConfig::Nfs {
                server: "fileserver".to_string(),
                port: 2049,
                export_path: "/data".to_string(),
                prefix: None,
                uid: Some(1000),
                gid: Some(1000),
            },
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        assert_eq!(
            ep.to_storage_url(Some("project1")),
            "nfs://fileserver:2049/data:/project1?uid=1000&gid=1000"
        );
    }

    #[test]
    fn test_s3_storage_url() {
        let ep = Endpoint {
            id: "3".to_string(),
            name: "s3-backup".to_string(),
            endpoint_type: EndpointType::S3,
            config: EndpointConfig::S3 {
                access_key: "AKID".to_string(),
                secret_key: "SECRET".to_string(),
                bucket: "mybucket".to_string(),
                host: "s3.example.com".to_string(),
                port: 9000,
                prefix: None,
                protocol: S3Protocol::Http,
            },
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        assert_eq!(
            ep.to_storage_url(Some("archives")),
            "s3://AKID:SECRET@mybucket.s3.example.com:9000/archives"
        );
    }
}
