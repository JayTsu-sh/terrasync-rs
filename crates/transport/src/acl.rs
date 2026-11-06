//! 远端 ACL 帧的编码；借用已捕获的原生载荷，不在序列化前复制 ACL 字节。

use data_mover::model::{AclEncoding, AclMetadata};
use serde::{Deserialize, Serialize};

use crate::error::{Result, TransportError};

#[derive(Serialize)]
struct WireAclRef<'a> {
    encoding: u8,
    access: Option<&'a [u8]>,
    default: Option<&'a [u8]>,
}

#[derive(Deserialize)]
struct WireAcl {
    encoding: u8,
    access: Option<Vec<u8>>,
    default: Option<Vec<u8>>,
}

pub fn encode_acl(value: &AclMetadata) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&WireAclRef {
        encoding: match value.encoding() {
            AclEncoding::Posix => 0,
            AclEncoding::NfsV4 => 1,
            AclEncoding::WindowsSecurityDescriptor => 2,
        },
        access: value.access(),
        default: value.default_acl(),
    })?)
}

pub fn decode_acl(bytes: &[u8]) -> Result<AclMetadata> {
    let wire: WireAcl = serde_json::from_slice(bytes)?;
    Ok(match wire.encoding {
        0 => AclMetadata::new_posix(wire.access, wire.default)?,
        1 => AclMetadata::new(AclEncoding::NfsV4, wire.access.unwrap_or_default())?,
        2 => AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, wire.access.unwrap_or_default())?,
        encoding => return Err(TransportError::InvalidAclEncoding { encoding }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_json_keeps_the_current_wire_bytes() -> Result<()> {
        let acl = AclMetadata::new_posix(Some(vec![1, 2]), Some(vec![3, 4]))?;
        let bytes = encode_acl(&acl)?;
        assert_eq!(bytes, br#"{"encoding":0,"access":[1,2],"default":[3,4]}"#);
        assert_eq!(decode_acl(&bytes)?, acl);
        for (encoding, tag) in [(AclEncoding::NfsV4, 1), (AclEncoding::WindowsSecurityDescriptor, 2)] {
            let acl = AclMetadata::new(encoding, vec![5, 6])?;
            let expected = format!("{{\"encoding\":{tag},\"access\":[5,6],\"default\":null}}");
            assert_eq!(encode_acl(&acl)?, expected.as_bytes());
            assert_eq!(decode_acl(expected.as_bytes())?, acl);
        }
        Ok(())
    }

    #[test]
    fn unknown_acl_encoding_keeps_a_typed_protocol_failure() {
        assert!(matches!(
            decode_acl(br#"{"encoding":255,"access":null,"default":null}"#),
            Err(TransportError::InvalidAclEncoding { encoding: 255 })
        ));
    }
}
