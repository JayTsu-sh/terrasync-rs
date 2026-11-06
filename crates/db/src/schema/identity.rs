//! 目录跨轮次关联身份的资格与持久化编码。
use data_mover::model::{BackendKind, ObservedEntry};

pub(crate) const PERSISTED_IDENTITY_PREFIX: &str = "54534934";

pub(crate) fn has_cross_scan_identity(entry: &ObservedEntry) -> bool {
    entry.backend_kind() != BackendKind::Local && entry.source_identity().stable_bytes().is_some()
}

pub(crate) fn encoded_file_handle(entry: &ObservedEntry) -> Option<String> {
    if !has_cross_scan_identity(entry) {
        return None;
    }
    let mut value = String::with_capacity(72);
    value.push_str(PERSISTED_IDENTITY_PREFIX);
    for byte in entry.identity_key().as_bytes() {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 15) as usize] as char);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use data_mover::model::{BackendIdentity, EntryKind, IdentityStrength, SourceIdentity, StoragePath};

    #[test]
    fn local_runtime_identity_is_not_a_cross_scan_rename_key() -> Result<()> {
        for (kind, expected) in [(BackendKind::Local, false), (BackendKind::Nfs, true)] {
            let backend = BackendIdentity::new(kind, "identity-policy")?;
            let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"stable-id")?;
            let entry = ObservedEntry::new(StoragePath::new("file")?, EntryKind::File, Some(1), None, source)?;
            assert_eq!(has_cross_scan_identity(&entry), expected);
            let encoded = encoded_file_handle(&entry);
            if expected {
                assert_eq!(
                    encoded,
                    Some(format!("54534934{}", hex::encode(entry.identity_key().as_bytes())))
                );
            } else {
                assert_eq!(encoded, None);
            }
        }
        Ok(())
    }
}
