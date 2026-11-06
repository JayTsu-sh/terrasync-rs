//! 数据库语义比较摘要；原值始终由观察 snapshot 持有。

use blake3::Hasher;
use data_mover_model::{AclEncoding, MetadataObservation, MetadataObservations};

/// 固定顺序为 ACL、xattr、tags；权限和 mtime 直接比较已有标量。
/// 域与编码版本属于持久化比较契约；来源为内联还是附加调用不影响语义。
pub fn metadata_digests(facts: &MetadataObservations) -> Vec<String> {
    vec![
        digest(0, facts.acl(), |hash, acl| {
            hash.update(&[match acl.encoding() {
                AclEncoding::Posix => 0,
                AclEncoding::NfsV4 => 1,
                AclEncoding::WindowsSecurityDescriptor => 2,
            }]);
            optional_bytes(hash, acl.access());
            optional_bytes(hash, acl.default_acl());
        }),
        digest(1, facts.xattrs(), |hash, attrs| {
            let mut attrs: Vec<_> = attrs.iter().collect();
            attrs.sort_unstable_by(|a, b| (a.name(), a.value()).cmp(&(b.name(), b.value())));
            for attr in attrs {
                bytes(hash, attr.name());
                bytes(hash, attr.value());
            }
        }),
        digest(2, facts.tags(), |hash, tags| {
            let mut tags: Vec<_> = tags.iter().collect();
            tags.sort_unstable_by(|a, b| (a.key(), a.value()).cmp(&(b.key(), b.value())));
            for tag in tags {
                bytes(hash, tag.key().as_bytes());
                bytes(hash, tag.value().as_bytes());
            }
        }),
    ]
}

fn digest<T>(family: u8, observation: &MetadataObservation<T>, value: impl FnOnce(&mut Hasher, &T)) -> String {
    let observed = match observation {
        MetadataObservation::Value { value, .. } => value,
        MetadataObservation::NotRequested => return "not_requested".into(),
        MetadataObservation::NotApplicable => return "not_applicable".into(),
        MetadataObservation::Unsupported => return "unsupported".into(),
        MetadataObservation::Failed { class, transience } => return format!("failed:{class:?}/{transience:?}"),
    };
    let mut hash = Hasher::new();
    hash.update(b"terrasync metadata family v1\0");
    hash.update(&[family]);
    value(&mut hash, observed);
    hash.finalize().to_hex().to_string()
}

fn optional_bytes(hash: &mut Hasher, value: Option<&[u8]>) {
    hash.update(&[u8::from(value.is_some())]);
    if let Some(value) = value {
        bytes(hash, value);
    }
}

fn bytes(hash: &mut Hasher, value: &[u8]) {
    hash.update(&(value.len() as u64).to_le_bytes());
    hash.update(value);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use data_mover_model::{AclMetadata, MetadataProvenance, ObjectTag};

    fn tags(values: &[(&str, &str)], provenance: MetadataProvenance) -> MetadataObservations {
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::Value {
                value: values
                    .iter()
                    .map(|(key, value)| ObjectTag::new(*key, *value).unwrap())
                    .collect(),
                provenance,
            },
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
        )
        .unwrap()
    }

    #[test]
    fn tag_order_and_observation_transport_do_not_create_changes() {
        assert_eq!(
            metadata_digests(&tags(&[("a", "1"), ("b", "2")], MetadataProvenance::Inline)),
            metadata_digests(&tags(&[("b", "2"), ("a", "1")], MetadataProvenance::AdditionalCall))
        );
        assert_ne!(
            metadata_digests(&tags(&[], MetadataProvenance::Inline)),
            metadata_digests(&MetadataObservations::default())
        );
        assert_ne!(
            metadata_digests(&tags(&[("a", "1")], MetadataProvenance::Inline)),
            metadata_digests(&tags(&[("a", "2")], MetadataProvenance::Inline))
        );
    }

    #[test]
    fn native_acl_bytes_keep_their_meaningful_order() {
        let acl = |bytes| {
            MetadataObservations::new(
                MetadataObservation::Value {
                    value: AclMetadata::new(AclEncoding::NfsV4, bytes).unwrap(),
                    provenance: MetadataProvenance::Inline,
                },
                MetadataObservation::NotRequested,
                MetadataObservation::NotRequested,
                MetadataObservation::NotRequested,
                MetadataObservation::NotRequested,
            )
            .unwrap()
        };
        assert_ne!(metadata_digests(&acl(vec![1, 2])), metadata_digests(&acl(vec![2, 1])));
    }
}
