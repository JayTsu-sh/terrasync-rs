//! wire 只编码原生 opaque snapshot；后端身份随上下文传递以核验解码结果。
use data_mover::model::{BackendIdentity, BackendKind, ObservedEntry};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError, ser::Error as SerError, ser::SerializeSeq,
};
use std::sync::Arc;
use task_model::observation::ObservedItem;

struct SnapshotBytes<'a>(&'a [u8]);
impl Serialize for SnapshotBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}
struct WireRef<'a>(&'a ObservedEntry);
impl Serialize for WireRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let snapshot = self.0.try_encode_snapshot().map_err(S::Error::custom)?;
        let backend = self.0.source_identity().backend();
        (
            backend.kind().as_str(),
            backend.stable_id(),
            SnapshotBytes(snapshot.as_bytes()),
        )
            .serialize(serializer)
    }
}

fn decode<E: DeError>((kind, identity, bytes): (String, String, Vec<u8>)) -> Result<ObservedEntry, E> {
    let kind = kind.parse::<BackendKind>().map_err(E::custom)?;
    let backend = BackendIdentity::new(kind, identity).map_err(E::custom)?;
    ObservedEntry::decode_snapshot(&bytes, &backend).map_err(E::custom)
}

pub mod shared {
    use super::*;
    pub fn serialize<S: Serializer>(entry: &Arc<ObservedEntry>, serializer: S) -> Result<S::Ok, S::Error> {
        WireRef(entry).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<ObservedEntry>, D::Error> {
        decode(<(String, String, Vec<u8>)>::deserialize(deserializer)?).map(Arc::new)
    }
}

pub mod batch {
    use super::*;
    pub fn serialize<S: Serializer>(entries: &[Arc<ObservedEntry>], serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(entries.len()))?;
        for entry in entries {
            sequence.serialize_element(&WireRef(entry))?;
        }
        sequence.end()
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Arc<ObservedEntry>>, D::Error> {
        Vec::<(String, String, Vec<u8>)>::deserialize(deserializer)?
            .into_iter()
            .map(|wire| decode(wire).map(Arc::new))
            .collect()
    }
}

pub mod selected {
    use super::*;
    pub fn serialize<S: Serializer>(item: &ObservedItem, serializer: S) -> Result<S::Ok, S::Error> {
        let snapshot = match item.version_index {
            Some(index) => item.entry.try_encode_version_snapshot(index),
            None => item.entry.try_encode_snapshot(),
        }
        .map_err(S::Error::custom)?;
        let backend = item.entry.source_identity().backend();
        (
            backend.kind().as_str(),
            backend.stable_id(),
            SnapshotBytes(snapshot.as_bytes()),
        )
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ObservedItem, D::Error> {
        shared::deserialize(deserializer).map(ObservedItem::from)
    }
}
