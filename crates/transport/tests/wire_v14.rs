#![cfg(feature = "quic")]
#![allow(clippy::unwrap_used)]

use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use bytes::Bytes;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, HistoryOrder, IdentityStrength, ObjectHistory, ObservedEntry, RecordKind,
    SourceIdentity, StoragePath, VersionRecord,
};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use task_model::observation::ObservedItem;
use transport::chunk::DataChunk;
use transport::directory::{DirPageResult, NdxEntry};
use transport::message::{ReceiverMsg, SenderMsg, TransferDecision};

fn selected_file_page() -> SenderMsg {
    let backend = BackendIdentity::new(BackendKind::S3, "wire-v14").unwrap();
    let versions = (1..=2)
        .map(|size| {
            let id = format!("v{size}");
            let identity =
                SourceIdentity::new(backend.clone(), IdentityStrength::VersionScoped, id.as_bytes()).unwrap();
            VersionRecord::from_listing(
                id,
                size == 2,
                RecordKind::Object {
                    size,
                    etag: "tag".into(),
                    modified: None,
                },
                identity,
            )
            .unwrap()
        })
        .collect();
    let entry = ObservedEntry::new(
        StoragePath::new("file").unwrap(),
        EntryKind::File,
        Some(2),
        None,
        SourceIdentity::new(backend, IdentityStrength::PathScoped, b"file").unwrap(),
    )
    .unwrap()
    .with_history(Some(Box::new(
        ObjectHistory::whole(versions, HistoryOrder::Server).unwrap(),
    )))
    .unwrap();
    SenderMsg::FilePage(DirPageResult {
        dir_path: String::new(),
        ndx_start: 7,
        files: vec![NdxEntry {
            ndx: 7,
            entry: ObservedItem {
                entry: Arc::new(entry),
                version_index: Some(0),
            },
        }],
        subdirs: Vec::new(),
        gap_ndx: 8,
    })
}

// 从拆分 transport 依赖前的 v14 编码采集。保留消息判别值、字段顺序与所选对象版本。
fn assert_wire_bytes<T: Serialize + DeserializeOwned>(message: T, expected: &[u8]) {
    assert_eq!(encode_to_vec(message, config::standard()).unwrap(), expected);
    let (decoded, consumed): (T, _) = decode_from_slice(expected, config::standard()).unwrap();
    assert_eq!(consumed, expected.len());
    assert_eq!(encode_to_vec(decoded, config::standard()).unwrap(), expected);
}

#[test]
fn wire_v14_sender_messages() {
    assert_wire_bytes(selected_file_page(), &FILE_PAGE_V14);
    assert_wire_bytes(
        SenderMsg::FileData {
            ndx: 7,
            chunk: DataChunk {
                offset: 256,
                data: Bytes::from_static(b"abc"),
            },
        },
        &FILE_DATA_V14,
    );
    assert_wire_bytes(SenderMsg::TransferDone, &TRANSFER_DONE_V14);
}

#[test]
fn wire_v14_receiver_messages() {
    assert_wire_bytes(
        ReceiverMsg::TransferRequest {
            ndx: 7,
            decision: TransferDecision::DeltaTransfer,
        },
        &[2, 14, 1],
    );
    assert_wire_bytes(ReceiverMsg::Success { ndx: 7 }, &[12, 14]);
    assert_wire_bytes(ReceiverMsg::AllDone, &[16]);
}

const FILE_PAGE_V14: [u8; 152] = [
    3, 0, 14, 1, 14, 2, 115, 51, 8, 119, 105, 114, 101, 45, 118, 49, 52, 132, 68, 77, 69, 83, 11, 3, 4, 0, 0, 0, 102,
    105, 108, 101, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 34, 210, 35, 165, 2, 0, 0, 0, 118, 49, 2, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 1, 7, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 2, 0, 0, 0, 118, 49, 1, 0, 0, 0, 0, 0, 0,
    0, 3, 0, 0, 0, 116, 97, 103, 0, 2, 2, 0, 0, 0, 118, 49, 0, 0, 0, 113, 107, 213, 244, 223, 215, 123, 140, 158, 237,
    156, 251, 112, 248, 208, 30, 125, 155, 247, 110, 162, 238, 255, 53, 49, 62, 209, 24, 116, 177, 97, 22, 0, 16,
];

const FILE_DATA_V14: [u8; 9] = [11, 14, 251, 0, 1, 3, 97, 98, 99];

const TRANSFER_DONE_V14: [u8; 1] = [18];
