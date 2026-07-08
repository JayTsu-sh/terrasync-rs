#![allow(clippy::unwrap_used, clippy::expect_used)]
// 用 in-process transport 断言 Sender 流式发送序列 + 源 hash 正确。
use std::path::PathBuf;
use std::sync::Arc;

use bytes::BytesMut;
use data_mover::{EntryEnum, NASEntry, StorageEnum, create_storage};
use transport::in_process::create_in_process_pair;
use transport::message::SenderMsg;
use transport::traits::ReceiverTransport;

// 帮助函数：在临时目录建一个 Local StorageEnum 和一个 size 字节的确定性文件，返回 (storage, entry, bytes)
async fn local_file(dir: &std::path::Path, name: &str, size: usize) -> (Arc<StorageEnum>, Arc<EntryEnum>, Vec<u8>) {
    let bytes = vec![0xABu8; size];
    tokio::fs::write(dir.join(name), &bytes).await.unwrap();

    let storage = create_storage(&dir.to_string_lossy(), None, false).await.unwrap();
    let entry = EntryEnum::NAS(NASEntry {
        name: name.to_string(),
        relative_path: PathBuf::from(name),
        extension: None,
        is_dir: false,
        size: size as u64,
        atime: 0,
        ctime: 0,
        mtime: 0,
        mode: 0o644,
        is_symlink: false,
        hard_links: Some(1),
        uid: None,
        gid: None,
        ino: None,
        file_handle: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    });

    (Arc::new(storage), Arc::new(entry), bytes)
}

#[tokio::test]
async fn sender_streams_file_and_sends_correct_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, entry, bytes) = local_file(tmp.path(), "big.bin", 9 * 1024 * 1024).await;
    let (sender_t, receiver_t) = create_in_process_pair();

    let entry2 = entry.clone();
    let jh = tokio::spawn(async move {
        app::remote_sync::handle_full_transfer(&sender_t, &src, &entry2, None, true, false).await
    });

    // 收集 Sender 消息，重组文件字节
    let mut got = BytesMut::new();
    let source_hash = loop {
        match receiver_t.recv().await {
            Some(SenderMsg::FileBegin { .. }) => {}
            Some(SenderMsg::FileData { chunk, .. }) => got.extend_from_slice(&chunk.data),
            Some(SenderMsg::EndOfFile { source_hash, .. }) => break source_hash,
            other => panic!("unexpected {other:?}"),
        }
    };
    jh.await.unwrap().unwrap();
    assert_eq!(&got[..], &bytes[..], "重组字节应等于源文件");
    assert_eq!(source_hash.unwrap(), blake3::hash(&bytes).to_hex().to_string());
}
