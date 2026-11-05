//! 源端原生目录块的产品清单：排序、会话编号和有界目录页。
use async_channel::Receiver;
use data_mover::traversal::{TraversalItem, TraversalOutcome};
use std::sync::Arc;
use storage_adapter::{Result as StorageResult, StorageEnum, WalkOptions};
use task_model::ObservedEntryExt;
use task_model::observation::ObservedItem;
use transport::directory::{DirPageResult, NdxEntry};

/// 远端清单生成器交付的产品事件。
#[derive(Debug)]
pub(crate) enum NdxEvent {
    /// 一整页目录内容，DFS 顺序产出
    Page(DirPageResult),
    /// 遍历过程中的 per-directory 错误（不中断整棵树）
    Error { path: String, reason: String },
    /// 整棵树遍历完成，对应 rsync `NDX_DONE` = -1
    Done,
}

pub(crate) type DirectoryPages = Receiver<NdxEvent>;

pub(crate) fn directory_pages(storage: &StorageEnum, options: WalkOptions) -> StorageResult<DirectoryPages> {
    let mut session = storage.directory_inventory(&options)?;
    let (tx, rx) = async_channel::bounded(2);
    tokio::spawn(async move {
        let mut files: Vec<ObservedItem> = Vec::new();
        let mut subdirs: Vec<ObservedItem> = Vec::new();
        let mut next = 0i32;
        while let Some(item) = session.next().await {
            let item = match item {
                Ok(item) => item,
                Err(error) => {
                    let _ = tx
                        .send(NdxEvent::Error {
                            path: String::new(),
                            reason: error.to_string(),
                        })
                        .await;
                    return;
                }
            };
            match item {
                TraversalItem::Entry(entry) => {
                    let entry = Arc::new(*entry);
                    if entry.history().is_some() {
                        files.extend(ObservedItem::all(&entry));
                    } else if entry.get_is_dir() {
                        subdirs.push(entry.into());
                    } else {
                        files.push(entry.into());
                    }
                }
                TraversalItem::DirectoryListed(listed) => {
                    files.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                    subdirs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                    let start = next;
                    let mut indexed = |entries: &mut Vec<ObservedItem>| -> Vec<NdxEntry> {
                        entries
                            .drain(..)
                            .map(|entry| {
                                let ndx = next;
                                next = next.saturating_add(1);
                                NdxEntry { ndx, entry }
                            })
                            .collect()
                    };
                    let files = indexed(&mut files);
                    let subdirs = indexed(&mut subdirs);
                    let gap_ndx = next;
                    next = next.saturating_add(1);
                    if tx
                        .send(NdxEvent::Page(DirPageResult {
                            dir_path: listed.path.as_str().into(),
                            ndx_start: start,
                            files,
                            subdirs,
                            gap_ndx,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                TraversalItem::Failure(f) => {
                    if tx
                        .send(NdxEvent::Error {
                            path: f.path.as_str().into(),
                            reason: format!("{:?}: {:?}", f.scope, f.failure.class()),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                TraversalItem::ObservationFailure(f) => {
                    if tx
                        .send(NdxEvent::Error {
                            path: f.path.as_str().into(),
                            reason: format!("{:?}: {:?}", f.family, f.class),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                _ => {}
            }
        }
        match session.finish().await {
            Ok(TraversalOutcome::Completed(_)) => {
                let _ = tx.send(NdxEvent::Done).await;
            }
            result => {
                let _ = tx
                    .send(NdxEvent::Error {
                        path: String::new(),
                        reason: format!("traversal did not complete: {result:?}"),
                    })
                    .await;
            }
        }
    });
    Ok(rx)
}
