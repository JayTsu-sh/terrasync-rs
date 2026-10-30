use crate::connected::path;
use crate::filter::FilterExpression;
use crate::projection::{HistoryProjection, observation_plan};
use crate::{dir_tree::*, error::operation, *};
use chrono::NaiveDate;
use data_mover::{
    DslTraversalFilter,
    model::{EntryKind, EntryOperationFailure, MetadataObservations, MetadataRequirements},
    storage::{
        DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, DeleteTreeTerminalFailure, StorageRoleFailure,
        delete_tree,
    },
    traversal::{
        Checkpoint, Resumable, StorageTraversalSource, TraversalCandidate, TraversalDecision, TraversalFilter,
        TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest, TraversalSession, TraversalSource,
    },
};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

impl ConnectedStorage {
    fn traversal_request(&self, sub: Option<&Path>, options: &WalkOptions, blocks: bool) -> Result<TraversalRequest> {
        let root = path(sub.unwrap_or(Path::new("")))?;
        let mut request = TraversalRequest::new(root)
            .observation_plan(observation_plan(options.include_tags))
            .max_inflight_operations(NonZeroUsize::new(options.concurrency).unwrap_or(NonZeroUsize::MIN))
            .max_depth(options.depth.and_then(NonZeroUsize::new));
        if blocks {
            request = request.order(TraversalOrder::Blocks);
        }
        if options.packaged {
            request = request.filter(Arc::new(PackageFilter::new(options)));
        } else if options.match_expressions.is_some() || options.exclude_expressions.is_some() {
            request = request.filter(Arc::new(DslTraversalFilter::new(
                options.match_expressions.clone(),
                options.exclude_expressions.clone(),
            )));
        }
        Ok(request)
    }
    fn traversal(&self, sub: Option<&Path>, options: &WalkOptions, blocks: bool) -> Result<TraversalSession> {
        Ok(StorageTraversalSource::new(&self.roles)?.traverse(self.traversal_request(sub, options, blocks)?))
    }

    /// 保存检查点前先持久化条目；恢复时可能重放一个目录。
    pub fn resumable_scan(
        &self, options: &WalkOptions, checkpoint: Option<Vec<u8>>, cancel: CancellationToken,
        every: std::time::Duration,
    ) -> Result<ResumableScan> {
        let mut request = self
            .traversal_request(None, options, false)?
            .cancel(cancel)
            .resumable(Resumable::On { every });
        if let Some(bytes) = checkpoint {
            request = request.resume_from(Checkpoint::from_bytes(bytes.into()));
        }
        Ok(ResumableScan {
            session: StorageTraversalSource::new(&self.roles)?.traverse(request),
            connected: self.clone(),
            histories: HistoryProjection::default(),
        })
    }
    pub async fn walkdir(&self, sub: Option<&Path>, options: WalkOptions) -> Result<WalkDirAsyncIterator> {
        let mut session = self.traversal(sub, &options, false)?;
        let package = PackageFilter::new(&options);
        let root = sub.unwrap_or(Path::new("")).to_path_buf();
        let (tx, rx) = async_channel::bounded(128);
        let connected = self.clone();
        tokio::spawn(async move {
            let mut histories = HistoryProjection::default();
            while let Some(item) = session.next_item().await {
                let message = match item {
                    TraversalItem::Entry(entry) => {
                        let projected = match connected.project_history(&entry, &mut histories).await {
                            Ok(entries) => entries,
                            Err(error) => {
                                let _ = tx
                                    .send(StorageEntryMessage::Error {
                                        event: ErrorEvent::Scan,
                                        path: entry.path().as_str().into(),
                                        entry: None,
                                        reason: error.to_string(),
                                    })
                                    .await;
                                return;
                            }
                        };
                        for projected in projected {
                            let relative = projected
                                .get_relative_path()
                                .strip_prefix(&root)
                                .unwrap_or(projected.get_relative_path());
                            let message =
                                if projected.get_is_dir() && package.packaged_path(&relative.to_string_lossy()) {
                                    StorageEntryMessage::Packaged(Arc::new(projected))
                                } else {
                                    StorageEntryMessage::Scanned(Arc::new(projected))
                                };
                            if tx.send(message).await.is_err() {
                                return;
                            }
                        }
                        continue;
                    }
                    TraversalItem::Failure(failure) => StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::from(failure.path.as_str()),
                        entry: None,
                        reason: format!("{:?}: {:?}", failure.scope, failure.failure.class()),
                    },
                    TraversalItem::ObservationFailure(failure) => StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::from(failure.path.as_str()),
                        entry: None,
                        reason: format!("{:?}: {:?}", failure.family, failure.class),
                    },
                    _ => continue,
                };
                if tx.send(message).await.is_err() {
                    return;
                }
            }
            if !histories.is_complete() {
                let _ = tx
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::new(),
                        entry: None,
                        reason: "incomplete object history".into(),
                    })
                    .await;
                return;
            }
            match session.finish().await {
                Ok(TraversalOutcome::Completed(_)) => {}
                result => {
                    let _ = tx
                        .send(StorageEntryMessage::Error {
                            event: ErrorEvent::Scan,
                            path: PathBuf::new(),
                            entry: None,
                            reason: format!("traversal did not complete: {result:?}"),
                        })
                        .await;
                }
            }
        });
        Ok(AsyncReceiver::new(rx))
    }
    pub async fn walkdir_2(
        &self, sub: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize, include_tags: bool,
    ) -> Result<WalkDirAsyncIterator2> {
        let mut session = self.traversal(
            sub,
            &WalkOptions {
                depth,
                match_expressions,
                exclude_expressions,
                concurrency,
                include_tags,
                ..Default::default()
            },
            true,
        )?;
        let (tx, rx) = async_channel::bounded(2);
        let connected = self.clone();
        tokio::spawn(async move {
            let mut histories = HistoryProjection::default();
            let mut files = Vec::new();
            let mut subdirs = Vec::new();
            let mut next = 0i32;
            while let Some(item) = session.next_item().await {
                match item {
                    TraversalItem::Entry(entry) => {
                        let projected = match connected.project_history(&entry, &mut histories).await {
                            Ok(entries) => entries,
                            Err(error) => {
                                let _ = tx
                                    .send(NdxEvent::Error {
                                        path: entry.path().as_str().into(),
                                        reason: error.to_string(),
                                    })
                                    .await;
                                return;
                            }
                        };
                        for entry in projected {
                            let entry = Arc::new(entry);
                            if entry.get_is_dir() {
                                subdirs.push(entry);
                            } else {
                                files.push(entry);
                            }
                        }
                    }
                    TraversalItem::DirectoryListed(listed) => {
                        files.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                        subdirs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                        let start = next;
                        let mut indexed = |entries: &mut Vec<Arc<EntryEnum>>| -> Vec<NdxEntry> {
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
            if !histories.is_complete() {
                let _ = tx
                    .send(NdxEvent::Error {
                        path: String::new(),
                        reason: "incomplete object history".into(),
                    })
                    .await;
                return;
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
        Ok(AsyncReceiver::new(rx))
    }
    pub async fn delete_dir_all_with_progress(
        &self, sub: Option<&Path>, concurrency: usize,
    ) -> Result<AsyncReceiver<DeleteEvent>> {
        let mut session = delete_tree(
            &self.roles,
            DeleteTreeRequest {
                root: path(sub.unwrap_or(Path::new("")))?,
                delete_root: true,
                max_inflight_operations: NonZeroUsize::new(concurrency).unwrap_or(NonZeroUsize::MIN),
                max_buffered_items: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
                cancel: CancellationToken::new(),
            },
        )
        .map_err(StorageError::from)?;
        let (tx, rx) = async_channel::bounded(128);
        tokio::spawn(async move {
            while let Some(item) = session.next_item().await {
                let event = match item {
                    DeleteTreeItem::Deleted { path, kind } => DeleteEvent {
                        relative_path: path.as_str().into(),
                        is_dir: kind == EntryKind::Directory,
                        error: None,
                        failure: None,
                    },
                    DeleteTreeItem::EntryFailure(f) => DeleteEvent {
                        relative_path: f.path().as_str().into(),
                        is_dir: false,
                        error: Some(f.to_string()),
                        failure: Some(DeleteEventFailure::Entry(f)),
                    },
                };
                if tx.send(event).await.is_err() {
                    return;
                }
            }
            let result = session.finish().await;
            if !matches!(&result, Ok(DeleteTreeOutcome::Completed(_))) {
                let _ = tx
                    .send(DeleteEvent {
                        relative_path: PathBuf::new(),
                        is_dir: false,
                        error: Some("recursive deletion did not complete".into()),
                        failure: result.err().map(DeleteEventFailure::Terminal),
                    })
                    .await;
            }
        });
        Ok(AsyncReceiver::new(rx))
    }
    pub async fn delete_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        let events = self
            .delete_dir_all_with_progress(Some(entry.get_relative_path()), 4)
            .await?;
        while let Some(event) = events.next().await {
            if let Some(error) = event.error {
                return Err(match event.failure {
                    Some(DeleteEventFailure::Entry(source)) => StorageRoleFailure::Entry(source).into(),
                    Some(DeleteEventFailure::Terminal(source)) => source.into(),
                    None => operation(error),
                });
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeleteEvent {
    pub relative_path: PathBuf,
    pub is_dir: bool,
    pub error: Option<String>,
    // 仅在适配层内保留原始来源；对外进度消息继续使用原有文本。
    failure: Option<DeleteEventFailure>,
}

#[derive(Debug, Clone)]
enum DeleteEventFailure {
    Entry(EntryOperationFailure),
    Terminal(DeleteTreeTerminalFailure),
}

/// 按交付顺序返回产品条目及其 opaque 遍历检查点。
pub enum ResumableScanItem {
    Entries(Vec<EntryEnum>),
    Checkpoint(Vec<u8>),
}

pub struct ResumableScan {
    session: TraversalSession,
    connected: ConnectedStorage,
    histories: HistoryProjection,
}

impl ResumableScan {
    pub async fn next(&mut self) -> Option<Result<ResumableScanItem>> {
        while let Some(item) = self.session.next_item().await {
            match item {
                TraversalItem::Entry(entry) => {
                    match self.connected.project_history(&entry, &mut self.histories).await {
                        Ok(entries) if entries.is_empty() => continue,
                        Ok(entries) => return Some(Ok(ResumableScanItem::Entries(entries))),
                        Err(error) => return Some(Err(error)),
                    }
                }
                // 尚未完成的历史记录还未交给持久化 sink。
                TraversalItem::Checkpoint(checkpoint) if self.histories.is_complete() => {
                    return Some(Ok(ResumableScanItem::Checkpoint(checkpoint.as_bytes().to_vec())));
                }
                TraversalItem::Failure(failure) => {
                    return Some(Err(StorageError::TraversalEntry {
                        path: failure.path,
                        scope: failure.scope,
                        delivered: failure.delivered,
                        source: failure.failure,
                    }));
                }
                TraversalItem::ObservationFailure(failure) => {
                    return Some(Err(StorageError::TraversalObservation(failure)));
                }
                _ => {}
            }
        }
        None
    }

    pub async fn finish(self) -> Result<TraversalOutcome> {
        if !self.histories.is_complete() {
            return Err(operation("scan ended with an incomplete version history"));
        }
        self.session.finish().await.map_err(StorageError::from)
    }
}
pub type DeleteDirIterator = AsyncReceiver<DeleteEvent>;

/// 打包选择属于产品层；遍历仍由上游按页执行，在选中目录处停止下钻。
#[derive(Debug)]
struct PackageFilter {
    dsl: DslTraversalFilter,
    enabled: bool,
    extra_depth: usize,
    maximum: Option<usize>,
}
impl PackageFilter {
    fn new(options: &WalkOptions) -> Self {
        Self {
            dsl: DslTraversalFilter::new(options.match_expressions.clone(), options.exclude_expressions.clone()),
            enabled: options.packaged && options.match_expressions.is_some(),
            extra_depth: options.package_depth,
            maximum: options.depth.filter(|v| *v > 0),
        }
    }
    fn target_depth(&self, value: &str, is_dir: bool) -> Option<usize> {
        if !self.enabled {
            return None;
        }
        let parts: Vec<_> = value.split('/').filter(|v| !v.is_empty()).collect();
        let directories = parts.len().saturating_sub(usize::from(!is_dir));
        for index in 0..directories {
            if !date_name(parts[index]) {
                continue;
            }
            let ancestor = parts[..=index].join("/");
            let metadata = MetadataObservations::default();
            let candidate = TraversalCandidate {
                path: &ancestor,
                name: parts[index],
                kind: EntryKind::Directory,
                size: None,
                modified: None,
                metadata: &metadata,
            };
            if self.dsl.decide(&candidate).emit {
                let target = index + 1 + self.extra_depth;
                return Some(target);
            }
        }
        None
    }
    fn packaged_path(&self, value: &str) -> bool {
        self.target_depth(value, true) == Some(value.split('/').filter(|v| !v.is_empty()).count())
    }
}
impl TraversalFilter for PackageFilter {
    fn required_metadata(&self) -> MetadataRequirements {
        self.dsl.required_metadata()
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let mut decision = self.dsl.decide(candidate);
        let is_dir = candidate.kind == EntryKind::Directory;
        if let Some(target) = self.target_depth(candidate.path, is_dir) {
            let depth = candidate.path.split('/').filter(|v| !v.is_empty()).count();
            if self.maximum.is_some_and(|maximum| target > maximum) {
                decision.emit = false;
                decision.descend = false;
            } else if depth == target && is_dir {
                decision.descend = false;
            } else {
                decision.emit = false;
            }
        }
        // 必须在每层评估打包深度；普通 DSL 可以提前放行整个子树。
        decision.filter_children = true;
        decision
    }
}
fn date_name(name: &str) -> bool {
    name.char_indices().any(|(start, _)| {
        [(10, "%Y-%m-%d"), (8, "%Y%m%d"), (6, "%y%m%d")]
            .into_iter()
            .any(|(length, format)| {
                name.get(start..start + length)
                    .is_some_and(|value| NaiveDate::parse_from_str(value, format).is_ok())
            })
    })
}
