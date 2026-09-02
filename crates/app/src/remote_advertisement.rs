//! 双进程文件列表的产品侧 projection。
//!
//! data-mover 只产生 bounded `TraversalSession`；本模块分配 session-local NDX、分页
//! opaque snapshot，并把 entry failure 与唯一 terminal evidence 投影到 transport wire。

use std::collections::HashMap;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use data_mover::model::{BackendSessionFailure, EntryOperationFailure, ObservedEntry};
use data_mover::traversal::{TraversalItem, TraversalOutcome, TraversalSession, TraversalTerminalFailure};
use transport::message::{
    AdvertisedObservation, AdvertisementEvent, AdvertisementFailure, AdvertisementFailureScope, AdvertisementTerminal,
    ObservationPage, SessionNdx,
};
use transport::traits::SenderTransport;

use crate::error::{AppError, Result};

#[async_trait]
pub trait RemoteAdvertisementSink: Send {
    async fn send(&mut self, event: AdvertisementEvent) -> Result<()>;
}

/// 把 target advertisement 事件写入现有 transport 的薄 adapter。
pub struct TransportAdvertisementSink<'a> {
    transport: &'a dyn SenderTransport,
}

impl<'a> TransportAdvertisementSink<'a> {
    #[must_use]
    pub const fn new(transport: &'a dyn SenderTransport) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl RemoteAdvertisementSink for TransportAdvertisementSink<'_> {
    async fn send(&mut self, event: AdvertisementEvent) -> Result<()> {
        self.transport
            .send(transport::message::SenderMsg::Advertisement(event))
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RemoteAdvertisement {
    entries: HashMap<SessionNdx, ObservedEntry>,
    failures: Vec<AdvertisementFailure>,
    pub page_count: u64,
    pub observed_entries: u64,
    pub entry_failures: u64,
}

impl RemoteAdvertisement {
    #[must_use]
    pub fn get(&self, ndx: SessionNdx) -> Option<&ObservedEntry> {
        self.entries.get(&ndx)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn failures(&self) -> &[AdvertisementFailure] {
        &self.failures
    }
}

/// Receiver 每接收一个 wire event 后产生的有界增量结果。
pub enum AdvertisementReceipt {
    Page(ReceivedObservationPage),
    EntryFailure(AdvertisementFailure),
    Completed(RemoteAdvertisementSummary),
}

/// 已验证并解码的一页 observation；消费后即可释放，不需要等待整个 `FileList`。
pub struct ReceivedObservationPage {
    pub sequence: u64,
    pub entries: Vec<(SessionNdx, ObservedEntry)>,
}

/// 完整 advertisement 的终态计数证据。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteAdvertisementSummary {
    pub page_count: u64,
    pub observed_entries: u64,
    pub entry_failures: u64,
}

/// Receiver 侧的增量 validator；只保留页号、NDX 与计数，不访问 storage backend。
pub struct RemoteAdvertisementReceiver {
    maximum_page_entries: usize,
    expected_page: u64,
    expected_ndx: u64,
    entry_failures: u64,
    terminated: bool,
}

impl RemoteAdvertisementReceiver {
    #[must_use]
    pub fn new(maximum_page_entries: NonZeroUsize) -> Self {
        Self {
            maximum_page_entries: maximum_page_entries.get(),
            expected_page: 0,
            expected_ndx: 0,
            entry_failures: 0,
            terminated: false,
        }
    }

    /// 消费一个有序 wire event，并立即返回该页或失败的有界 projection。
    ///
    /// # Errors
    /// event 在 terminal 之后到达，或违反 page/NDX/snapshot/terminal 合同时返回错误。
    pub fn accept(&mut self, event: AdvertisementEvent) -> Result<AdvertisementReceipt> {
        if self.terminated {
            return Err(protocol_error("event received after terminal".to_string()));
        }
        match event {
            AdvertisementEvent::Page(page) => {
                if page.sequence != self.expected_page {
                    return Err(protocol_error(format!(
                        "expected page {}, received {}",
                        self.expected_page, page.sequence
                    )));
                }
                if page.entries.is_empty() || page.entries.len() > self.maximum_page_entries {
                    return Err(protocol_error(format!(
                        "page {} contains {} entries, allowed range is 1..={}",
                        page.sequence,
                        page.entries.len(),
                        self.maximum_page_entries
                    )));
                }
                let mut entries = Vec::with_capacity(page.entries.len());
                for advertised in page.entries {
                    if advertised.ndx != SessionNdx(self.expected_ndx) {
                        return Err(protocol_error(format!(
                            "expected session NDX {}, received {}",
                            self.expected_ndx, advertised.ndx.0
                        )));
                    }
                    let entry = ObservedEntry::decode_snapshot(&advertised.snapshot)
                        .map_err(|error| protocol_error(error.to_string()))?;
                    entries.push((advertised.ndx, entry));
                    self.expected_ndx = self
                        .expected_ndx
                        .checked_add(1)
                        .ok_or_else(|| protocol_error("session NDX exhausted".to_string()))?;
                }
                self.expected_page = self
                    .expected_page
                    .checked_add(1)
                    .ok_or_else(|| protocol_error("page sequence exhausted".to_string()))?;
                Ok(AdvertisementReceipt::Page(ReceivedObservationPage {
                    sequence: page.sequence,
                    entries,
                }))
            }
            AdvertisementEvent::EntryFailure(failure) => {
                if failure.scope != AdvertisementFailureScope::Entry {
                    return Err(protocol_error(
                        "entry-failure event did not have Entry scope".to_string(),
                    ));
                }
                self.entry_failures = self
                    .entry_failures
                    .checked_add(1)
                    .ok_or_else(|| protocol_error("entry-failure count exhausted".to_string()))?;
                Ok(AdvertisementReceipt::EntryFailure(failure))
            }
            AdvertisementEvent::Terminal(terminal) => {
                self.terminated = true;
                let observed_entries = self.expected_ndx;
                let entry_failures = self.entry_failures;
                match terminal {
                    AdvertisementTerminal::Completed {
                        observed_entries: reported_entries,
                        entry_failures: reported_failures,
                    } => {
                        validate_counts(observed_entries, entry_failures, reported_entries, reported_failures)?;
                        Ok(AdvertisementReceipt::Completed(RemoteAdvertisementSummary {
                            page_count: self.expected_page,
                            observed_entries,
                            entry_failures,
                        }))
                    }
                    AdvertisementTerminal::Cancelled {
                        observed_entries: reported_entries,
                        entry_failures: reported_failures,
                    } => {
                        validate_counts(observed_entries, entry_failures, reported_entries, reported_failures)?;
                        Err(AppError::RemoteAdvertisementCancelled {
                            observed_entries,
                            entry_failures,
                        })
                    }
                    AdvertisementTerminal::Failed {
                        observed_entries: reported_entries,
                        entry_failures: reported_failures,
                        failure,
                    } => {
                        validate_counts(observed_entries, entry_failures, reported_entries, reported_failures)?;
                        Err(AppError::RemoteAdvertisementPeerFailed {
                            diagnostic: failure.diagnostic,
                        })
                    }
                }
            }
        }
    }

    /// 接收一个完整 advertisement event 流并返回可供后续 transfer correlation 使用的 ledger。
    ///
    /// # Errors
    /// wire 顺序、snapshot、计数或 terminal 不合法时返回 typed protocol error。
    pub fn receive<I>(mut self, events: I) -> Result<RemoteAdvertisement>
    where
        I: IntoIterator<Item = AdvertisementEvent>,
    {
        let mut entries = HashMap::new();
        let mut failures = Vec::new();
        let mut events = events.into_iter();
        while let Some(event) = events.next() {
            match self.accept(event)? {
                AdvertisementReceipt::Page(page) => {
                    entries.extend(page.entries);
                }
                AdvertisementReceipt::EntryFailure(failure) => failures.push(failure),
                AdvertisementReceipt::Completed(summary) => {
                    if events.next().is_some() {
                        return Err(protocol_error("event received after terminal".to_string()));
                    }
                    return Ok(RemoteAdvertisement {
                        entries,
                        failures,
                        page_count: summary.page_count,
                        observed_entries: summary.observed_entries,
                        entry_failures: summary.entry_failures,
                    });
                }
            }
        }
        Err(protocol_error("advertisement ended without terminal".to_string()))
    }
}

fn validate_counts(
    observed_entries: u64, entry_failures: u64, reported_entries: u64, reported_failures: u64,
) -> Result<()> {
    if observed_entries == reported_entries && entry_failures == reported_failures {
        return Ok(());
    }
    Err(AppError::RemoteAdvertisementCountMismatch {
        projected_entries: observed_entries,
        projected_failures: entry_failures,
        reported_entries,
        reported_failures,
    })
}

fn protocol_error(reason: String) -> AppError {
    AppError::RemoteAdvertisementProtocol { reason }
}

/// 将 traversal 投影成 remote advertisement。
///
/// # Errors
/// sink、traversal terminal、取消或 completion evidence 不一致时返回 typed error。
pub async fn advertise_remote(
    mut session: TraversalSession, page_size: NonZeroUsize, sink: &mut dyn RemoteAdvertisementSink,
) -> Result<RemoteAdvertisement> {
    let mut projection = AdvertisementProjection::new(page_size.get());
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                if let Some(page) = projection.push_entry(*entry)? {
                    sink.send(AdvertisementEvent::Page(page)).await?;
                }
            }
            TraversalItem::EntryFailure(failure) => {
                if let Some(page) = projection.take_page() {
                    sink.send(AdvertisementEvent::Page(page)).await?;
                }
                projection.entry_failures += 1;
                let failure = project_entry_failure(&failure);
                projection.failures.push(failure.clone());
                sink.send(AdvertisementEvent::EntryFailure(failure)).await?;
            }
        }
    }
    if let Some(page) = projection.take_page() {
        sink.send(AdvertisementEvent::Page(page)).await?;
    }

    match session.finish().await {
        Ok(TraversalOutcome::Completed(completion)) => {
            if projection.observed_entries != completion.observed_entries
                || projection.entry_failures != completion.entry_failures
            {
                let error = AppError::RemoteAdvertisementCountMismatch {
                    projected_entries: projection.observed_entries,
                    projected_failures: projection.entry_failures,
                    reported_entries: completion.observed_entries,
                    reported_failures: completion.entry_failures,
                };
                sink.send(AdvertisementEvent::Terminal(AdvertisementTerminal::Failed {
                    observed_entries: projection.observed_entries,
                    entry_failures: projection.entry_failures,
                    failure: runtime_failure(error.to_string()),
                }))
                .await?;
                return Err(error);
            }
            sink.send(AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                observed_entries: projection.observed_entries,
                entry_failures: projection.entry_failures,
            }))
            .await?;
            Ok(projection.finish())
        }
        Ok(TraversalOutcome::Cancelled) => {
            sink.send(AdvertisementEvent::Terminal(AdvertisementTerminal::Cancelled {
                observed_entries: projection.observed_entries,
                entry_failures: projection.entry_failures,
            }))
            .await?;
            Err(AppError::RemoteAdvertisementCancelled {
                observed_entries: projection.observed_entries,
                entry_failures: projection.entry_failures,
            })
        }
        Err(error) => {
            sink.send(AdvertisementEvent::Terminal(AdvertisementTerminal::Failed {
                observed_entries: projection.observed_entries,
                entry_failures: projection.entry_failures,
                failure: project_terminal_failure(&error),
            }))
            .await?;
            Err(error.into())
        }
    }
}

struct AdvertisementProjection {
    page_size: usize,
    next_page: u64,
    pending: Vec<AdvertisedObservation>,
    entries: HashMap<SessionNdx, ObservedEntry>,
    failures: Vec<AdvertisementFailure>,
    observed_entries: u64,
    entry_failures: u64,
}

impl AdvertisementProjection {
    fn new(page_size: usize) -> Self {
        Self {
            page_size,
            next_page: 0,
            pending: Vec::with_capacity(page_size),
            entries: HashMap::new(),
            failures: Vec::new(),
            observed_entries: 0,
            entry_failures: 0,
        }
    }

    fn push_entry(&mut self, entry: ObservedEntry) -> Result<Option<ObservationPage>> {
        let ndx = SessionNdx(self.observed_entries);
        self.observed_entries = self
            .observed_entries
            .checked_add(1)
            .ok_or_else(|| AppError::CopyError("remote advertisement NDX exhausted".to_string()))?;
        self.pending.push(AdvertisedObservation {
            ndx,
            snapshot: entry.encode_snapshot().as_bytes().to_vec(),
        });
        self.entries.insert(ndx, entry);
        Ok((self.pending.len() == self.page_size).then(|| self.build_page()))
    }

    fn take_page(&mut self) -> Option<ObservationPage> {
        (!self.pending.is_empty()).then(|| self.build_page())
    }

    fn build_page(&mut self) -> ObservationPage {
        let page = ObservationPage {
            sequence: self.next_page,
            entries: std::mem::replace(&mut self.pending, Vec::with_capacity(self.page_size)),
        };
        self.next_page += 1;
        page
    }

    fn finish(self) -> RemoteAdvertisement {
        RemoteAdvertisement {
            entries: self.entries,
            failures: self.failures,
            page_count: self.next_page,
            observed_entries: self.observed_entries,
            entry_failures: self.entry_failures,
        }
    }
}

fn project_entry_failure(failure: &EntryOperationFailure) -> AdvertisementFailure {
    AdvertisementFailure {
        scope: AdvertisementFailureScope::Entry,
        path: Some(failure.path().as_str().to_string()),
        identity: failure
            .identity()
            .map(data_mover::model::EntryFailureIdentity::as_bytes),
        operation: Some(format!("{:?}", failure.operation())),
        class: Some(format!("{:?}", failure.class())),
        transience: Some(format!("{:?}", failure.transience())),
        diagnostic: failure.diagnostic().to_string(),
    }
}

fn project_terminal_failure(failure: &TraversalTerminalFailure) -> AdvertisementFailure {
    match failure {
        TraversalTerminalFailure::Session(failure) => project_backend_failure(failure),
        TraversalTerminalFailure::Internal => runtime_failure("traversal runtime failed".to_string()),
    }
}

fn project_backend_failure(failure: &BackendSessionFailure) -> AdvertisementFailure {
    AdvertisementFailure {
        scope: AdvertisementFailureScope::BackendSession,
        path: None,
        identity: None,
        operation: Some(format!("{:?}", failure.operation())),
        class: Some(format!("{:?}", failure.class())),
        transience: Some(format!("{:?}", failure.transience())),
        diagnostic: failure.diagnostic().to_string(),
    }
}

fn runtime_failure(diagnostic: String) -> AdvertisementFailure {
    AdvertisementFailure {
        scope: AdvertisementFailureScope::Runtime,
        path: None,
        identity: None,
        operation: None,
        class: None,
        transience: None,
        diagnostic,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
        StoragePath, Transience,
    };
    use data_mover::traversal::{TraversalCompletion, TraversalItem, TraversalOutcome, TraversalTerminalFailure};
    use tokio_util::sync::CancellationToken;
    use transport::message::{AdvertisementFailureScope, AdvertisementTerminal};
    use transport::traits::ReceiverTransport;

    use super::*;

    #[derive(Default)]
    struct Sink {
        events: Arc<Mutex<Vec<AdvertisementEvent>>>,
    }

    #[async_trait]
    impl RemoteAdvertisementSink for Sink {
        async fn send(&mut self, event: AdvertisementEvent) -> Result<()> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn entry(path: &str, size: u64) -> TraversalItem {
        let backend = BackendIdentity::new(BackendKind::Local, "remote-fixture").unwrap();
        let identity = SourceIdentity::new(backend, IdentityStrength::PathScoped, path).unwrap();
        TraversalItem::Entry(Box::new(
            ObservedEntry::new(
                StoragePath::new(path).unwrap(),
                EntryKind::File,
                Some(size),
                None,
                identity,
            )
            .unwrap(),
        ))
    }

    fn failure(path: &str) -> TraversalItem {
        TraversalItem::EntryFailure(
            data_mover::model::EntryOperationFailure::new(
                StoragePath::new(path).unwrap(),
                Operation::Observe,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn pages_opaque_snapshots_and_assigns_session_local_indexes() {
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        let producer_task = tokio::spawn(async move {
            for (path, size) in [("a", 1), ("b", 2), ("c", 3)] {
                producer.send(entry(path, size)).await.unwrap();
            }
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 3,
                entry_failures: 0,
            })));
        });
        let mut sink = Sink::default();

        let result = advertise_remote(session, NonZeroUsize::new(2).unwrap(), &mut sink)
            .await
            .unwrap();
        producer_task.await.unwrap();

        assert_eq!(result.page_count, 2);
        assert_eq!(result.len(), 3);
        assert_eq!(result.get(SessionNdx(0)).unwrap().path().as_str(), "a");
        let events = sink.events.lock().unwrap();
        let AdvertisementEvent::Page(first) = &events[0] else {
            panic!("expected first page")
        };
        assert_eq!(first.sequence, 0);
        assert_eq!(first.entries.len(), 2);
        assert_eq!(first.entries[0].ndx, SessionNdx(0));
        assert_eq!(
            ObservedEntry::decode_snapshot(&first.entries[0].snapshot)
                .unwrap()
                .path()
                .as_str(),
            "a"
        );
        assert!(matches!(
            events.last(),
            Some(AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                observed_entries: 3,
                entry_failures: 0
            }))
        ));
    }

    #[tokio::test]
    async fn entry_failure_is_ordered_and_does_not_hide_completion() {
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(2).unwrap(), CancellationToken::new());
        let producer_task = tokio::spawn(async move {
            producer.send(entry("before", 1)).await.unwrap();
            producer.send(failure("denied")).await.unwrap();
            producer.send(entry("after", 2)).await.unwrap();
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 2,
                entry_failures: 1,
            })));
        });
        let mut sink = Sink::default();

        let result = advertise_remote(session, NonZeroUsize::new(8).unwrap(), &mut sink)
            .await
            .unwrap();
        producer_task.await.unwrap();

        assert_eq!(result.observed_entries, 2);
        assert_eq!(result.entry_failures, 1);
        assert_eq!(result.failures().len(), 1);
        let events = sink.events.lock().unwrap();
        assert!(matches!(&events[0], AdvertisementEvent::Page(page) if page.entries.len() == 1));
        assert!(matches!(
            &events[1],
            AdvertisementEvent::EntryFailure(failure)
                if failure.scope == AdvertisementFailureScope::Entry
                    && failure.path.as_deref() == Some("denied")
        ));
        assert!(matches!(&events[2], AdvertisementEvent::Page(page) if page.entries.len() == 1));
        assert!(matches!(
            &events[3],
            AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                observed_entries: 2,
                entry_failures: 1
            })
        ));
    }

    #[tokio::test]
    async fn cancellation_emits_a_non_success_terminal() {
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        producer.finish(Ok(TraversalOutcome::Cancelled));
        let mut sink = Sink::default();

        let error = advertise_remote(session, NonZeroUsize::new(1).unwrap(), &mut sink)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::error::AppError::RemoteAdvertisementCancelled {
                observed_entries: 0,
                entry_failures: 0
            }
        ));
        assert!(matches!(
            sink.events.lock().unwrap().last(),
            Some(AdvertisementEvent::Terminal(AdvertisementTerminal::Cancelled { .. }))
        ));
    }

    #[tokio::test]
    async fn backend_session_failure_is_terminal_and_keeps_failure_scope() {
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        producer.finish(Err(TraversalTerminalFailure::Session(
            data_mover::model::BackendSessionFailure::new(
                Operation::Traverse,
                FailureClass::Connectivity,
                Transience::Transient,
                "session unavailable",
            )
            .unwrap(),
        )));
        let mut sink = Sink::default();

        let error = advertise_remote(session, NonZeroUsize::new(1).unwrap(), &mut sink)
            .await
            .unwrap_err();

        assert!(matches!(error, crate::error::AppError::TraversalTerminal(_)));
        assert!(matches!(
            sink.events.lock().unwrap().last(),
            Some(AdvertisementEvent::Terminal(AdvertisementTerminal::Failed { failure, .. }))
                if failure.scope == AdvertisementFailureScope::BackendSession
                    && failure.path.is_none()
        ));
    }

    #[tokio::test]
    async fn inconsistent_completion_evidence_sends_failed_terminal() {
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 1,
            entry_failures: 0,
        })));
        let mut sink = Sink::default();

        let error = advertise_remote(session, NonZeroUsize::new(1).unwrap(), &mut sink)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::error::AppError::RemoteAdvertisementCountMismatch { .. }
        ));
        assert!(matches!(
            sink.events.lock().unwrap().last(),
            Some(AdvertisementEvent::Terminal(AdvertisementTerminal::Failed { failure, .. }))
                if failure.scope == AdvertisementFailureScope::Runtime
        ));
    }

    #[tokio::test]
    async fn bounded_transport_applies_backpressure_until_each_page_is_received() {
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), CancellationToken::new());
        let producer_task = tokio::spawn(async move {
            producer.send(entry("a", 1)).await.unwrap();
            producer.send(entry("b", 2)).await.unwrap();
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 2,
                entry_failures: 0,
            })));
        });
        let (sender, receiver) = transport::in_process::create_in_process_pair_with_capacity(1);
        let advertise_task = tokio::spawn(async move {
            let mut sink = TransportAdvertisementSink::new(&sender);
            advertise_remote(session, NonZeroUsize::new(1).unwrap(), &mut sink).await
        });

        tokio::task::yield_now().await;
        assert!(!advertise_task.is_finished());
        assert!(matches!(
            receiver.recv().await,
            Some(transport::message::SenderMsg::Advertisement(AdvertisementEvent::Page(page)))
                if page.sequence == 0
        ));
        assert!(!advertise_task.is_finished());
        assert!(matches!(
            receiver.recv().await,
            Some(transport::message::SenderMsg::Advertisement(AdvertisementEvent::Page(page)))
                if page.sequence == 1
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(transport::message::SenderMsg::Advertisement(
                AdvertisementEvent::Terminal(AdvertisementTerminal::Completed { .. })
            ))
        ));
        let result = advertise_task.await.unwrap().unwrap();
        producer_task.await.unwrap();
        assert_eq!(result.observed_entries, 2);
    }

    #[test]
    fn receiver_reconstructs_only_from_opaque_snapshots_and_validates_terminal() {
        let a = match entry("a", 1) {
            TraversalItem::Entry(entry) => *entry,
            TraversalItem::EntryFailure(_) => unreachable!(),
        };
        let b = match entry("b", 2) {
            TraversalItem::Entry(entry) => *entry,
            TraversalItem::EntryFailure(_) => unreachable!(),
        };
        let events = vec![
            AdvertisementEvent::Page(ObservationPage {
                sequence: 0,
                entries: vec![
                    AdvertisedObservation {
                        ndx: SessionNdx(0),
                        snapshot: a.encode_snapshot().as_bytes().to_vec(),
                    },
                    AdvertisedObservation {
                        ndx: SessionNdx(1),
                        snapshot: b.encode_snapshot().as_bytes().to_vec(),
                    },
                ],
            }),
            AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                observed_entries: 2,
                entry_failures: 0,
            }),
        ];

        let received = RemoteAdvertisementReceiver::new(NonZeroUsize::new(2).unwrap())
            .receive(events)
            .unwrap();

        assert_eq!(received.get(SessionNdx(0)).unwrap(), &a);
        assert_eq!(received.get(SessionNdx(1)).unwrap(), &b);
        assert!(received.failures().is_empty());
    }

    #[test]
    fn receiver_yields_a_bounded_page_before_file_list_terminal() {
        let observed = match entry("early", 1) {
            TraversalItem::Entry(entry) => *entry,
            TraversalItem::EntryFailure(_) => unreachable!(),
        };
        let mut receiver = RemoteAdvertisementReceiver::new(NonZeroUsize::new(1).unwrap());

        let receipt = receiver
            .accept(AdvertisementEvent::Page(ObservationPage {
                sequence: 0,
                entries: vec![AdvertisedObservation {
                    ndx: SessionNdx(0),
                    snapshot: observed.encode_snapshot().as_bytes().to_vec(),
                }],
            }))
            .unwrap();

        let AdvertisementReceipt::Page(page) = receipt else {
            panic!("expected an immediately consumable page")
        };
        assert_eq!(page.entries[0].1, observed);
        assert!(matches!(
            receiver
                .accept(AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                    observed_entries: 1,
                    entry_failures: 0,
                }))
                .unwrap(),
            AdvertisementReceipt::Completed(RemoteAdvertisementSummary {
                page_count: 1,
                observed_entries: 1,
                entry_failures: 0
            })
        ));
    }

    #[test]
    fn receiver_rejects_invalid_snapshot_sequence_and_completion_evidence() {
        let observed = match entry("a", 1) {
            TraversalItem::Entry(entry) => *entry,
            TraversalItem::EntryFailure(_) => unreachable!(),
        };
        let valid = AdvertisedObservation {
            ndx: SessionNdx(0),
            snapshot: observed.encode_snapshot().as_bytes().to_vec(),
        };
        let receiver = || RemoteAdvertisementReceiver::new(NonZeroUsize::new(2).unwrap());

        let malformed = receiver()
            .receive([
                AdvertisementEvent::Page(ObservationPage {
                    sequence: 0,
                    entries: vec![AdvertisedObservation {
                        ndx: SessionNdx(0),
                        snapshot: vec![1, 2, 3],
                    }],
                }),
                AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                    observed_entries: 1,
                    entry_failures: 0,
                }),
            ])
            .unwrap_err();
        assert!(matches!(
            malformed,
            crate::error::AppError::RemoteAdvertisementProtocol { .. }
        ));

        let out_of_sequence = receiver()
            .receive([
                AdvertisementEvent::Page(ObservationPage {
                    sequence: 1,
                    entries: vec![valid.clone()],
                }),
                AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                    observed_entries: 1,
                    entry_failures: 0,
                }),
            ])
            .unwrap_err();
        assert!(matches!(
            out_of_sequence,
            crate::error::AppError::RemoteAdvertisementProtocol { .. }
        ));

        let false_completion = receiver()
            .receive([
                AdvertisementEvent::Page(ObservationPage {
                    sequence: 0,
                    entries: vec![valid.clone()],
                }),
                AdvertisementEvent::Terminal(AdvertisementTerminal::Completed {
                    observed_entries: 2,
                    entry_failures: 0,
                }),
            ])
            .unwrap_err();
        assert!(matches!(
            false_completion,
            crate::error::AppError::RemoteAdvertisementCountMismatch { .. }
        ));

        let missing_terminal = receiver()
            .receive([AdvertisementEvent::Page(ObservationPage {
                sequence: 0,
                entries: vec![valid],
            })])
            .unwrap_err();
        assert!(matches!(
            missing_terminal,
            crate::error::AppError::RemoteAdvertisementProtocol { .. }
        ));
    }
}
