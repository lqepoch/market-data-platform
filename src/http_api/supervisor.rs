use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinHandle, JoinSet},
};

#[cfg(target_os = "linux")]
use crate::archive::capture_pair_v2::reader::{
    LocalCapturePairV2Reader, VerifiedCapturePairChunkV2,
};
use crate::{
    MarketDataError,
    aggregate::TradeMinuteBarV1,
    cancellation::CancellationToken,
    remote_query::{DatasetNamespace, RemoteArchiveReader, RemoteQuerySummary},
};

use super::QUERY_DEADLINE;

const MAX_ACTIVE_QUERIES: usize = 2;
const QUERY_QUEUE_CAPACITY: usize = 2;
type CancellationRegistry = Arc<Mutex<HashMap<Id, CancellationToken>>>;

pub(super) struct QuerySupervisor {
    shutdown: watch::Sender<bool>,
    owner: Option<JoinHandle<()>>,
    cancellations: CancellationRegistry,
}

#[derive(Clone)]
pub(super) struct QueryClient {
    sender: mpsc::Sender<SupervisorMessage>,
    deadline: std::time::Duration,
}

enum SupervisorMessage {
    Query(QueryJob),
    #[cfg(target_os = "linux")]
    VerifyPair(PairVerifyJob),
}

struct QueryJob {
    namespace: DatasetNamespace,
    dataset_id: String,
    symbol: String,
    cancellation: CancellationToken,
    response: oneshot::Sender<crate::Result<(Vec<TradeMinuteBarV1>, RemoteQuerySummary)>>,
}

#[cfg(target_os = "linux")]
struct PairVerifyJob {
    receipt_name: String,
    cancellation: CancellationToken,
    response: oneshot::Sender<crate::Result<VerifiedCapturePairChunkV2>>,
}

#[derive(Debug)]
pub(super) enum QueryFailure {
    Timeout,
    Unavailable,
    Data(MarketDataError),
}

impl QuerySupervisor {
    pub(super) fn start(
        reader: Arc<RemoteArchiveReader>,
        #[cfg(target_os = "linux")] pair_reader: Option<Arc<LocalCapturePairV2Reader>>,
    ) -> (Self, QueryClient) {
        Self::start_with_deadline(
            reader,
            #[cfg(target_os = "linux")]
            pair_reader,
            QUERY_DEADLINE,
        )
    }

    #[cfg(all(test, feature = "offline-capture-synthetic", target_os = "linux"))]
    pub(super) fn start_with_deadline_for_test(
        reader: Arc<RemoteArchiveReader>,
        #[cfg(target_os = "linux")] pair_reader: Option<Arc<LocalCapturePairV2Reader>>,
        deadline: std::time::Duration,
    ) -> (Self, QueryClient) {
        Self::start_with_deadline(
            reader,
            #[cfg(target_os = "linux")]
            pair_reader,
            deadline,
        )
    }

    fn start_with_deadline(
        reader: Arc<RemoteArchiveReader>,
        #[cfg(target_os = "linux")] pair_reader: Option<Arc<LocalCapturePairV2Reader>>,
        deadline: std::time::Duration,
    ) -> (Self, QueryClient) {
        let (sender, receiver) = mpsc::channel(QUERY_QUEUE_CAPACITY);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let cancellations = Arc::new(Mutex::new(HashMap::new()));
        let owner = tokio::spawn(run_owner(
            receiver,
            shutdown_receiver,
            reader,
            #[cfg(target_os = "linux")]
            pair_reader,
            Arc::clone(&cancellations),
        ));
        (
            Self {
                shutdown,
                owner: Some(owner),
                cancellations,
            },
            QueryClient { sender, deadline },
        )
    }

    pub(super) async fn shutdown(&mut self) -> crate::Result<()> {
        self.shutdown.send_replace(true);
        cancel_registered(&self.cancellations);
        if let Some(owner) = self.owner.take() {
            owner.await.map_err(|_| {
                crate::MarketDataError::Storage(crate::error::StorageFailure::CommandFailed)
            })?;
        }
        Ok(())
    }

    pub(super) fn shutdown_signal(&self) -> watch::Sender<bool> {
        self.shutdown.clone()
    }
}

impl Drop for QuerySupervisor {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        cancel_registered(&self.cancellations);
    }
}

impl QueryClient {
    pub(super) async fn query(
        &self,
        namespace: DatasetNamespace,
        dataset_id: String,
        symbol: String,
    ) -> std::result::Result<(Vec<TradeMinuteBarV1>, RemoteQuerySummary), QueryFailure> {
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (response, receiver) = oneshot::channel();
        let job = QueryJob {
            namespace,
            dataset_id,
            symbol,
            cancellation,
            response,
        };
        if let Err(error) = self.sender.try_send(SupervisorMessage::Query(job)) {
            drop(error);
            tracing::warn!("MDP query supervisor is unavailable or at capacity");
            return Err(QueryFailure::Unavailable);
        }
        match tokio::time::timeout(self.deadline, receiver).await {
            Err(_) => Err(QueryFailure::Timeout),
            Ok(Err(_)) => {
                tracing::error!("MDP query worker ended without a response");
                Err(QueryFailure::Unavailable)
            }
            Ok(Ok(Err(error))) => Err(QueryFailure::Data(error)),
            Ok(Ok(Ok(result))) => Ok(result),
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) async fn verify_pair(
        &self,
        receipt_name: String,
    ) -> std::result::Result<VerifiedCapturePairChunkV2, QueryFailure> {
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (response, receiver) = oneshot::channel();
        let job = PairVerifyJob {
            receipt_name,
            cancellation,
            response,
        };
        if let Err(error) = self.sender.try_send(SupervisorMessage::VerifyPair(job)) {
            drop(error);
            tracing::warn!("MDP query supervisor is unavailable or at capacity");
            return Err(QueryFailure::Unavailable);
        }
        match tokio::time::timeout(self.deadline, receiver).await {
            Err(_) => Err(QueryFailure::Timeout),
            Ok(Err(_)) => {
                tracing::error!("MDP pair verifier ended without a response");
                Err(QueryFailure::Unavailable)
            }
            Ok(Ok(Err(error))) => Err(QueryFailure::Data(error)),
            Ok(Ok(Ok(result))) => Ok(result),
        }
    }
}

#[cfg(test)]
pub(super) struct HeldFullQueryQueue {
    _receiver: mpsc::Receiver<SupervisorMessage>,
}

#[cfg(test)]
impl QueryClient {
    pub(super) fn with_full_queue_for_test() -> (Self, HeldFullQueryQueue) {
        let (sender, receiver) = mpsc::channel(QUERY_QUEUE_CAPACITY);
        for _ in 0..QUERY_QUEUE_CAPACITY {
            let (response, _result) = oneshot::channel();
            assert!(
                sender
                    .try_send(SupervisorMessage::Query(QueryJob {
                        namespace: DatasetNamespace::Diagnostic,
                        dataset_id: "synthetic-dataset".to_owned(),
                        symbol: "QQQ".to_owned(),
                        cancellation: CancellationToken::new(),
                        response,
                    }))
                    .is_ok()
            );
        }
        (
            Self {
                sender,
                deadline: QUERY_DEADLINE,
            },
            HeldFullQueryQueue {
                _receiver: receiver,
            },
        )
    }
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn run_owner(
    mut receiver: mpsc::Receiver<SupervisorMessage>,
    mut shutdown: watch::Receiver<bool>,
    reader: Arc<RemoteArchiveReader>,
    #[cfg(target_os = "linux")] pair_reader: Option<Arc<LocalCapturePairV2Reader>>,
    cancellations: CancellationRegistry,
) {
    let mut active = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                cancel_and_join(&mut active, &cancellations, &mut receiver).await;
                return;
            }
            message = receiver.recv(), if active.len() < MAX_ACTIVE_QUERIES => {
                match message {
                    Some(SupervisorMessage::Query(job)) => {
                        let QueryJob { namespace, dataset_id, symbol, cancellation, response } = job;
                        let job_cancellation = cancellation.clone();
                        let job_reader = Arc::clone(&reader);
                        let abort = active.spawn_blocking(move || {
                            let result = job_reader.query_bars_cancellable(
                                namespace,
                                &dataset_id,
                                Some(&symbol),
                                &job_cancellation,
                            );
                            let _ = response.send(result);
                        });
                        registry_lock(&cancellations).insert(abort.id(), cancellation);
                    }
                    #[cfg(target_os = "linux")]
                    Some(SupervisorMessage::VerifyPair(job)) => {
                        let PairVerifyJob { receipt_name, cancellation, response } = job;
                        let job_cancellation = cancellation.clone();
                        let job_reader = pair_reader.clone();
                        let abort = active.spawn_blocking(move || {
                            let result = job_reader.ok_or(MarketDataError::PublicationNotAuthorized)
                                .and_then(|reader| reader.verify_chunk_cancellable(
                                    &receipt_name,
                                    &job_cancellation,
                                ));
                            let _ = response.send(result);
                        });
                        registry_lock(&cancellations).insert(abort.id(), cancellation);
                    }
                    None => {
                        cancel_and_join(&mut active, &cancellations, &mut receiver).await;
                        return;
                    }
                }
            }
            joined = active.join_next_with_id(), if !active.is_empty() => {
                remove_finished(joined, &cancellations);
            }
        }
    }
}

fn remove_finished(
    joined: Option<std::result::Result<(Id, ()), tokio::task::JoinError>>,
    cancellations: &CancellationRegistry,
) {
    match joined {
        Some(Ok((id, ()))) => {
            registry_lock(cancellations).remove(&id);
        }
        Some(Err(error)) => {
            tracing::error!(error = %error, "MDP query worker task failed");
            if let Some(cancellation) = registry_lock(cancellations).remove(&error.id()) {
                cancellation.cancel();
            }
        }
        None => {}
    }
}

async fn cancel_and_join(
    active: &mut JoinSet<()>,
    cancellations: &CancellationRegistry,
    receiver: &mut mpsc::Receiver<SupervisorMessage>,
) {
    for cancellation in cancellation_snapshot(cancellations) {
        cancellation.cancel();
    }
    while let Some(joined) = active.join_next_with_id().await {
        remove_finished(Some(joined), cancellations);
    }
    while let Ok(message) = receiver.try_recv() {
        drop(message);
    }
    registry_lock(cancellations).clear();
}

fn registry_lock(
    registry: &CancellationRegistry,
) -> MutexGuard<'_, HashMap<Id, CancellationToken>> {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn cancellation_snapshot(registry: &CancellationRegistry) -> Vec<CancellationToken> {
    registry_lock(registry).values().cloned().collect()
}

fn cancel_registered(registry: &CancellationRegistry) {
    for cancellation in cancellation_snapshot(registry) {
        cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn saturated_query_queue_rejects_excess_work_without_waiting() {
        let (sender, mut receiver) = mpsc::channel(QUERY_QUEUE_CAPACITY);
        let client = QueryClient {
            sender: sender.clone(),
            deadline: QUERY_DEADLINE,
        };
        for _ in 0..QUERY_QUEUE_CAPACITY {
            let (response, _receiver) = oneshot::channel();
            assert!(
                sender
                    .try_send(SupervisorMessage::Query(QueryJob {
                        namespace: DatasetNamespace::Diagnostic,
                        dataset_id: "synthetic-dataset".to_owned(),
                        symbol: "QQQ".to_owned(),
                        cancellation: CancellationToken::new(),
                        response,
                    }))
                    .is_ok()
            );
        }

        let rejected = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            client.query(
                DatasetNamespace::Diagnostic,
                "synthetic-dataset".to_owned(),
                "QQQ".to_owned(),
            ),
        )
        .await
        .expect("a full bounded queue must reject immediately");
        assert!(matches!(rejected, Err(QueryFailure::Unavailable)));

        drop(sender);
        while receiver.try_recv().is_ok() {}
    }

    #[tokio::test]
    async fn dropping_supervisor_cancels_registered_work_and_signals_owner() {
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(async {});
        let task_id = task.id();
        task.await.unwrap();
        let cancellation = CancellationToken::new();
        let cancellations = Arc::new(Mutex::new(HashMap::from([(task_id, cancellation.clone())])));
        let supervisor = QuerySupervisor {
            shutdown,
            owner: None,
            cancellations,
        };

        drop(supervisor);

        assert!(cancellation.is_cancelled());
        assert!(*shutdown_receiver.borrow());
    }
}
