use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
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
    MarketBars(MarketBarsJob),
    #[cfg(target_os = "linux")]
    VerifyPair(PairVerifyJob),
}

type MarketBarsFuture =
    Pin<Box<dyn Future<Output = crate::Result<market_contracts::BarsResponseV1>> + Send + 'static>>;
type MarketBarsExecutor = Box<dyn FnOnce(CancellationToken) -> MarketBarsFuture + Send + 'static>;

struct QueryJob {
    namespace: DatasetNamespace,
    dataset_id: String,
    symbol: String,
    cancellation: CancellationToken,
    response: oneshot::Sender<crate::Result<(Vec<TradeMinuteBarV1>, RemoteQuerySummary)>>,
}

struct MarketBarsJob {
    cancellation: CancellationToken,
    execute: MarketBarsExecutor,
    response: oneshot::Sender<crate::Result<market_contracts::BarsResponseV1>>,
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

    /// Runs preflight, provider fetch, and page planning through the shared bounded supervisor.
    /// Preflight and planning are joined blocking stages. The provider future must use
    /// cancellation-safe async I/O and must not detach child tasks; any blocking operation
    /// belongs in one of the joined closures below. The planner owns provider-error mapping so
    /// it can return either an explicit partial response or the precise data error for HTTP.
    pub(super) async fn market_bars<Prepare, Prepared, P, Fut, Raw, ProviderError, Plan>(
        &self,
        prepare: Prepare,
        provider: P,
        planner: Plan,
    ) -> std::result::Result<market_contracts::BarsResponseV1, QueryFailure>
    where
        Prepare: FnOnce(CancellationToken) -> crate::Result<Prepared> + Send + 'static,
        Prepared: Send + Sync + 'static,
        P: FnOnce(Arc<Prepared>, CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<Raw, ProviderError>> + Send + 'static,
        Raw: Send + 'static,
        ProviderError: Send + 'static,
        Plan: FnOnce(
                Arc<Prepared>,
                std::result::Result<Raw, ProviderError>,
                CancellationToken,
            ) -> crate::Result<market_contracts::BarsResponseV1>
            + Send
            + 'static,
    {
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (response, receiver) = oneshot::channel();
        let job = MarketBarsJob {
            cancellation,
            execute: Box::new(move |cancellation| {
                Box::pin(execute_market_bars(
                    prepare,
                    provider,
                    planner,
                    cancellation,
                ))
            }),
            response,
        };
        if let Err(error) = self.sender.try_send(SupervisorMessage::MarketBars(job)) {
            drop(error);
            tracing::warn!("MDP query supervisor is unavailable or at capacity");
            return Err(QueryFailure::Unavailable);
        }
        match tokio::time::timeout(self.deadline, receiver).await {
            Err(_) => Err(QueryFailure::Timeout),
            Ok(Err(_)) => {
                tracing::error!("MDP market-bars worker ended without a response");
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
                    Some(SupervisorMessage::MarketBars(job)) => {
                        let MarketBarsJob { cancellation, execute, response } = job;
                        let job_cancellation = cancellation.clone();
                        let abort = active.spawn(async move {
                            let result = execute(job_cancellation.clone()).await;
                            if !job_cancellation.is_cancelled() {
                                let _ = response.send(result);
                            }
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

async fn execute_market_bars<Prepare, Prepared, P, Fut, Raw, ProviderError, Plan>(
    prepare: Prepare,
    provider: P,
    planner: Plan,
    cancellation: CancellationToken,
) -> crate::Result<market_contracts::BarsResponseV1>
where
    Prepare: FnOnce(CancellationToken) -> crate::Result<Prepared> + Send + 'static,
    Prepared: Send + Sync + 'static,
    P: FnOnce(Arc<Prepared>, CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = std::result::Result<Raw, ProviderError>> + Send + 'static,
    Raw: Send + 'static,
    ProviderError: Send + 'static,
    Plan: FnOnce(
            Arc<Prepared>,
            std::result::Result<Raw, ProviderError>,
            CancellationToken,
        ) -> crate::Result<market_contracts::BarsResponseV1>
        + Send
        + 'static,
{
    if cancellation.is_cancelled() {
        return Err(cancelled_market_bars());
    }
    let prepare_cancellation = cancellation.clone();
    let prepared = tokio::task::spawn_blocking(move || {
        if prepare_cancellation.is_cancelled() {
            return Err(cancelled_market_bars());
        }
        prepare(prepare_cancellation)
    })
    .await
    .map_err(|_| MarketDataError::Storage(crate::error::StorageFailure::CommandFailed))??;
    if cancellation.is_cancelled() {
        return Err(cancelled_market_bars());
    }
    let prepared = Arc::new(prepared);

    let provider_result = await_provider_or_cancel(
        provider(Arc::clone(&prepared), cancellation.clone()),
        &cancellation,
    )
    .await
    .ok_or_else(cancelled_market_bars)?;
    if cancellation.is_cancelled() {
        return Err(cancelled_market_bars());
    }

    let planner_cancellation = cancellation.clone();
    let planner_result = tokio::task::spawn_blocking(move || {
        if planner_cancellation.is_cancelled() {
            return Err(cancelled_market_bars());
        }
        planner(prepared, provider_result, planner_cancellation)
    })
    .await
    .map_err(|_| MarketDataError::Storage(crate::error::StorageFailure::CommandFailed))?;

    if cancellation.is_cancelled() {
        return Err(cancelled_market_bars());
    }
    planner_result
}

async fn await_provider_or_cancel<F>(
    future: F,
    cancellation: &CancellationToken,
) -> Option<F::Output>
where
    F: Future,
{
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                if cancellation.is_cancelled() {
                    return None;
                }
            }
            output = &mut future => return Some(output),
        }
    }
}

fn cancelled_market_bars() -> MarketDataError {
    MarketDataError::Storage(crate::error::StorageFailure::Cancelled)
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
    async fn market_bars_uses_the_shared_bounded_queue() {
        let (client, _held_queue) = QueryClient::with_full_queue_for_test();
        let provider_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let planner_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider_called_in_job = Arc::clone(&provider_called);
        let planner_called_in_job = Arc::clone(&planner_called);

        let result = client
            .market_bars(
                |_| Ok::<u8, MarketDataError>(1),
                move |_| async move {
                    provider_called_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok::<u8, &'static str>(1)
                },
                move |_, _, _| {
                    planner_called_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    Err(MarketDataError::Contract)
                },
            )
            .await;

        assert!(matches!(result, Err(QueryFailure::Unavailable)));
        assert!(!provider_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!planner_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn market_bars_passes_provider_errors_to_planner_on_blocking_worker() {
        let caller_thread = std::thread::current().id();
        let caller_thread_for_prepare = caller_thread;
        let caller_thread_for_planner = caller_thread;
        let planner_observed_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let prepare_observed_worker = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let prepare_observed_worker_in_job = Arc::clone(&prepare_observed_worker);
        let planner_observed_error_in_job = Arc::clone(&planner_observed_error);
        let result = execute_market_bars(
            move |_| {
                assert_ne!(std::thread::current().id(), caller_thread_for_prepare);
                prepare_observed_worker_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(7u8)
            },
            |prepared, _| async move {
                assert_eq!(*prepared, 7);
                Err::<u8, &'static str>("synthetic provider error")
            },
            move |prepared, provider_result, _| {
                assert_ne!(std::thread::current().id(), caller_thread_for_planner);
                assert_eq!(*prepared, 7);
                assert_eq!(provider_result, Err("synthetic provider error"));
                planner_observed_error_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(MarketDataError::Contract)
            },
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, Err(MarketDataError::Contract)));
        assert!(prepare_observed_worker.load(std::sync::atomic::Ordering::SeqCst));
        assert!(planner_observed_error.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelling_preflight_joins_before_returning_and_skips_provider() {
        let cancellation = CancellationToken::new();
        let cancellation_for_job = cancellation.clone();
        let prepare_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let prepare_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let prepare_started_in_job = Arc::clone(&prepare_started);
        let prepare_finished_in_job = Arc::clone(&prepare_finished);
        let provider_called_in_job = Arc::clone(&provider_called);
        let task = tokio::spawn(async move {
            execute_market_bars(
                move |cancellation| {
                    prepare_started_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    while !cancellation.is_cancelled() {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    prepare_finished_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    Err(MarketDataError::Storage(
                        crate::error::StorageFailure::Cancelled,
                    ))
                },
                move |_, _| async move {
                    provider_called_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok::<u8, &'static str>(1)
                },
                |_, _, _| Err(MarketDataError::Contract),
                cancellation_for_job,
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            while !prepare_started.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("preflight must start");
        cancellation.cancel();
        let result = task.await.expect("market-bars task must not panic");

        assert!(matches!(
            result,
            Err(MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled
            ))
        ));
        assert!(prepare_finished.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!provider_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelling_market_bars_drops_provider_future_promptly() {
        struct DropMarker(Arc<std::sync::atomic::AtomicBool>);

        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let cancellation = CancellationToken::new();
        let cancellation_for_job = cancellation.clone();
        let provider_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider_started_in_job = Arc::clone(&provider_started);
        let provider_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider_dropped_in_job = Arc::clone(&provider_dropped);
        let task = tokio::spawn(async move {
            execute_market_bars(
                |_| Ok::<u8, MarketDataError>(1),
                move |_, _| async move {
                    provider_started_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    let _drop_marker = DropMarker(provider_dropped_in_job);
                    std::future::pending::<std::result::Result<u8, &'static str>>().await
                },
                |_, _, _| Err(MarketDataError::Contract),
                cancellation_for_job,
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            while !provider_started.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provider future must start");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        cancellation.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_millis(250), task)
            .await
            .expect("provider cancellation must be observed promptly")
            .expect("market-bars task must not panic");

        assert!(matches!(
            result,
            Err(MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled
            ))
        ));
        assert!(provider_dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn planner_cancellation_is_cooperative_and_joined() {
        let cancellation = CancellationToken::new();
        let cancellation_for_job = cancellation.clone();
        let planner_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let planner_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let planner_started_in_job = Arc::clone(&planner_started);
        let planner_finished_in_job = Arc::clone(&planner_finished);
        let task = tokio::spawn(async move {
            execute_market_bars(
                |_| Ok::<u8, MarketDataError>(1),
                |_, _| async { Ok::<u8, &'static str>(1) },
                move |_, _, cancellation| {
                    planner_started_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    while !cancellation.is_cancelled() {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    planner_finished_in_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    Err(MarketDataError::Storage(
                        crate::error::StorageFailure::Cancelled,
                    ))
                },
                cancellation_for_job,
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            while !planner_started.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("planner must start");
        cancellation.cancel();
        let result = task.await.expect("market-bars task must not panic");

        assert!(matches!(
            result,
            Err(MarketDataError::Storage(
                crate::error::StorageFailure::Cancelled
            ))
        ));
        assert!(planner_finished.load(std::sync::atomic::Ordering::SeqCst));
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
