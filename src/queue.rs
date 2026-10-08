//! Bounded collection queues, sequence quality checks, and durable gap records.

use fs2::FileExt;
use market_contracts::{
    ControlEventEnvelopeV1, EventMetadataV1, MarketEventEnvelopeV1, MarketEventV1,
};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};
use tokio::sync::{mpsc, oneshot};

use crate::{MarketDataError, Result};

pub const MAX_TRACKED_SOURCES: usize = 64;
pub const MAX_COLLECTION_WRITER_WORKERS: usize = 32;
pub const DEFAULT_GAP_LEDGER_MAX_BYTES: u64 = 64 * 1024 * 1024;
static ACTIVE_COLLECTION_WRITERS: AtomicUsize = AtomicUsize::new(0);

struct CollectionWorkerPermit;

impl CollectionWorkerPermit {
    fn acquire() -> Result<Self> {
        ACTIVE_COLLECTION_WRITERS
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_COLLECTION_WRITER_WORKERS).then_some(active + 1)
            })
            .map_err(|_| MarketDataError::WriterLimit)?;
        Ok(Self)
    }
}

impl Drop for CollectionWorkerPermit {
    fn drop(&mut self) {
        ACTIVE_COLLECTION_WRITERS.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug)]
pub enum CollectionMessage {
    Market(MarketEventEnvelopeV1),
    Control(ControlEventEnvelopeV1),
}

impl CollectionMessage {
    pub fn metadata(&self) -> &EventMetadataV1 {
        match self {
            Self::Market(event) => &event.metadata,
            Self::Control(event) => &event.metadata,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Market(event) => event.validate(),
            Self::Control(event) => event.validate(),
        }
        .map_err(|_| MarketDataError::Contract)
    }

    fn symbol(&self) -> Option<&str> {
        match self {
            Self::Market(event) => match &event.event {
                MarketEventV1::StockQuote { symbol, .. }
                | MarketEventV1::StockTrade { symbol, .. }
                | MarketEventV1::OptionQuote { symbol, .. }
                | MarketEventV1::OptionTrade { symbol, .. } => Some(symbol),
            },
            Self::Control(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    SequenceGap,
    StaleGeneration,
    DuplicateOrReordered,
    QueueFull,
    QueueClosed,
}

#[derive(Serialize)]
struct GapRecord<'a> {
    dataset_id: &'a str,
    reason: GapReason,
    provider: &'a str,
    feed: &'a str,
    symbol: Option<&'a str>,
    generation: String,
    sequence: String,
    missing_sequence_start: Option<String>,
    missing_sequence_end: Option<String>,
    source_timestamp: Option<&'a str>,
    received_timestamp: &'a str,
}

/// Appends a JSONL gap record and synchronizes it before the dropped item is reported.
#[derive(Clone, Debug)]
pub struct GapLedger {
    dataset_id: String,
    path: PathBuf,
    max_bytes: u64,
}

impl GapLedger {
    pub fn new(dataset_id: impl Into<String>, path: impl Into<PathBuf>) -> Result<Self> {
        Self::with_max_bytes(dataset_id, path, DEFAULT_GAP_LEDGER_MAX_BYTES)
    }

    pub fn with_max_bytes(
        dataset_id: impl Into<String>,
        path: impl Into<PathBuf>,
        max_bytes: u64,
    ) -> Result<Self> {
        let dataset_id = dataset_id.into();
        if !safe_component(&dataset_id) || max_bytes == 0 {
            return Err(MarketDataError::InvalidInput);
        }
        Ok(Self {
            dataset_id,
            path: path.into(),
            max_bytes,
        })
    }

    fn record(
        &self,
        message: &CollectionMessage,
        reason: GapReason,
        missing: Option<(u64, u64)>,
    ) -> Result<()> {
        let metadata = message.metadata();
        let record = GapRecord {
            dataset_id: &self.dataset_id,
            reason,
            provider: &metadata.source.provider,
            feed: &metadata.source.feed,
            symbol: message.symbol(),
            generation: metadata.generation.to_string(),
            sequence: metadata.sequence.to_string(),
            missing_sequence_start: missing.map(|range| range.0.to_string()),
            missing_sequence_end: missing.map(|range| range.1.to_string()),
            source_timestamp: metadata.source_timestamp.as_ref().map(|time| time.as_str()),
            received_timestamp: metadata.received_timestamp.as_str(),
        };
        let bytes = serde_json::to_vec(&record)?;
        let append_len = u64::try_from(bytes.len().saturating_add(1))
            .map_err(|_| MarketDataError::GapLedgerFull)?;
        if let Some(parent) = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => return Err(MarketDataError::InvalidInput),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(MarketDataError::Io(error)),
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.lock_exclusive()?;
        let current_len = file.metadata()?.len();
        if current_len.saturating_add(append_len) > self.max_bytes {
            return Err(MarketDataError::GapLedgerFull);
        }
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        if let Some(parent) = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn has_gaps(&self) -> Result<bool> {
        if !self.path.exists() {
            return Ok(false);
        }
        Ok(fs::metadata(&self.path)?.len() > 0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequenceDecision {
    Accept,
    Gap {
        first_missing: u64,
        last_missing: u64,
    },
    StaleGeneration,
    DuplicateOrReordered,
}

#[derive(Default)]
pub struct SequenceTracker {
    cursors: HashMap<(String, String), Cursor>,
}

#[derive(Default)]
struct Cursor {
    generation: u64,
    sequence: u64,
}

impl SequenceTracker {
    /// Update a feed cursor; callers must retain gaps as incomplete evidence.
    pub fn observe(&mut self, metadata: &EventMetadataV1) -> Result<SequenceDecision> {
        metadata.validate().map_err(|_| MarketDataError::Contract)?;
        let key = (
            metadata.source.provider.clone(),
            metadata.source.feed.clone(),
        );
        if !self.cursors.contains_key(&key) && self.cursors.len() >= MAX_TRACKED_SOURCES {
            return Err(MarketDataError::SourceLimit);
        }
        let cursor = self.cursors.entry(key).or_default();
        if cursor.generation != 0 && metadata.generation < cursor.generation {
            return Ok(SequenceDecision::StaleGeneration);
        }
        if metadata.generation > cursor.generation {
            cursor.generation = metadata.generation;
            cursor.sequence = metadata.sequence;
            return Ok(SequenceDecision::Accept);
        }
        if metadata.sequence <= cursor.sequence {
            return Ok(SequenceDecision::DuplicateOrReordered);
        }
        let decision = if metadata.sequence > cursor.sequence.saturating_add(1) {
            SequenceDecision::Gap {
                first_missing: cursor.sequence.saturating_add(1),
                last_missing: metadata.sequence - 1,
            }
        } else {
            SequenceDecision::Accept
        };
        cursor.sequence = metadata.sequence;
        Ok(decision)
    }
}

pub struct CollectionReceiver {
    receiver: mpsc::Receiver<CollectionMessage>,
}

impl CollectionReceiver {
    pub async fn recv(&mut self) -> Option<CollectionMessage> {
        self.receiver.recv().await
    }
}

struct Submission {
    message: CollectionMessage,
    result: oneshot::Sender<Result<SequenceDecision>>,
}

#[derive(Clone)]
pub struct CollectionSubmitter {
    sender: mpsc::Sender<Submission>,
    gap_ledger_path: PathBuf,
    poisoned: Arc<AtomicBool>,
}

impl CollectionSubmitter {
    pub fn bounded(
        dataset_id: impl Into<String>,
        capacity: usize,
        gap_ledger_path: impl Into<PathBuf>,
    ) -> Result<(Self, CollectionReceiver)> {
        Self::bounded_with_limits(
            dataset_id,
            capacity,
            capacity,
            DEFAULT_GAP_LEDGER_MAX_BYTES,
            gap_ledger_path,
        )
    }

    pub fn bounded_with_limits(
        dataset_id: impl Into<String>,
        event_capacity: usize,
        submit_capacity: usize,
        ledger_max_bytes: u64,
        gap_ledger_path: impl Into<PathBuf>,
    ) -> Result<(Self, CollectionReceiver)> {
        if event_capacity == 0 || submit_capacity == 0 {
            return Err(MarketDataError::InvalidInput);
        }
        let ledger = GapLedger::with_max_bytes(dataset_id, gap_ledger_path, ledger_max_bytes)?;
        let ledger_path = ledger.path().to_path_buf();
        let (sender, mut submissions) = mpsc::channel::<Submission>(submit_capacity);
        let (event_sender, receiver) = mpsc::channel(event_capacity);
        let worker_permit = CollectionWorkerPermit::acquire()?;
        let poisoned = Arc::new(AtomicBool::new(false));
        let worker_poisoned = Arc::clone(&poisoned);
        thread::Builder::new()
            .name("mdp-collection-writer".to_owned())
            .spawn(move || {
                let _worker_permit = worker_permit;
                let mut tracker = SequenceTracker::default();
                while let Some(submission) = submissions.blocking_recv() {
                    let result = if worker_poisoned.load(Ordering::Acquire) {
                        Err(MarketDataError::WriterPoisoned)
                    } else {
                        let result = process_submission(
                            submission.message,
                            &mut tracker,
                            &ledger,
                            &event_sender,
                        );
                        if result.is_err() {
                            worker_poisoned.store(true, Ordering::Release);
                        }
                        result
                    };
                    let _ = submission.result.send(result);
                }
            })
            .map_err(|_| MarketDataError::InvalidInput)?;
        Ok((
            Self {
                sender,
                gap_ledger_path: ledger_path,
                poisoned,
            },
            CollectionReceiver { receiver },
        ))
    }

    /// Enqueue through one bounded writer. Disk sync happens on its dedicated blocking thread.
    pub async fn submit(&self, message: CollectionMessage) -> Result<SequenceDecision> {
        let (result, response) = oneshot::channel();
        self.sender
            .send(Submission { message, result })
            .await
            .map_err(|_| MarketDataError::QueueClosed)?;
        response.await.map_err(|_| MarketDataError::QueueClosed)?
    }

    pub fn has_gaps(&self) -> Result<bool> {
        if self.poisoned.load(Ordering::Acquire) {
            return Ok(true);
        }
        if !self.gap_ledger_path.exists() {
            return Ok(false);
        }
        Ok(fs::metadata(&self.gap_ledger_path)?.len() > 0)
    }
}

fn process_submission(
    message: CollectionMessage,
    tracker: &mut SequenceTracker,
    ledger: &GapLedger,
    sender: &mpsc::Sender<CollectionMessage>,
) -> Result<SequenceDecision> {
    message.validate()?;
    let decision = tracker.observe(message.metadata())?;
    match decision {
        SequenceDecision::StaleGeneration => {
            ledger.record(&message, GapReason::StaleGeneration, None)?;
            return Ok(decision);
        }
        SequenceDecision::DuplicateOrReordered => {
            ledger.record(&message, GapReason::DuplicateOrReordered, None)?;
            return Ok(decision);
        }
        SequenceDecision::Gap {
            first_missing,
            last_missing,
        } => ledger.record(
            &message,
            GapReason::SequenceGap,
            Some((first_missing, last_missing)),
        )?,
        SequenceDecision::Accept => {}
    }
    match sender.try_send(message) {
        Ok(()) => Ok(decision),
        Err(mpsc::error::TrySendError::Full(message)) => {
            ledger.record(&message, GapReason::QueueFull, None)?;
            Err(MarketDataError::QueueFull)
        }
        Err(mpsc::error::TrySendError::Closed(message)) => {
            ledger.record(&message, GapReason::QueueClosed, None)?;
            Err(MarketDataError::QueueClosed)
        }
    }
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.contains("..")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_contracts::{
        DecimalString, EntitlementState, MarketControlEventV1, MarketDataSourceV1,
        NumericEncodingV1, UtcTimestamp,
    };
    use tempfile::tempdir;

    fn event(generation: u64, sequence: u64) -> CollectionMessage {
        CollectionMessage::Market(MarketEventEnvelopeV1 {
            metadata: EventMetadataV1 {
                schema_version: 1,
                source: MarketDataSourceV1::new(
                    "synthetic",
                    "synthetic",
                    EntitlementState::Unknown,
                    NumericEncodingV1::DecimalToken,
                    None,
                )
                .unwrap(),
                generation,
                sequence,
                raw_frame_sha256: None,
                source_timestamp: Some(UtcTimestamp::parse("2026-10-08T14:30:00Z").unwrap()),
                received_timestamp: UtcTimestamp::parse("2026-10-08T14:30:00.1Z").unwrap(),
            },
            event: MarketEventV1::StockTrade {
                symbol: "QQQ".into(),
                price: DecimalString::new("600.1250").unwrap(),
                size: DecimalString::new("2").unwrap(),
            },
        })
    }

    #[test]
    fn sequence_tracker_rejects_stale_generation_and_duplicates_and_records_gaps() {
        let mut tracker = SequenceTracker::default();
        assert_eq!(
            tracker.observe(event(4, 10).metadata()).unwrap(),
            SequenceDecision::Accept
        );
        assert_eq!(
            tracker.observe(event(3, 11).metadata()).unwrap(),
            SequenceDecision::StaleGeneration
        );
        assert_eq!(
            tracker.observe(event(4, 10).metadata()).unwrap(),
            SequenceDecision::DuplicateOrReordered
        );
        assert_eq!(
            tracker.observe(event(4, 13).metadata()).unwrap(),
            SequenceDecision::Gap {
                first_missing: 11,
                last_missing: 12
            }
        );
        assert_eq!(
            tracker.observe(event(5, 1).metadata()).unwrap(),
            SequenceDecision::Accept
        );
    }

    #[tokio::test]
    async fn full_queue_durably_records_source_range_before_returning_error() {
        let temp = tempdir().unwrap();
        let gap_path = temp.path().join("gaps.jsonl");
        let (submitter, _receiver) =
            CollectionSubmitter::bounded("synthetic-v1", 1, &gap_path).unwrap();
        assert_eq!(
            submitter.submit(event(1, 1)).await.unwrap(),
            SequenceDecision::Accept
        );
        assert!(matches!(
            submitter.submit(event(1, 2)).await,
            Err(MarketDataError::QueueFull)
        ));
        assert!(submitter.has_gaps().unwrap());
        let gap = std::fs::read_to_string(gap_path).unwrap();
        assert!(gap.contains("\"reason\":\"queue_full\""));
        assert!(gap.contains("\"sequence\":\"2\""));
        assert!(gap.contains("2026-10-08T14:30:00Z"));
    }

    #[tokio::test]
    async fn control_messages_use_the_same_generation_sequence_guard() {
        let temp = tempdir().unwrap();
        let (submitter, mut receiver) =
            CollectionSubmitter::bounded("synthetic-v1", 4, temp.path().join("gaps.jsonl"))
                .unwrap();
        let message = event(7, 1);
        let metadata = message.metadata().clone();
        let control = CollectionMessage::Control(ControlEventEnvelopeV1 {
            metadata,
            control: MarketControlEventV1::SubscriptionAck {
                request_id: "req-1".into(),
                subscription_id: "session-7".into(),
                acknowledged: vec!["QQQ".into()],
                rejected: vec![],
            },
        });
        assert_eq!(
            submitter.submit(control).await.unwrap(),
            SequenceDecision::Accept
        );
        assert_eq!(
            submitter.submit(event(7, 2)).await.unwrap(),
            SequenceDecision::Accept
        );
        assert!(matches!(
            receiver.recv().await,
            Some(CollectionMessage::Control(_))
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(CollectionMessage::Market(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_submitters_cannot_enqueue_out_of_source_order_silently() {
        let temp = tempdir().unwrap();
        let (submitter, mut receiver) =
            CollectionSubmitter::bounded("synthetic-v1", 4, temp.path().join("gaps.jsonl"))
                .unwrap();
        let (first, second) =
            tokio::join!(submitter.submit(event(2, 1)), submitter.submit(event(2, 2)),);
        let _ = (first, second);
        let mut sequences = Vec::new();
        for _ in 0..2 {
            if let Some(message) = receiver.recv().await {
                sequences.push(message.metadata().sequence);
            }
        }
        assert!(
            sequences.windows(2).all(|pair| pair[0] < pair[1]) || submitter.has_gaps().unwrap()
        );
    }

    #[test]
    fn tracker_rejects_unbounded_source_cardinality() {
        let mut tracker = SequenceTracker::default();
        for index in 0..MAX_TRACKED_SOURCES {
            let mut metadata = event(1, 1).metadata().clone();
            metadata.source.provider = format!("provider-{index}");
            metadata.source.feed = format!("feed-{index}");
            assert_eq!(
                tracker.observe(&metadata).unwrap(),
                SequenceDecision::Accept
            );
        }
        let mut metadata = event(1, 1).metadata().clone();
        metadata.source.provider = "provider-over-limit".into();
        metadata.source.feed = "feed-over-limit".into();
        assert!(matches!(
            tracker.observe(&metadata),
            Err(MarketDataError::SourceLimit)
        ));
    }

    #[tokio::test]
    async fn ledger_capacity_exhaustion_is_fatal_and_not_reported_as_durable() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("gaps.jsonl");
        let (submitter, _receiver) =
            CollectionSubmitter::bounded_with_limits("synthetic-v1", 1, 1, 1, &path).unwrap();
        submitter.submit(event(1, 1)).await.unwrap();
        assert!(matches!(
            submitter.submit(event(1, 2)).await,
            Err(MarketDataError::GapLedgerFull)
        ));
        assert!(matches!(
            submitter.submit(event(1, 3)).await,
            Err(MarketDataError::WriterPoisoned)
        ));
        assert!(submitter.has_gaps().unwrap());
    }
}
