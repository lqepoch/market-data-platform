//! Default-off CLI composition for the reviewed Alpaca MessagePack fake-wire fixture.
//!
//! This module calls the broker-owned offline runner. It does not implement an Alpaca
//! protocol client, decoder, connector, credentials source, or provider entitlement check.

use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use alpaca_stream::offline_test_support::{ReviewedFixtureId, capture_reviewed_fixture};
use broker_ports::{
    MarketDataItem, RawCaptureInstanceId, RawFrameSink, RawFrameSinkError, RawFrameSinkFactory,
    RawMarketFrame,
};
use market_contracts::{EntitlementState, MarketEventEnvelopeV1};
use serde::Serialize;

use crate::{
    MarketDataError, Result,
    archive::{
        ArchiveLimits, ArchivePublisher, LocalRawFrameSpoolFactory, RawFrameSpoolLimits,
        capture_pair_v2::offline_fixture::OfflineFixturePairSummaryV1,
    },
    storage::LocalTestTransport,
};

#[derive(Serialize)]
pub struct CaptureSyntheticReport {
    status: &'static str,
    source_kind: &'static str,
    market_data_authority: &'static str,
    manifest_completion_source_kind: &'static str,
    protocol_provider: &'static str,
    protocol_feed: &'static str,
    entitlement: &'static str,
    source_completeness: &'static str,
    local_pair: OfflineFixturePairSummaryV1,
}

/// Runs the one reviewed, compile-time fixture without reading any provider credential.
/// The output directory must not exist; this command is intentionally create-only.
pub async fn run(
    output: PathBuf,
    shutdown_signal: impl Future<Output = io::Result<()>> + Send + 'static,
) -> Result<CaptureSyntheticReport> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (output, shutdown_signal);
        return Err(MarketDataError::PublicationNotAuthorized);
    }

    #[cfg(target_os = "linux")]
    {
        prepare_output_root(&output)?;
        let spool = Arc::new(
            LocalRawFrameSpoolFactory::open(
                output.join("raw-spool"),
                RawFrameSpoolLimits {
                    max_capture_bytes: 64 * 1024,
                    max_total_bytes: 64 * 1024,
                    max_frames_per_capture: 8,
                    max_capture_identities: 1,
                },
            )
            .map_err(|_| MarketDataError::IncompleteWindow)?,
        );
        let sink_factory = Arc::new(RecordingSinkFactory::new(Arc::clone(&spool)));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let capture_future = capture_reviewed_fixture(
            ReviewedFixtureId::AlpacaOpraTradeV1,
            sink_factory.clone(),
            cancel_rx,
        );
        tokio::pin!(capture_future);
        let capture_result = tokio::select! {
            result = &mut capture_future => result.map_err(|_| MarketDataError::IncompleteWindow),
            _ = shutdown_signal => {
                cancel_tx.send_replace(true);
                let _ = capture_future.await;
                Err(MarketDataError::IncompleteWindow)
            }
        };
        // The runner future has fully returned here, so its capture and finalization ACKs are
        // complete and no SDK task can still write. Keep the spool open for the bounded
        // readback/publish path, then close it on both success and failure.
        let result = match capture_result {
            Err(error) => Err(error),
            Ok(capture) => (|| {
                let (items, receipt) = capture.into_parts();
                let capture_instance_id = sink_factory
                    .capture_instance_id()
                    .ok_or(MarketDataError::IncompleteWindow)?;
                let (raw_frames, events) = split_items(&items)?;
                let publisher = ArchivePublisher::local_test(
                    LocalTestTransport::new(output.join("local-test-archive"))?,
                    output.join("archive-state"),
                    output.join("staging"),
                    ArchiveLimits::default(),
                )?;
                let pair = publisher.publish_local_offline_alpaca_fixture_capture_pair_v2(
                    &spool,
                    capture_instance_id,
                    &items,
                    &raw_frames,
                    &events,
                    &receipt,
                )?;
                Ok(CaptureSyntheticReport {
                    status: "LOCAL_TEST_SYNTHETIC_FIXTURE_REPLAY_ONLY",
                    source_kind: "synthetic_replay",
                    market_data_authority: "SYNTHETIC_NOT_REAL_OPRA_NOT_LIVE",
                    manifest_completion_source_kind: "FINITE_BATCH_SOURCE_KIND_LOCAL_ARCHIVE",
                    protocol_provider: "alpaca",
                    protocol_feed: "opra",
                    entitlement: "unknown",
                    source_completeness: "NOT_ASSERTED",
                    local_pair: pair,
                })
            })(),
        };
        spool.shutdown().await;
        result
    }
}

struct RecordingSinkFactory {
    spool: Arc<LocalRawFrameSpoolFactory>,
    created: Mutex<Option<(RawCaptureInstanceId, Arc<dyn RawFrameSink>)>>,
}

impl RecordingSinkFactory {
    fn new(spool: Arc<LocalRawFrameSpoolFactory>) -> Self {
        Self {
            spool,
            created: Mutex::new(None),
        }
    }

    fn capture_instance_id(&self) -> Option<RawCaptureInstanceId> {
        self.created
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(capture_instance_id, _sink)| *capture_instance_id)
    }
}

impl RawFrameSinkFactory for RecordingSinkFactory {
    fn create_sink(
        &self,
        provider: &str,
        feed: &str,
    ) -> std::result::Result<Arc<dyn RawFrameSink>, RawFrameSinkError> {
        if provider != "alpaca" || feed != "opra" {
            return Err(RawFrameSinkError::Unavailable);
        }
        let mut created = self
            .created
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if created.is_some() {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
        let sink = self.spool.create_sink(provider, feed)?;
        let capture_instance_id = sink.capture_instance_id();
        // The spool factory indexes active sinks weakly; retain this strong owner through
        // post-run readback so the completed capture remains addressable after the SDK drops its
        // subscription task's sink handle.
        *created = Some((capture_instance_id, Arc::clone(&sink)));
        Ok(sink)
    }
}

fn split_items(
    items: &[MarketDataItem],
) -> Result<(Vec<RawMarketFrame>, Vec<MarketEventEnvelopeV1>)> {
    if items.is_empty() || items.len() > 64 {
        return Err(MarketDataError::InputLimit);
    }
    let mut raw_frames = Vec::new();
    let mut events = Vec::new();
    for item in items {
        match item {
            MarketDataItem::RawFrame(frame) => raw_frames.push(frame.clone()),
            MarketDataItem::Event { envelope, .. } => events.push(envelope.clone()),
            MarketDataItem::Control(_) => {}
        }
    }
    if raw_frames.is_empty()
        || raw_frames.len() > 8
        || events.is_empty()
        || events.len() > 64
        || raw_frames.iter().any(|frame| {
            frame.provider != "alpaca"
                || frame.feed != "opra"
                || frame.entitlement != EntitlementState::Unknown
        })
    {
        return Err(MarketDataError::IncompleteWindow);
    }
    Ok((raw_frames, events))
}

#[cfg(unix)]
fn prepare_output_root(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

    if path.file_name().is_none() {
        return Err(MarketDataError::InvalidInput);
    }
    match std::fs::symlink_metadata(path) {
        Ok(_) => return Err(MarketDataError::Conflict),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let metadata = std::fs::symlink_metadata(parent)?;
    if !metadata.file_type().is_dir() {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700).create(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        return Err(MarketDataError::PublicationNotAuthorized);
    }
    Ok(())
}

#[cfg(not(unix))]
fn prepare_output_root(_path: &Path) -> Result<()> {
    Err(MarketDataError::PublicationNotAuthorized)
}
