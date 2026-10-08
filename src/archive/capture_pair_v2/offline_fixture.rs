//! Private evidence for the reviewed Alpaca MessagePack fake-wire replay.
//!
//! This receipt binds one compile-time fixture to the exact frames returned by the
//! broker runner, the finalized local spool, and the already read-back-verified Pair
//! rollup. It never establishes provider completeness or market-data entitlement.

use std::path::Path;

use alpaca_stream::offline_test_support::{
    OfflineFixtureReceipt, OfflineFixtureTerminal, ReviewedFixtureId,
};
use broker_ports::{
    MarketDataItem, RawCaptureInstanceId, RawFrameDisposition, RawFrameWireEncoding, RawMarketFrame,
};
use market_contracts::{
    ConnectionState, EntitlementState, MarketControlEventV1, MarketEventEnvelopeV1,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    MarketDataError, Result,
    archive::{LocalRawFrameSpoolFactory, capture_pair_v2::valid_local_pair_source},
};

use super::rollup::LocalCapturePairRollupV2;
use super::{decode_capture_instance_id, decode_sha256, entitlement_tag, valid_sha256};
use crate::archive::raw_spool::SpooledRawFrame;

const FIXTURE_SCRIPT_FRAME_COUNT: u32 = 4;
const MAX_FIXTURE_ITEMS: usize = 64;
const MAX_FIXTURE_CAPTURED_FRAMES: u32 = 8;
const MAX_FIXTURE_CAPTURED_BYTES: u64 = 64 * 1024;
const FIXTURE_FRESHNESS_CLOCK_UNIX_SECONDS: u64 = 1_791_460_800;
const RECEIPT_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.alpaca-offline-fixture-replay-receipt.v1\0";
const FIXTURE_TERMINAL_SCOPE: &str = "FIXTURE_END_TEST_CONTROL_ONLY";
const CAPTURE_MODE: &str = "SYNTHETIC_REPLAY_FIXTURE";
const MANIFEST_COMPLETION_SOURCE_KIND: &str = "FINITE_BATCH_SOURCE_KIND_LOCAL_ARCHIVE";
const SOURCE_COMPLETENESS: &str = "NOT_ASSERTED";
const RECEIPT_FILE_SUFFIX: &str = "offline-fixture-replay.receipt.json";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct OfflineFixturePairSummaryV1 {
    pub schema_version: u32,
    pub fixture_id: String,
    pub fixture_sha256: String,
    pub fixture_freshness_clock_unix_seconds: String,
    pub manifest_completion_source_kind: String,
    pub capture_instance_id: String,
    pub protocol_provider: String,
    pub protocol_feed: String,
    pub entitlement: String,
    pub capture_mode: String,
    pub terminal_scope: String,
    pub source_completeness: String,
    pub script_frame_count: u32,
    pub runner_received_frame_count: u32,
    pub runner_received_digest_sha256: String,
    pub captured_frame_count: u32,
    pub raw_market_frame_count: u32,
    pub predecode_ack_count: u32,
    pub finalization_ack_count: u32,
    pub captured_bytes: String,
    pub ordered_raw_frames_sha256: String,
    pub finalization_rollup_sha256: String,
    pub output_item_count: u32,
    pub pair_rollup_sha256: String,
    pub receipt_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct OfflineFixturePairReceiptV1 {
    schema_version: u32,
    fixture_id: String,
    fixture_sha256: String,
    #[serde(with = "market_contracts::wire_u64")]
    fixture_freshness_clock_unix_seconds: u64,
    manifest_completion_source_kind: String,
    capture_instance_id: String,
    protocol_provider: String,
    protocol_feed: String,
    entitlement: EntitlementState,
    capture_mode: String,
    terminal_scope: String,
    source_completeness: String,
    script_frame_count: u32,
    runner_received_frame_count: u32,
    runner_received_digest_sha256: String,
    captured_frame_count: u32,
    raw_market_frame_count: u32,
    predecode_ack_count: u32,
    finalization_ack_count: u32,
    #[serde(with = "market_contracts::wire_u64")]
    captured_bytes: u64,
    ordered_raw_frames_sha256: String,
    finalization_rollup_sha256: String,
    output_item_count: u32,
    pair_rollup_sha256: String,
    receipt_sha256: String,
}

pub(super) fn validate_broker_fixture_receipt(
    receipt: &OfflineFixtureReceipt,
    items: &[MarketDataItem],
    raw_frames: &[RawMarketFrame],
    events: &[MarketEventEnvelopeV1],
) -> Result<()> {
    let fixture_id = ReviewedFixtureId::AlpacaOpraTradeV1;
    if receipt.fixture_id() != fixture_id
        || receipt.fixture_sha256() != fixture_id.expected_sha256()
        || receipt.terminal_state() != OfflineFixtureTerminal::FixtureEnd
        || receipt.script_frame_count() != FIXTURE_SCRIPT_FRAME_COUNT
        || receipt.runner_received_frame_count() != FIXTURE_SCRIPT_FRAME_COUNT
        || receipt.runner_received_digest_sha256() != fixture_id.expected_sha256()
        || receipt.runner_received_digest_sha256() != receipt.fixture_sha256()
        || receipt.fixture_freshness_clock_unix_seconds() != FIXTURE_FRESHNESS_CLOCK_UNIX_SECONDS
        || receipt.captured_frame_count() != raw_frames.len() as u32
        || receipt.raw_market_frame_count()
            != u32::try_from(
                raw_frames
                    .iter()
                    .filter(|frame| frame.event_count > 0)
                    .count(),
            )
            .map_err(|_| MarketDataError::InputLimit)?
        || receipt.predecode_ack_count() != receipt.captured_frame_count()
        || receipt.finalization_ack_count() != receipt.captured_frame_count()
        || receipt.output_item_count() != items.len() as u32
        || items.is_empty()
        || items.len() > MAX_FIXTURE_ITEMS
        || raw_frames.is_empty()
        || raw_frames.len() > MAX_FIXTURE_CAPTURED_FRAMES as usize
        || events.is_empty()
        || events.len() > MAX_FIXTURE_ITEMS
    {
        return Err(MarketDataError::IncompleteWindow);
    }

    let mut item_raw_index = 0_usize;
    let mut item_event_index = 0_usize;
    let mut subscription_ack_count = 0_u32;
    for item in items {
        match item {
            MarketDataItem::RawFrame(frame) => {
                if raw_frames.get(item_raw_index) != Some(frame) {
                    return Err(MarketDataError::Contract);
                }
                item_raw_index += 1;
            }
            MarketDataItem::Event {
                envelope,
                raw_frame,
            } => {
                envelope.validate().map_err(|_| MarketDataError::Contract)?;
                if raw_frame.is_none() || events.get(item_event_index) != Some(envelope) {
                    return Err(MarketDataError::Contract);
                }
                item_event_index += 1;
            }
            MarketDataItem::Control(control) => {
                control.validate().map_err(|_| MarketDataError::Contract)?;
                match &control.control {
                    MarketControlEventV1::ConnectionStatus {
                        state: ConnectionState::Connecting | ConnectionState::Connected,
                    } => {}
                    MarketControlEventV1::SubscriptionAck { rejected, .. }
                        if rejected.is_empty() =>
                    {
                        subscription_ack_count = subscription_ack_count
                            .checked_add(1)
                            .ok_or(MarketDataError::InputLimit)?;
                    }
                    _ => return Err(MarketDataError::IncompleteWindow),
                }
            }
        }
    }
    if item_raw_index != raw_frames.len()
        || item_event_index != events.len()
        || subscription_ack_count != 1
    {
        return Err(MarketDataError::IncompleteWindow);
    }

    let mut total_bytes = 0_u64;
    for frame in raw_frames {
        let capture_key = frame
            .capture_key
            .as_ref()
            .ok_or(MarketDataError::IncompleteWindow)?;
        if frame.provider != "alpaca"
            || frame.feed != "opra"
            || frame.entitlement != EntitlementState::Unknown
            || frame.wire_encoding != RawFrameWireEncoding::MessagePack
            || frame.disposition == RawFrameDisposition::DecodeFailure
            || frame.disposition == RawFrameDisposition::UnknownMessage
            || frame.disposition == RawFrameDisposition::ProviderError
            || frame.event_count == 0 && frame.disposition != RawFrameDisposition::ControlMessage
            || frame.event_count > 0 && frame.disposition != RawFrameDisposition::DecodedMarketData
            || capture_key.frame_sha256() != frame.payload.sha256()
            || capture_key.frame_sequence() != frame.frame_sequence
            || total_bytes
                .checked_add(frame.payload.as_bytes().len() as u64)
                .filter(|next| *next <= MAX_FIXTURE_CAPTURED_BYTES)
                .is_none()
        {
            return Err(MarketDataError::Contract);
        }
        total_bytes += frame.payload.as_bytes().len() as u64;
    }

    if total_bytes != receipt.captured_bytes()
        || ordered_raw_frames_sha256(raw_frames)? != receipt.ordered_raw_frames_sha256()
    {
        return Err(MarketDataError::Conflict);
    }
    Ok(())
}

pub(super) fn validate_spooled_fixture_frames(
    spool: &LocalRawFrameSpoolFactory,
    capture_instance_id: RawCaptureInstanceId,
    receipt: &OfflineFixtureReceipt,
    raw_frames: &[RawMarketFrame],
) -> Result<()> {
    let mut reader = spool
        .open_current_capture_reader(capture_instance_id)
        .map_err(|_| MarketDataError::IncompleteWindow)?;
    let mut spooled_frames = Vec::new();
    while let Some(chunk) = reader
        .next_chunk()
        .map_err(|_| MarketDataError::IncompleteWindow)?
    {
        if spooled_frames.len().saturating_add(chunk.len()) > MAX_FIXTURE_CAPTURED_FRAMES as usize {
            return Err(MarketDataError::InputLimit);
        }
        spooled_frames.extend(chunk);
    }
    if spooled_frames.len() != raw_frames.len()
        || spooled_frames.len() as u32 != receipt.captured_frame_count()
        || spooled_frames.len() as u32 != receipt.predecode_ack_count()
        || spooled_frames.len() as u32 != receipt.finalization_ack_count()
    {
        return Err(MarketDataError::IncompleteWindow);
    }

    for (spooled, frame) in spooled_frames.iter().zip(raw_frames) {
        let capture = &spooled.capture;
        let Some(projected_key) = frame.capture_key.as_ref() else {
            return Err(MarketDataError::IncompleteWindow);
        };
        if capture.capture_instance_id() != capture_instance_id
            || capture.provider() != "alpaca"
            || capture.feed() != "opra"
            || capture.entitlement() != EntitlementState::Unknown
            || capture.wire_encoding() != RawFrameWireEncoding::MessagePack
            || capture.capture_key() != projected_key
            || capture.source_generation() != projected_key.source_generation()
            || capture.frame_sequence() != projected_key.frame_sequence()
            || capture.payload().as_bytes() != frame.payload.as_bytes()
            || spooled.finalization.event_count() != frame.event_count
            || spooled.finalization.symbols() != frame.symbols
            || spooled.finalization.numeric_encoding() != frame.numeric_encoding
            || spooled.finalization.disposition() != frame.disposition
        {
            return Err(MarketDataError::Conflict);
        }
    }
    if finalization_rollup_sha256(&spooled_frames)? != receipt.finalization_rollup_sha256() {
        return Err(MarketDataError::Conflict);
    }
    Ok(())
}

pub(super) fn persist_offline_fixture_receipt(
    state_root: &Path,
    capture_instance_id: RawCaptureInstanceId,
    broker_receipt: &OfflineFixtureReceipt,
    rollup: &LocalCapturePairRollupV2,
) -> Result<OfflineFixturePairSummaryV1> {
    if !valid_local_pair_source(&rollup.provider, &rollup.feed)
        || rollup.provider != "alpaca"
        || rollup.feed != "opra"
        || rollup.entitlement != EntitlementState::Unknown
        || rollup.capture_instance_id != hex::encode(capture_instance_id.as_bytes())
    {
        return Err(MarketDataError::Conflict);
    }
    let fixture_id = ReviewedFixtureId::AlpacaOpraTradeV1;
    let mut receipt = OfflineFixturePairReceiptV1 {
        schema_version: 1,
        fixture_id: fixture_id.as_str().to_owned(),
        fixture_sha256: broker_receipt.fixture_sha256().to_owned(),
        fixture_freshness_clock_unix_seconds: broker_receipt.fixture_freshness_clock_unix_seconds(),
        manifest_completion_source_kind: MANIFEST_COMPLETION_SOURCE_KIND.to_owned(),
        capture_instance_id: hex::encode(capture_instance_id.as_bytes()),
        protocol_provider: "alpaca".to_owned(),
        protocol_feed: "opra".to_owned(),
        entitlement: EntitlementState::Unknown,
        capture_mode: CAPTURE_MODE.to_owned(),
        terminal_scope: FIXTURE_TERMINAL_SCOPE.to_owned(),
        source_completeness: SOURCE_COMPLETENESS.to_owned(),
        script_frame_count: broker_receipt.script_frame_count(),
        runner_received_frame_count: broker_receipt.runner_received_frame_count(),
        runner_received_digest_sha256: broker_receipt.runner_received_digest_sha256().to_owned(),
        captured_frame_count: broker_receipt.captured_frame_count(),
        raw_market_frame_count: broker_receipt.raw_market_frame_count(),
        predecode_ack_count: broker_receipt.predecode_ack_count(),
        finalization_ack_count: broker_receipt.finalization_ack_count(),
        captured_bytes: broker_receipt.captured_bytes(),
        ordered_raw_frames_sha256: broker_receipt.ordered_raw_frames_sha256().to_owned(),
        finalization_rollup_sha256: broker_receipt.finalization_rollup_sha256().to_owned(),
        output_item_count: broker_receipt.output_item_count(),
        pair_rollup_sha256: rollup.rollup_sha256.clone(),
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.compute_sha256()?;
    receipt.validate()?;
    let bytes = serde_json::to_vec(&receipt).map_err(|_| MarketDataError::Contract)?;
    let capture_hex = hex::encode(capture_instance_id.as_bytes());
    super::publish::write_private_capture_record(
        state_root,
        &capture_hex,
        RECEIPT_FILE_SUFFIX,
        &bytes,
    )?;
    Ok(OfflineFixturePairSummaryV1::from(&receipt))
}

fn ordered_raw_frames_sha256(frames: &[RawMarketFrame]) -> Result<String> {
    let frame_count = u32::try_from(frames.len()).map_err(|_| MarketDataError::InputLimit)?;
    let mut hasher = Sha256::new();
    hasher.update(b"eqoboard.alpaca.offline-fixture-raw-frames.v1\0");
    hasher.update(frame_count.to_be_bytes());
    for frame in frames {
        let key = frame
            .capture_key
            .as_ref()
            .ok_or(MarketDataError::IncompleteWindow)?;
        let payload_len = u32::try_from(frame.payload.as_bytes().len())
            .map_err(|_| MarketDataError::InputLimit)?;
        hasher.update(key.capture_instance_id().as_bytes());
        hasher.update(key.source_generation().to_be_bytes());
        hasher.update(key.frame_sequence().to_be_bytes());
        hasher.update(payload_len.to_be_bytes());
        hasher.update(frame.payload.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

fn finalization_rollup_sha256(frames: &[SpooledRawFrame]) -> Result<String> {
    let frame_count = u32::try_from(frames.len()).map_err(|_| MarketDataError::InputLimit)?;
    let mut hasher = Sha256::new();
    hasher.update(b"eqoboard.alpaca.offline-fixture-finalization.v1\0");
    hasher.update(frame_count.to_be_bytes());
    for frame in frames {
        let capture = &frame.capture;
        hasher.update(capture.capture_instance_id().as_bytes());
        hasher.update(capture.source_generation().to_be_bytes());
        hasher.update(capture.frame_sequence().to_be_bytes());
        hasher.update(
            decode_sha256(capture.payload().sha256()).map_err(|_| MarketDataError::Contract)?,
        );
        hasher.update(
            decode_sha256(&frame.finalization_summary_sha256)
                .map_err(|_| MarketDataError::Contract)?,
        );
    }
    Ok(hex::encode(hasher.finalize()))
}

impl OfflineFixturePairReceiptV1 {
    fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || self.fixture_id != ReviewedFixtureId::AlpacaOpraTradeV1.as_str()
            || self.fixture_sha256 != ReviewedFixtureId::AlpacaOpraTradeV1.expected_sha256()
            || self.fixture_freshness_clock_unix_seconds != FIXTURE_FRESHNESS_CLOCK_UNIX_SECONDS
            || self.manifest_completion_source_kind != MANIFEST_COMPLETION_SOURCE_KIND
            || self.protocol_provider != "alpaca"
            || self.protocol_feed != "opra"
            || self.entitlement != EntitlementState::Unknown
            || self.capture_mode != CAPTURE_MODE
            || self.terminal_scope != FIXTURE_TERMINAL_SCOPE
            || self.source_completeness != SOURCE_COMPLETENESS
            || self.script_frame_count != FIXTURE_SCRIPT_FRAME_COUNT
            || self.runner_received_frame_count != self.script_frame_count
            || self.runner_received_digest_sha256 != self.fixture_sha256
            || self.captured_frame_count == 0
            || self.captured_frame_count > MAX_FIXTURE_CAPTURED_FRAMES
            || self.raw_market_frame_count > self.captured_frame_count
            || self.predecode_ack_count != self.captured_frame_count
            || self.finalization_ack_count != self.captured_frame_count
            || self.captured_bytes > MAX_FIXTURE_CAPTURED_BYTES
            || self.output_item_count == 0
            || self.output_item_count as usize > MAX_FIXTURE_ITEMS
            || !valid_sha256(&self.fixture_sha256)
            || !valid_sha256(&self.runner_received_digest_sha256)
            || !valid_sha256(&self.ordered_raw_frames_sha256)
            || !valid_sha256(&self.finalization_rollup_sha256)
            || !valid_sha256(&self.pair_rollup_sha256)
            || !valid_sha256(&self.receipt_sha256)
            || decode_capture_instance_id(&self.capture_instance_id).is_err()
            || self.compute_sha256()? != self.receipt_sha256
        {
            return Err(MarketDataError::Contract);
        }
        Ok(())
    }

    fn compute_sha256(&self) -> Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(RECEIPT_HASH_DOMAIN);
        hasher.update(self.schema_version.to_be_bytes());
        hasher.update(
            decode_capture_instance_id(&self.capture_instance_id)
                .map_err(|_| MarketDataError::Contract)?,
        );
        append_string(&mut hasher, &self.fixture_id)?;
        hasher.update(decode_sha256(&self.fixture_sha256).map_err(|_| MarketDataError::Contract)?);
        hasher.update(self.fixture_freshness_clock_unix_seconds.to_be_bytes());
        append_string(&mut hasher, &self.manifest_completion_source_kind)?;
        append_string(&mut hasher, &self.protocol_provider)?;
        append_string(&mut hasher, &self.protocol_feed)?;
        hasher.update([entitlement_tag(self.entitlement)]);
        append_string(&mut hasher, &self.capture_mode)?;
        append_string(&mut hasher, &self.terminal_scope)?;
        append_string(&mut hasher, &self.source_completeness)?;
        hasher.update(self.script_frame_count.to_be_bytes());
        hasher.update(self.runner_received_frame_count.to_be_bytes());
        hasher.update(
            decode_sha256(&self.runner_received_digest_sha256)
                .map_err(|_| MarketDataError::Contract)?,
        );
        hasher.update(self.captured_frame_count.to_be_bytes());
        hasher.update(self.raw_market_frame_count.to_be_bytes());
        hasher.update(self.predecode_ack_count.to_be_bytes());
        hasher.update(self.finalization_ack_count.to_be_bytes());
        hasher.update(self.captured_bytes.to_be_bytes());
        hasher.update(
            decode_sha256(&self.ordered_raw_frames_sha256)
                .map_err(|_| MarketDataError::Contract)?,
        );
        hasher.update(
            decode_sha256(&self.finalization_rollup_sha256)
                .map_err(|_| MarketDataError::Contract)?,
        );
        hasher.update(self.output_item_count.to_be_bytes());
        hasher.update(
            decode_sha256(&self.pair_rollup_sha256).map_err(|_| MarketDataError::Contract)?,
        );
        Ok(hex::encode(hasher.finalize()))
    }
}

impl From<&OfflineFixturePairReceiptV1> for OfflineFixturePairSummaryV1 {
    fn from(receipt: &OfflineFixturePairReceiptV1) -> Self {
        Self {
            schema_version: receipt.schema_version,
            fixture_id: receipt.fixture_id.clone(),
            fixture_sha256: receipt.fixture_sha256.clone(),
            fixture_freshness_clock_unix_seconds: receipt
                .fixture_freshness_clock_unix_seconds
                .to_string(),
            manifest_completion_source_kind: receipt.manifest_completion_source_kind.clone(),
            capture_instance_id: receipt.capture_instance_id.clone(),
            protocol_provider: receipt.protocol_provider.clone(),
            protocol_feed: receipt.protocol_feed.clone(),
            entitlement: "unknown".to_owned(),
            capture_mode: receipt.capture_mode.clone(),
            terminal_scope: receipt.terminal_scope.clone(),
            source_completeness: receipt.source_completeness.clone(),
            script_frame_count: receipt.script_frame_count,
            runner_received_frame_count: receipt.runner_received_frame_count,
            runner_received_digest_sha256: receipt.runner_received_digest_sha256.clone(),
            captured_frame_count: receipt.captured_frame_count,
            raw_market_frame_count: receipt.raw_market_frame_count,
            predecode_ack_count: receipt.predecode_ack_count,
            finalization_ack_count: receipt.finalization_ack_count,
            captured_bytes: receipt.captured_bytes.to_string(),
            ordered_raw_frames_sha256: receipt.ordered_raw_frames_sha256.clone(),
            finalization_rollup_sha256: receipt.finalization_rollup_sha256.clone(),
            output_item_count: receipt.output_item_count,
            pair_rollup_sha256: receipt.pair_rollup_sha256.clone(),
            receipt_sha256: receipt.receipt_sha256.clone(),
        }
    }
}

fn append_string(hasher: &mut Sha256, value: &str) -> Result<()> {
    let length = u16::try_from(value.len()).map_err(|_| MarketDataError::InputLimit)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests;
