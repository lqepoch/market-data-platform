//! Strict event-to-minute trade bars with caller-supplied session lineage.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use exact_decimal::ExactDecimal;
use market_contracts::{DecimalString, EntitlementState, MarketEventV1, UtcTimestamp, wire_u64};
use serde::{Deserialize, Serialize};

use crate::{MarketDataError, Result, queue::CollectionMessage};

const MINUTE_NS: i64 = 60_000_000_000;
const MAX_AGGREGATION_INPUT_RECORDS: usize = 100_000;
pub const MAX_AGGREGATION_OUTPUT_ROWS: u64 = 100_000;
const MAX_EXPECTED_SYMBOLS: usize = 500;
const MAX_WINDOW_MINUTES: u64 = 390;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWindow {
    pub trade_date: String,
    pub session_id: String,
    pub timezone: String,
    pub policy_id: String,
    pub policy_sha256: String,
    pub session_start: UtcTimestamp,
    pub session_end_exclusive: UtcTimestamp,
    pub window_start: UtcTimestamp,
    pub window_end_exclusive: UtcTimestamp,
    pub expected_symbols: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompletionEvidence {
    pub mode: CompletionMode,
    pub input_eof: bool,
    /// Explicitly distinguishes paged REST history from non-paged event streams.
    pub source_is_paged: bool,
    /// Paged inputs require `Some(true)`; non-paged sources may use `None`.
    pub source_pages_exhausted: Option<bool>,
    /// Actual local collection completion time, never substituted for source time.
    pub available_at: UtcTimestamp,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionMode {
    SyntheticEof,
    HistoricalEof,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeMinuteBarV1 {
    pub schema_version: u32,
    pub source_provider: String,
    pub source_feed: String,
    pub source_entitlement: String,
    pub source_numeric_encoding: String,
    pub symbol: String,
    pub bar_start_utc: UtcTimestamp,
    pub bar_end_exclusive_utc: UtcTimestamp,
    pub available_at_utc: UtcTimestamp,
    pub trade_date: String,
    pub session_id: String,
    pub session_timezone: String,
    pub session_policy_id: String,
    pub session_policy_sha256: String,
    pub session_start_utc: UtcTimestamp,
    pub session_end_exclusive_utc: UtcTimestamp,
    pub window_start_utc: UtcTimestamp,
    pub window_end_exclusive_utc: UtcTimestamp,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    #[serde(with = "wire_u64")]
    pub trade_count: u64,
    #[serde(with = "wire_u64")]
    pub quote_events_excluded: u64,
    #[serde(with = "wire_u64")]
    pub source_timestamp_missing_rows: u64,
    #[serde(with = "wire_u64")]
    pub sequence_gap_count: u64,
    #[serde(with = "wire_u64")]
    pub late_event_count: u64,
    #[serde(with = "wire_u64")]
    pub window_expected_minutes: u64,
    #[serde(with = "wire_u64")]
    pub window_empty_trade_minutes: u64,
    pub source_start_utc: UtcTimestamp,
    pub source_end_exclusive_utc: UtcTimestamp,
    pub window_input_eof: bool,
    pub source_pages_exhausted: Option<bool>,
    pub completion_mode: String,
    pub nbbo_input_status: String,
}

#[derive(Default)]
struct MinuteBucket {
    trades: Vec<TradePoint>,
    quote_count: u64,
}

struct TradePoint {
    timestamp: DateTime<Utc>,
    sequence: u64,
    price_token: String,
    price: ExactDecimal,
    size: ExactDecimal,
}

/// Aggregate only complete, source-timestamped equity trade windows.
///
/// Quotes are counted for lineage but never affect OHLCV. A gap, missing timestamp, late event,
/// unsupported option event, empty expected minute, or partial input prevents any bars escaping.
pub fn aggregate_trade_bars(
    messages: &[CollectionMessage],
    window: &SessionWindow,
    completion: &CompletionEvidence,
) -> Result<Vec<TradeMinuteBarV1>> {
    validate_window(window, completion)?;
    if messages.is_empty() || !completion.input_eof {
        return Err(MarketDataError::IncompleteWindow);
    }
    if messages.len() > MAX_AGGREGATION_INPUT_RECORDS {
        return Err(MarketDataError::InputLimit);
    }
    let expected: BTreeSet<_> = window.expected_symbols.iter().cloned().collect();
    if expected.len() != window.expected_symbols.len()
        || expected.is_empty()
        || expected.len() > MAX_EXPECTED_SYMBOLS
    {
        return Err(MarketDataError::InvalidInput);
    }

    let mut provenance: Option<(String, String, String, String)> = None;
    let mut buckets: BTreeMap<(String, i64), MinuteBucket> = BTreeMap::new();
    let start_ns = timestamp_ns(&window.window_start)?;
    let end_ns = timestamp_ns(&window.window_end_exclusive)?;
    let duration_ns = end_ns
        .checked_sub(start_ns)
        .ok_or(MarketDataError::InvalidInput)?;
    let expected_minutes =
        u64::try_from(duration_ns / MINUTE_NS).map_err(|_| MarketDataError::InvalidInput)?;
    let output_rows = u64::try_from(expected.len())
        .ok()
        .and_then(|symbols| symbols.checked_mul(expected_minutes))
        .ok_or(MarketDataError::InputLimit)?;
    if expected_minutes == 0
        || expected_minutes > MAX_WINDOW_MINUTES
        || output_rows > MAX_AGGREGATION_OUTPUT_ROWS
    {
        return Err(MarketDataError::InputLimit);
    }

    for message in messages {
        message.validate()?;
        if let CollectionMessage::Market(envelope) = message {
            let source_time = envelope
                .metadata
                .source_timestamp
                .as_ref()
                .ok_or(MarketDataError::IncompleteWindow)?;
            let source_dt = DateTime::parse_from_rfc3339(source_time.as_str())
                .map_err(|_| MarketDataError::Contract)?
                .with_timezone(&Utc);
            let received_dt =
                DateTime::parse_from_rfc3339(envelope.metadata.received_timestamp.as_str())
                    .map_err(|_| MarketDataError::Contract)?
                    .with_timezone(&Utc);
            let source_ns = source_dt
                .timestamp_nanos_opt()
                .ok_or(MarketDataError::InvalidInput)?;
            if source_ns < start_ns || source_ns >= end_ns {
                return Err(MarketDataError::IncompleteWindow);
            }
            let minute_start = start_ns + ((source_ns - start_ns) / MINUTE_NS) * MINUTE_NS;
            if received_dt.timestamp_nanos_opt().is_none_or(|value| {
                timestamp_ns(&completion.available_at).is_ok_and(|available| value > available)
            }) {
                return Err(MarketDataError::IncompleteWindow);
            }
            let source = &envelope.metadata.source;
            let numeric_encoding = source.numeric_encoding.as_str();
            if numeric_encoding == "unspecified" {
                return Err(MarketDataError::Contract);
            }
            let identity = (
                source.provider.clone(),
                source.feed.clone(),
                entitlement_name(source.entitlement).to_owned(),
                numeric_encoding.to_owned(),
            );
            if provenance
                .as_ref()
                .is_some_and(|existing| existing != &identity)
            {
                return Err(MarketDataError::MixedProvenance);
            }
            provenance.get_or_insert(identity);
            match &envelope.event {
                MarketEventV1::StockTrade {
                    symbol,
                    price,
                    size,
                } => {
                    if !expected.contains(symbol) {
                        return Err(MarketDataError::UnexpectedSymbol);
                    }
                    let price_exact = ExactDecimal::parse_json_number(price.as_str())
                        .map_err(|_| MarketDataError::Contract)?;
                    let size_exact = ExactDecimal::parse_json_number(size.as_str())
                        .map_err(|_| MarketDataError::Contract)?;
                    buckets
                        .entry((symbol.clone(), minute_start))
                        .or_default()
                        .trades
                        .push(TradePoint {
                            timestamp: source_dt,
                            sequence: envelope.metadata.sequence,
                            price_token: price.as_str().to_owned(),
                            price: price_exact,
                            size: size_exact,
                        });
                }
                MarketEventV1::StockQuote { symbol, .. } => {
                    if !expected.contains(symbol) {
                        return Err(MarketDataError::UnexpectedSymbol);
                    }
                    buckets
                        .entry((symbol.clone(), minute_start))
                        .or_default()
                        .quote_count += 1;
                }
                MarketEventV1::OptionQuote { .. } | MarketEventV1::OptionTrade { .. } => {
                    return Err(MarketDataError::UnsupportedEvent);
                }
            }
        }
    }

    let (provider, feed, entitlement, numeric_encoding) =
        provenance.ok_or(MarketDataError::IncompleteWindow)?;
    let completion_mode = match completion.mode {
        CompletionMode::SyntheticEof
            if provider == "synthetic"
                && !completion.source_is_paged
                && completion.source_pages_exhausted.is_none() =>
        {
            "synthetic_eof"
        }
        CompletionMode::HistoricalEof
            if provider != "synthetic"
                && completion.source_is_paged
                && completion.source_pages_exhausted == Some(true) =>
        {
            "historical_eof_paged"
        }
        CompletionMode::HistoricalEof
            if provider != "synthetic"
                && !completion.source_is_paged
                && completion.source_pages_exhausted.is_none() =>
        {
            "historical_eof_nonpaged"
        }
        _ => return Err(MarketDataError::IncompleteWindow),
    };

    let mut bars =
        Vec::with_capacity(usize::try_from(output_rows).map_err(|_| MarketDataError::InputLimit)?);
    for symbol in expected {
        for offset in 0..expected_minutes {
            let bar_start_ns = start_ns
                .checked_add(
                    i64::try_from(offset)
                        .map_err(|_| MarketDataError::InvalidInput)?
                        .checked_mul(MINUTE_NS)
                        .ok_or(MarketDataError::InvalidInput)?,
                )
                .ok_or(MarketDataError::InvalidInput)?;
            let key = (symbol.clone(), bar_start_ns);
            let bucket = buckets.get(&key).ok_or(MarketDataError::IncompleteWindow)?;
            if bucket.trades.is_empty() {
                return Err(MarketDataError::IncompleteWindow);
            }
            let mut trades = bucket.trades.iter().collect::<Vec<_>>();
            trades.sort_by(|left, right| {
                left.timestamp
                    .cmp(&right.timestamp)
                    .then(left.sequence.cmp(&right.sequence))
            });
            let open = trades.first().ok_or(MarketDataError::IncompleteWindow)?;
            let close = trades.last().ok_or(MarketDataError::IncompleteWindow)?;
            let mut high = open.price;
            let mut low = open.price;
            let mut volume = ExactDecimal::ZERO;
            let mut source_start = open.timestamp;
            let mut source_end_ns = open
                .timestamp
                .timestamp_nanos_opt()
                .ok_or(MarketDataError::InvalidInput)?
                .checked_add(1)
                .ok_or(MarketDataError::InvalidInput)?;
            for trade in &trades {
                if trade.price > high {
                    high = trade.price;
                }
                if trade.price < low {
                    low = trade.price;
                }
                volume = volume
                    .checked_add(trade.size)
                    .map_err(|_| MarketDataError::DecimalOverflow)?;
                if trade.timestamp < source_start {
                    source_start = trade.timestamp;
                }
                source_end_ns = source_end_ns.max(
                    trade
                        .timestamp
                        .timestamp_nanos_opt()
                        .ok_or(MarketDataError::InvalidInput)?
                        .checked_add(1)
                        .ok_or(MarketDataError::InvalidInput)?,
                );
            }
            let bar_end_ns = bar_start_ns
                .checked_add(MINUTE_NS)
                .ok_or(MarketDataError::InvalidInput)?;
            let available = completion.available_at.clone();
            if timestamp_ns(&available)? < bar_end_ns {
                return Err(MarketDataError::IncompleteWindow);
            }
            bars.push(TradeMinuteBarV1 {
                schema_version: 1,
                source_provider: provider.clone(),
                source_feed: feed.clone(),
                source_entitlement: entitlement.clone(),
                source_numeric_encoding: numeric_encoding.clone(),
                symbol: symbol.clone(),
                bar_start_utc: timestamp_from_ns(bar_start_ns)?,
                bar_end_exclusive_utc: timestamp_from_ns(bar_end_ns)?,
                available_at_utc: available,
                trade_date: window.trade_date.clone(),
                session_id: window.session_id.clone(),
                session_timezone: window.timezone.clone(),
                session_policy_id: window.policy_id.clone(),
                session_policy_sha256: window.policy_sha256.clone(),
                session_start_utc: window.session_start.clone(),
                session_end_exclusive_utc: window.session_end_exclusive.clone(),
                window_start_utc: window.window_start.clone(),
                window_end_exclusive_utc: window.window_end_exclusive.clone(),
                open: open.price_token.clone(),
                high: high.to_string(),
                low: low.to_string(),
                close: close.price_token.clone(),
                volume: volume.to_string(),
                trade_count: u64::try_from(trades.len())
                    .map_err(|_| MarketDataError::InvalidInput)?,
                quote_events_excluded: bucket.quote_count,
                source_timestamp_missing_rows: 0,
                sequence_gap_count: 0,
                // Historical API response time does not define point-in-time availability or
                // imply source-side lateness; real-time watermark mode is not implemented.
                late_event_count: 0,
                window_expected_minutes: expected_minutes,
                window_empty_trade_minutes: 0,
                source_start_utc: timestamp_from_ns(
                    source_start
                        .timestamp_nanos_opt()
                        .ok_or(MarketDataError::InvalidInput)?,
                )?,
                source_end_exclusive_utc: timestamp_from_ns(source_end_ns)?,
                window_input_eof: completion.input_eof,
                source_pages_exhausted: completion.source_pages_exhausted,
                completion_mode: completion_mode.to_owned(),
                nbbo_input_status: "excluded".to_owned(),
            });
        }
    }
    Ok(bars)
}

fn entitlement_name(entitlement: EntitlementState) -> &'static str {
    match entitlement {
        EntitlementState::Unknown => "unknown",
        EntitlementState::Authorized => "authorized",
        EntitlementState::Unauthorized => "unauthorized",
    }
}

fn validate_window(window: &SessionWindow, completion: &CompletionEvidence) -> Result<()> {
    if !completion.input_eof
        || completion.source_pages_exhausted == Some(false)
        || completion.source_is_paged != completion.source_pages_exhausted.is_some()
        || window.trade_date.len() != 10
        || !valid_metadata_label(&window.session_id, 256)
        || !valid_metadata_label(&window.timezone, 128)
        || !valid_metadata_label(&window.policy_id, 256)
        || !NaiveDate::parse_from_str(&window.trade_date, "%Y-%m-%d")
            .is_ok_and(|date| date.to_string() == window.trade_date)
        || !valid_sha256(&window.policy_sha256)
        || window.session_start > window.window_start
        || window.window_start >= window.window_end_exclusive
        || window.window_end_exclusive > window.session_end_exclusive
    {
        return Err(MarketDataError::InvalidInput);
    }
    let session_start = timestamp_ns(&window.session_start)?;
    let session_end = timestamp_ns(&window.session_end_exclusive)?;
    let window_start = timestamp_ns(&window.window_start)?;
    let window_end = timestamp_ns(&window.window_end_exclusive)?;
    let offset_from_session_start = window_start
        .checked_sub(session_start)
        .ok_or(MarketDataError::InvalidInput)?;
    let window_duration = window_end
        .checked_sub(window_start)
        .ok_or(MarketDataError::InvalidInput)?;
    let session_duration = session_end
        .checked_sub(session_start)
        .ok_or(MarketDataError::InvalidInput)?;
    if offset_from_session_start % MINUTE_NS != 0
        || window_duration % MINUTE_NS != 0
        || session_duration % MINUTE_NS != 0
        || u64::try_from(window_duration / MINUTE_NS).map_err(|_| MarketDataError::InvalidInput)?
            > MAX_WINDOW_MINUTES
        || timestamp_ns(&completion.available_at)? < window_end
    {
        return Err(MarketDataError::InvalidInput);
    }
    for symbol in &window.expected_symbols {
        validate_stock_symbol(symbol)?;
    }
    Ok(())
}

pub(crate) fn validate_stock_symbol(symbol: &str) -> Result<()> {
    let event = MarketEventV1::StockTrade {
        symbol: symbol.to_owned(),
        price: DecimalString::new("1").map_err(|_| MarketDataError::Contract)?,
        size: DecimalString::new("1").map_err(|_| MarketDataError::Contract)?,
    };
    event.validate().map_err(|_| MarketDataError::Contract)
}

fn valid_metadata_label(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn timestamp_ns(timestamp: &UtcTimestamp) -> Result<i64> {
    DateTime::parse_from_rfc3339(timestamp.as_str())
        .map_err(|_| MarketDataError::Contract)?
        .timestamp_nanos_opt()
        .ok_or(MarketDataError::InvalidInput)
}

fn timestamp_from_ns(nanos: i64) -> Result<UtcTimestamp> {
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsec = nanos.rem_euclid(1_000_000_000) as u32;
    let value =
        DateTime::<Utc>::from_timestamp(seconds, subsec).ok_or(MarketDataError::InvalidInput)?;
    UtcTimestamp::parse(&value.to_rfc3339()).map_err(|_| MarketDataError::Contract)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::CollectionMessage;
    use market_contracts::{
        DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1,
        MarketEventEnvelopeV1, NumericEncodingV1,
    };

    fn window() -> SessionWindow {
        SessionWindow {
            trade_date: "2026-10-08".into(),
            session_id: "synthetic-regular-2026-10-08".into(),
            timezone: "America/New_York".into(),
            policy_id: "synthetic-session-policy-v1".into(),
            policy_sha256: "a".repeat(64),
            session_start: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            session_end_exclusive: UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap(),
            window_start: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            window_end_exclusive: UtcTimestamp::parse("2026-10-08T13:32:00Z").unwrap(),
            expected_symbols: vec!["QQQ".into()],
        }
    }

    fn trade(sequence: u64, timestamp: &str, price: &str, size: &str) -> CollectionMessage {
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
                generation: 1,
                sequence,
                raw_frame_sha256: None,
                source_timestamp: Some(UtcTimestamp::parse(timestamp).unwrap()),
                received_timestamp: UtcTimestamp::parse(timestamp).unwrap(),
            },
            event: MarketEventV1::StockTrade {
                symbol: "QQQ".into(),
                price: DecimalString::new(price).unwrap(),
                size: DecimalString::new(size).unwrap(),
            },
        })
    }

    fn completion() -> CompletionEvidence {
        CompletionEvidence {
            mode: CompletionMode::SyntheticEof,
            input_eof: true,
            source_is_paged: false,
            source_pages_exhausted: None,
            available_at: UtcTimestamp::parse("2026-10-08T13:32:00Z").unwrap(),
        }
    }

    #[test]
    fn completed_bars_keep_exact_decimal_and_source_time_lineage() {
        let messages = vec![
            trade(1, "2026-10-08T13:30:10Z", "10.2500", "2"),
            trade(2, "2026-10-08T13:30:50Z", "10.5", "3"),
            trade(3, "2026-10-08T13:31:02Z", "10.125", "1"),
        ];
        let bars = aggregate_trade_bars(&messages, &window(), &completion()).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].open, "10.2500");
        assert_eq!(bars[0].close, "10.5");
        assert_eq!(bars[0].volume, "5");
        assert_eq!(bars[0].source_start_utc.as_str(), "2026-10-08T13:30:10Z");
        assert_eq!(
            bars[0].source_end_exclusive_utc.as_str(),
            "2026-10-08T13:30:50.000000001Z"
        );
        assert_eq!(bars[0].available_at_utc.as_str(), "2026-10-08T13:32:00Z");
        assert_eq!(bars[0].nbbo_input_status, "excluded");
        assert_eq!(bars[0].source_numeric_encoding, "decimal_token");
        let serialized = serde_json::to_value(&bars[0]).unwrap();
        assert_eq!(serialized["trade_count"], "2");
    }

    #[test]
    fn empty_minutes_and_missing_source_time_fail_closed() {
        assert!(matches!(
            aggregate_trade_bars(
                &[trade(1, "2026-10-08T13:30:10Z", "10", "1")],
                &window(),
                &completion()
            ),
            Err(MarketDataError::IncompleteWindow)
        ));
        let mut missing = trade(1, "2026-10-08T13:30:10Z", "10", "1");
        if let CollectionMessage::Market(event) = &mut missing {
            event.metadata.source_timestamp = None;
        }
        assert!(matches!(
            aggregate_trade_bars(&[missing], &window(), &completion()),
            Err(MarketDataError::IncompleteWindow)
        ));
    }

    #[test]
    fn aggregation_rejects_output_explosion_before_allocating_bars() {
        let mut oversized = window();
        oversized.window_end_exclusive = UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap();
        oversized.expected_symbols = (0..258).map(|index| format!("SYM{index:03}")).collect();
        let mut evidence = completion();
        evidence.available_at = UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap();
        assert!(matches!(
            aggregate_trade_bars(
                &[trade(1, "2026-10-08T13:30:10Z", "10", "1")],
                &oversized,
                &evidence
            ),
            Err(MarketDataError::InputLimit)
        ));
    }

    #[test]
    fn historical_bars_preserve_explicit_pagination_context() {
        let mut events = vec![
            trade(1, "2026-10-08T13:30:10Z", "10", "1"),
            trade(2, "2026-10-08T13:31:10Z", "11", "1"),
        ];
        for message in &mut events {
            if let CollectionMessage::Market(event) = message {
                event.metadata.source = market_contracts::MarketDataSourceV1::new(
                    "fixture-provider",
                    "sip",
                    EntitlementState::Unknown,
                    NumericEncodingV1::DecimalToken,
                    None,
                )
                .unwrap();
            }
        }
        let mut evidence = completion();
        evidence.mode = CompletionMode::HistoricalEof;
        assert_eq!(
            aggregate_trade_bars(&events, &window(), &evidence).unwrap()[0].completion_mode,
            "historical_eof_nonpaged",
            "non-paged historical EOF is valid when explicitly configured"
        );
        evidence.source_is_paged = true;
        assert!(matches!(
            aggregate_trade_bars(&events, &window(), &evidence),
            Err(MarketDataError::InvalidInput)
        ));
        evidence.source_pages_exhausted = Some(true);
        let paged = aggregate_trade_bars(&events, &window(), &evidence).unwrap();
        assert_eq!(paged.len(), 2);
        assert_eq!(paged[0].completion_mode, "historical_eof_paged");
        evidence.source_pages_exhausted = Some(false);
        assert!(matches!(
            aggregate_trade_bars(&events, &window(), &evidence),
            Err(MarketDataError::InvalidInput)
        ));
    }

    #[test]
    fn window_rejects_invalid_symbols_and_unbounded_or_controlled_ids() {
        let messages = vec![
            trade(1, "2026-10-08T13:30:10Z", "10", "1"),
            trade(2, "2026-10-08T13:31:10Z", "11", "1"),
        ];
        let mut invalid_symbol = window();
        invalid_symbol.expected_symbols = vec![format!("{}!", "Q".repeat(256))];
        assert!(aggregate_trade_bars(&messages, &invalid_symbol, &completion()).is_err());

        let mut invalid_session = window();
        invalid_session.session_id = "s".repeat(257);
        assert!(aggregate_trade_bars(&messages, &invalid_session, &completion()).is_err());

        let mut invalid_policy = window();
        invalid_policy.policy_id = "policy\nspoof".into();
        assert!(aggregate_trade_bars(&messages, &invalid_policy, &completion()).is_err());
    }
}
