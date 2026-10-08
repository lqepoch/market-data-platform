use std::collections::{BTreeMap, BTreeSet};

use super::{rows::source_from_bar, *};
use market_contracts::{NumericEncodingV1, RawFrameStorageRecordV1};

#[derive(Default)]
pub(super) struct DatasetFacts {
    source: Option<MarketDataSourceV1>,
    symbols: BTreeSet<String>,
    minimum_source_ns: Option<i64>,
    maximum_source_end_ns: Option<i64>,
    source_timestamp_missing_rows: u64,
    bar_window: Option<BarWindowIdentity>,
    bar_keys: BTreeSet<(String, i64)>,
    bar_empty_trade_minutes: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BarWindowIdentity {
    provider: String,
    feed: String,
    entitlement: String,
    numeric_encoding: String,
    trade_date: String,
    session_id: String,
    timezone: String,
    policy_id: String,
    policy_sha256: String,
    session_start: String,
    session_end_exclusive: String,
    window_start: String,
    window_end_exclusive: String,
    available_at: String,
    expected_minutes: u64,
    input_eof: bool,
    source_pages_exhausted: Option<bool>,
    completion_mode: String,
    nbbo_input_status: String,
}

impl BarWindowIdentity {
    fn from_row(row: &TradeMinuteBarV1) -> Self {
        Self {
            provider: row.source_provider.clone(),
            feed: row.source_feed.clone(),
            entitlement: row.source_entitlement.clone(),
            numeric_encoding: row.source_numeric_encoding.clone(),
            trade_date: row.trade_date.clone(),
            session_id: row.session_id.clone(),
            timezone: row.session_timezone.clone(),
            policy_id: row.session_policy_id.clone(),
            policy_sha256: row.session_policy_sha256.clone(),
            session_start: row.session_start_utc.as_str().to_owned(),
            session_end_exclusive: row.session_end_exclusive_utc.as_str().to_owned(),
            window_start: row.window_start_utc.as_str().to_owned(),
            window_end_exclusive: row.window_end_exclusive_utc.as_str().to_owned(),
            available_at: row.available_at_utc.as_str().to_owned(),
            expected_minutes: row.window_expected_minutes,
            input_eof: row.window_input_eof,
            source_pages_exhausted: row.source_pages_exhausted,
            completion_mode: row.completion_mode.clone(),
            nbbo_input_status: row.nbbo_input_status.clone(),
        }
    }
}

impl DatasetFacts {
    pub(super) fn observe_source(&mut self, mut source: MarketDataSourceV1) -> Result<()> {
        source.source_record_id = None;
        if self
            .source
            .as_ref()
            .is_some_and(|existing| existing != &source)
        {
            return Err(MarketDataError::MixedProvenance);
        }
        self.source.get_or_insert(source);
        Ok(())
    }

    pub(super) fn observe_source_interval(
        &mut self,
        start_ns: i64,
        end_exclusive_ns: i64,
    ) -> Result<()> {
        if start_ns >= end_exclusive_ns {
            return Err(MarketDataError::Contract);
        }
        self.minimum_source_ns = Some(
            self.minimum_source_ns
                .map_or(start_ns, |current| current.min(start_ns)),
        );
        self.maximum_source_end_ns = Some(
            self.maximum_source_end_ns
                .map_or(end_exclusive_ns, |current| current.max(end_exclusive_ns)),
        );
        Ok(())
    }

    pub(super) fn observe_event(&mut self, row: &MarketEventEnvelopeV1) -> Result<()> {
        self.observe_source(row.metadata.source.clone())?;
        let symbol = match &row.event {
            MarketEventV1::StockQuote { symbol, .. }
            | MarketEventV1::StockTrade { symbol, .. }
            | MarketEventV1::OptionQuote { symbol, .. }
            | MarketEventV1::OptionTrade { symbol, .. } => symbol,
        };
        self.symbols.insert(symbol.clone());
        if let Some(timestamp) = &row.metadata.source_timestamp {
            let start_ns = timestamp_to_ns(timestamp.as_str())?;
            self.observe_source_interval(
                start_ns,
                start_ns
                    .checked_add(1)
                    .ok_or(MarketDataError::InvalidInput)?,
            )?;
        } else {
            self.source_timestamp_missing_rows = self
                .source_timestamp_missing_rows
                .checked_add(1)
                .ok_or(MarketDataError::InputLimit)?;
        }
        Ok(())
    }

    pub(super) fn observe_raw_frame(&mut self, row: &RawFrameStorageRecordV1) -> Result<()> {
        row.validate().map_err(|_| MarketDataError::Contract)?;
        let source = MarketDataSourceV1 {
            provider: row.provider.clone(),
            feed: row.feed.clone(),
            entitlement: row.entitlement,
            numeric_encoding: NumericEncodingV1::RawMessagePackBytes,
            source_record_id: None,
        };
        self.observe_source(source)?;
        self.symbols
            .extend(row.symbols().map_err(|_| MarketDataError::Contract)?);
        self.source_timestamp_missing_rows = self
            .source_timestamp_missing_rows
            .checked_add(1)
            .ok_or(MarketDataError::InputLimit)?;
        Ok(())
    }

    pub(super) fn observe_bar(&mut self, row: &TradeMinuteBarV1) -> Result<()> {
        let source = source_from_bar(row)?;
        self.observe_source(source)?;
        self.symbols.insert(row.symbol.clone());
        let start_ns = timestamp_to_ns(row.source_start_utc.as_str())?;
        let end_ns = timestamp_to_ns(row.source_end_exclusive_utc.as_str())?;
        self.observe_source_interval(start_ns, end_ns)?;

        let identity = BarWindowIdentity::from_row(row);
        if self
            .bar_window
            .as_ref()
            .is_some_and(|existing| existing != &identity)
        {
            return Err(MarketDataError::Contract);
        }
        self.bar_window.get_or_insert(identity);
        match self.bar_empty_trade_minutes.get(&row.symbol) {
            Some(empty_minutes) if *empty_minutes != row.window_empty_trade_minutes => {
                return Err(MarketDataError::Contract);
            }
            Some(_) => {}
            None => {
                self.bar_empty_trade_minutes
                    .insert(row.symbol.clone(), row.window_empty_trade_minutes);
            }
        }
        let bar_start_ns = timestamp_to_ns(row.bar_start_utc.as_str())?;
        if !self.bar_keys.insert((row.symbol.clone(), bar_start_ns)) {
            return Err(MarketDataError::Contract);
        }
        Ok(())
    }

    pub(super) fn validate_complete_bar_grid(&self) -> Result<()> {
        let Some(identity) = &self.bar_window else {
            return Ok(());
        };
        let mut expected_rows = 0_u64;
        let mut observed_by_symbol = BTreeMap::<&str, u64>::new();
        for (symbol, _) in &self.bar_keys {
            let observed = observed_by_symbol.entry(symbol.as_str()).or_default();
            *observed = observed.checked_add(1).ok_or(MarketDataError::InputLimit)?;
        }
        for (symbol, empty_minutes) in &self.bar_empty_trade_minutes {
            if *empty_minutes >= identity.expected_minutes {
                return Err(MarketDataError::IncompleteWindow);
            }
            let required_rows = identity
                .expected_minutes
                .checked_sub(*empty_minutes)
                .ok_or(MarketDataError::IncompleteWindow)?;
            if observed_by_symbol.get(symbol.as_str()).copied() != Some(required_rows) {
                return Err(MarketDataError::IncompleteWindow);
            }
            expected_rows = expected_rows
                .checked_add(required_rows)
                .ok_or(MarketDataError::InputLimit)?;
        }
        if observed_by_symbol.len() != self.bar_empty_trade_minutes.len()
            || self.bar_empty_trade_minutes.len() != self.symbols.len()
        {
            return Err(MarketDataError::IncompleteWindow);
        }
        if expected_rows != self.bar_keys.len() as u64 {
            return Err(MarketDataError::IncompleteWindow);
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
    ) -> Result<(
        MarketDataSourceV1,
        Vec<String>,
        Option<DatasetTimeRangeV1>,
        u64,
    )> {
        self.validate_complete_bar_grid()?;
        let source = self.source.ok_or(MarketDataError::IncompleteWindow)?;
        let time_range = match (self.minimum_source_ns, self.maximum_source_end_ns) {
            (Some(start), Some(end)) => Some(DatasetTimeRangeV1 {
                start_inclusive: timestamp_from_ns(start)?,
                end_exclusive: timestamp_from_ns(end)?,
            }),
            (None, None) => None,
            _ => return Err(MarketDataError::Contract),
        };
        Ok((
            source,
            self.symbols.into_iter().collect(),
            time_range,
            self.source_timestamp_missing_rows,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_contracts::UtcTimestamp;
    use tempfile::tempdir;

    use crate::{
        MarketDataError,
        aggregate::TradeMinuteBarV1,
        parquet_store::{query_bars, rows::validate_bar_row, write_bars},
    };

    fn bar(symbol: &str, minute: u8, empty_trade_minutes: u64) -> TradeMinuteBarV1 {
        let (bar_start, bar_end, source_time) = match minute {
            0 => (
                "2026-10-08T13:30:00Z",
                "2026-10-08T13:31:00Z",
                "2026-10-08T13:30:15Z",
            ),
            1 => (
                "2026-10-08T13:31:00Z",
                "2026-10-08T13:32:00Z",
                "2026-10-08T13:31:15Z",
            ),
            _ => panic!("fixture minute out of range"),
        };
        let source_start = UtcTimestamp::parse(source_time).unwrap();
        let source_end = UtcTimestamp::parse(match minute {
            0 => "2026-10-08T13:30:15.000000001Z",
            1 => "2026-10-08T13:31:15.000000001Z",
            _ => unreachable!(),
        })
        .unwrap();
        TradeMinuteBarV1 {
            schema_version: 1,
            source_provider: "synthetic".into(),
            source_feed: "synthetic".into(),
            source_entitlement: "unknown".into(),
            source_numeric_encoding: "decimal_token".into(),
            symbol: symbol.to_owned(),
            bar_start_utc: UtcTimestamp::parse(bar_start).unwrap(),
            bar_end_exclusive_utc: UtcTimestamp::parse(bar_end).unwrap(),
            available_at_utc: UtcTimestamp::parse("2026-10-08T13:32:00Z").unwrap(),
            trade_date: "2026-10-08".into(),
            session_id: "synthetic-session".into(),
            session_timezone: "America/New_York".into(),
            session_policy_id: "synthetic-policy-v1".into(),
            session_policy_sha256: "a".repeat(64),
            session_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            session_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap(),
            window_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            window_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:32:00Z").unwrap(),
            open: "10".into(),
            high: "10".into(),
            low: "10".into(),
            close: "10".into(),
            volume: "1".into(),
            trade_count: 1,
            quote_events_excluded: 0,
            source_timestamp_missing_rows: 0,
            sequence_gap_count: 0,
            late_event_count: 0,
            window_expected_minutes: 2,
            window_empty_trade_minutes: empty_trade_minutes,
            source_start_utc: source_start,
            source_end_exclusive_utc: source_end,
            window_input_eof: true,
            source_pages_exhausted: None,
            completion_mode: "synthetic_eof".into(),
            nbbo_input_status: "excluded".into(),
        }
    }

    #[test]
    fn sparse_verified_bar_window_roundtrips_when_empty_minutes_are_declared() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("sparse.parquet");
        let sparse = bar("QQQ", 0, 1);
        assert!(validate_bar_row(&sparse).is_ok());
        let verification = write_bars(&path, std::slice::from_ref(&sparse)).unwrap();
        assert_eq!(verification.footer_rows, 1);

        let rows = query_bars(&path, None).unwrap();
        assert_eq!(rows, [sparse]);
        assert_eq!(rows[0].window_expected_minutes, 2);
        assert_eq!(rows[0].window_empty_trade_minutes, 1);
    }

    #[test]
    fn reader_rejects_undeclared_missing_bars_and_empty_counts_equal_to_window() {
        let undeclared = bar("QQQ", 0, 0);
        assert!(validate_bar_row(&undeclared).is_ok());
        let mut facts = DatasetFacts::default();
        facts.observe_bar(&undeclared).unwrap();
        assert!(matches!(
            facts.finish(),
            Err(MarketDataError::IncompleteWindow)
        ));

        let temp = tempdir().unwrap();
        let path = temp.path().join("undeclared-gap.parquet");
        assert!(matches!(
            write_bars(&path, &[undeclared]),
            Err(MarketDataError::IncompleteWindow)
        ));
        assert!(!path.exists());

        let all_empty_but_row = bar("QQQ", 0, 2);
        assert!(validate_bar_row(&all_empty_but_row).is_err());
    }

    #[test]
    fn window_empty_count_is_coherent_per_symbol_but_can_differ_between_symbols() {
        let mut facts = DatasetFacts::default();
        let qqq_sparse = bar("QQQ", 0, 1);
        let spy_first = bar("SPY", 0, 0);
        let spy_second = bar("SPY", 1, 0);
        for row in [&qqq_sparse, &spy_first, &spy_second] {
            facts.observe_bar(row).unwrap();
        }
        assert!(facts.finish().is_ok());

        let mut inconsistent = DatasetFacts::default();
        let first = bar("QQQ", 0, 1);
        let second = bar("QQQ", 1, 0);
        inconsistent.observe_bar(&first).unwrap();
        assert!(matches!(
            inconsistent.observe_bar(&second),
            Err(MarketDataError::Contract)
        ));
    }
}
