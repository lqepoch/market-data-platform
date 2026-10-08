use std::collections::BTreeSet;

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
        let start_ns = timestamp_to_ns(&identity.window_start)?;
        let mut expected_rows = 0_u64;
        for symbol in &self.symbols {
            for offset in 0..identity.expected_minutes {
                let offset_ns = i64::try_from(offset)
                    .map_err(|_| MarketDataError::InputLimit)?
                    .checked_mul(60_000_000_000)
                    .ok_or(MarketDataError::InputLimit)?;
                let bar_start_ns = start_ns
                    .checked_add(offset_ns)
                    .ok_or(MarketDataError::InputLimit)?;
                if !self.bar_keys.contains(&(symbol.clone(), bar_start_ns)) {
                    return Err(MarketDataError::IncompleteWindow);
                }
                expected_rows = expected_rows
                    .checked_add(1)
                    .ok_or(MarketDataError::InputLimit)?;
            }
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
