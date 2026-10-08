use super::*;

pub(super) fn decode_event_batch(batch: &RecordBatch) -> Result<Vec<MarketEventEnvelopeV1>> {
    validate_no_unexpected_nulls(batch)?;
    let col = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let strings = |name: &str| -> Result<&StringArray> { string_column(batch, col(name)?) };
    let timestamps = |name: &str| -> Result<&TimestampNanosecondArray> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let u64s = |name: &str| -> Result<&UInt64Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let u32s = |name: &str| -> Result<&UInt32Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let source = MarketDataSourceV1::new(
            value_string(strings("provider")?, index)?.to_owned(),
            value_string(strings("feed")?, index)?.to_owned(),
            parse_entitlement(value_string(strings("entitlement")?, index)?)?,
            parse_numeric_encoding(value_string(strings("numeric_encoding")?, index)?)?,
            optional_string_value(strings("source_record_id")?, index).map(str::to_owned),
        )
        .map_err(|_| MarketDataError::Contract)?;
        let metadata = EventMetadataV1 {
            schema_version: u32_value(u32s("schema_version")?, index)?,
            source,
            generation: u64_value(u64s("generation")?, index)?,
            sequence: u64_value(u64s("sequence")?, index)?,
            raw_frame_sha256: optional_string_value(strings("raw_frame_sha256")?, index)
                .map(str::to_owned),
            source_timestamp: optional_timestamp_value(timestamps("source_timestamp")?, index)?,
            received_timestamp: timestamp_value(timestamps("received_timestamp")?, index)?,
        };
        let kind = value_string(strings("event_kind")?, index)?;
        let symbol = value_string(strings("symbol")?, index)?.to_owned();
        let price = optional_decimal_value(strings("price")?, index)?;
        let size = optional_decimal_value(strings("size")?, index)?;
        let bid = optional_decimal_value(strings("bid")?, index)?;
        let ask = optional_decimal_value(strings("ask")?, index)?;
        let bid_size = optional_decimal_value(strings("bid_size")?, index)?;
        let ask_size = optional_decimal_value(strings("ask_size")?, index)?;
        let event = match kind {
            "stock_quote" => {
                if price.is_some() || size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::StockQuote {
                    symbol,
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                }
            }
            "stock_trade" => {
                if bid.is_some() || ask.is_some() || bid_size.is_some() || ask_size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::StockTrade {
                    symbol,
                    price: price.ok_or(MarketDataError::Contract)?,
                    size: size.ok_or(MarketDataError::Contract)?,
                }
            }
            "option_quote" => {
                if price.is_some() || size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::OptionQuote {
                    symbol,
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                }
            }
            "option_trade" => {
                if bid.is_some() || ask.is_some() || bid_size.is_some() || ask_size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::OptionTrade {
                    symbol,
                    price: price.ok_or(MarketDataError::Contract)?,
                    size: size.ok_or(MarketDataError::Contract)?,
                }
            }
            _ => return Err(MarketDataError::Contract),
        };
        let row = MarketEventEnvelopeV1 { metadata, event };
        row.validate().map_err(|_| MarketDataError::Contract)?;
        rows.push(row);
    }
    Ok(rows)
}

pub(super) fn decode_event_v2_batch(batch: &RecordBatch) -> Result<Vec<MarketEventParquetRowV2>> {
    let events = decode_event_batch(batch)?;
    let index = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let u64s = |name: &str| -> Result<&UInt64Array> {
        batch
            .column(index(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let u32s = |name: &str| -> Result<&UInt32Array> {
        batch
            .column(index(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let generation = u64s("raw_frame_generation")?;
    let sequence = u64s("raw_frame_sequence")?;
    let ordinal = u32s("raw_frame_event_ordinal")?;
    let count = u32s("raw_frame_event_count")?;
    let mut rows = Vec::with_capacity(events.len());
    for (index, event) in events.into_iter().enumerate() {
        let present = [
            !generation.is_null(index),
            !sequence.is_null(index),
            !ordinal.is_null(index),
            !count.is_null(index),
        ];
        let raw_frame_reference = match present {
            [false, false, false, false] => None,
            [true, true, true, true] => Some(market_contracts::RawFrameReferenceV2 {
                raw_frame_generation: generation.value(index),
                raw_frame_sequence: sequence.value(index),
                raw_frame_event_ordinal: ordinal.value(index),
                raw_frame_event_count: count.value(index),
            }),
            _ => return Err(MarketDataError::Contract),
        };
        let row = MarketEventParquetRowV2 {
            event,
            raw_frame_reference,
        };
        row.validate().map_err(|_| MarketDataError::Contract)?;
        rows.push(row);
    }
    Ok(rows)
}

pub(super) fn decode_bar_batch(batch: &RecordBatch) -> Result<Vec<TradeMinuteBarV1>> {
    validate_no_unexpected_nulls(batch)?;
    let col = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let strings = |name: &str| -> Result<&StringArray> { string_column(batch, col(name)?) };
    let timestamps = |name: &str| -> Result<&TimestampNanosecondArray> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let u64s = |name: &str| -> Result<&UInt64Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let u32s = |name: &str| -> Result<&UInt32Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let bools = |name: &str| -> Result<&BooleanArray> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let row = TradeMinuteBarV1 {
            schema_version: u32_value(u32s("schema_version")?, index)?,
            source_provider: value_string(strings("source_provider")?, index)?.to_owned(),
            source_feed: value_string(strings("source_feed")?, index)?.to_owned(),
            source_entitlement: value_string(strings("source_entitlement")?, index)?.to_owned(),
            source_numeric_encoding: value_string(strings("source_numeric_encoding")?, index)?
                .to_owned(),
            symbol: value_string(strings("symbol")?, index)?.to_owned(),
            bar_start_utc: timestamp_value(timestamps("bar_start_utc")?, index)?,
            bar_end_exclusive_utc: timestamp_value(timestamps("bar_end_exclusive_utc")?, index)?,
            available_at_utc: timestamp_value(timestamps("available_at_utc")?, index)?,
            trade_date: value_string(strings("trade_date")?, index)?.to_owned(),
            session_id: value_string(strings("session_id")?, index)?.to_owned(),
            session_timezone: value_string(strings("session_timezone")?, index)?.to_owned(),
            session_policy_id: value_string(strings("session_policy_id")?, index)?.to_owned(),
            session_policy_sha256: value_string(strings("session_policy_sha256")?, index)?
                .to_owned(),
            session_start_utc: timestamp_value(timestamps("session_start_utc")?, index)?,
            session_end_exclusive_utc: timestamp_value(
                timestamps("session_end_exclusive_utc")?,
                index,
            )?,
            window_start_utc: timestamp_value(timestamps("window_start_utc")?, index)?,
            window_end_exclusive_utc: timestamp_value(
                timestamps("window_end_exclusive_utc")?,
                index,
            )?,
            open: value_string(strings("open")?, index)?.to_owned(),
            high: value_string(strings("high")?, index)?.to_owned(),
            low: value_string(strings("low")?, index)?.to_owned(),
            close: value_string(strings("close")?, index)?.to_owned(),
            volume: value_string(strings("volume")?, index)?.to_owned(),
            trade_count: u64_value(u64s("trade_count")?, index)?,
            quote_events_excluded: u64_value(u64s("quote_events_excluded")?, index)?,
            source_timestamp_missing_rows: u64_value(
                u64s("source_timestamp_missing_rows")?,
                index,
            )?,
            sequence_gap_count: u64_value(u64s("sequence_gap_count")?, index)?,
            late_event_count: u64_value(u64s("late_event_count")?, index)?,
            window_expected_minutes: u64_value(u64s("window_expected_minutes")?, index)?,
            window_empty_trade_minutes: u64_value(u64s("window_empty_trade_minutes")?, index)?,
            source_start_utc: timestamp_value(timestamps("source_start_utc")?, index)?,
            source_end_exclusive_utc: timestamp_value(
                timestamps("source_end_exclusive_utc")?,
                index,
            )?,
            window_input_eof: bool_value(bools("window_input_eof")?, index)?,
            source_pages_exhausted: if bools("source_pages_exhausted")?.is_null(index) {
                None
            } else {
                Some(bool_value(bools("source_pages_exhausted")?, index)?)
            },
            completion_mode: value_string(strings("completion_mode")?, index)?.to_owned(),
            nbbo_input_status: value_string(strings("nbbo_input_status")?, index)?.to_owned(),
        };
        validate_bar_row(&row)?;
        rows.push(row);
    }
    Ok(rows)
}

pub(super) fn validate_bar_row(row: &TradeMinuteBarV1) -> Result<()> {
    let entitlement = parse_entitlement(&row.source_entitlement)?;
    let encoding = parse_numeric_encoding(&row.source_numeric_encoding)?;
    MarketDataSourceV1::new(
        row.source_provider.clone(),
        row.source_feed.clone(),
        entitlement,
        encoding,
        None,
    )
    .map_err(|_| MarketDataError::Contract)?;
    if row.schema_version != 1
        || !valid_metadata_label(&row.session_id, 256)
        || !valid_metadata_label(&row.session_timezone, 128)
        || !valid_metadata_label(&row.session_policy_id, 256)
        || !valid_sha256(&row.session_policy_sha256)
        || row.completion_mode.is_empty()
        || row.nbbo_input_status != "excluded"
        || !row.window_input_eof
        || row.source_timestamp_missing_rows != 0
        || row.sequence_gap_count != 0
        || row.late_event_count != 0
        || row.trade_count == 0
        || !NaiveDate::parse_from_str(&row.trade_date, "%Y-%m-%d")
            .is_ok_and(|date| date.to_string() == row.trade_date)
    {
        return Err(MarketDataError::Contract);
    }
    validate_stock_symbol(&row.symbol)?;

    let session_start = timestamp_to_ns(row.session_start_utc.as_str())?;
    let session_end = timestamp_to_ns(row.session_end_exclusive_utc.as_str())?;
    let window_start = timestamp_to_ns(row.window_start_utc.as_str())?;
    let window_end = timestamp_to_ns(row.window_end_exclusive_utc.as_str())?;
    let bar_start = timestamp_to_ns(row.bar_start_utc.as_str())?;
    let bar_end = timestamp_to_ns(row.bar_end_exclusive_utc.as_str())?;
    let available_at = timestamp_to_ns(row.available_at_utc.as_str())?;
    let source_start = timestamp_to_ns(row.source_start_utc.as_str())?;
    let source_end = timestamp_to_ns(row.source_end_exclusive_utc.as_str())?;
    let window_duration = window_end
        .checked_sub(window_start)
        .ok_or(MarketDataError::Contract)?;
    let session_duration = session_end
        .checked_sub(session_start)
        .ok_or(MarketDataError::Contract)?;
    let offset = window_start
        .checked_sub(session_start)
        .ok_or(MarketDataError::Contract)?;
    let expected_minutes =
        u64::try_from(window_duration / 60_000_000_000).map_err(|_| MarketDataError::Contract)?;
    if session_start > window_start
        || window_start >= window_end
        || window_end > session_end
        || session_duration <= 0
        || window_duration <= 0
        || offset % 60_000_000_000 != 0
        || session_duration % 60_000_000_000 != 0
        || window_duration % 60_000_000_000 != 0
        || expected_minutes == 0
        || expected_minutes > 390
        || row.window_expected_minutes != expected_minutes
        || row.window_empty_trade_minutes >= expected_minutes
        || bar_start % 60_000_000_000 != 0
        || bar_start < window_start
        || bar_end
            != bar_start
                .checked_add(60_000_000_000)
                .ok_or(MarketDataError::Contract)?
        || bar_end > window_end
        || available_at < window_end
        || source_start < bar_start
        || source_start >= source_end
        || source_end > bar_end
    {
        return Err(MarketDataError::Contract);
    }

    match row.completion_mode.as_str() {
        "synthetic_eof"
            if row.source_provider == "synthetic"
                && row.source_feed == "synthetic"
                && entitlement == EntitlementState::Unknown
                && encoding == NumericEncodingV1::DecimalToken
                && row.source_pages_exhausted.is_none() => {}
        "historical_eof_paged"
            if row.source_provider != "synthetic" && row.source_pages_exhausted == Some(true) => {}
        "historical_eof_nonpaged"
            if row.source_provider != "synthetic" && row.source_pages_exhausted.is_none() => {}
        _ => return Err(MarketDataError::Contract),
    }

    let open = positive_decimal(&row.open)?;
    let high = positive_decimal(&row.high)?;
    let low = positive_decimal(&row.low)?;
    let close = positive_decimal(&row.close)?;
    let volume = nonnegative_decimal(&row.volume)?;
    if volume <= ExactDecimal::ZERO
        || low > high
        || high < open
        || high < close
        || low > open
        || low > close
    {
        return Err(MarketDataError::Contract);
    }
    Ok(())
}

pub(super) fn source_from_bar(row: &TradeMinuteBarV1) -> Result<MarketDataSourceV1> {
    MarketDataSourceV1::new(
        row.source_provider.clone(),
        row.source_feed.clone(),
        parse_entitlement(&row.source_entitlement)?,
        parse_numeric_encoding(&row.source_numeric_encoding)?,
        None,
    )
    .map_err(|_| MarketDataError::Contract)
}

fn positive_decimal(value: &str) -> Result<ExactDecimal> {
    DecimalString::new(value.to_owned()).map_err(|_| MarketDataError::Contract)?;
    let value = ExactDecimal::parse_json_number(value).map_err(|_| MarketDataError::Contract)?;
    if value <= ExactDecimal::ZERO {
        return Err(MarketDataError::Contract);
    }
    Ok(value)
}

fn nonnegative_decimal(value: &str) -> Result<ExactDecimal> {
    DecimalString::new(value.to_owned()).map_err(|_| MarketDataError::Contract)?;
    let value = ExactDecimal::parse_json_number(value).map_err(|_| MarketDataError::Contract)?;
    if value < ExactDecimal::ZERO {
        return Err(MarketDataError::Contract);
    }
    Ok(value)
}

pub(super) fn parse_entitlement(value: &str) -> Result<EntitlementState> {
    match value {
        "unknown" => Ok(EntitlementState::Unknown),
        "authorized" => Ok(EntitlementState::Authorized),
        "unauthorized" => Ok(EntitlementState::Unauthorized),
        _ => Err(MarketDataError::Contract),
    }
}

pub(super) fn parse_numeric_encoding(value: &str) -> Result<NumericEncodingV1> {
    match value {
        "decimal_token" => Ok(NumericEncodingV1::DecimalToken),
        "integer_token" => Ok(NumericEncodingV1::IntegerToken),
        "binary_float64_shortest_decimal" => Ok(NumericEncodingV1::BinaryFloat64ShortestDecimal),
        "binary_float32_shortest_decimal" => Ok(NumericEncodingV1::BinaryFloat32ShortestDecimal),
        "unspecified" => Err(MarketDataError::Contract),
        _ => Err(MarketDataError::Contract),
    }
}

fn optional_decimal_value(array: &StringArray, index: usize) -> Result<Option<DecimalString>> {
    optional_string_value(array, index)
        .map(|value| DecimalString::new(value.to_owned()).map_err(|_| MarketDataError::Contract))
        .transpose()
}

pub(super) fn optional_string_value(array: &StringArray, index: usize) -> Option<&str> {
    (!array.is_null(index)).then(|| array.value(index))
}

pub(super) fn optional_timestamp_value(
    array: &TimestampNanosecondArray,
    index: usize,
) -> Result<Option<UtcTimestamp>> {
    if array.is_null(index) {
        Ok(None)
    } else {
        timestamp_value(array, index).map(Some)
    }
}
