//! Bounded JSONL ingress for the shared market and control envelopes.
//!
//! Provider wire formats and MessagePack decoding belong to `broker-connectors`. This module only
//! accepts already-adapted shared-contract JSON and enforces the raw-frame bound before parsing.

use std::io::{self, BufRead};

use market_contracts::{
    ControlEventEnvelopeV1, MAX_MARKET_FRAME_BYTES, MarketEventEnvelopeV1,
    validate_market_frame_len,
};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use std::{collections::HashSet, fmt};

use crate::{MarketDataError, Result, queue::CollectionMessage};

pub const DEFAULT_MAX_JSONL_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_JSONL_RECORDS: usize = 100_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IngestStats {
    pub records: usize,
    pub bytes: u64,
}

/// Read JSONL envelopes without allowing a single line to exceed the shared wire bound.
pub fn read_envelopes<R: BufRead>(mut reader: R) -> Result<Vec<CollectionMessage>> {
    let mut messages = Vec::new();
    visit_envelopes(
        &mut reader,
        DEFAULT_MAX_JSONL_BYTES,
        DEFAULT_MAX_JSONL_RECORDS,
        |message| {
            messages.push(message);
            Ok(())
        },
    )?;
    Ok(messages)
}

/// Stream validated records through a callback under explicit total byte and record bounds.
pub fn visit_envelopes<R, F>(
    mut reader: R,
    max_bytes: u64,
    max_records: usize,
    mut consume: F,
) -> Result<IngestStats>
where
    R: BufRead,
    F: FnMut(CollectionMessage) -> Result<()>,
{
    if max_bytes == 0
        || max_bytes > DEFAULT_MAX_JSONL_BYTES
        || max_records == 0
        || max_records > DEFAULT_MAX_JSONL_RECORDS
    {
        return Err(MarketDataError::InvalidInput);
    }
    let mut stats = IngestStats::default();
    let mut line = Vec::with_capacity(1024);
    loop {
        line.clear();
        let mut terminated = false;
        while !terminated {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                break;
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let take = newline.map_or(available.len(), |index| index + 1);
            let max_line_bytes = MAX_MARKET_FRAME_BYTES.saturating_add(2);
            let remaining = max_line_bytes.saturating_add(1).saturating_sub(line.len());
            let copied = take.min(remaining);
            line.extend_from_slice(&available[..copied]);
            reader.consume(take);
            terminated = newline.is_some();
            if line.len() > max_line_bytes || copied != take {
                return Err(MarketDataError::FrameTooLarge);
            }
        }
        if line.is_empty() {
            break;
        }
        let raw_len = u64::try_from(line.len()).map_err(|_| MarketDataError::InputLimit)?;
        stats.bytes = stats
            .bytes
            .checked_add(raw_len)
            .ok_or(MarketDataError::InputLimit)?;
        if stats.bytes > max_bytes {
            return Err(MarketDataError::InputLimit);
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        validate_market_frame_len(line.len()).map_err(|_| MarketDataError::FrameTooLarge)?;
        if line.iter().all(u8::is_ascii_whitespace) {
            if !terminated {
                break;
            }
            continue;
        }
        let shape: EnvelopeShape = serde_json::from_slice(&line)?;
        let message = match shape {
            EnvelopeShape::Market => {
                CollectionMessage::Market(serde_json::from_slice::<MarketEventEnvelopeV1>(&line)?)
            }
            EnvelopeShape::Control => {
                CollectionMessage::Control(serde_json::from_slice::<ControlEventEnvelopeV1>(&line)?)
            }
        };
        message.validate()?;
        stats.records = stats
            .records
            .checked_add(1)
            .ok_or(MarketDataError::InputLimit)?;
        if stats.records > max_records {
            return Err(MarketDataError::InputLimit);
        }
        consume(message)?;
        if !terminated {
            break;
        }
    }
    Ok(stats)
}

#[derive(Clone, Copy)]
enum EnvelopeShape {
    Market,
    Control,
}

impl<'de> serde::Deserialize<'de> for EnvelopeShape {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ShapeVisitor;

        impl<'de> Visitor<'de> for ShapeVisitor {
            type Value = EnvelopeShape;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("one strict market or control envelope object")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut keys = HashSet::new();
                let mut event = false;
                let mut control = false;
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate envelope field"));
                    }
                    match key.as_str() {
                        "schema_version" | "source" | "generation" | "sequence"
                        | "source_timestamp" | "received_timestamp" => {
                            map.next_value::<IgnoredAny>()?;
                        }
                        "event" => {
                            event = true;
                            map.next_value::<IgnoredAny>()?;
                        }
                        "control" => {
                            control = true;
                            map.next_value::<IgnoredAny>()?;
                        }
                        _ => {
                            return Err(de::Error::unknown_field(
                                &key,
                                &[
                                    "schema_version",
                                    "source",
                                    "generation",
                                    "sequence",
                                    "source_timestamp",
                                    "received_timestamp",
                                    "event",
                                    "control",
                                ],
                            ));
                        }
                    }
                }
                match (event, control) {
                    (true, false) => Ok(EnvelopeShape::Market),
                    (false, true) => Ok(EnvelopeShape::Control),
                    _ => Err(de::Error::custom(
                        "exactly one event or control payload is required",
                    )),
                }
            }
        }

        deserializer.deserialize_map(ShapeVisitor)
    }
}

pub fn read_envelopes_from_path(path: &std::path::Path) -> Result<Vec<CollectionMessage>> {
    let file = std::fs::File::open(path)?;
    read_envelopes(io::BufReader::new(file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn rejects_oversized_raw_line_before_json_decode() {
        let bytes = vec![b' '; MAX_MARKET_FRAME_BYTES + 1];
        assert!(matches!(
            read_envelopes(Cursor::new(bytes)),
            Err(MarketDataError::FrameTooLarge)
        ));
    }

    #[test]
    fn accepts_only_valid_shared_contract_messages() {
        let event = r#"{"schema_version":1,"source":{"provider":"synthetic","feed":"synthetic","entitlement":"unknown","numeric_encoding":"decimal_token"},"generation":"1","sequence":"1","source_timestamp":"2026-10-08T13:30:01Z","received_timestamp":"2026-10-08T13:30:01.1Z","event":{"kind":"stock_trade","symbol":"QQQ","price":"600.25","size":"3"}}"#;
        let messages = read_envelopes(Cursor::new(format!("{event}\n"))).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].metadata().sequence, 1);
    }

    #[test]
    fn rejects_duplicate_unknown_and_mixed_envelope_fields() {
        let duplicate = r#"{"schema_version":1,"source":{"provider":"synthetic","feed":"synthetic","entitlement":"unknown","numeric_encoding":"decimal_token"},"generation":"1","sequence":"1","sequence":"2","received_timestamp":"2026-10-08T13:30:01Z","event":{"kind":"stock_trade","symbol":"QQQ","price":"1","size":"1"}}"#;
        let nested_duplicate = r#"{"schema_version":1,"source":{"provider":"synthetic","feed":"synthetic","entitlement":"unknown","numeric_encoding":"decimal_token","numeric_encoding":"decimal_token"},"generation":"1","sequence":"1","received_timestamp":"2026-10-08T13:30:01Z","event":{"kind":"stock_trade","symbol":"QQQ","price":"1","size":"1"}}"#;
        let unknown = r#"{"schema_version":1,"source":{"provider":"synthetic","feed":"synthetic","entitlement":"unknown","numeric_encoding":"decimal_token"},"generation":"1","sequence":"1","received_timestamp":"2026-10-08T13:30:01Z","event":{"kind":"unknown_kind","symbol":"QQQ"}}"#;
        let mixed = r#"{"schema_version":1,"source":{"provider":"synthetic","feed":"synthetic","entitlement":"unknown","numeric_encoding":"decimal_token"},"generation":"1","sequence":"1","received_timestamp":"2026-10-08T13:30:01Z","event":{"kind":"stock_trade","symbol":"QQQ","price":"1","size":"1"},"control":{"kind":"connection_status","state":"connected"}}"#;
        for input in [duplicate, nested_duplicate, unknown, mixed] {
            assert!(read_envelopes(Cursor::new(input)).is_err());
        }
    }
}
