//! Offline-first market ingestion, normalization, query, and immutable archive.

pub mod aggregate;
pub mod archive;
pub mod config;
pub mod error;
pub mod parquet_store;
pub mod pipeline;
pub mod protocol;
pub mod queue;
pub mod remote_query;
pub mod schema;
pub mod storage;

pub use error::{MarketDataError, Result};
