//! Offline-first market ingestion, normalization, query, and immutable archive.

pub mod aggregate;
pub mod archive;
pub mod cancellation;
#[cfg(feature = "offline-capture-synthetic")]
#[doc(hidden)]
pub mod capture_synthetic;
pub mod config;
pub mod error;
pub mod http_api;
pub mod parquet_store;
#[doc(hidden)]
pub mod parquet_worker;
pub mod pipeline;
pub mod protocol;
pub mod queue;
pub mod remote_query;
pub mod schema;
pub mod storage;

pub use error::{MarketDataError, Result};
