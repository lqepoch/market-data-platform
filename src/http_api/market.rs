//! Read-only market capability discovery. Registration is not provider acceptance.

use std::sync::Arc;

use axum::{Json, Router, http::StatusCode, routing::get};
use serde_json::{Value, json};

use super::ServiceState;

pub(super) fn routes() -> Router<Arc<ServiceState>> {
    Router::new()
        .route("/v1/providers/capabilities", get(capabilities))
        .route("/v1/market/quotes", get(unsupported_quotes))
        .route("/v1/market/trades", get(unsupported_trades))
        .route("/v1/news", get(unsupported_news))
        .route("/v1/market/cache/status", get(cache_status))
}

async fn capabilities() -> Json<Value> {
    Json(json!({
        "schema_version": "provider-capabilities-v1",
        "execution_enabled": false,
        "providers": [
            {"provider": "alpaca", "feeds": ["sip", "boats", "overnight"],
             "asset_classes": ["stock", "etf"], "data_types": ["bars", "quotes", "trades"],
             "entitlement_status": "unknown", "implementation_owner": "broker-connectors",
             "runtime_status": "not_configured", "verification_status": "BLOCKED",
             "historical_bars_feeds": ["sip", "boats"],
             "notes": "overnight is distinct from boats; feed names do not establish entitlement"},
            {"provider": "futu", "feeds": ["futu"], "asset_classes": ["stock", "etf"],
             "data_types": ["bars", "quotes", "trades"], "entitlement_status": "unknown",
             "implementation_owner": "EqoBoard", "runtime_status": "not_configured",
             "verification_status": "BLOCKED", "notes": "loopback OpenD capabilities must be verified"},
            {"provider": "yfinance", "source": "Yahoo Finance", "feeds": ["yahoo_finance"],
             "asset_classes": ["stock", "etf"], "data_types": ["bars"], "resolutions": ["1d"],
             "implementation_owner": "stock_trading_bot", "runtime_status": "not_configured",
             "verification_status": "BLOCKED", "entitlement_status": "unknown",
             "execution_eligible": false, "notes": "internal read-only bridge; network permission required"},
            {"provider": "yahoo_finance_rss", "data_types": ["news"],
             "implementation_owner": "EqoBoard", "runtime_status": "unsupported_in_mdp"},
            {"provider": "google_news_rss", "data_types": ["news"],
             "implementation_owner": "EqoBoard", "runtime_status": "unsupported_in_mdp"},
            {"provider": "fred", "data_types": ["macro"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "ecb", "data_types": ["macro"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "sec", "data_types": ["filings"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "finra", "data_types": ["research"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "nasdaq", "data_types": ["research"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "tradingview", "data_types": ["research"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "stooq", "data_types": ["bars", "quotes"], "implementation_owner": "EqoBoard",
             "runtime_status": "unsupported_in_mdp", "source_status": "adapter_present", "entitlement_status": "unknown"},
            {"provider": "binance", "asset_classes": ["crypto"], "data_types": ["bars", "quotes"],
             "implementation_owner": "EqoBoard", "runtime_status": "unsupported_in_mdp", "entitlement_status": "unknown"},
            {"provider": "coingecko", "asset_classes": ["crypto"], "data_types": ["research"],
             "implementation_owner": "EqoBoard", "runtime_status": "unsupported_in_mdp", "entitlement_status": "unknown"}
        ],
        "coverage_asserted": false,
        "licensing_status": "operator_verification_required"
    }))
}

fn unsupported(data_type: &'static str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "UNSUPPORTED_PROVIDER_CAPABILITY", "data_type": data_type,
            "coverage_status": "unsupported", "rows": [], "entitlement_status": "unknown"
        })),
    )
}

async fn unsupported_quotes() -> (StatusCode, Json<Value>) {
    unsupported("quotes")
}

async fn unsupported_trades() -> (StatusCode, Json<Value>) {
    unsupported("trades")
}

async fn unsupported_news() -> (StatusCode, Json<Value>) {
    unsupported("news")
}

async fn cache_status() -> Json<Value> {
    Json(json!({
        "schema_version": "market-cache-status-v1", "cache_status": "unavailable",
        "canonical_market_cache_configured": false, "coverage_asserted": false,
        "note": "legacy archive cache is separate; discovery does not inspect private storage"
    }))
}
