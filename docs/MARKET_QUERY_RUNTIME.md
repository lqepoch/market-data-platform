# Unified read-only market query runtime

The opt-in `/v1/market/bars` route uses trading-core `BarsRequestV1` and
`BarsResponseV1` pinned at `f793be47ab90ebff9f530c8b08f41db5e774e4e5`.
It is separate from the unchanged dataset archive query. `/v1/market/sessions`
returns the stable cash-calendar identity and original session identities by
default. Supplying both `symbol` and `asset_class=stock|etf` opts into a
currently trusted, signed overnight-overlay selection for that instrument and
window. Both routes use the existing independent 60-second `market:read` MDP
verifier; a Gateway bearer passed for Futu never authenticates the MDP request. Research mode requires
the verified research channel. No order operation is added.

## Configuration and rollback

- `MDP_MARKET_BARS_ENABLED=true` enables the runtime; default is disabled. Revert
  this flag and EqoBoard `EQO_MDP_MARKET_BARS_ENABLED` to return to existing paths.
- Optional `MDP_MARKET_CURSOR_SIGNING_KEY` is a dedicated persistent cursor secret. It must be at
  least 32 UTF-8 bytes and contain no whitespace or control characters. Generate it from a
  cryptographically secure random source; do not reuse broker, Gateway, OIDC, or other signing
  credentials. When absent, the service creates a process-local key from the operating-system
  CSPRNG, so restart invalidates outstanding cursors. Restarting with a changed configured key also
  invalidates existing cursors. Replicas that serve the same clients must use the same dedicated
  key. The key is held server-side and never enters a cursor, cache record, log, or debug output.
- Set `MDP_MARKET_CALENDAR_FILE` and `MDP_MARKET_CALENDAR_SHA256` together. Startup
  verifies the bounded 16 MiB snapshot, its SHA and structural session identity.
  The calendar's own verification status remains independent of its content hash.
  Startup currently loads cash sessions only. There is no environment, HTTP, or
  file-based switch that can install overnight authority; a reviewed in-process
  owner must supply a signed overlay and independent verifier. Without that
  integration, the session endpoint reports `no_overlay` for instrument-scoped
  requests and bars planning leaves OVER uncovered.
- Optional server-only `ALPACA_KEY` and `ALPACA_SECRET` must be supplied together.
  Configured historical SIP/BOATS calls are attempts, not evidence of Plus, an
  accepted credential, actual response feed or comprehensive historical coverage.
  Derived `overnight` is not treated as raw BOATS.
- `MDP_FUTU_GATEWAY_PORT` defaults to 8080. The adapter uses only the fixed
  `http://127.0.0.1:<port>/api/v1/stocks/bars` path. Futu capability remains
  ineligible until independently verified provider admission is available.
- `MDP_YFINANCE_BRIDGE_ENABLED` defaults false; the fixed loopback bridge port
  `MDP_YFINANCE_BRIDGE_PORT` defaults to 8765. The caller-only
  `PUBLIC_MDP_YFINANCE_BRIDGE_BEARER_TOKEN` must match the Stock service's
  `TRADING_RESEARCH_YFINANCE_BRIDGE_BEARER_TOKEN`. Despite its existing variable
  name, the caller token is a server secret and must never be exposed to a browser.
  Stock controls network permission separately. The bridge preserves Yahoo's
  native research observations; `auto_adjust=false` does not prove as-traded
  unadjusted prices. Unknown adjustment semantics therefore cannot satisfy the
  strict shared Bars V1 contract, including requests before 2016.
- Optional `MDP_MARKET_CACHE_ROOT` must be dedicated and empty/new or bear the
  cache's exact private ownership marker. Non-empty foreign directories are rejected
  without permission/content changes. Do not select QQQ Drive or Stock S3/OSS roots.
  `MDP_MARKET_CACHE_SCOPE_ID` is a stable **non-secret** rights/account-profile
  identifier; rotate it when the account or permission realm changes. Its digest
  combines with the verified principal identity to isolate cache reads. Configuration
  or a cache hit does not prove current data rights.
- Persistent exact Parquet readback uses the existing bounded Linux subprocess
  worker. Other platforms return unsupported; no inline decoder or Docker fallback
  is started. On macOS the persistent-cache production acceptance remains blocked.

## Routing, paging and response meaning

Default `auto` plans US stock/ETF intervals using calendar instances in order.
The current started valid session is NEW, its previous valid session is BUFFER,
and older completed sessions are OLD. Futu is the preferred NEW/BUFFER source only
when its capability has verified admission; otherwise return a precise gap.
OLD SIP and BOATS are distinct historical routes. Weekends, holidays, early closes
and daylight saving come from the pinned calendar, not elapsed-hour heuristics.
The supplied XNYS cash calendar alone does not establish a supported overnight
session. Each bars request rechecks overlay signature trust, lease, revocation,
instrument eligibility and window coverage using the current wall clock. The
planner receives only OVER capabilities whose exact provider/feed route appears
in that current selection. Cash capabilities are unchanged; the calendar does
not create provider capabilities or establish feed, entitlement, coverage, or
provider health. In particular, Alpaca SIP never matches a signed BOATS route.
The response exposes the public cash/overlay component identities and selection
status; it does not expose signature/key material. The selected composite
calendar version binds planner and cursor identity, so a changed, expired, or
revoked overlay rejects a continuation rather than resuming an old plan.
Synthetic fixtures exercise this wiring only and do not verify real overnight
availability.

`GET /v1/market/sessions` keeps its required UTC `start` and `end` parameters.
`symbol` and `asset_class` are optional as a pair: omit both for the stable
cash-only view, or provide both to select the same instrument-scoped overlay
used by bars. A partial pair or an unsupported asset class is rejected. The
response includes `calendar_overlay_status`, public `calendar_components`, and
`allowed_overnight_routes`; those routes describe calendar compatibility only,
not provider authorization or data entitlement. `calendar_overlay_status` is
`null` for the default cash-only query. A rejected, expired, revoked, or
ineligible overlay is reported by status while the response stays on the cash
calendar.

The boundary is 2016-01-01 midnight America/New_York, or 05:00 UTC that day.
Earlier stock/ETF history is daily Yahoo research only. Earlier intraday ranges
return unsupported coverage; `allow_daily_downgrade=true` explicitly downgrades the
whole sequence to daily. Other asset classes are not assigned this boundary.

All query fields except `cursor` are required. Supported resolutions are 1m, 5m,
15m, 1h and 1d; `limit` is 1–1000. Reject unknown or duplicate parameters and
non-UTC timestamps. Pages advance through contiguous provider runs in source-time
order. Their opaque cursor fixes the plan cutoff and request; continue every page
before claiming coverage. The SDK's private cursor is cached only with validated
provider, feed, rights realm, payload hash and expiration. The provider-local continuation
locator is embedded in the signed outer token, but is never a credential or part of the cache
identity. Expired or absent page receipts cause a cache miss.

Outer continuations use versioned `mdp2` tokens authenticated with HMAC-SHA256. The
MAC covers the request and plan digests, fixed snapshot time, provider-run position,
provider cursor, issuance/expiry times, admission set, and authenticated principal
rights-profile digest. Tokens are limited to 30 minutes and reject payload, tag,
runtime-key, or principal-profile changes before planning or fetching. The previous
unsigned `mdp1` form is rejected; clients must restart the query after deployment.
The token is authenticated, not encrypted; provider adapters must never put credentials
or Gateway delegations in provider-local cursor values.

Raw observations and calendar-normalized canonical projections are distinct
immutable Parquet artifacts with exact decimal OHLCV lineage. Receive/fetch time
never substitutes for a missing source cutoff. Partial/unknown responses use 206;
unsupported uses 422, and authentication, quotas, timeouts and expired cursors keep
specific statuses. Unknown actual feed and entitlement remain unknown. Returned
source segments and rows cannot be replaced by a single source label.

Cache configuration does not make provisional input authoritative. Production
provider admission and material conflict checks are required for trusted historical
promotion; the initial runtime has no trusted provider issuer. Synthetic test
admission is excluded from production. Existing archive publication gates remain
closed. See `policies/mdp-cache-reconciliation-v1.md` for lineage and rollback.

`GET /v1/market/cache/status` requires the same verified `market:read` delegation and includes at
most 128 in-memory invalidation/readback summaries filtered by the verified principal and current
operator cache profile. The response is `no-store` and omits principal/profile hashes, cache
fingerprints, rows, and credentials. The event buffer is process-local and is lost on restart; an
event records a logical-cache pointer change and reread only. `coverage_asserted` is always false,
so these events do not prove completeness, market-data coverage, freshness, or entitlement. When
the market-bars runtime is absent, the route returns an empty list with
`invalidation_events_status=unavailable`.

## Verification boundary

Public builds, Rust tests/clippy and persistent worker tests run only in GitHub
Actions on the PR head. Local review may use formatting, metadata and static source
checks; do not start Docker or perform a local public build. Public Actions do not
probe providers, use market credentials or upload data artifacts. A green synthetic
CI result is not ProviderVerified or ReleaseAccepted. Real Futu login, current
Alpaca rights/actual feed, full historical coverage and Yahoo price adjustment
proof remain separate acceptance evidence.
