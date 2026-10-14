# US stock and ETF session planning

`src/session_router.rs` plans provider slices from an explicit UTC request, a caller-supplied
capability list, and a validated calendar snapshot. It is a pure planner: it does not fetch
calendar data, open provider clients, or prove account entitlements. A missing provider capability
or calendar phase is returned as a gap.

## Calendar authority

Runtime input is the native snapshot JSON produced by
`tools/xnys_snapshot_adapter.py`. The adapter consumes the standard Stock XNYS calendar artifact
as offline data, verifies its exact input SHA-256 and embedded canonical checksum, and retains its
source, generator, version, compiler configuration, and source commit metadata. Runtime parsing
accepts only the native snapshot, requires a second operator pin for its exact bytes, rejects
unknown fields, and validates bounded, contiguous New York date rows and ordered non-overlapping
phase instances. It does not read the Stock artifact or reconstruct timezone rules at startup.

Convert an operator-held source artifact with its separately recorded raw SHA-256:

```sh
SOURCE_SHA256="<64 lowercase hex characters from shasum -a 256>"
python3 tools/xnys_snapshot_adapter.py \
  --input xnys-calendar-v1.json \
  --output xnys-native-snapshot.json \
  --expected-sha256 "$SOURCE_SHA256"
shasum -a 256 xnys-native-snapshot.json
```

The runtime pin is the SHA-256 of `xnys-native-snapshot.json`; the native metadata separately
records the pinned source-artifact SHA-256 and embedded artifact checksum. Do not commit generated
calendar snapshots or publish local source paths in metadata.

The adapter uses `America/New_York` from the host IANA timezone database to encode each civil
date's UTC day bounds. It preserves the source artifact's session timestamps, including historical
delayed opens and early closes. The current source artifact covers modeled dates from 1990-01-01
through 2031-12-31, while official-fixture verification covers only 2026-01-01 through 2028-12-31.
Other weekdays remain `provisional`; weekend rows are `modeled_weekend`. The native XNYS snapshot
contains PRE, REGULAR, and AFTER instances only. It does not provide OVER or BOATS calendar
evidence, so the adapter never creates an overnight instance.

## Optional signed OVER overlay

`parse_calendar_snapshot` remains an XNYS cash-only parser and rejects OVER instances. A trusted
in-process owner may separately call `LoadedMarketCalendar::with_signed_overnight_overlay` with a
bounded, exact-SHA-pinned artifact and an injected `TrustedProviderAdmissionVerifierV1`. The
artifact uses its own `mdp.trusted-overnight-calendar.v1` signature domain and the fixed
`market-calendar:overnight` role. The verifier must use an independently reviewed issuer/key
policy for that calendar role; reusing A04's signature-verification primitive does not turn a
provider-rights issuer into a calendar authority. No environment variable, HTTP request, or
provider credential can install this overlay. `from_environment()` continues to load cash only.

The admitted loader keeps the parsed snapshot and exact native-input SHA private and exposes
read-only accessors. HTTP planning consumes `cash_snapshot_for_selection()` for symbol-free
sessions and `snapshot_for_instrument()` for bars. Both no-overlay and fallback selections use a
cached `cash.v1.<sha256>` version derived from cash source id, source version, and exact native
input SHA; component metadata continues to report the original source version. This prevents two
different pinned snapshots that reuse a source label from sharing a cursor/cache identity.

The signed claims bind the exact cash source id, version, and native input SHA-256; retain a
separate source provider/feed, calendar source id/version/reference; declare a sorted, bounded set
of eligible Stock/ETF symbols; and list every UTC OVER window explicitly. They also sign a bounded
`compatible_routes` list, which can contain only `(alpaca, boats)` and/or `(futu, futu)`. Calendar
source identity and market-data routes are separate claims. Runtime selection returns only those
signed routes, and the provider's actual feed entitlement remains an independent requirement.

Each window must belong to an open cash trade date and lie inside the 20:00 previous New York
civil-day to 04:00 trade-date band. The UTC boundaries come from adjacent, contiguous 23/24/25-hour
cash day rows, so runtime does not reconstruct DST rules or infer sessions from weekdays. The
overlay can add only signed windows; omitted nights remain unmodeled gaps. For an eligible request
that intersects cash-calendar coverage, selection merges all signed windows so current/buffer/old
phase ranking retains the complete history. It rechecks the issuer verifier, issue/expiry, explicit
revocation, symbol, asset class, and request coverage each time. Any failure returns the original
cash-only calendar contents under its stable selection version and a status. A request outside
the cash snapshot's UTC bounds receives `no_window_coverage`; the overlay is never clipped to
the requested interval because later ranking must retain every signed historical OVER instance.

The response-facing component metadata exposes the exact overlay artifact SHA-256 and public
source identity, never the signature or key id. Applied `calendar_version` combines cash
source/version/input identity with the overlay source/version and a canonical content fingerprint.
The content fingerprint excludes lease timestamps, signature, and exact signed-envelope bytes, so
re-signing unchanged calendar facts does not invalidate historical cache identity. Expired,
rejected, or absent overlays remain cash-only. No production issuer or provider-specific overnight
artifact is supplied in this repository; actual Futu/BOATS schedule and data rights remain
`BLOCKED_EXTERNAL`.

Current read-only source evidence from the standard artifact:

| Evidence | Value |
| --- | --- |
| Raw artifact SHA-256 | `5e0a0b25a0c6d1cc201be03f8a6fd6da5b4c56dc440a84979fdabd0b0cdc4708` |
| Embedded canonical checksum | `4ce159e36b3b749f05f3e78e03743863b665e07c6afdff4c9e84f880bbfa1a10` |
| XNYS coverage | 1990-01-01 through 2031-12-31 |
| Official-fixture verified range | 2026-01-01 through 2028-12-31 |
| Source | [NYSE Holidays & Trading Hours](https://www.nyse.com/trade/hours-calendars) |

The adapter rejects input over 32 MiB, native snapshots over 16 MiB, date coverage over 50,000
rows, and phase-instance counts over 200,000. `parse_calendar_snapshot(bytes, expected_sha256)`
checks the native byte pin and XNYS provenance; `validate_calendar_snapshot` is also available to
callers that need to validate an already parsed snapshot before planning.

## Planning rules

- Requests stay UTC half-open windows. Calendar date rows provide New York midnight-to-midnight
  bounds as UTC, including 23-hour and 25-hour DST dates. Phase windows are also half-open.
- Intraday requests beginning before `2016-01-01T05:00:00Z` (2016-01-01 midnight in New York)
  are unsupported. With `allow_daily_downgrade`, the entire request becomes daily; pre-cutoff
  dates can route only to labeled YFinance research daily data. The cutoff uses the requested UTC
  boundary, not a phase's `trade_date`, which may identify the next date for an overnight phase.
- `auto` routes the active/newest and immediately previous ordered phase instances to Futu and
  older phases to Alpaca SIP. Alpaca BOATS is selected only for an explicitly supplied OVER
  instance; XNYS supplies none. `alpaca_only` and `futu_only` do not fall back to the other source.
- `research` supports labeled YFinance daily only. Intraday research requests fail unless the
  caller explicitly allows a whole-request daily downgrade.
- A provider route is usable only when a matching provider/feed/phase/interval capability permits
  historical access or realtime access for the exact active window. The most recently completed
  Futu phase can be marked provisional while closed; that does not represent data validation.
- Closed calendar dates produce no NEW or realtime route. An open date with an unattested phase
  produces `missing_calendar_phase`, never a synthetic live session.

The planner's capability list is a routing input, not evidence that MDP has working credentials,
provider permissions, or source coverage. Futu and Alpaca provider capability claims remain
`BLOCKED_EXTERNAL` until separately proven. BOATS overnight coverage is also `BLOCKED_EXTERNAL`.

## Local checks

```sh
cargo +1.99.0 test --locked --test session_router # public GitHub Actions runner only
python3 tools/test_xnys_snapshot_adapter.py
```
