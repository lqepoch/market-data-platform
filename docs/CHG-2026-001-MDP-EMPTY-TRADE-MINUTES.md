# CHG-2026-001: V1 empty trade-minute accounting

The V1 bar schema already carries `window_expected_minutes` and
`window_empty_trade_minutes`. This change aligns aggregation, Parquet validation,
and reads with those existing fields; it does not change the core descriptor,
field order, fingerprint, or HTTP response version.

For each expected symbol, aggregation emits one row for each minute with at least
one timestamped trade. A minute with no observed trade, including a quote-only
minute, has no zero-valued bar. Every emitted row for that symbol carries the
same expected and empty-minute counts. A symbol with no trade rows remains
incomplete because V1 has no row on which to carry its window identity.

The Parquet row validator requires `window_empty_trade_minutes` to be less than
the window length. Dataset verification requires unique row count per symbol to
equal `window_expected_minutes - window_empty_trade_minutes` and requires the
empty-minute count to remain stable across rows for that symbol. Different
symbols may have different counts. A file with an omitted row and no matching
empty-minute declaration fails verification.

These counts describe absence in the finite input consumed by MDP. EOF and
pagination metadata do not prove that a provider delivered every market event,
that the account has SIP/OPRA entitlement, or that an empty market minute was
economically complete.

Regression coverage includes a synthetic quote-only sparse-minute case, an all-empty-symbol
failure, a positive Parquet write/read roundtrip, undeclared-row and inconsistent-count failures,
and different empty counts across symbols. The writer checks dataset coverage before creating a
Parquet file, while readback independently checks the same facts after decoding.

## Local validation

The frozen source passed:

- `cargo +1.98.1 fmt --all -- --check`
- `CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline --locked` (98 unit tests, 5 CLI integration tests, and doctests)
- `CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline --locked --all-targets --all-features -- -D warnings`
- `cargo +1.98.1 deny --all-features check all`
- `python3 -m unittest discover -s tests -p 'test_supply_chain.py'` (3 tests)
- `python3 scripts/supply_chain.py --write` followed by `--check`
- `git diff --check`
