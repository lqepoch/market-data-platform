# CHG-2026-001 MDP HTTP slice provenance

This service slice was started from frozen predecessor commit
`3844d2bf2868dbfc6e2363e76dbaad8cff067345`, when `origin/main` was the MDP initialization commit
`f1a2a54c0596ee8a44f4a295a982188eb2a562f3`. After the predecessor PR merged, the HTTP commits were
semantically rebased onto reviewed `origin/main` `b5c4596377a0f54d5b2fe49ccf1e250b3c496e5d`.
The rebase preserves the predecessor's source-metadata logging fixes, public CI governance and
null-definition validation work.

The implementation pins jsonwebtoken 10.3.0 with only the AWS-LC backend and restricts token
validation to HS256. Its service endpoint reuses the existing V1 bar rows and summary facts, with an
HTTP-only projection that encodes uint64 summary counts as canonical decimal strings. The existing
CLI summary JSON remains unchanged. No BarV2 or DatasetManifestV2 completion-evidence claim is made.

## Local gates

The final source tree passed:

- `cargo +1.98.1 fmt --all -- --check`
- `CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline --locked --all-targets -- -D warnings`
- `CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline --locked` (90 unit tests and 5 CLI integration tests, including the null-definition negative test with a valid positive control)
- `cargo +1.98.1 deny --all-features check all`
- `python3 -m unittest discover -s tests -p 'test_supply_chain.py'`
- `python3 scripts/supply_chain.py --write` followed by `--check`
- Shell syntax, OpenAPI YAML parsing, and `git diff --check`

## Runtime-only container probe

The offline smoke used cached image ID
`sha256:bf44cdfcb76cd3b41e879bc058fc37ec5872002ccfde7fcb765e218cde0cd79c` (Linux amd64,
176,134,735 bytes), with `--pull=false` and `--network=none`. It copied the host-built MDP binary
after verifying its SHA-256, then checked startup, liveness/readiness, unauthenticated rejection,
authenticated synthetic diagnostic V1 data, and graceful shutdown. The copied binary SHA-256 was
`e0470c477491ac67eaaf4cb5ef39be991694da1930fe5dba18a69fee5a420b1f`. Host and runtime were both
`x86_64`; host glibc was `2.39` and runtime-image glibc was `2.41`. Successful startup verifies this
host-built executable against that local runtime image only. Docker did not build Rust source, and
this probe does not validate rclone, Drive, a production registry digest, provider entitlement, or
the EqoBoard BFF deployment.
