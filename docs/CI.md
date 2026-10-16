# Continuous integration

The unified market-query integration uses Rust 1.99.0 because the existing Broker Alpaca REST
SDK declares that minimum. The earlier archive baseline used 1.98.1. CLI, non-compiling local checks, supply-chain
generation and both public workflow toolchains must use the same version; lowering a dependency's
declared minimum does not establish compatibility. No manual workflow dispatch is required.

Pull requests targeting `main` and pushes to `main` run the Rust and supply-chain verification commands below in GitHub Actions.
Public build/test compilation must run on the PR head in Actions, not locally. The workflow uses Rust 1.99.0, `cargo-deny` 0.20.2, and
`cargo-cyclonedx` 0.5.9. It does not read stored repository, organization, or provider secrets,
connect to market-data providers, perform OAuth, read an operator's rclone configuration, query or
publish remote archives, or upload data and test artifacts. The verification job's automatic
`GITHUB_TOKEN` is limited to `contents: read` for checkout and is not persisted in the worktree.

The Linux verification job uses the official `actions/checkout` action, pinned to
full commit SHA `3d3c42e5aac5ba805825da76410c181273ba90b1` (release `v7.0.1`). It disables persisted
checkout credentials. The job token has `contents: read`; all other workflow permissions are
disabled.

## Windows local-evaluation runtime

The separate `Windows Core Bars runtime` job uses `windows-2022`, Rust 1.99.0, Python 3.14.6 and
NASM 2.16.03 (archive SHA-256 `3ee4782247bcb874378d02f7eab4e294a84d3d15f3f6ee2de2f47a46aa7226e6`).
It checks portable market-query/session contracts, builds the locked release executable and
uses `scripts/windows_runtime.py` to package and run the actual executable. The smoke uses its own
empty LocalTest root, archive-reader cache and independently generated synthetic verifier keys.
It verifies loopback startup, authentication, no-store/readiness and the unconfigured-provider
failure boundary. It does not add a production synthetic-provider mode or call real providers.
Positive synthetic Core Bars tests remain test-only. The existing Linux suite remains mandatory.
The Linux job also runs `python3 -m unittest discover -s tests -p 'test_windows_runtime.py'`
to reject candidate mismatch, tampered package members, duplicate hashes and incorrect PE architecture.
Package smoke cleanup terminates and reaps only its owned child; Windows Ctrl-C graceful shutdown
is not assessed by this lane.

`MDP_MARKET_CACHE_ROOT` is omitted on Windows: isolated Parquet decoding and Pair verification remain
Linux-only and fail `Unsupported` elsewhere. An empty LocalTest root permits HTTP service startup;
it does not prove Windows archive-query support. Readiness is not market readiness or entitlement.
An operator must independently provide a real, hash-pinned calendar and authorized Gateway access
before requesting actual Futu bars. No CI fixture calendar is a deployable market calendar.

Only after the executable smoke succeeds, the job uploads the executable package and archive hash
with official `actions/upload-artifact` v7.0.2 pinned to
`cf430e030ddbb5b0abf93d22962f4752f3646cd9`. The package binds the exact candidate SHA and file hashes,
and includes LICENSE/NOTICE. It excludes environment files, calendars, market/test data and logs.
This is a narrow binary-delivery exception to the no-data-artifact policy; all job permissions
remain `contents: read`. Downloading and executing this Actions-built package needs no local
compilation. `LOCAL_EVALUATION_ONLY` is not production release or complete license acceptance.

## Public PR verification commands (GitHub Actions)

The public runner installs pinned tools and fetches locked public dependencies:

```sh
rustup toolchain install 1.99.0 --profile minimal --component rustfmt --component clippy
cargo +1.99.0 install cargo-deny --version 0.20.2 --locked
cargo +1.99.0 install cargo-cyclonedx --version 0.5.9 --locked
cargo +1.99.0 fetch --locked
```

The public runner executes from the repository root:

```sh
cargo +1.99.0 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy --offline --all-targets -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.99.0 clippy --offline --all-targets --features offline-capture-synthetic -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --offline
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --offline --features offline-capture-synthetic --lib
CARGO_BUILD_JOBS=2 cargo +1.99.0 test --offline --features offline-capture-synthetic --test offline_capture_cli
cargo +1.99.0 deny --all-features check all
python3 -m unittest discover -s tests -p 'test_supply_chain.py'
python3 scripts/supply_chain.py --check
```

The dependency fetch and pinned tool installs use public crates.io and the public Git sources
declared in `Cargo.toml`. The Rust, CLI, and Python tests use local fixtures and fake transports;
they do not establish provider entitlement, remote archive access, or publication authorization.
The `offline_capture_cli` integration test also exercises the exact single-receipt LocalTest
pair verifier through the CLI, including missing/tampered inputs and a permissive-umask run; it
checks that staging is private per invocation, shared-budget lock contention fails closed and
recovers after unlock, and only the persistent owner-only lock remains after per-run cleanup. It
also rejects symlink and hardlink archive objects and budget-lock paths without changing their
external targets or leaving a private run directory behind.
Worker-dependent success and intentional overload tests share the existing test-only mutex. This isolates independent cases while overload scenarios still verify the unchanged production two-worker cap, cancellation and reap behavior; no production retry or capacity widening is introduced.

The feature-gated library tests also exercise the conditional one-chunk HTTP Pair route through the
same query supervisor, including authorization before storage access, compact/no-store responses,
tamper rejection, deadline/drop/shutdown cancellation, worker reap, staging cleanup, and capacity
recovery. They do not establish a capture rollup or provider completeness.

## Existing offline HTTP container smoke (not a local task gate)

This task does not start local Docker or run this host-build helper. The existing script is retained
for its separately authorized environment; public CI and controlled private native validation remain
separate. `scripts/container_smoke.sh` requires `MDP_RUNTIME_IMAGE_ID` to name an already cached immutable
Linux image; it does not pull images or compile Rust inside Docker. The script builds the host binary
with `--offline --locked`, then copies it into the runtime-only image and exercises LocalTest HTTP
startup, authentication, a synthetic V1 query, and graceful signal shutdown. Set
`MDP_SHARED_TARGET` to point at a warm Cargo target, `CARGO_BUILD_JOBS` to control host build
parallelism (default `2`), and `CARGO_INCREMENTAL=0` to disable incremental compilation when
working within a disk budget. This smoke does not exercise the opt-in capture-pair V2/V3 command or
validate a production image/deployment.

## CodeQL

`codeql.yml` runs a separate `rust` and `python` matrix on pull requests to `main` and pushes to
`main`. Each leg uses the official `github/codeql-action` v4 commit
`2892aa5e19bbd11bc0cff5427e3b750a04d9e3c2`, the `security-extended` query suite, and
`build-mode: none`. CodeQL supports this mode for Rust; it creates a database from repository Rust sources
without a full Cargo build. The Rust matrix leg installs the repository-pinned Rust 1.99.0 toolchain;
the extractor can use `rust-analyzer` for `build.rs` and macro code, so it may resolve public
dependencies. The repository's Git dependencies are public and no private source or provider
credential is configured. Python is analyzed as an interpreted language. The
CodeQL job has `contents: read`, `actions: read`, and `security-events: write` so the official action
can read source and upload SARIF; it does not receive stored secrets. GitHub restricts token write
permissions on fork pull requests, so CodeQL skips fork PRs instead of attempting an unauthorized
result upload. The main-branch scan and trusted same-repository PR scan still run.

Default setup can automatically detect new supported languages after they reach the default branch.
This repository's current default setup reports `languages: []`, and the documented REST API language
enum does not include Rust. The advanced workflow makes both required analyzers explicit instead of
assuming an empty default-setup response means either language was analyzed. GitHub permits only one
active CodeQL setup, so before relying on this workflow the repository administrator must disable
the existing default setup and verify the returned state:

```sh
gh api --method PATCH repos/lqepoch/market-data-platform/code-scanning/default-setup \
  -H 'Accept: application/vnd.github+json' \
  -H 'X-GitHub-Api-Version: 2022-11-28' \
  -f state=not-configured
gh api repos/lqepoch/market-data-platform/code-scanning/default-setup
```

The GET response must report `state: not-configured`. The API mutation requires repository
Administration write permission. Once this workflow is part of the PR commit, use the natural PR or
`main` push run for evidence; do not dispatch a manual run or treat a configured file as a completed
analysis. See GitHub's [setup types](https://docs.github.com/en/code-security/concepts/code-scanning/setup-types),
[advanced setup](https://docs.github.com/en/code-security/how-tos/find-and-fix-code-vulnerabilities/configure-code-scanning/configuring-advanced-setup-for-code-scanning),
[Rust build modes](https://docs.github.com/en/code-security/reference/code-scanning/codeql/build-options-for-compiled-languages),
and [default setup REST endpoint](https://docs.github.com/en/rest/code-scanning/code-scanning?apiVersion=2022-11-28#update-a-code-scanning-default-setup-configuration).
