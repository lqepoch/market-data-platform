# Continuous integration

Pull requests targeting `main` and pushes to `main` run the same local Rust and supply-chain
verification commands. The workflow uses Rust 1.98.1, `cargo-deny` 0.20.2, and
`cargo-cyclonedx` 0.5.9. It does not read stored repository, organization, or provider secrets,
connect to market-data providers, perform OAuth, read an operator's rclone configuration, query or
publish remote archives, or upload data and test artifacts. The verification job's automatic
`GITHUB_TOKEN` is limited to `contents: read` for checkout and is not persisted in the worktree.

The only action in the verification workflow is the official `actions/checkout` action, pinned to
full commit SHA `3d3c42e5aac5ba805825da76410c181273ba90b1` (release `v7.0.1`). It disables persisted
checkout credentials. The job token has `contents: read`; all other workflow permissions are
disabled.

## Reproduce the PR checks locally

Install the repository toolchain and the pinned standalone supply-chain tools, then fetch the locked
public dependencies once:

```sh
rustup toolchain install 1.98.1 --profile minimal --component rustfmt --component clippy
cargo +1.98.1 install cargo-deny --version 0.20.2 --locked
cargo +1.98.1 install cargo-cyclonedx --version 0.5.9 --locked
cargo +1.98.1 fetch --locked
```

Run the checks from the repository root:

```sh
cargo +1.98.1 fmt --all -- --check
CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline --all-targets -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.98.1 clippy --offline --all-targets --features offline-capture-synthetic -- -D warnings
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline
CARGO_BUILD_JOBS=2 cargo +1.98.1 test --offline --features offline-capture-synthetic --test offline_capture_cli
cargo +1.98.1 deny --all-features check all
python3 -m unittest discover -s tests -p 'test_supply_chain.py'
python3 scripts/supply_chain.py --check
```

The dependency fetch and pinned tool installs use public crates.io and the public Git sources
declared in `Cargo.toml`. The Rust, CLI, and Python tests use local fixtures and fake transports;
they do not establish provider entitlement, remote archive access, or publication authorization.

## Optional offline HTTP container smoke

`scripts/container_smoke.sh` requires `MDP_RUNTIME_IMAGE_ID` to name an already cached immutable
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
without a full Cargo build. The Rust matrix leg installs the repository-pinned Rust 1.98.1 toolchain;
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
