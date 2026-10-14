# Supply-chain policy and records

The repository pins Rust dependencies in `Cargo.lock` and checks the complete all-feature, all-target
graph with `deny.toml`. The policy denies known advisories, yanked or unmaintained crates, unknown
licenses and sources, wildcard versions, and unreviewed duplicate versions. There are no advisory
exceptions. The `bans.skip` entries name only exact currently observed duplicate package versions
and include the reason for each compatibility exception; new duplicate versions remain denied.
The JWT verifier pins jsonwebtoken 10.3.0 with default features disabled and only its `aws_lc_rs`
backend enabled. Application validation still allows HS256 only. Runtime market-bar cursor
authentication also uses the already-pinned `aws-lc-rs 1.18.1` HMAC-SHA256 implementation, so the
repository does not add a second crypto backend solely for cursor MACs. This avoids pulling the
optional RSA implementation into the JWT verifier's dependency graph; no advisory is suppressed to
obtain either result.

The current lock resolves `rustls 0.23.46`. Its [upstream release notes](https://github.com/rustls/rustls/releases/tag/v/0.23.46)
list security-relevant fixes, and the [upstream manifest](https://raw.githubusercontent.com/rustls/rustls/v/0.23.46/rustls/Cargo.toml)
declares `Apache-2.0 OR ISC OR MIT`, which fits the license allowlist below. Keep the RustSec
advisory ignore list empty and update the lock to a fixed upstream release when new advisories apply.

The license allowlist contains only SPDX identifiers observed in the locked graph:
Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, CC0-1.0, ISC, MIT,
Unicode-3.0, and Unlicense. License expressions offering an allowed choice are accepted by cargo-deny; an
unapproved license cannot be waived by the NOTICE inventory. `supply-chain/NOTICE.txt` lists every
locked package, its declared license expression, and its lock source. It is an inventory, not a
replacement for upstream license texts or notices.

Git dependencies are limited by `deny.toml` to the trading-core and broker-connectors origins. The
stricter `supply-chain/git-source-pins.json` and generator verify that every Git dependency in
`Cargo.toml` uses a full 40-character revision and that each Git package in `Cargo.lock` resolves to
exactly that revision. Cargo path dependencies are rejected. When a shared-core or broker pin
changes, update the pin file, regenerate the artifacts, and review the complete SBOM/source-manifest
diff in the same change.

## Generate and verify

Use Rust 1.99.0 and cargo-deny 0.20.2. Install the SBOM generator as an isolated operator tool with
the pinned version and its published lockfile:

```sh
cargo install cargo-cyclonedx --version 0.5.9 --locked
cargo-cyclonedx cyclonedx --version
```

The generator is not an application dependency and does not change this repository's
`Cargo.lock`. It emits CycloneDX 1.5. On first generation, the update script initializes
`SOURCE_DATE_EPOCH` from `HEAD`; later regenerations reuse the checked-in `source_date_epoch` so a
commit does not make its own SBOM immediately stale. That timestamp is a reproducible artifact input,
not a claim about the wall-clock time of generation. Use `--refresh-source-date-epoch` only when a
new artifact timestamp is intentionally required. The script normalizes local package references so
the SBOM contains no workstation or worktree paths, and verifies that the package set matches the
locked normal/build graph across all targets. Test-only/dev dependencies are inventoried in the
source manifest and NOTICE and are included in cargo-deny's audit; they are not reported as shipped
application components.

The generator's `INPUT_PATHS` also includes `README.md` and `AGENTS.md`; changes to any listed
input require the same regeneration, even when the change is documentation-only.

Run the following after changing code, dependencies, source pins, or supply-chain policy:

```sh
cargo +1.99.0 deny --all-features check all
python3 -m unittest discover -s tests -p 'test_supply_chain.py'
python3 scripts/supply_chain.py --write
python3 scripts/supply_chain.py --check
```

`SOURCE-MANIFEST.json` records the exact locked package inventory and checksums, direct git source
revisions, hashes of policy/input files (including the runtime-only `Dockerfile`, HTTP OpenAPI
contract, offline container-smoke script, and change-specific provenance record), tool versions,
generated artifact hashes, and a deterministic first-party source-tree hash over Git-tracked Rust and
Python sources in the configured source-tree paths. Untracked work-in-progress files are excluded;
files intended for a release must be tracked before generating the artifacts. The tree hash is
SHA-256 over path-sorted entries encoded as
`u32be(path UTF-8 length) || path UTF-8 || raw SHA-256(file contents)`. The source manifest does not
hash itself, sign the build, establish CI identity, attest a release, or prove that dependencies are
safe beyond the checks recorded here. SBOM generation and local tests do not connect to Alpaca or
Google Drive and do not use market-data credentials.

## Exact Broker REST TLS certificate-data review

The pinned Broker REST adapter adds `reqwest 0.13.5` → `rustls-platform-verifier 0.7.1` → `webpki-root-certs 1.0.9`. The last package distributes Mozilla trust-anchor data under [CDLA-Permissive-2.0](https://spdx.org/licenses/CDLA-Permissive-2.0.html). Its redistribution condition is met by retaining the complete package license at `supply-chain/licenses/webpki-root-certs-1.0.9.LICENSE` (SHA-256 `e271993808fec50ab29350b39539cdec611a9103f827e0aa26d61da70e2d33f8`). `Cargo.lock` retains the registry package checksum. The cargo-deny exception is restricted to this exact package/version, and the supply-chain validator rejects any wider exception set; advisories, duplicate-version and unknown-source rejection remain in effect. This is certificate-data licensing, not a Yahoo/Alpaca market-data license or entitlement grant.

The runtime-only LocalTest Dockerfile accompanies its binary with the repository
LICENSE, locked dependency NOTICE, and reviewed dependency license texts under
`/usr/share/doc/mdp`. Binary distributors must retain these accompanying notices;
this repository does not establish an image publication or production acceptance.
Public image builds belong in GitHub Actions; this task does not start local Docker.
