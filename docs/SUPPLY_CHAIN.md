# Supply-chain policy and records

The repository pins Rust dependencies in `Cargo.lock` and checks the complete all-feature, all-target
graph with `deny.toml`. The policy denies known advisories, yanked or unmaintained crates, unknown
licenses and sources, wildcard versions, and unreviewed duplicate versions. There are no advisory
exceptions. The four `bans.skip` entries name only exact currently observed duplicate package
versions and include the reason for each compatibility exception; new duplicate versions remain
denied.

The license allowlist contains only SPDX identifiers observed in the locked graph:
Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, CC0-1.0, MIT, Unicode-3.0,
and Unlicense. License expressions offering an allowed choice are accepted by cargo-deny; an
unapproved license cannot be waived by the NOTICE inventory. `supply-chain/NOTICE.txt` lists every
locked package, its declared license expression, and its lock source. It is an inventory, not a
replacement for upstream license texts or notices.

Git dependencies are limited by `deny.toml` to the trading-core origin. The stricter
`supply-chain/git-source-pins.json` and generator verify that every Git dependency in `Cargo.toml`
uses a full 40-character revision and that each Git package in `Cargo.lock` resolves to exactly that
revision. Cargo path dependencies are rejected. When a shared-core or broker pin changes, update the
pin file, regenerate the artifacts, and review the complete SBOM/source-manifest diff in the same
change.

## Generate and verify

Use Rust 1.98.1 and cargo-deny 0.20.2. Install the SBOM generator as an isolated operator tool with
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

Run the following after changing code, dependencies, source pins, or supply-chain policy:

```sh
cargo +1.98.1 deny --all-features check all
python3 -m unittest discover -s tests -p 'test_supply_chain.py'
python3 scripts/supply_chain.py --write
python3 scripts/supply_chain.py --check
```

`SOURCE-MANIFEST.json` records the exact locked package inventory and checksums, direct git source
revisions, hashes of policy/input files, tool versions, generated artifact hashes, and a deterministic
first-party source-tree hash. The tree hash is SHA-256 over path-sorted entries encoded as
`u32be(path UTF-8 length) || path UTF-8 || raw SHA-256(file contents)`. The source manifest does not
hash itself, sign the build, establish CI identity, attest a release, or prove that dependencies are
safe beyond the checks recorded here. SBOM generation and local tests do not connect to Alpaca or
Google Drive and do not use market-data credentials.
