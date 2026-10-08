#!/usr/bin/env python3
"""Generate and verify the repository's locked dependency provenance artifacts."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parents[1]
SUPPLY_CHAIN = ROOT / "supply-chain"
SBOM_PATH = SUPPLY_CHAIN / "market-data-platform.cdx.json"
NOTICE_PATH = SUPPLY_CHAIN / "NOTICE.txt"
MANIFEST_PATH = SUPPLY_CHAIN / "SOURCE-MANIFEST.json"
PINS_PATH = SUPPLY_CHAIN / "git-source-pins.json"
RUST_TOOLCHAIN = "1.98.1"
DENY_VERSION = "0.20.2"
CYCLONEDX_VERSION = "0.5.9"
SBOM_SPEC = "1.5"
INPUT_PATHS = (
    "Cargo.toml",
    "Cargo.lock",
    "Dockerfile",
    "deny.toml",
    "LICENSE",
    "README.md",
    "AGENTS.md",
    ".github/workflows/ci.yml",
    "docs/CI.md",
    "docs/ARCHITECTURE.md",
    "docs/CHG-2026-001-MDP-HTTP-PROVENANCE.md",
    "docs/CHG-2026-001-MDP-EMPTY-TRADE-MINUTES.md",
    "docs/CHG-2026-001-MDP-DURABLE-PREDECODE-SPOOL.md",
    "docs/policies/mdp-alpaca-offline-fixture-v1.md",
    "docs/policies/mdp-capture-pair-v2.md",
    "docs/fixtures/raw-event-pair-input-v2.json",
    "docs/fixtures/raw-event-pair-receipt-v2.json",
    "docs/SUPPLY_CHAIN.md",
    "docs/openapi-v1.yaml",
    "supply-chain/git-source-pins.json",
    "scripts/supply_chain.py",
    "scripts/container_smoke.sh",
    "tests/test_supply_chain.py",
)
TREE_GLOBS = ("src/**/*.rs", "tests/**/*", "scripts/*.py")
FULL_REV = re.compile(r"^[0-9a-f]{40}$")
ALLOWED_LICENSES = {
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "CC0-1.0",
    "ISC",
    "MIT",
    "Unicode-3.0",
    "Unlicense",
}
ALLOWED_REGISTRIES = {"https://github.com/rust-lang/crates.io-index"}
EXPECTED_DUPLICATE_SKIPS = {
    ("getrandom", "=0.2.17"),
    ("getrandom", "=0.3.4"),
    ("r-efi", "=5.3.0"),
    ("syn", "=2.0.119"),
    ("block-buffer", "=0.10.4"),
    ("block-buffer", "=0.12.1"),
    ("cpufeatures", "=0.2.17"),
    ("cpufeatures", "=0.3.1"),
    ("crypto-common", "=0.1.7"),
    ("crypto-common", "=0.2.2"),
    ("digest", "=0.10.7"),
    ("digest", "=0.11.3"),
    ("base64", "=0.22.1"),
    ("untrusted", "=0.7.1"),
}


class SupplyChainError(RuntimeError):
    pass


def run(command: list[str], *, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise SupplyChainError(f"command failed ({result.returncode}): {' '.join(command)}\n{details}")
    return result.stdout.strip()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SupplyChainError(f"cannot read JSON file {path.relative_to(ROOT)}: {error}") from error


def load_metadata() -> dict[str, Any]:
    output = run([
        "cargo",
        f"+{RUST_TOOLCHAIN}",
        "metadata",
        "--offline",
        "--all-features",
        "--locked",
        "--format-version",
        "1",
    ])
    return json.loads(output)


def cargo_tool_version(tool: str, expected: str) -> str:
    output = run(["cargo", f"+{RUST_TOOLCHAIN}", tool, "--version"])
    match = re.search(r"\b(\d+\.\d+\.\d+)\b", output)
    if match is None or match.group(1) != expected:
        raise SupplyChainError(f"expected {tool} {expected}, got {output!r}")
    return match.group(1)


def executable_version(command: list[str], expected: str) -> str:
    output = run(command)
    match = re.search(r"\b(\d+\.\d+\.\d+)\b", output)
    if match is None or match.group(1) != expected:
        raise SupplyChainError(f"expected tool version {expected}, got {output!r}")
    return match.group(1)


def load_lockfile() -> list[dict[str, Any]]:
    try:
        lock = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SupplyChainError(f"cannot parse Cargo.lock: {error}") from error
    return lock.get("package", [])


def declared_dependencies(manifest: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    sections = ("dependencies", "dev-dependencies", "build-dependencies")
    for section in sections:
        for name, value in manifest.get(section, {}).items():
            yield name, value if isinstance(value, dict) else {"version": value}
    for name, value in manifest.get("workspace", {}).get("dependencies", {}).items():
        yield name, value if isinstance(value, dict) else {"version": value}
    for target in manifest.get("target", {}).values():
        for section in sections:
            for name, value in target.get(section, {}).items():
                yield name, value if isinstance(value, dict) else {"version": value}


def load_pins() -> list[dict[str, Any]]:
    pins = read_json(PINS_PATH)
    if pins.get("schema_version") != 1 or not isinstance(pins.get("sources"), list):
        raise SupplyChainError("git-source-pins.json must use schema_version 1 and a sources array")
    return pins["sources"]


def validate_deny_policy(pins: list[dict[str, Any]]) -> None:
    try:
        policy = tomllib.loads((ROOT / "deny.toml").read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SupplyChainError(f"cannot parse deny.toml: {error}") from error
    advisories = policy.get("advisories", {})
    bans = policy.get("bans", {})
    licenses = policy.get("licenses", {})
    sources = policy.get("sources", {})
    if (
        advisories.get("version") != 2
        or advisories.get("yanked") != "deny"
        or advisories.get("unmaintained") != "all"
        or advisories.get("unsound") != "all"
        or advisories.get("ignore") != []
    ):
        raise SupplyChainError("deny.toml must reject yanked, unmaintained, unsound, and ignored advisories")
    if (
        bans.get("multiple-versions") != "deny"
        or bans.get("multiple-versions-include-dev") is not True
        or bans.get("wildcards") != "deny"
        or bans.get("deny") != []
    ):
        raise SupplyChainError("deny.toml must reject wildcard and unreviewed duplicate dependencies")
    skips = {
        (entry.get("name"), entry.get("version"))
        for entry in policy.get("bans", {}).get("skip", [])
        if entry.get("reason", "").strip()
    }
    if skips != EXPECTED_DUPLICATE_SKIPS or len(skips) != len(policy.get("bans", {}).get("skip", [])):
        raise SupplyChainError("deny.toml duplicate skips must remain exact, versioned, and reasoned")
    if (
        set(licenses.get("allow", [])) != ALLOWED_LICENSES
        or licenses.get("exceptions") != []
        or licenses.get("include-dev") is not True
        or licenses.get("include-build") is not True
    ):
        raise SupplyChainError("deny.toml license policy differs from the reviewed exact allowlist")
    expected_git = sorted(pin["origin"] for pin in pins)
    if (
        sources.get("unknown-registry") != "deny"
        or sources.get("unknown-git") != "deny"
        or sources.get("required-git-spec") != "rev"
        or set(sources.get("allow-registry", [])) != ALLOWED_REGISTRIES
        or sorted(sources.get("allow-git", [])) != expected_git
    ):
        raise SupplyChainError("deny.toml registry or git source policy is broader than the reviewed pins")


def parse_git_source(source: str) -> tuple[str, str, str]:
    if not source.startswith("git+"):
        raise SupplyChainError(f"not a git package source: {source}")
    parsed = urlsplit(source[4:])
    query = parse_qs(parsed.query, strict_parsing=True)
    rev_values = query.get("rev", [])
    commit = parsed.fragment
    origin = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    if len(rev_values) != 1 or not FULL_REV.fullmatch(rev_values[0]) or not FULL_REV.fullmatch(commit):
        raise SupplyChainError(f"git package source is not locked to one full revision: {source}")
    if rev_values[0] != commit:
        raise SupplyChainError(f"git rev and resolved commit differ: {source}")
    return origin, rev_values[0], commit


def validate_pins(lock_packages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pins = load_pins()
    expected: dict[tuple[str, str], set[str]] = {}
    for pin in pins:
        origin = pin.get("origin", "")
        rev = pin.get("rev", "")
        packages = pin.get("packages")
        if not origin.startswith("https://github.com/") or origin.endswith("/"):
            raise SupplyChainError(f"git source origin is not canonical HTTPS: {origin!r}")
        if not FULL_REV.fullmatch(rev) or not isinstance(packages, list) or not packages:
            raise SupplyChainError(f"git source pin requires a full revision and package allowlist: {origin}")
        if packages != sorted(set(packages)):
            raise SupplyChainError(f"git source package names must be sorted and unique: {origin}")
        key = (origin, rev)
        if key in expected:
            raise SupplyChainError(f"duplicate git source pin: {origin}@{rev}")
        expected[key] = set(packages)

    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    declared_git: set[tuple[str, str, str]] = set()
    for alias, dependency in declared_dependencies(manifest):
        if "path" in dependency:
            raise SupplyChainError(f"path dependencies are not allowed: {alias}")
        if "git" not in dependency:
            continue
        origin = dependency["git"].rstrip("/")
        rev = dependency.get("rev")
        package_name = dependency.get("package", alias)
        if not isinstance(rev, str) or not FULL_REV.fullmatch(rev):
            raise SupplyChainError(f"git dependency {alias} must use a full 40-character rev")
        if (origin, rev) not in expected or package_name not in expected[(origin, rev)]:
            raise SupplyChainError(f"undeclared git dependency pin: {package_name} from {origin}@{rev}")
        declared_git.add((origin, rev, package_name))

    if not declared_git:
        raise SupplyChainError("Cargo.toml has no explicitly pinned git dependencies")

    observed: dict[tuple[str, str], set[str]] = {}
    for package in lock_packages:
        source = package.get("source") or ""
        if source.startswith("git+"):
            origin, rev, _commit = parse_git_source(source)
            observed.setdefault((origin, rev), set()).add(package["name"])
        elif source and not source.startswith("registry+"):
            raise SupplyChainError(f"unapproved Cargo.lock package source: {source}")

    if observed != expected:
        raise SupplyChainError(
            "Cargo.lock git package set differs from supply-chain/git-source-pins.json: "
            f"expected={{{', '.join(f'{k}: {sorted(v)}' for k, v in expected.items())}}}, "
            f"observed={{{', '.join(f'{k}: {sorted(v)}' for k, v in observed.items())}}}"
        )
    return [
        {"origin": origin, "rev": rev, "packages": sorted(packages)}
        for (origin, rev), packages in sorted(observed.items())
    ]


def package_map(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    packages = metadata.get("packages", [])
    workspace_root = Path(metadata.get("workspace_root", "")).resolve()
    root_package = next((p for p in packages if p.get("name") == "market-data-platform"), None)
    if root_package is None or Path(root_package.get("manifest_path", "")).resolve() != workspace_root / "Cargo.toml":
        raise SupplyChainError("Cargo metadata did not resolve the expected workspace root package")
    for package in packages:
        if not package.get("license"):
            raise SupplyChainError(f"package has no declared license expression: {package['name']}@{package['version']}")
        if package is not root_package and package.get("source") is None:
            raise SupplyChainError(f"non-root path package is not permitted: {package['name']}@{package['version']}")
    return {package["id"]: package for package in packages}


def production_package_ids(metadata: dict[str, Any]) -> set[str]:
    packages = package_map(metadata)
    root_id = next(package["id"] for package in packages.values() if package["name"] == "market-data-platform")
    nodes = {node["id"]: node for node in metadata.get("resolve", {}).get("nodes", [])}
    if root_id not in nodes:
        raise SupplyChainError("Cargo metadata has no resolved root dependency node")
    reachable: set[str] = set()
    pending = [root_id]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            raise SupplyChainError(f"Cargo metadata dependency node is missing: {package_id}")
        for dependency in node.get("deps", []):
            kinds = dependency.get("dep_kinds", [])
            if kinds and all(kind.get("kind") == "dev" for kind in kinds):
                continue
            pending.append(dependency["pkg"])
    reachable.discard(root_id)
    return reachable


def component_source_key(source: str | None) -> str | None:
    if source is None:
        return None
    return source.split("#", 1)[0]


def verify_sbom_components(bom: dict[str, Any], metadata: dict[str, Any]) -> None:
    packages = package_map(metadata)
    lock_by_name_version: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for package in load_lockfile():
        lock_by_name_version.setdefault((package["name"], package["version"]), []).append(package)

    expected: set[tuple[str, str, str | None]] = set()
    for package_id in production_package_ids(metadata):
        package = packages[package_id]
        lock_matches = lock_by_name_version.get((package["name"], package["version"]), [])
        if len(lock_matches) != 1:
            raise SupplyChainError(f"expected one Cargo.lock entry for {package['name']}@{package['version']}")
        lock_source = lock_matches[0].get("source")
        expected.add((package["name"], package["version"], component_source_key(lock_source)))

    actual: set[tuple[str, str, str | None]] = set()
    for component in bom.get("components", []):
        ref = component.get("bom-ref", "")
        source_key = ref.split("#", 1)[0] if "#" in ref else None
        actual.add((component.get("name", ""), component.get("version", ""), source_key))
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise SupplyChainError(f"CycloneDX dependency set differs from locked normal/build graph; missing={missing}, extra={extra}")


def timestamp_from_epoch(epoch: int) -> str:
    value = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S") + ".000000000Z"


def snapshot_epoch(*, refresh: bool) -> int:
    if not refresh and MANIFEST_PATH.is_file():
        saved_manifest = read_json(MANIFEST_PATH)
        value = saved_manifest.get("source_date_epoch", saved_manifest.get("source_snapshot_epoch"))
        if not isinstance(value, int) or value < 0:
            raise SupplyChainError("existing SOURCE-MANIFEST.json has no valid source_date_epoch")
        return value
    output = run(["git", "log", "-1", "--format=%ct", "HEAD"])
    try:
        return int(output)
    except ValueError as error:
        raise SupplyChainError("cannot obtain the source snapshot commit timestamp") from error


def normalize_sbom(raw: dict[str, Any], epoch: int) -> dict[str, Any]:
    metadata = raw.get("metadata", {})
    component = metadata.get("component", {})
    name = component.get("name")
    version = component.get("version")
    if not name or not version:
        raise SupplyChainError("CycloneDX output is missing the root package identity")

    root_ref = component.get("bom-ref")
    if not root_ref or not root_ref.startswith("path+file://"):
        raise SupplyChainError("CycloneDX root reference is not the expected local path reference")
    stable_root_ref = f"pkg:cargo/{name}@{version}"
    reference_map = {root_ref: stable_root_ref}
    component["bom-ref"] = stable_root_ref
    component["purl"] = stable_root_ref

    for target in component.get("components", []):
        old_ref = target.get("bom-ref", "")
        target_type = target.get("type", "target")
        target_name = target.get("name", "target")
        target_key = "lib" if target_type == "library" else f"bin:{target_name}"
        new_ref = f"{stable_root_ref}#target={target_key}"
        if old_ref:
            reference_map[old_ref] = new_ref
        target["bom-ref"] = new_ref
        target.pop("purl", None)

    def remap(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: remap(child) for key, child in value.items()}
        if isinstance(value, list):
            return [remap(child) for child in value]
        if isinstance(value, str):
            return reference_map.get(value, value)
        return value

    raw = remap(raw)
    raw.setdefault("metadata", {})["timestamp"] = timestamp_from_epoch(epoch)
    serialized = json.dumps(raw, ensure_ascii=False, indent=2) + "\n"
    if "file://" in serialized or "path+file://" in serialized or str(ROOT) in serialized:
        raise SupplyChainError("normalized SBOM contains a local filesystem path")
    return raw


def generate_sbom(metadata: dict[str, Any], epoch: int) -> bytes:
    executable = os.environ.get("CARGO_CYCLONEDX") or shutil.which("cargo-cyclonedx")
    if not executable:
        raise SupplyChainError("cargo-cyclonedx is not installed; see docs/SUPPLY_CHAIN.md")
    executable_version([executable, "cyclonedx", "--version"], CYCLONEDX_VERSION)

    fd, stem = tempfile.mkstemp(prefix=".mdp-sbom-", dir=ROOT)
    os.close(fd)
    Path(stem).unlink(missing_ok=True)
    raw_path = Path(f"{stem}.json")
    environment = os.environ.copy()
    environment["SOURCE_DATE_EPOCH"] = str(epoch)
    command = [
        executable,
        "cyclonedx",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
        "--all-features",
        "--target",
        "all",
        "--format",
        "json",
        "--spec-version",
        SBOM_SPEC,
        "--override-filename",
        Path(stem).name,
    ]
    try:
        run_with_env(command, environment)
        if not raw_path.is_file():
            raise SupplyChainError("cargo-cyclonedx did not create the expected JSON SBOM")
        raw = read_json(raw_path)
        normalized = normalize_sbom(raw, epoch)
        verify_sbom_components(normalized, metadata)
        return canonical_json(normalized)
    finally:
        raw_path.unlink(missing_ok=True)


def run_with_env(command: list[str], environment: dict[str, str]) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise SupplyChainError(f"SBOM generator failed ({result.returncode}): {details}")
    return result.stdout.strip()


def package_inventory(metadata: dict[str, Any], lock_packages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    licenses: dict[tuple[str, str], list[str]] = {}
    for package in metadata.get("packages", []):
        licenses.setdefault((package["name"], package["version"]), []).append(package["license"])
    inventory = []
    for package in lock_packages:
        expressions = sorted(set(licenses.get((package["name"], package["version"]), [])))
        if len(expressions) != 1:
            raise SupplyChainError(f"package license metadata is ambiguous: {package['name']}@{package['version']}")
        inventory.append({
            "name": package["name"],
            "version": package["version"],
            "source": package.get("source") or "workspace",
            "checksum": package.get("checksum"),
            "license": expressions[0],
        })
    return sorted(inventory, key=lambda item: (item["name"], item["version"], item["source"]))


def render_notice(inventory: list[dict[str, Any]]) -> bytes:
    lines = [
        "Third-party dependency inventory",
        "",
        "This file lists the locked Cargo packages and license expressions declared by their manifests.",
        "It is an inventory, not a reproduction of upstream license texts or package-specific notices.",
        "The upstream licenses and attribution terms continue to apply; see each package source and",
        "the repository Cargo.lock. Cargo-deny validates the complete all-target dependency graph,",
        "including build and development dependencies, against deny.toml.",
        "",
        "| Package | Version | Declared license | Locked source |",
        "| --- | --- | --- | --- |",
    ]
    for package in inventory:
        if package["source"] == "workspace":
            continue
        source = package["source"].replace("|", "\\|")
        license_expression = package["license"].replace("|", "\\|")
        lines.append(f"| {package['name']} | {package['version']} | {license_expression} | {source} |")
    return ("\n".join(lines) + "\n").encode("utf-8")


def source_tree() -> tuple[list[dict[str, str]], str]:
    tracked = subprocess.run(
        ["git", "ls-files", "--cached", "-z"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if tracked.returncode != 0:
        details = tracked.stderr.decode("utf-8", errors="replace").strip()
        raise SupplyChainError(f"cannot enumerate tracked source files: {details}")
    try:
        tracked_paths = {
            ROOT / Path(item.decode("utf-8"))
            for item in tracked.stdout.split(b"\0")
            if item
        }
    except UnicodeDecodeError as error:
        raise SupplyChainError("tracked source path is not valid UTF-8") from error

    files: set[Path] = set()
    for pattern in TREE_GLOBS:
        files.update(
            path
            for path in ROOT.glob(pattern)
            if path in tracked_paths and path.is_file() and path.suffix in {".rs", ".py"}
        )
    entries = []
    tree_hash = hashlib.sha256()
    for path in sorted(files, key=lambda p: p.relative_to(ROOT).as_posix()):
        relative = path.relative_to(ROOT).as_posix()
        encoded_path = relative.encode("utf-8")
        file_hash = sha256_file(path)
        tree_hash.update(len(encoded_path).to_bytes(4, "big"))
        tree_hash.update(encoded_path)
        tree_hash.update(bytes.fromhex(file_hash))
        entries.append({"path": relative, "sha256": file_hash})
    return entries, tree_hash.hexdigest()


def build_source_manifest(
    metadata: dict[str, Any],
    lock_packages: list[dict[str, Any]],
    git_sources: list[dict[str, Any]],
    epoch: int,
    sbom: bytes,
    notice: bytes,
) -> bytes:
    root_package = next(p for p in metadata["packages"] if p["name"] == "market-data-platform")
    code_files, code_hash = source_tree()
    inputs = []
    for relative in INPUT_PATHS:
        path = ROOT / relative
        if not path.is_file():
            raise SupplyChainError(f"required source manifest input is missing: {relative}")
        inputs.append({"path": relative, "sha256": sha256_file(path)})
    manifest = {
        "schema_version": 1,
        "component": {
            "name": root_package["name"],
            "version": root_package["version"],
            "license": root_package["license"],
        },
        "source_date_epoch": epoch,
        "tools": {
            "rust_toolchain": RUST_TOOLCHAIN,
            "cargo_deny": DENY_VERSION,
            "cargo_cyclonedx": CYCLONEDX_VERSION,
            "cargo_cyclonedx_command": "cargo install cargo-cyclonedx --version 0.5.9 --locked",
            "sbom_specification": f"CycloneDX {SBOM_SPEC}",
        },
        "inputs": inputs,
        "source_tree": {
            "algorithm": "sha256 over sorted entries: u32be(path UTF-8 length) || path UTF-8 || raw file SHA-256",
            "files": code_files,
            "sha256": code_hash,
        },
        "git_sources": git_sources,
        "locked_packages": package_inventory(metadata, lock_packages),
        "artifacts": {
            "supply-chain/NOTICE.txt": sha256_bytes(notice),
            "supply-chain/market-data-platform.cdx.json": sha256_bytes(sbom),
        },
        "sbom_scope": (
            "All-target, all-feature normal and build dependency graph. Dev-only packages are listed "
            "in locked_packages and NOTICE.txt and are checked by cargo-deny; they are not shipped "
            "application components in this CycloneDX BOM."
        ),
    }
    return canonical_json(manifest)


def atomic_write(path: Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as output:
        temporary = Path(output.name)
        output.write(contents)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def build_expected_artifacts(*, refresh_epoch: bool) -> dict[Path, bytes]:
    cargo_tool_version("deny", DENY_VERSION)
    metadata = load_metadata()
    lock_packages = load_lockfile()
    pins = load_pins()
    validate_deny_policy(pins)
    git_sources = validate_pins(lock_packages)
    epoch = snapshot_epoch(refresh=refresh_epoch)
    sbom = generate_sbom(metadata, epoch)
    notice = render_notice(package_inventory(metadata, lock_packages))
    manifest = build_source_manifest(metadata, lock_packages, git_sources, epoch, sbom, notice)
    return {SBOM_PATH: sbom, NOTICE_PATH: notice, MANIFEST_PATH: manifest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true", help="atomically refresh SBOM, NOTICE, and source manifest")
    action.add_argument("--check", action="store_true", help="regenerate in memory and verify committed artifacts")
    parser.add_argument(
        "--refresh-source-date-epoch",
        action="store_true",
        help="reset the reproducible SBOM timestamp from current HEAD; valid only with --write",
    )
    args = parser.parse_args()

    try:
        if args.refresh_source_date_epoch and not args.write:
            raise SupplyChainError("--refresh-source-date-epoch requires --write")
        expected = build_expected_artifacts(refresh_epoch=args.refresh_source_date_epoch)
        if args.write:
            for path, contents in expected.items():
                atomic_write(path, contents)
                print(f"wrote {path.relative_to(ROOT)} sha256={sha256_bytes(contents)}")
            return 0

        failures = []
        for path, contents in expected.items():
            if not path.is_file():
                failures.append(f"missing {path.relative_to(ROOT)}")
            elif path.read_bytes() != contents:
                failures.append(f"stale {path.relative_to(ROOT)}")
            else:
                print(f"verified {path.relative_to(ROOT)} sha256={sha256_bytes(contents)}")
        if failures:
            raise SupplyChainError("; ".join(failures) + "; run python3 scripts/supply_chain.py --write")
        return 0
    except (SupplyChainError, OSError, KeyError, ValueError, TypeError) as error:
        print(f"supply-chain: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
