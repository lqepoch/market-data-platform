#!/usr/bin/env python3
"""Package and smoke-test the Windows MDP runtime without provider credentials.

The smoke process is intentionally fail-closed: it verifies service/auth routes and the
unconfigured Core Bars response. Synthetic provider responses remain cfg(test)-only.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlencode
import zipfile


PACKAGE_NAME = "market-data-platform"
PACKAGE_VERSION = "0.1.0"
RUNTIME_BINARY = "mdp.exe"
RUNTIME_FILES = ("LICENSE", "NOTICE.txt", RUNTIME_BINARY, "runtime.json", "SHA256SUMS")
ARCHIVE_NAME = "mdp-windows-x64.zip"
ARCHIVE_SUMS_NAME = "ARCHIVE_SHA256SUMS"
RUST_VERSION = "1.99.0"
NASM_VERSION = "2.16.03"
TARGET = "x86_64-pc-windows-msvc"
ACCEPTANCE = "LOCAL_EVALUATION_ONLY"
MAX_HTTP_BODY_BYTES = 1024 * 1024
STARTUP_TIMEOUT_SECONDS = 30
HTTP_TIMEOUT_SECONDS = 2
PROCESS_EXIT_TIMEOUT_SECONDS = 5


class RuntimeFailure(Exception):
    """A safe, user-facing validation failure without process or environment details."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_candidate_sha(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise RuntimeFailure("candidate SHA must be 40 lowercase hexadecimal characters")
    return value


def read_candidate_provenance(repo_root: Path, expected_candidate: str) -> str:
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        manifest = subprocess.run(
            ["git", "show", "HEAD:supply-chain/SOURCE-MANIFEST.json"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        raise RuntimeFailure("candidate Git provenance is unavailable") from None
    if validate_candidate_sha(revision) != expected_candidate or not manifest:
        raise RuntimeFailure("checked-out Git HEAD does not match the candidate")
    # Hash Git's committed blob bytes so Windows CRLF checkout conversion cannot change the pin.
    return hashlib.sha256(manifest).hexdigest()


def runner_temp_root() -> Path:
    value = os.environ.get("RUNNER_TEMP")
    if not value:
        raise RuntimeFailure("RUNNER_TEMP is required")
    root = Path(value).resolve(strict=True)
    if not root.is_dir():
        raise RuntimeFailure("RUNNER_TEMP is not a directory")
    return root


def is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((os.path.normcase(str(path)), os.path.normcase(str(root)))) == os.path.normcase(str(root))
    except ValueError:
        return False


def resolve_output_root(value: Path) -> tuple[Path, Path]:
    temp_root = runner_temp_root()
    output = value.absolute()
    # Resolve the existing parent, but do not follow a possibly existing final path.
    parent = output.parent.resolve(strict=True)
    output = parent / output.name
    if not is_within(output, temp_root) or output == temp_root:
        raise RuntimeFailure("package output must be a child of RUNNER_TEMP")
    if output.exists() or output.is_symlink():
        raise RuntimeFailure("package output already exists")
    return output, temp_root


def validate_pe_x64(path: Path) -> None:
    if path.is_symlink() or not path.is_file() or path.stat().st_size < 256:
        raise RuntimeFailure("release executable is missing or invalid")
    with path.open("rb") as stream:
        if stream.read(2) != b"MZ":
            raise RuntimeFailure("release executable is not a Windows PE file")
        stream.seek(0x3C)
        raw_offset = stream.read(4)
        if len(raw_offset) != 4:
            raise RuntimeFailure("release executable has an invalid PE header")
        pe_offset = struct.unpack("<I", raw_offset)[0]
        if pe_offset > path.stat().st_size - 26:
            raise RuntimeFailure("release executable has an invalid PE offset")
        stream.seek(pe_offset)
        if stream.read(4) != b"PE\0\0":
            raise RuntimeFailure("release executable has an invalid PE signature")
        machine = stream.read(2)
        stream.seek(pe_offset + 24)
        optional_magic = stream.read(2)
    if machine != b"\x64\x86" or optional_magic != b"\x0b\x02":
        raise RuntimeFailure("release executable is not x86_64 Windows PE32+")


def write_json(path: Path, value: object) -> None:
    contents = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    path.write_bytes(contents.encode("utf-8"))


def write_file_checksums(directory: Path, filenames: tuple[str, ...]) -> None:
    lines = [f"{sha256_file(directory / name)}  {name}" for name in sorted(filenames)]
    (directory / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="ascii", newline="\n")


def make_zip(directory: Path, archive_path: Path) -> None:
    # Fixed timestamps and sorted names make the archive independent of runner wall time.
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(RUNTIME_FILES):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, (directory / name).read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)


def package_runtime(binary: Path, output_arg: Path, candidate: str) -> None:
    candidate = validate_candidate_sha(candidate)
    output, _ = resolve_output_root(output_arg)
    repo_root = Path(__file__).resolve().parents[1]
    source_manifest_sha256 = read_candidate_provenance(repo_root, candidate)
    if binary.is_symlink():
        raise RuntimeFailure("release executable must not be a symbolic link")
    binary = binary.resolve(strict=True)
    validate_pe_x64(binary)

    license_path = repo_root / "LICENSE"
    notice_path = repo_root / "supply-chain" / "NOTICE.txt"
    if any(path.is_symlink() or not path.is_file() for path in (license_path, notice_path)):
        raise RuntimeFailure("required license inventory is missing")

    output.mkdir()
    runtime = output / "runtime"
    runtime.mkdir()
    shutil.copyfile(binary, runtime / RUNTIME_BINARY)
    shutil.copyfile(license_path, runtime / "LICENSE")
    shutil.copyfile(notice_path, runtime / "NOTICE.txt")

    binary_path = runtime / RUNTIME_BINARY
    metadata = {
        "releaseAcceptance": ACCEPTANCE,
        "arch": "x86_64",
        "binary": {
            "name": RUNTIME_BINARY,
            "sha256": sha256_file(binary_path),
            "sizeBytes": binary_path.stat().st_size,
        },
        "candidateSha": candidate,
        "packageName": PACKAGE_NAME,
        "packageVersion": PACKAGE_VERSION,
        "platform": "win32",
        "rustVersion": RUST_VERSION,
        "nasmVersion": NASM_VERSION,
        "schemaVersion": 1,
        "sourceManifestSha256": source_manifest_sha256,
        "target": TARGET,
    }
    write_json(runtime / "runtime.json", metadata)
    write_file_checksums(runtime, RUNTIME_FILES[:-1])

    archive_path = output / ARCHIVE_NAME
    make_zip(runtime, archive_path)
    archive_sum = sha256_file(archive_path)
    (output / ARCHIVE_SUMS_NAME).write_text(
        f"{archive_sum}  {ARCHIVE_NAME}\n", encoding="ascii", newline="\n"
    )
    print(f"Packaged {TARGET} runtime for candidate {candidate}.")


def parse_checksum_file(path: Path, expected_names: tuple[str, ...]) -> dict[str, str]:
    if path.is_symlink() or not path.is_file():
        raise RuntimeFailure("checksum file is missing")
    found: dict[str, str] = {}
    for line in path.read_text(encoding="ascii").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9._-]+)", line)
        if match is None or match.group(2) in found:
            raise RuntimeFailure("checksum file is malformed")
        found[match.group(2)] = match.group(1)
    if set(found) != set(expected_names):
        raise RuntimeFailure("checksum file contents do not match the runtime package")
    return found


def verify_runtime_directory(directory: Path, candidate: str, source_manifest_sha256: str) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise RuntimeFailure("runtime package directory is missing")
    children = {item.name for item in directory.iterdir()}
    if children != set(RUNTIME_FILES):
        raise RuntimeFailure("runtime package contains an unexpected file set")
    checksums = parse_checksum_file(directory / "SHA256SUMS", RUNTIME_FILES[:-1])
    for name, expected in checksums.items():
        path = directory / name
        if path.is_symlink() or not path.is_file() or sha256_file(path) != expected:
            raise RuntimeFailure("runtime package checksum verification failed")
    try:
        metadata = json.loads((directory / "runtime.json").read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise RuntimeFailure("runtime metadata is invalid") from None
    if not isinstance(metadata, dict):
        raise RuntimeFailure("runtime metadata is invalid")
    expected_metadata = {
        "releaseAcceptance": ACCEPTANCE,
        "arch": "x86_64",
        "candidateSha": candidate,
        "packageName": PACKAGE_NAME,
        "packageVersion": PACKAGE_VERSION,
        "platform": "win32",
        "rustVersion": RUST_VERSION,
        "nasmVersion": NASM_VERSION,
        "schemaVersion": 1,
        "target": TARGET,
    }
    if not re.fullmatch(r"[0-9a-f]{64}", source_manifest_sha256):
        raise RuntimeFailure("expected source manifest hash is invalid")
    if (
        any(metadata.get(key) != value for key, value in expected_metadata.items())
        or metadata.get("sourceManifestSha256") != source_manifest_sha256
    ):
        raise RuntimeFailure("runtime provenance does not match this candidate")
    if set(metadata) != set(expected_metadata) | {"binary", "sourceManifestSha256"}:
        raise RuntimeFailure("runtime metadata contains unexpected fields")
    binary_metadata = metadata.get("binary")
    binary_path = directory / RUNTIME_BINARY
    if (
        not isinstance(binary_metadata, dict)
        or binary_metadata.get("name") != RUNTIME_BINARY
        or binary_metadata.get("sha256") != sha256_file(binary_path)
        or binary_metadata.get("sizeBytes") != binary_path.stat().st_size
    ):
        raise RuntimeFailure("runtime binary metadata is invalid")
    validate_pe_x64(binary_path)


def verify_archive(directory: Path) -> None:
    output = directory.parent
    archive_path = output / ARCHIVE_NAME
    sums_path = output / ARCHIVE_SUMS_NAME
    checksums = parse_checksum_file(sums_path, (ARCHIVE_NAME,))
    if archive_path.is_symlink() or not archive_path.is_file() or sha256_file(archive_path) != checksums[ARCHIVE_NAME]:
        raise RuntimeFailure("runtime archive checksum verification failed")
    try:
        with zipfile.ZipFile(archive_path) as archive:
            names = archive.namelist()
            if names != sorted(RUNTIME_FILES):
                raise RuntimeFailure("runtime archive contains an unexpected file set")
            for name in RUNTIME_FILES:
                if archive.read(name) != (directory / name).read_bytes():
                    raise RuntimeFailure("runtime archive differs from the verified package")
    except (OSError, zipfile.BadZipFile, KeyError):
        raise RuntimeFailure("runtime archive is invalid") from None


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def make_market_token(secret: str) -> str:
    issuer = "eqoboard-openterminal"
    kid = "mdp-terminal"
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT", "kid": kid}
    claims = {
        "iss": issuer,
        "aud": "lqepoch-market-data",
        "sub": "windows-runtime-smoke",
        "idp_iss": "https://smoke.invalid/issuer",
        "jti": secrets.token_hex(16),
        "iat": now,
        "exp": now + 30,
        "scope": ["market:read"],
    }
    signing_input = f"{b64url(json.dumps(header, separators=(',', ':')).encode())}.{b64url(json.dumps(claims, separators=(',', ':')).encode())}"
    signature = hmac.new(secret.encode("ascii"), signing_input.encode("ascii"), hashlib.sha256).digest()
    return f"{signing_input}.{b64url(signature)}"


def decode_json_response(response: http.client.HTTPResponse) -> dict[str, object]:
    body = response.read(MAX_HTTP_BODY_BYTES + 1)
    if len(body) > MAX_HTTP_BODY_BYTES:
        raise RuntimeFailure("HTTP response exceeded the smoke-test size limit")
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise RuntimeFailure("HTTP response was not valid UTF-8 JSON") from None
    if not isinstance(decoded, dict):
        raise RuntimeFailure("HTTP response JSON has an unexpected shape")
    return decoded


def request_json(port: int, path: str, token: str | None = None) -> tuple[int, dict[str, object], dict[str, str]]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=HTTP_TIMEOUT_SECONDS)
    headers = {"Accept": "application/json", "Connection": "close"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    try:
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        status = response.status
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        body = decode_json_response(response)
        return status, body, response_headers
    except (OSError, http.client.HTTPException):
        raise RuntimeFailure("local HTTP request failed") from None
    finally:
        connection.close()


def reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def child_environment(smoke_root: Path) -> dict[str, str]:
    inherited = os.environ
    allowlist = ("SystemRoot", "WINDIR", "PATH", "PATHEXT", "COMSPEC")
    environment = {key: inherited[key] for key in allowlist if inherited.get(key)}
    environment["TEMP"] = str(smoke_root / "temp")
    environment["TMP"] = str(smoke_root / "temp")
    environment["RUST_LOG"] = "off"
    environment["MDP_TERMINAL_JWT_SECRET"] = secrets.token_hex(64)
    environment["MDP_RESEARCH_JWT_SECRET"] = secrets.token_hex(64)
    forbidden_markers = ("ALPACA", "APCA", "FUTU", "RCLONE", "YFINANCE", "PROVIDER", "BROKER")
    if any(marker in name.upper() for name in environment for marker in forbidden_markers):
        raise RuntimeFailure("provider environment leaked into the smoke process")
    return environment


def smoke_runtime(directory: Path, candidate: str) -> None:
    candidate = validate_candidate_sha(candidate)
    repo_root = Path(__file__).resolve().parents[1]
    source_manifest_sha256 = read_candidate_provenance(repo_root, candidate)
    verify_runtime_directory(directory, candidate, source_manifest_sha256)
    verify_archive(directory)

    runner_temp = runner_temp_root()
    with tempfile.TemporaryDirectory(prefix="mdp-windows-smoke-", dir=runner_temp) as smoke_path:
        smoke_root = Path(smoke_path).resolve(strict=True)
        (smoke_root / "temp").mkdir()
        local_test_root = smoke_root / "empty-local-test-root"
        cache_root = smoke_root / "cache"
        local_test_root.mkdir()
        cache_root.mkdir()
        environment = child_environment(smoke_root)
        binary = directory / RUNTIME_BINARY
        port = reserve_loopback_port()
        command = [
            str(binary), "serve", "--bind", f"127.0.0.1:{port}",
            "--local-test-root", str(local_test_root), "--cache-dir", str(cache_root),
        ]
        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=environment,
                close_fds=True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
            )
            deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
            health: tuple[int, dict[str, object], dict[str, str]] | None = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeFailure("runtime process exited before becoming healthy")
                try:
                    health = request_json(port, "/healthz")
                    if health[0] == 200:
                        break
                except RuntimeFailure:
                    time.sleep(0.15)
            if health is None or health[0] != 200 or health[1].get("status") != "alive":
                raise RuntimeFailure("runtime health endpoint did not become ready")
            if "no-store" not in health[2].get("cache-control", "").lower():
                raise RuntimeFailure("runtime health endpoint omitted no-store")

            readiness_status, readiness, readiness_headers = request_json(port, "/readyz")
            if (
                readiness_status != 200
                or readiness.get("status") != "ready"
                or readiness.get("market_ready") is not False
                or readiness.get("market_calendar_configured") is not False
                or readiness.get("source_entitlement") != "unverified"
                or "no-store" not in readiness_headers.get("cache-control", "").lower()
            ):
                raise RuntimeFailure("runtime readiness contract failed")

            query = urlencode({
                "symbol": "SPY", "asset_class": "etf",
                "start": "2024-01-02T14:30:00Z", "end": "2024-01-02T21:00:00Z",
                "resolution": "1m", "session_scope": "all", "adjustment": "unadjusted",
                "provider_mode": "auto", "allow_daily_downgrade": "true", "limit": "390",
            })
            path = f"/v1/market/bars?{query}"
            unauthorized_status, unauthorized, _ = request_json(port, path)
            if unauthorized_status != 401 or unauthorized.get("error") != "unauthorized":
                raise RuntimeFailure("protected market route did not reject an unauthenticated request")

            terminal_token = make_market_token(environment["MDP_TERMINAL_JWT_SECRET"])
            bars_status, bars, bars_headers = request_json(port, path, terminal_token)
            if (
                bars_status != 503
                or bars.get("error") != "market_data_unavailable"
                or "no-store" not in bars_headers.get("cache-control", "").lower()
            ):
                raise RuntimeFailure("unconfigured Core Bars route did not fail closed")
            if process.poll() is not None:
                raise RuntimeFailure("runtime process exited during the smoke checks")
        finally:
            try:
                if process is not None and process.poll() is None:
                    # Kill only this process handle; this is not graceful-shutdown evidence.
                    process.kill()
                    process.wait(timeout=PROCESS_EXIT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                raise RuntimeFailure("owned runtime process did not exit after forced cleanup") from None
            except OSError:
                raise RuntimeFailure("owned runtime process cleanup failed") from None
            finally:
                for name in ("MDP_TERMINAL_JWT_SECRET", "MDP_RESEARCH_JWT_SECRET"):
                    environment.pop(name, None)
    print("Windows runtime smoke passed; graceful shutdown was not assessed.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    package = commands.add_parser("package", help="create a hashed Windows x64 runtime package")
    package.add_argument("--binary", type=Path, required=True)
    package.add_argument("--output", type=Path, required=True)
    package.add_argument("--candidate", required=True)
    smoke = commands.add_parser("smoke", help="exercise a package with synthetic local auth only")
    smoke.add_argument("--package", type=Path, required=True)
    smoke.add_argument("--candidate", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "package":
            package_runtime(args.binary, args.output, args.candidate)
        else:
            smoke_runtime(args.package, args.candidate)
        return 0
    except RuntimeFailure as failure:
        print(f"Windows runtime {args.command} failed: {failure}", file=sys.stderr)
        return 1
    except Exception:
        # Unexpected exceptions may contain process paths or implementation detail; CI logs only
        # a stable category and retains the actual failure status.
        print(f"Windows runtime {args.command} failed: internal error", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
