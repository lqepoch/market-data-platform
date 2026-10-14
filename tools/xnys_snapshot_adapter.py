#!/usr/bin/env python3
"""Convert the pinned Stock XNYS artifact to the router's strict native snapshot JSON.

This offline adapter consumes artifact data only. It does not copy or import the Stock runtime
loader, contact the network, or generate overnight/BOATS windows. The caller must pin the exact
input artifact SHA-256 on every invocation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

CALENDAR_ID = "XNYS"
CALENDAR_VERSION = "nyse.v1"
TIMEZONE = ZoneInfo("America/New_York")
UTC = timezone.utc
NYSE_REFERENCE = "https://www.nyse.com/trade/hours-calendars"
EXPECTED_SOURCE_NAME = "NYSE Holidays & Trading Hours"
EXPECTED_GENERATOR_PACKAGE = "pandas_market_calendars"
EXPECTED_SOURCE_STATUS = "verified_through_official_fixture"
MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
MAX_CALENDAR_DAYS = 50_000
MAX_CALENDAR_INSTANCES = MAX_CALENDAR_DAYS * 4
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")

ROOT_KEYS = {
    "schema_version",
    "calendar_id",
    "timezone",
    "valid_from",
    "valid_to",
    "source",
    "generated_at",
    "git_commit",
    "compiler_config_sha256",
    "overrides",
    "sessions",
    "closures",
    "canonical_checksum",
}
SOURCE_KEYS = {
    "name",
    "publication_date",
    "reference",
    "generator_package",
    "generator_version",
    "official_coverage_from",
    "verified_through",
    "verification_status",
    "next_review_at",
}
SESSION_KEYS = {
    "session_date",
    "pre_open",
    "open",
    "core_close",
    "post_close",
    "early_close",
    "closure_reason",
    "verification_status",
}
CLOSURE_KEYS = {"session_date", "closure_reason", "verification_status"}


class AdapterError(ValueError):
    """Raised when the pinned source artifact cannot be trusted or mapped safely."""


def _exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise AdapterError(f"{label} has an unexpected JSON shape")
    return value


def _parse_date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise AdapterError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise AdapterError(f"{label} must be an ISO date") from error
    if parsed.isoformat() != value:
        raise AdapterError(f"{label} must use canonical YYYY-MM-DD form")
    return parsed


def _parse_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise AdapterError(f"{label} must be UTC RFC3339 ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise AdapterError(f"{label} must be UTC RFC3339 ending in Z") from error
    return parsed.astimezone(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("ascii")


def _expected_status(trade_date: date, verified_from: date, verified_through: date) -> str:
    return "verified" if verified_from <= trade_date <= verified_through else "provisional"


def convert_artifact(data: bytes, expected_input_sha256: str) -> dict[str, Any]:
    """Verify an immutable Stock XNYS artifact and return the native router snapshot."""
    if not SHA256_RE.fullmatch(expected_input_sha256):
        raise AdapterError("expected input SHA-256 must be 64 lowercase hexadecimal characters")
    if not data or len(data) > MAX_INPUT_BYTES:
        raise AdapterError("input artifact is empty or exceeds the bounded 32 MiB limit")
    actual_input_sha256 = hashlib.sha256(data).hexdigest()
    if actual_input_sha256 != expected_input_sha256:
        raise AdapterError("input artifact does not match the operator-pinned SHA-256")
    try:
        artifact = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AdapterError("input artifact is not valid UTF-8 JSON") from error

    artifact = _exact_keys(artifact, ROOT_KEYS, "artifact")
    embedded_checksum = artifact["canonical_checksum"]
    if not isinstance(embedded_checksum, str) or not SHA256_RE.fullmatch(embedded_checksum):
        raise AdapterError("artifact canonical_checksum is invalid")
    checksum_document = dict(artifact)
    del checksum_document["canonical_checksum"]
    if hashlib.sha256(_canonical_bytes(checksum_document)).hexdigest() != embedded_checksum:
        raise AdapterError("artifact canonical_checksum mismatch")

    source = _exact_keys(artifact["source"], SOURCE_KEYS, "source metadata")
    if (
        type(artifact["schema_version"]) is not int
        or artifact["schema_version"] != 1
        or artifact["calendar_id"] != CALENDAR_ID
        or artifact["timezone"] != "America/New_York"
        or source["name"] != EXPECTED_SOURCE_NAME
        or source["reference"] != NYSE_REFERENCE
        or source["generator_package"] != EXPECTED_GENERATOR_PACKAGE
        or not isinstance(source["generator_version"], str)
        or not source["generator_version"].strip()
        or source["verification_status"] != EXPECTED_SOURCE_STATUS
    ):
        raise AdapterError("artifact identity or official source metadata is unsupported")

    valid_from = _parse_date(artifact["valid_from"], "valid_from")
    valid_to = _parse_date(artifact["valid_to"], "valid_to")
    verified_from = _parse_date(source["official_coverage_from"], "official_coverage_from")
    verified_through = _parse_date(source["verified_through"], "verified_through")
    publication_date = _parse_date(source["publication_date"], "publication_date")
    next_review_at = _parse_date(source["next_review_at"], "next_review_at")
    if not valid_from <= verified_from <= verified_through <= valid_to:
        raise AdapterError("official verification range is outside artifact coverage")
    if (valid_to - valid_from).days + 1 > MAX_CALENDAR_DAYS or valid_to == date.max:
        raise AdapterError("calendar coverage exceeds the bounded native snapshot limits")
    if not valid_from <= publication_date <= valid_to or not valid_from <= next_review_at <= valid_to:
        raise AdapterError("source publication or review date is outside artifact coverage")
    generated_at = _parse_utc(artifact["generated_at"], "generated_at")
    git_commit = artifact["git_commit"]
    compiler_config_sha256 = artifact["compiler_config_sha256"]
    if not isinstance(git_commit, str) or not GIT_COMMIT_RE.fullmatch(git_commit) or not SHA256_RE.fullmatch(
        compiler_config_sha256 if isinstance(compiler_config_sha256, str) else ""
    ):
        raise AdapterError("artifact generation metadata is invalid")
    if artifact["overrides"] != []:
        raise AdapterError("artifact overrides are unsupported by this read-only adapter")

    sessions: dict[date, dict[str, Any]] = {}
    previous_session_date: date | None = None
    for index, raw_session in enumerate(artifact["sessions"]):
        session = _exact_keys(raw_session, SESSION_KEYS, f"sessions[{index}]")
        session_date = _parse_date(session["session_date"], f"sessions[{index}].session_date")
        if previous_session_date is not None and session_date <= previous_session_date:
            raise AdapterError("session rows are not strictly date-sorted")
        previous_session_date = session_date
        if not valid_from <= session_date <= valid_to or session_date.weekday() >= 5:
            raise AdapterError("session date is outside weekday artifact coverage")
        if session["verification_status"] != _expected_status(
            session_date, verified_from, verified_through
        ):
            raise AdapterError(f"session verification status is inconsistent: {session_date}")
        if not isinstance(session["early_close"], bool) or session["closure_reason"] != "":
            raise AdapterError(f"session metadata is inconsistent: {session_date}")
        pre_open = _parse_utc(session["pre_open"], "session.pre_open")
        open_at = _parse_utc(session["open"], "session.open")
        close_at = _parse_utc(session["core_close"], "session.core_close")
        post_close = _parse_utc(session["post_close"], "session.post_close")
        if not pre_open < open_at < close_at < post_close:
            raise AdapterError(f"session windows are unordered: {session_date}")
        for timestamp in (pre_open, open_at, close_at, post_close):
            if timestamp.astimezone(TIMEZONE).date() != session_date:
                raise AdapterError(f"session timestamp has the wrong New York date: {session_date}")
        local_pre = pre_open.astimezone(TIMEZONE)
        local_open = open_at.astimezone(TIMEZONE)
        local_close = close_at.astimezone(TIMEZONE)
        local_post = post_close.astimezone(TIMEZONE)
        if (
            (local_pre.hour, local_pre.minute) != (4, 0)
            or local_open.time() < time(9, 30)
            or (local_post.hour, local_post.minute) != (20, 0)
            or session["early_close"] != (local_close.time() < time(16, 0))
        ):
            raise AdapterError(f"session times do not match the XNYS artifact profile: {session_date}")
        sessions[session_date] = session
    if len(sessions) * 3 > MAX_CALENDAR_INSTANCES:
        raise AdapterError("session instances exceed the bounded native snapshot limits")

    closures: dict[date, dict[str, Any]] = {}
    previous_closure_date: date | None = None
    for index, raw_closure in enumerate(artifact["closures"]):
        closure = _exact_keys(raw_closure, CLOSURE_KEYS, f"closures[{index}]")
        closure_date = _parse_date(closure["session_date"], f"closures[{index}].session_date")
        if previous_closure_date is not None and closure_date <= previous_closure_date:
            raise AdapterError("closure rows are not strictly date-sorted")
        previous_closure_date = closure_date
        if not valid_from <= closure_date <= valid_to or closure_date.weekday() >= 5:
            raise AdapterError("closure date is outside weekday artifact coverage")
        if closure_date in sessions or not isinstance(closure["closure_reason"], str) or not closure[
            "closure_reason"
        ].strip():
            raise AdapterError(f"closure metadata is inconsistent: {closure_date}")
        if closure["verification_status"] != _expected_status(
            closure_date, verified_from, verified_through
        ):
            raise AdapterError(f"closure verification status is inconsistent: {closure_date}")
        closures[closure_date] = closure

    day_rows: list[dict[str, Any]] = []
    instances: list[dict[str, Any]] = []
    cursor = valid_from
    while cursor <= valid_to:
        local_start = datetime.combine(cursor, time.min, tzinfo=TIMEZONE)
        local_end = datetime.combine(cursor + timedelta(days=1), time.min, tzinfo=TIMEZONE)
        verification = (
            "modeled_weekend"
            if cursor.weekday() >= 5
            else _expected_status(cursor, verified_from, verified_through)
        )
        session = sessions.get(cursor)
        closure = closures.get(cursor)
        if cursor.weekday() >= 5:
            if session is not None or closure is not None:
                raise AdapterError(f"weekend has a weekday session disposition: {cursor}")
            state = {"status": "closed", "reason": "weekend"}
        elif session is not None:
            state = {"status": "open", "session_id": f"XNYS:{cursor.isoformat()}"}
            for phase, start_key, end_key in (
                ("pre", "pre_open", "open"),
                ("regular", "open", "core_close"),
                ("after", "core_close", "post_close"),
            ):
                instances.append(
                    {
                        "session_id": state["session_id"],
                        "session_instance_id": f"XNYS:{cursor.isoformat()}:{phase}",
                        "trade_date": cursor.isoformat(),
                        "phase": phase,
                        "window": {
                            "start": _iso_utc(_parse_utc(session[start_key], start_key)),
                            "end_exclusive": _iso_utc(_parse_utc(session[end_key], end_key)),
                        },
                    }
                )
        else:
            if closure is None:
                raise AdapterError(f"weekday has no session or closure disposition: {cursor}")
            state = {"status": "closed", "reason": closure["closure_reason"]}
        day_rows.append(
            {
                "trade_date": cursor.isoformat(),
                "day_window": {
                    "start": _iso_utc(local_start),
                    "end_exclusive": _iso_utc(local_end),
                },
                "state": state,
                "verification_status": verification,
            }
        )
        cursor += timedelta(days=1)

    snapshot = {
        "source_id": CALENDAR_ID,
        "version": CALENDAR_VERSION,
        "source_metadata": {
            "source_name": source["name"],
            "source_reference": source["reference"],
            "generator_package": source["generator_package"],
            "generator_version": source["generator_version"],
            "valid_from": valid_from.isoformat(),
            "valid_to": valid_to.isoformat(),
            "official_verified_from": verified_from.isoformat(),
            "official_verified_through": verified_through.isoformat(),
            "verification_status": source["verification_status"],
            "artifact_checksum": embedded_checksum,
            "pinned_input_sha256": actual_input_sha256,
            "generated_at": _iso_utc(generated_at),
            "source_git_commit": git_commit,
            "compiler_config_sha256": compiler_config_sha256,
            "next_review_at": next_review_at.isoformat(),
        },
        "coverage_start": valid_from.isoformat(),
        "coverage_end_exclusive": (valid_to + timedelta(days=1)).isoformat(),
        "days": day_rows,
        "instances": instances,
    }
    if len(instances) > MAX_CALENDAR_INSTANCES:
        raise AdapterError("session instances exceed the bounded native snapshot limits")
    if len(_canonical_bytes(snapshot)) > MAX_SNAPSHOT_BYTES:
        raise AdapterError("native snapshot exceeds the 16 MiB runtime limit")
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="pinned xnys-calendar-v1.json")
    parser.add_argument("--output", required=True, type=Path, help="native router snapshot JSON")
    parser.add_argument("--expected-sha256", required=True, help="operator-pinned raw input SHA-256")
    args = parser.parse_args()
    with args.input.open("rb") as source_file:
        data = source_file.read(MAX_INPUT_BYTES + 1)
    snapshot = convert_artifact(data, args.expected_sha256)
    output = _canonical_bytes(snapshot)
    args.output.write_bytes(output)
    print(
        f"wrote {args.output} days={len(snapshot['days'])} instances={len(snapshot['instances'])} "
        f"source_sha256={snapshot['source_metadata']['pinned_input_sha256']} "
        f"artifact_checksum={snapshot['source_metadata']['artifact_checksum']}"
    )


if __name__ == "__main__":
    main()
