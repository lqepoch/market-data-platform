from __future__ import annotations

import hashlib
import json
import unittest
from xnys_snapshot_adapter import AdapterError, _canonical_bytes, convert_artifact


def _session(trade_date: str, pre: str, open_at: str, close: str, post: str) -> dict[str, object]:
    return {
        "session_date": trade_date,
        "pre_open": pre,
        "open": open_at,
        "core_close": close,
        "post_close": post,
        "early_close": close.endswith("T17:00:00Z"),
        "closure_reason": "",
        "verification_status": "verified",
    }


def _artifact_bytes() -> bytes:
    artifact: dict[str, object] = {
        "schema_version": 1,
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "valid_from": "2026-03-08",
        "valid_to": "2026-03-10",
        "source": {
            "name": "NYSE Holidays & Trading Hours",
            "publication_date": "2026-03-08",
            "reference": "https://www.nyse.com/trade/hours-calendars",
            "generator_package": "pandas_market_calendars",
            "generator_version": "5.4.0",
            "official_coverage_from": "2026-03-08",
            "verified_through": "2026-03-10",
            "verification_status": "verified_through_official_fixture",
            "next_review_at": "2026-03-10",
        },
        "generated_at": "2026-03-08T00:00:00Z",
        "git_commit": "a" * 40,
        "compiler_config_sha256": "b" * 64,
        "overrides": [],
        "sessions": [
            _session("2026-03-09", "2026-03-09T08:00:00Z", "2026-03-09T13:30:00Z", "2026-03-09T20:00:00Z", "2026-03-10T00:00:00Z"),
            _session("2026-03-10", "2026-03-10T08:00:00Z", "2026-03-10T13:30:00Z", "2026-03-10T17:00:00Z", "2026-03-11T00:00:00Z"),
        ],
        "closures": [],
    }
    artifact["canonical_checksum"] = hashlib.sha256(_canonical_bytes(artifact)).hexdigest()
    return _canonical_bytes(artifact)


class XnysSnapshotAdapterTests(unittest.TestCase):
    def test_pinned_artifact_maps_to_dst_aware_pre_regular_after_only(self) -> None:
        data = _artifact_bytes()
        digest = hashlib.sha256(data).hexdigest()
        snapshot = convert_artifact(data, digest)

        self.assertEqual(snapshot["source_id"], "XNYS")
        self.assertEqual(snapshot["version"], "nyse.v1")
        self.assertEqual(snapshot["source_metadata"]["pinned_input_sha256"], digest)
        self.assertEqual(snapshot["source_metadata"]["artifact_checksum"], json.loads(data)["canonical_checksum"])
        self.assertEqual(len(snapshot["days"]), 3)
        self.assertEqual(snapshot["days"][0]["verification_status"], "modeled_weekend")
        self.assertEqual(snapshot["days"][0]["day_window"]["start"], "2026-03-08T05:00:00Z")
        self.assertEqual(snapshot["days"][0]["day_window"]["end_exclusive"], "2026-03-09T04:00:00Z")

        phases = snapshot["instances"]
        self.assertEqual([instance["phase"] for instance in phases], ["pre", "regular", "after"] * 2)
        self.assertTrue(all(instance["phase"] != "over" for instance in phases))
        self.assertEqual(phases[0]["window"]["start"], "2026-03-09T08:00:00Z")
        self.assertEqual(phases[4]["window"]["end_exclusive"], "2026-03-10T17:00:00Z")
        self.assertEqual(snapshot["days"][2]["verification_status"], "verified")

    def test_input_pin_checksum_and_unknown_fields_fail_closed(self) -> None:
        data = _artifact_bytes()
        digest = hashlib.sha256(data).hexdigest()
        with self.assertRaisesRegex(AdapterError, "operator-pinned"):
            convert_artifact(data, "0" * 64)
        with self.assertRaisesRegex(AdapterError, "lowercase"):
            convert_artifact(data, "A" * 64)

        artifact = json.loads(data)
        artifact["unexpected"] = True
        changed = _canonical_bytes(artifact)
        with self.assertRaisesRegex(AdapterError, "unexpected JSON shape"):
            convert_artifact(changed, hashlib.sha256(changed).hexdigest())

        artifact = json.loads(data)
        artifact["canonical_checksum"] = "0" * 64
        changed = _canonical_bytes(artifact)
        with self.assertRaisesRegex(AdapterError, "canonical_checksum mismatch"):
            convert_artifact(changed, hashlib.sha256(changed).hexdigest())

    def test_delayed_source_open_is_preserved_without_rewriting_calendar_times(self) -> None:
        artifact = json.loads(_artifact_bytes())
        artifact["sessions"][0]["open"] = "2026-03-09T13:31:00Z"
        checksum_document = {
            key: value for key, value in artifact.items() if key != "canonical_checksum"
        }
        artifact["canonical_checksum"] = hashlib.sha256(
            _canonical_bytes(checksum_document)
        ).hexdigest()
        data = _canonical_bytes(artifact)
        snapshot = convert_artifact(data, hashlib.sha256(data).hexdigest())

        self.assertEqual(snapshot["instances"][1]["window"]["start"], "2026-03-09T13:31:00Z")

    def test_calendar_expansion_is_bounded_before_materializing_rows(self) -> None:
        artifact = json.loads(_artifact_bytes())
        artifact["valid_from"] = "1800-01-01"
        artifact["valid_to"] = "2031-12-31"
        checksum_document = {
            key: value for key, value in artifact.items() if key != "canonical_checksum"
        }
        artifact["canonical_checksum"] = hashlib.sha256(
            _canonical_bytes(checksum_document)
        ).hexdigest()
        data = _canonical_bytes(artifact)

        with self.assertRaisesRegex(AdapterError, "bounded native snapshot limits"):
            convert_artifact(data, hashlib.sha256(data).hexdigest())

    def test_unverified_calendar_rows_remain_provisional(self) -> None:
        artifact = json.loads(_artifact_bytes())
        artifact["valid_from"] = "2025-12-31"
        artifact["valid_to"] = "2026-01-02"
        artifact["source"]["official_coverage_from"] = "2026-01-02"
        artifact["source"]["verified_through"] = "2026-01-02"
        artifact["source"]["publication_date"] = "2025-12-31"
        artifact["source"]["next_review_at"] = "2026-01-02"
        artifact["sessions"] = [
            _session(
                "2026-01-02",
                "2026-01-02T09:00:00Z",
                "2026-01-02T14:30:00Z",
                "2026-01-02T21:00:00Z",
                "2026-01-03T01:00:00Z",
            )
        ]
        artifact["sessions"][0]["verification_status"] = "verified"
        artifact["closures"] = [
            {
                "session_date": "2025-12-31",
                "closure_reason": "engine_non_session",
                "verification_status": "provisional",
            },
            {
                "session_date": "2026-01-01",
                "closure_reason": "official_closure",
                "verification_status": "provisional",
            },
        ]
        artifact["canonical_checksum"] = hashlib.sha256(
            _canonical_bytes({key: value for key, value in artifact.items() if key != "canonical_checksum"})
        ).hexdigest()
        data = _canonical_bytes(artifact)
        snapshot = convert_artifact(data, hashlib.sha256(data).hexdigest())
        rows = {row["trade_date"]: row for row in snapshot["days"]}
        self.assertEqual(rows["2025-12-31"]["verification_status"], "provisional")
        self.assertEqual(rows["2026-01-01"]["verification_status"], "provisional")
        self.assertEqual(rows["2026-01-02"]["verification_status"], "verified")


if __name__ == "__main__":
    unittest.main()
