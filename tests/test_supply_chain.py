"""Focused tests for path sanitization and immutable git source parsing."""

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("supply_chain", ROOT / "scripts/supply_chain.py")
assert SPEC and SPEC.loader
supply_chain = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supply_chain)


class GitSourceTests(unittest.TestCase):
    def test_git_lock_source_requires_matching_full_revision(self) -> None:
        revision = "4230418f7fe25f70e3011fed2ba7eb59c7e4d875"
        source = f"git+https://github.com/lqepoch/trading-core?rev={revision}#{revision}"
        self.assertEqual(
            supply_chain.parse_git_source(source),
            ("https://github.com/lqepoch/trading-core", revision, revision),
        )

    def test_git_source_rejects_branch_or_mismatching_commit(self) -> None:
        with self.assertRaises(supply_chain.SupplyChainError):
            supply_chain.parse_git_source("git+https://github.com/lqepoch/trading-core?branch=main#main")
        with self.assertRaises(supply_chain.SupplyChainError):
            supply_chain.parse_git_source(
                "git+https://github.com/lqepoch/trading-core?rev=4230418f7fe25f70e3011fed2ba7eb59c7e4d875#"
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )


class LicensePolicyTests(unittest.TestCase):
    def test_certificate_data_exception_rejects_new_package_versions(self) -> None:
        pins = supply_chain.load_pins()
        supply_chain.validate_deny_policy(pins)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            license_path = root / "supply-chain/licenses/webpki-root-certs-1.0.9.LICENSE"
            license_path.parent.mkdir(parents=True)
            license_path.write_bytes((ROOT / license_path.relative_to(root)).read_bytes())
            (root / "deny.toml").write_text((ROOT / "deny.toml").read_text().replace(
                'crate = "webpki-root-certs@1.0.9"', 'crate = "webpki-root-certs@1.0.10"'))
            with patch.object(supply_chain, "ROOT", root), self.assertRaises(supply_chain.SupplyChainError):
                supply_chain.validate_deny_policy(pins)

    def test_certificate_data_license_must_be_preserved(self) -> None:
        pins = supply_chain.load_pins()
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "deny.toml").write_bytes((ROOT / "deny.toml").read_bytes())
            license_path = root / "supply-chain/licenses/webpki-root-certs-1.0.9.LICENSE"
            license_path.parent.mkdir(parents=True)
            license_path.write_text("incomplete license")
            with patch.object(supply_chain, "ROOT", root), self.assertRaises(supply_chain.SupplyChainError):
                supply_chain.validate_deny_policy(pins)


class SourceTreeTests(unittest.TestCase):
    def test_untracked_source_files_are_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            tracked = root / "src" / "archive" / "tracked.rs"
            untracked = root / "src" / "archive" / "draft.rs"
            tracked.parent.mkdir(parents=True)
            tracked.write_text("tracked source\n", encoding="utf-8")
            untracked.write_text("untracked draft\n", encoding="utf-8")
            completed = supply_chain.subprocess.CompletedProcess(
                args=["git", "ls-files", "--cached", "-z"],
                returncode=0,
                stdout=b"src/archive/tracked.rs\0",
                stderr=b"",
            )

            with (
                patch.object(supply_chain, "ROOT", root),
                patch.object(supply_chain, "TREE_GLOBS", ("src/**/*.rs",)),
                patch.object(supply_chain.subprocess, "run", return_value=completed),
            ):
                entries, _ = supply_chain.source_tree()

        self.assertEqual([entry["path"] for entry in entries], ["src/archive/tracked.rs"])


class SbomNormalizationTests(unittest.TestCase):
    def test_local_paths_are_replaced_in_component_and_dependency_refs(self) -> None:
        local_root = "/private/worktree/market-data-platform"
        root_ref = f"path+file://{local_root}#market-data-platform@0.1.0"
        lib_ref = f"{root_ref} bin-target-0"
        bin_ref = f"{root_ref} bin-target-1"
        bom = {
            "metadata": {
                "component": {
                    "name": "market-data-platform",
                    "version": "0.1.0",
                    "bom-ref": root_ref,
                    "purl": "pkg:cargo/market-data-platform@0.1.0?download_url=file://.",
                    "components": [
                        {"type": "library", "name": "market_data_platform", "bom-ref": lib_ref, "purl": "file://lib"},
                        {"type": "application", "name": "market-data-platform", "bom-ref": bin_ref, "purl": "file://bin"},
                    ],
                }
            },
            "dependencies": [
                {"ref": root_ref, "dependsOn": [lib_ref, bin_ref]},
                {"ref": lib_ref, "dependsOn": []},
                {"ref": bin_ref, "dependsOn": []},
            ],
            "components": [],
        }

        normalized = supply_chain.normalize_sbom(bom, 1791446400)
        serialized = supply_chain.canonical_json(normalized).decode("utf-8")
        self.assertNotIn(local_root, serialized)
        self.assertNotIn("file://", serialized)
        self.assertEqual(
            normalized["dependencies"][0]["ref"],
            "pkg:cargo/market-data-platform@0.1.0",
        )
        self.assertEqual(
            normalized["dependencies"][0]["dependsOn"],
            [
                "pkg:cargo/market-data-platform@0.1.0#target=lib",
                "pkg:cargo/market-data-platform@0.1.0#target=bin:market-data-platform",
            ],
        )


if __name__ == "__main__":
    unittest.main()
