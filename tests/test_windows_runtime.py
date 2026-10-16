import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from scripts import windows_runtime as runtime


def synthetic_pe(*, machine: int = 0x8664) -> bytes:
    image = bytearray(256)
    image[:2] = b"MZ"
    struct.pack_into("<I", image, 0x3C, 0x80)
    image[0x80:0x84] = b"PE\0\0"
    struct.pack_into("<H", image, 0x84, machine)
    struct.pack_into("<H", image, 0x98, 0x20B)
    return bytes(image)


class WindowsRuntimePackageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "runtime"
        self.directory.mkdir()
        (self.directory / "LICENSE").write_text("test license\n", encoding="utf-8")
        (self.directory / "NOTICE.txt").write_text("test notice\n", encoding="utf-8")
        (self.directory / runtime.RUNTIME_BINARY).write_bytes(synthetic_pe())
        self.candidate = "1" * 40
        self.source_manifest_sha256 = "a" * 64
        binary = self.directory / runtime.RUNTIME_BINARY
        metadata = {
            "releaseAcceptance": runtime.ACCEPTANCE,
            "arch": "x86_64",
            "binary": {
                "name": runtime.RUNTIME_BINARY,
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "sizeBytes": binary.stat().st_size,
            },
            "candidateSha": self.candidate,
            "packageName": runtime.PACKAGE_NAME,
            "packageVersion": runtime.PACKAGE_VERSION,
            "platform": "win32",
            "rustVersion": runtime.RUST_VERSION,
            "nasmVersion": runtime.NASM_VERSION,
            "schemaVersion": 1,
            "sourceManifestSha256": self.source_manifest_sha256,
            "target": runtime.TARGET,
        }
        runtime.write_json(self.directory / "runtime.json", metadata)
        runtime.write_file_checksums(self.directory, runtime.RUNTIME_FILES[:-1])

    def test_valid_candidate_package_and_x64_image_are_accepted(self) -> None:
        runtime.verify_runtime_directory(
            self.directory, self.candidate, self.source_manifest_sha256
        )

    def test_candidate_mismatch_is_rejected(self) -> None:
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.verify_runtime_directory(
                self.directory, "2" * 40, self.source_manifest_sha256
            )

    def test_tampered_package_member_is_rejected(self) -> None:
        with (self.directory / "LICENSE").open("ab") as stream:
            stream.write(b"tamper")
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.verify_runtime_directory(
                self.directory, self.candidate, self.source_manifest_sha256
            )

    def test_duplicate_checksum_entry_is_rejected(self) -> None:
        checksum = runtime.sha256_file(self.directory / "LICENSE")
        (self.directory / "SHA256SUMS").write_text(
            f"{checksum}  LICENSE\n{checksum}  LICENSE\n", encoding="ascii"
        )
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.verify_runtime_directory(
                self.directory, self.candidate, self.source_manifest_sha256
            )

    def test_wrong_pe_machine_is_rejected(self) -> None:
        wrong_arch = self.directory / "wrong-arch.exe"
        wrong_arch.write_bytes(synthetic_pe(machine=0x014C))
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.validate_pe_x64(wrong_arch)

    def test_unrecognized_runtime_metadata_field_is_rejected(self) -> None:
        metadata_path = self.directory / "runtime.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["unexpected"] = "not allowed"
        runtime.write_json(metadata_path, metadata)
        runtime.write_file_checksums(self.directory, runtime.RUNTIME_FILES[:-1])
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.verify_runtime_directory(
                self.directory, self.candidate, self.source_manifest_sha256
            )


if __name__ == "__main__":
    unittest.main()
