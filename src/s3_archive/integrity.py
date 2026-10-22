"""Per-member integrity mismatches: a member whose decoded bytes fail the archive's own check.

zip and 7z store a CRC-32 and an uncompressed size for every member. When
the decoded bytes don't match them, the strict default (every iterator's
default) raises, exactly as before. A caller that opts in with
``tolerate_integrity_errors=True`` instead gets every decoded byte and an
:class:`IntegrityMismatch` on :attr:`s3_archive.members.ArchiveMember.integrity`
once the member's bytes are consumed — what Info-ZIP ``unzip`` and 7-Zip
do: write the member as decoded, warn, keep going.

The tar family has no per-member checksum, so nothing here applies to it;
see docs/DECISIONS.md.
"""

import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class IntegrityMismatch:
    """What the archive declared for a member against what its bytes decoded to.

    *check* names the check that failed, in the decoder's terms (e.g.
    ``"CRC-32"``, ``"uncompressed size"``, ``"compressed size"``). The
    expected values are the archive's own record (the zip central
    directory, the 7z header); the actual values are computed over the
    bytes that were decoded and handed to the consumer. A failed
    compressed-size check can leave CRC and size both matching — the
    mismatch is then in the compressed stream's framing, and the decoded
    bytes are still what was handed on.
    """

    check: str
    expected_crc32: int
    actual_crc32: int
    expected_size: int
    actual_size: int

    def describe(self) -> str:
        """One line for a log record or the extract ``.log`` sidecar."""
        return (
            f"{self.check} check failed: expected CRC-32 {self.expected_crc32:08x} "
            f"size {self.expected_size}, decoded CRC-32 {self.actual_crc32:08x} "
            f"size {self.actual_size}"
        )

    def to_json(self) -> dict[str, str | int]:
        """The JSON-safe shape the ``--resume`` control marker stores."""
        return {
            "check": self.check,
            "expected_crc32": self.expected_crc32,
            "actual_crc32": self.actual_crc32,
            "expected_size": self.expected_size,
            "actual_size": self.actual_size,
        }

    @classmethod
    def from_json(cls, data: dict) -> "IntegrityMismatch":
        """Inverse of :meth:`to_json`; a missing or mistyped field raises."""
        return cls(
            check=str(data["check"]),
            expected_crc32=int(data["expected_crc32"]),
            actual_crc32=int(data["actual_crc32"]),
            expected_size=int(data["expected_size"]),
            actual_size=int(data["actual_size"]),
        )


def mismatched_check(
    *, expected_crc32: int, actual_crc32: int, expected_size: int, actual_size: int
) -> str | None:
    """Name the failed check for a decoder that doesn't say, or ``None`` if both match."""
    crc_bad = expected_crc32 != actual_crc32
    size_bad = expected_size != actual_size
    if crc_bad and size_bad:
        return "CRC-32 and uncompressed size"
    if crc_bad:
        return "CRC-32"
    if size_bad:
        return "uncompressed size"
    return None


class Crc32Counter:
    """Running CRC-32 and byte count over the chunks passed through :meth:`tap`."""

    def __init__(self) -> None:
        self.crc32 = 0
        self.size = 0

    def tap(self, chunks: Iterable[bytes]) -> Iterator[bytes]:
        """Yield *chunks* unchanged, counting each one as it goes past."""
        for chunk in chunks:
            self.crc32 = zlib.crc32(chunk, self.crc32)
            self.size += len(chunk)
            yield chunk
