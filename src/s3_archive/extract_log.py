"""The ``<dir>.log`` sidecar every extract writes beside its destination folder.

Extracting to ``s3://bucket/Iraq/foo/`` writes ``s3://bucket/Iraq/foo.log``:
a plain-text provenance record of the run — source URL and ETag, run times,
member counts — plus one line per member written as decoded despite
failing the archive's own integrity check. It sits *beside* the folder, not
inside it, so the folder stays a faithful mirror of the archive, and it is
overwritten on every run, so a stale warning cannot outlive a clean re-run.

This module is the pure half: the key and the text. :mod:`s3_archive.extract`
writes it.
"""

from dataclasses import dataclass
from datetime import datetime

from s3_archive.integrity import IntegrityMismatch


def log_key(dest_prefix: str) -> str | None:
    """The sidecar's key for *dest_prefix*, or ``None`` at the bucket root.

    The destination prefix without its trailing slash, plus ``.log``. A
    bucket-root destination has no folder name to derive one from.
    """
    folder = dest_prefix.rstrip("/")
    if not folder:
        return None
    return folder + ".log"


@dataclass(frozen=True)
class ExtractLogRecord:
    """Everything the sidecar says about one extract run."""

    version: str
    source_url: str
    source_etag: str
    fmt: str
    destination_url: str
    resumed: bool
    started: datetime
    finished: datetime
    written: int
    already_present: int
    integrity_warnings: dict[str, IntegrityMismatch]


def render(record: ExtractLogRecord) -> str:
    """The sidecar's text: a header of ``key: value`` lines, then one line per warning."""
    lines = [
        f"s3-archive extract log (s3-archive {record.version})",
        f"source: {record.source_url}",
        f"source-etag: {record.source_etag}",
        f"format: {record.fmt}",
        f"destination: {record.destination_url}",
        f"resumed: {'yes' if record.resumed else 'no'}",
        f"started: {record.started.isoformat()}",
        f"finished: {record.finished.isoformat()}",
        f"members-written-this-run: {record.written}",
        f"members-already-present: {record.already_present}",
        f"integrity-warnings: {len(record.integrity_warnings)}",
    ]
    if record.integrity_warnings:
        lines.append("")
        lines.extend(
            f"WARNING {name}: {mismatch.describe()}; written as decoded"
            for name, mismatch in record.integrity_warnings.items()
        )
    return "\n".join(lines) + "\n"
