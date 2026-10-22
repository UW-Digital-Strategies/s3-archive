"""In-archive manifest primitives: tar-family / zip per-entry hashing and zip CD readers.

Consumed by downstream inventory walkers (for a single-pass inline
walk) and available for any caller that wants
per-entry triple-hashes from a streaming archive read.

These primitives are pure transducers over a byte source — they don't
talk to S3 themselves. Callers supply the byte source (an S3 GET
body, a local file handle, or a tap that mirrors bytes into a parallel
hasher). Peak memory per entry is ~one chunk regardless of member size.

Zips whose members are stored with data descriptors and no local-header
sizes (SwissTransfer / Drive-style generators) can't be walked forward-
only: :func:`build_manifest_zip_chunks` raises
:class:`~s3_archive.exceptions.ZipNotStreamableError` and the caller
retries with :func:`build_manifest_zip_seekable` over a seekable reader.
"""

import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from stream_unzip import NotStreamUnzippable

from s3_archive.constants import CHUNK_SIZE
from s3_archive.etag import is_precondition_failed
from s3_archive.exceptions import (
    ETagMismatchError,
    UnsupportedArchiveFormatError,
    ZipNotStreamableError,
)
from s3_archive.hashing import triple_hash
from s3_archive.log_config import get_logger
from s3_archive.paths import decode_zip_filename, safe_member_key
from s3_archive.seekable import open_seekable
from s3_archive.stream_decode import TAR_FAMILY_FORMATS, iter_zip_file_entries, open_tar_stream

log = get_logger(__name__)

# The public surface other code builds on: downstream inventory walkers and
# backends import these, and none of them is called from inside this repo.
__all__ = [
    "TAP_SUPPORTED_FORMATS",
    "ManifestEntry",
    "build_manifest_from_tap",
    "build_manifest_tar_fileobj",
    "build_manifest_zip_chunks",
    "build_manifest_zip_seekable",
    "zip_central_directory_from_path",
    "zip_central_directory_from_s3",
]


@dataclass
class ManifestEntry:
    key: str
    size: int
    md5: str
    sha256: str
    sha1: str = ""
    mtime: str = ""  # ISO 8601 UTC; "" if archive metadata didn't carry one


EntryObserver = Callable[[str, Iterator[bytes]], Iterator[bytes]]
"""Optional per-entry chunk hook for the manifest builders.

Called once per archive member as ``entry_observer(key, chunks)`` where
*key* is the member's final (safety-passed) manifest key and *chunks*
is the member's decoded byte stream. Must return an iterator that
yields **every byte unchanged** — the returned stream feeds the
member's triple-hash, so filtering or reordering would corrupt the
recorded hashes. Intended for byte-observation side channels (MIME
sniffing, entropy histograms, trailing-byte format checks) that want to
ride the existing decode pass instead of a second read."""


def _unix_to_iso(t: int | float) -> str:
    """Convert a Unix timestamp (tar mtime) to ISO 8601 UTC."""
    return datetime.fromtimestamp(t, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# General-purpose flag bit 11: the entry's name is stored as UTF-8
# (PKWARE APPNOTE Appendix D). Unset means CP437.
_ZIP_FLAG_UTF8 = 0x800


# Thin alias kept for existing references/tests. The canonical decoder
# (UTF-8→CP437 plus zip separator normalization) lives in
# :func:`s3_archive.paths.decode_zip_filename`; routing both the CD and
# LFH paths through it guarantees their keys move together.
_decode_zip_filename = decode_zip_filename


def _cd_mtimes_from_zipfile(zf: zipfile.ZipFile) -> dict[str, str]:
    """Extract ``{manifest_key: mtime_iso}`` from an open :class:`zipfile.ZipFile`.

    Keys must match what the LFH walk (:func:`build_manifest_zip_chunks`)
    produces from the raw name bytes, or the ``cd_mtimes.get(file_name)``
    lookup misses and the entry silently loses its mtime. stdlib decoded
    the stored bytes by flag bit 11 (UTF-8 when set, CP437 otherwise) —
    both encodings round-trip losslessly, so we recover the exact raw
    bytes from ``orig_filename`` and route them through the same
    :func:`decode_zip_filename` (UTF-8 first, CP437 fallback, separator
    normalization) the streaming walk uses. Entries whose ``date_time``
    is invalid are dropped (some archivers write zeros for "no mtime").
    """
    mtimes: dict[str, str] = {}
    for zi in zf.infolist():
        iso = _zipinfo_mtime_iso(zi)
        if not iso:
            continue
        encoding = "utf-8" if zi.flag_bits & _ZIP_FLAG_UTF8 else "cp437"
        raw_name = zi.orig_filename.encode(encoding)
        mtimes[decode_zip_filename(raw_name)] = iso
    return mtimes


def zip_central_directory_from_s3(
    client, bucket: str, key: str, size: int | None = None, *, if_match: str | None = None
) -> dict[str, str]:
    """Read a zip file's central directory from S3; return {filename: mtime_iso}.

    Stdlib ``zipfile`` parses the directory over
    :func:`~s3_archive.seekable.open_seekable` (ranged GETs + tail
    prefetch), so every layout stdlib can read works here — Zip64
    archives (>4 GiB or >65535 entries), central directories of any
    size, trailing comments. Typical cost is one ranged GET (the tail
    prefetch covers the whole directory); a directory larger than the
    prefetch costs a few more. When *size* is provided, the HEAD
    round-trip is skipped — pass it when you've already fetched the
    LIST entry.

    When *if_match* is provided (quoted or quote-stripped ETag), every
    read is pinned to that object generation, and a failed precondition
    RAISES instead of falling back — a 412 ``ClientError`` from a
    pinned GET, or :class:`~s3_archive.exceptions.ETagMismatchError`
    from the constructor HEAD when *size* wasn't provided. The object
    changed since it was listed, which the caller must treat as a
    changed entry, not a lost mtime.

    Returns an empty dict on any other failure — truncated or non-zip
    bytes, S3 errors — with a WARNING logged; callers fall back to
    empty mtime per entry. The walk itself doesn't depend on this; we
    just lose mtime for that archive's rows.
    """
    try:
        with zipfile.ZipFile(
            open_seekable(client, bucket, key, if_match=if_match, size=size)
        ) as zf:
            return _cd_mtimes_from_zipfile(zf)
    except Exception as exc:  # best-effort; fall back to empty
        if isinstance(exc, ETagMismatchError) or is_precondition_failed(exc):
            # The object changed between LIST and this read. That's not
            # a "lost mtime" — the caller's whole view of the entry is
            # stale. Fail fast rather than silently degrade.
            raise
        log.warning("zip central directory parse failed for %s: %s", key, exc)
        return {}


def zip_central_directory_from_path(path: Path) -> dict[str, str]:
    """Read a local zip file's central directory; return {filename: mtime_iso}.

    Stdlib ``zipfile`` on a seekable file — no Range-GET dance, and the
    same Zip64 / any-size-directory coverage as
    :func:`zip_central_directory_from_s3`. Returns an empty dict with a
    WARNING logged on parse failure.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            return _cd_mtimes_from_zipfile(zf)
    except Exception as exc:  # noqa: BLE001 — best-effort; fall back to empty
        log.warning("zip central directory parse failed for %s: %s", path, exc)
        return {}


def _entry_from_chunks(
    key: str,
    chunks: Iterator[bytes],
    mtime: str,
    entry_observer: EntryObserver | None,
) -> ManifestEntry:
    """Hash one member's chunk stream into a :class:`ManifestEntry`.

    The shared tail of all three builders: apply *entry_observer* (when
    given), triple-hash the resulting stream, and attach *key* and the
    per-format *mtime*. Peak memory is one chunk — entry contents are
    never buffered, which is what makes multi-GB members walkable.

    Callers own the chunk source's lifetime: a builder reading from a
    context manager (``zf.open(zi)``) keeps the ``with`` at the call
    site so the stream is fully consumed before it closes.
    """
    if entry_observer is not None:
        chunks = entry_observer(key, chunks)
    hashes = triple_hash(chunks)
    return ManifestEntry(
        key=key,
        size=hashes.size,
        md5=hashes.md5,
        sha1=hashes.sha1,
        sha256=hashes.sha256,
        mtime=mtime,
    )


def _iter_tar_member(fileobj) -> "Iterator[bytes]":
    """Yield bytes chunks from a tarfile member fileobj."""
    while True:
        chunk = fileobj.read(CHUNK_SIZE)
        if not chunk:
            break
        yield chunk


def _zipinfo_mtime_iso(zi: zipfile.ZipInfo) -> str:
    """Convert a ZipInfo's date_time tuple to ISO 8601 UTC; "" if invalid.

    ``ZipInfo.date_time`` is ``(year, month, day, hour, minute, second)``.
    Many archivers write zeros there for entries with no mtime; treat
    those as "no mtime" rather than 1980-01-00 (an invalid date).
    """
    year, month, day, hour, minute, second = zi.date_time
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except ValueError:
        return ""


def build_manifest_tar_fileobj(
    fileobj, archive_format: str, *, entry_observer: EntryObserver | None = None
) -> list[ManifestEntry]:
    """Stream a tar archive from any read(n)-compatible fileobj.

    *archive_format* selects the decompression layer: ``"tar"``
    (uncompressed), ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``, or
    ``"tar.zst"`` (:data:`TAR_FAMILY_FORMATS`); anything else raises
    :class:`~s3_archive.exceptions.UnsupportedArchiveFormatError` before
    any byte is read (see :func:`s3_archive.stream_decode.open_tar_stream`).

    Caller supplies the byte source — an S3 GET body, a local file
    handle, or a tap that mirrors bytes into a parallel hasher. Peak
    memory per entry is ~one chunk regardless of member size. Entry
    mtime comes from the tar header. *entry_observer*, when given,
    wraps each member's chunk stream — see :data:`EntryObserver`.
    """
    entries: list[ManifestEntry] = []
    with open_tar_stream(fileobj, archive_format) as tar:
        for member in tar:
            if not member.isfile():
                continue
            member_fileobj = tar.extractfile(member)
            if member_fileobj is None:
                continue

            key = safe_member_key(member.name)
            entries.append(
                _entry_from_chunks(
                    key,
                    _iter_tar_member(member_fileobj),
                    _unix_to_iso(member.mtime),
                    entry_observer,
                )
            )
    return entries


def build_manifest_zip_chunks(
    chunks_iter: Iterator[bytes],
    cd_mtimes: dict[str, str],
    *,
    entry_observer: EntryObserver | None = None,
) -> list[ManifestEntry]:
    """Stream a zip from a chunks iterator + a pre-fetched mtime map.

    ``stream_unzip`` doesn't surface mtime in its yield, so the caller
    is responsible for supplying the central-directory mtime lookup
    (empty dict is fine — all entries then get "" mtime). Peak memory
    per entry is ~one chunk. *entry_observer*, when given, wraps each
    member's chunk stream — see :data:`EntryObserver`; it must fully
    drain its input (stream_unzip requires each member consumed before
    the next is yielded).

    Raises :class:`ZipNotStreamableError` when a member is stored with
    a data descriptor and no size in its local file header — the zip
    isn't corrupt, just unwalkable forward-only; retry with
    :func:`build_manifest_zip_seekable`.
    """
    entries: list[ManifestEntry] = []
    try:
        # NotStreamUnzippable is raised lazily during iteration (at the
        # offending member's header parse), so the try encloses the loop.
        for file_name, _size, chunks in iter_zip_file_entries(chunks_iter):
            # mtime is keyed by the decoded (separator-normalized) name to
            # match the CD reader; the stored key gets the extra safety pass.
            key = safe_member_key(file_name)
            entries.append(
                _entry_from_chunks(key, chunks, cd_mtimes.get(file_name, ""), entry_observer)
            )
    except NotStreamUnzippable as exc:
        raise ZipNotStreamableError(decode_zip_filename(exc.args[0]), cause=exc) from exc
    return entries


def build_manifest_zip_seekable(
    fileobj,
    *,
    entry_observer: EntryObserver | None = None,
) -> list[ManifestEntry]:
    """Walk a zip via its central directory from a seekable fileobj.

    Fallback for zips :func:`build_manifest_zip_chunks` refuses with
    :class:`ZipNotStreamableError` — stored members with data
    descriptors and no local-header sizes. Stdlib ``zipfile`` reads the
    central directory (which carries the true sizes), so any well-formed
    zip walks here regardless of local-header shape.

    Like every builder in this module, it's a pure transducer — the
    caller supplies the seekable byte source. For an archive in S3 use
    :func:`s3_archive.seekable.open_seekable`::

        entries = build_manifest_zip_seekable(
            open_seekable(client, bucket, key, if_match=etag)
        )

    (not a bare ``io.BufferedReader(SeekableS3Object(...))`` — the
    BufferedReader default buffer is 8 KiB, one ranged GET per 8 KiB of
    body, ~40-70x slower than the tuned wrap on a ~100 MB archive), or
    an open local file in ``"rb"`` mode. Peak memory per entry is
    ~one chunk regardless of member size. Entry mtime comes from the
    central directory (no separate mtime map needed). *entry_observer*,
    when given, wraps each member's chunk stream — see
    :data:`EntryObserver`.

    Produces byte-identical manifests to
    :func:`build_manifest_zip_chunks` for any zip both paths can walk.
    """
    entries: list[ManifestEntry] = []
    with zipfile.ZipFile(fileobj) as zf:
        for zi in zf.infolist():
            file_name = decode_zip_filename(zi.filename)
            if file_name.endswith("/"):
                # Directory entry — skip, matching the streaming path's
                # check (a Windows-separator dir entry normalizes to "/"
                # here too, unlike ZipInfo.is_dir()).
                continue

            key = safe_member_key(file_name)
            with zf.open(zi) as member_fileobj:
                entry = _entry_from_chunks(
                    key,
                    _iter_tar_member(member_fileobj),
                    _zipinfo_mtime_iso(zi),
                    entry_observer,
                )
            entries.append(entry)
    return entries


TAP_SUPPORTED_FORMATS: tuple[str, ...] = TAR_FAMILY_FORMATS + ("zip",)
"""Formats :func:`build_manifest_from_tap` can walk from a forward-only byte tap.

``"7z"`` is intentionally absent — its decoder requires seeking to a
trailing header, incompatible with a forward-only stream. Callers
needing per-entry hashes from a ``.7z`` should drive the seekable
:func:`s3_archive.members.iter_archive_members` path instead.
"""

# 1 MiB outer-stream chunks for tap reads. Larger than CHUNK_SIZE (the
# per-member read size) because the tap drives a single byte pipe and
# benefits from amortising per-read overhead on multi-GB archives.
_TAP_READ_SIZE = 1024 * 1024


def build_manifest_from_tap(
    fmt: str,
    tap,
    cd_mtimes: dict[str, str] | None = None,
    *,
    entry_observer: EntryObserver | None = None,
) -> list[ManifestEntry]:
    """Dispatch to the tar-family or zip manifest builder by format.

    *tap* is any object exposing ``read(n) -> bytes`` over the archive's
    bytes — typically a :class:`s3_archive.hashing.HashingTap` so the
    caller can also recover the parent's triple-hash from the same pass.

    For ``"zip"``, *cd_mtimes* must be pre-fetched via
    :func:`zip_central_directory_from_s3` or
    :func:`zip_central_directory_from_path` (``{}`` is accepted; all
    entries then get ``""`` mtime). Tar formats ignore *cd_mtimes*.

    *entry_observer*, when given, wraps each member's decoded chunk
    stream — see :data:`EntryObserver`.

    Raises :class:`UnsupportedArchiveFormatError` for formats not in
    :data:`TAP_SUPPORTED_FORMATS` (notably ``"7z"``).

    For ``"zip"``, a stored-with-data-descriptor member raises
    :class:`~s3_archive.exceptions.ZipNotStreamableError` mid-walk (see
    :func:`build_manifest_zip_chunks`). At that point *tap* is only
    partially consumed — if it's a
    :class:`~s3_archive.hashing.HashingTap` being used to recover the
    parent archive's own hash from the same pass, that hash is
    **incomplete and must not be recorded**. The
    :func:`build_manifest_zip_seekable` retry reads via ranged GETs
    that bypass the tap entirely, so on that path compute the parent
    hash separately (e.g. one plain streaming read).
    """
    if fmt in TAR_FAMILY_FORMATS:
        return build_manifest_tar_fileobj(tap, fmt, entry_observer=entry_observer)
    if fmt == "zip":
        mtimes = {} if cd_mtimes is None else cd_mtimes

        def _chunks() -> Iterator[bytes]:
            while True:
                chunk = tap.read(_TAP_READ_SIZE)
                if not chunk:
                    break
                yield chunk

        return build_manifest_zip_chunks(_chunks(), mtimes, entry_observer=entry_observer)
    raise UnsupportedArchiveFormatError(
        f"Format {fmt!r} cannot be walked from a forward-only byte tap "
        f"(supported: {', '.join(TAP_SUPPORTED_FORMATS)}). "
        f"For .7z and other seek-only formats use iter_archive_members instead."
    )
