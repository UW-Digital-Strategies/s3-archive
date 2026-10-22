"""Streaming archive-member iteration across tar family + zip.

``iter_archive_members`` GETs an archive object from S3 and yields one
:class:`ArchiveMember` per file entry, in archive order, without
staging any member bytes on disk. Each member exposes a chunk
iterator the caller drives (``read_all``, ``drain``, or a custom
read loop).

Two consumption patterns are supported:

- **Capture this, drain that.** ``verify-against``-style use: collect
  bodies for the small subset of tag files in memory, drain
  multi-GB payload members without buffering them.
- **Drain every member.** Anywhere a single-pass walk needs to advance
  past each entry — e.g. tarfile-with-stream tap that wants per-entry
  hashes alongside the parent hash. The :mod:`s3_archive.manifest`
  builders handle this case directly; ``iter_archive_members`` is the
  more flexible primitive for callers that don't need the
  ``ManifestEntry`` shape.

Auto-drain on next-yield: non-seekable tar / zip streams require
in-order member consumption — advancing to the next member requires
the previous member's bytes to be consumed first. If the caller
forgets to consume a member, the iterator drains it on the caller's
behalf when the next member is requested. This is safer than
"document loudly that the caller must drain" — a forgotten drain
corrupts the next member's bytes.
"""

import contextlib
import tarfile
import zipfile
from collections.abc import Callable, Generator, Iterable, Iterator
from dataclasses import dataclass, field

from stream_unzip import (
    CompressedSizeIntegrityError,
    CRC32IntegrityError,
    NotStreamUnzippable,
    UncompressedSizeIntegrityError,
    UnzipError,
)

from s3_archive.constants import CHUNK_SIZE
from s3_archive.exceptions import ArchiveReadError, UnsupportedArchiveFormatError
from s3_archive.integrity import Crc32Counter, IntegrityMismatch
from s3_archive.iter import IterableFileobj
from s3_archive.log_config import get_logger
from s3_archive.paths import decode_zip_filename, safe_member_key
from s3_archive.retry import (
    DEFAULT_RETRY_DELAY_S,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    resumable_body_chunks,
)
from s3_archive.stream_decode import TAR_FAMILY_FORMATS, iter_zip_file_entries, open_tar_stream

log = get_logger(__name__)

# The decoder-native "these bytes are bad" exceptions, each paired with the
# message prefix naming which decoder noticed. `iter_archive_members` catches
# the whole set in one handler and re-raises ArchiveReadError, so every caller
# sees a single exception type while the operator-facing message stays
# specific. A new decoder is a new row here, not a new handler. Matched in
# order, so a subclass must precede its base.
_DECODE_ERROR_PREFIXES: tuple[tuple[type[Exception], str], ...] = (
    (tarfile.TarError, "tar decode failed"),
    (UnzipError, "zip decode failed"),
    # The seekable continuation opens the central directory with stdlib
    # zipfile; a zip that both fails to stream AND has no usable CD is
    # genuinely unreadable.
    (zipfile.BadZipFile, "zip central directory read failed"),
)

_DECODE_ERRORS = tuple(kind for kind, _ in _DECODE_ERROR_PREFIXES)

# Every format string iter_archive_members decodes.
ARCHIVE_FORMATS: tuple[str, ...] = (*TAR_FAMILY_FORMATS, "zip", "7z")


def check_archive_format(fmt: str) -> None:
    """Raise :class:`UnsupportedArchiveFormatError` unless *fmt* is in :data:`ARCHIVE_FORMATS`."""
    if fmt not in ARCHIVE_FORMATS:
        raise UnsupportedArchiveFormatError(f"Unsupported format: {fmt!r}")


# stream_unzip's per-member integrity failures, each with the check it names.
# All are raised *after* the member's last decoded byte has been yielded, so
# a tolerant walk has already handed on everything that decodes. HMAC (AES
# authentication) is deliberately absent: encrypted zips are unsupported.
_STREAM_INTEGRITY_CHECKS: dict[type[Exception], str] = {
    CRC32IntegrityError: "CRC-32",
    UncompressedSizeIntegrityError: "uncompressed size",
    CompressedSizeIntegrityError: "compressed size",
}
_STREAM_INTEGRITY_ERRORS = tuple(_STREAM_INTEGRITY_CHECKS)


@dataclass
class ArchiveMember:
    """One file entry inside an archive.

    *name* is the member's stored path after normalization — separators
    forward-slashed and Windows drive prefixes stripped for zip
    (:func:`s3_archive.paths.decode_zip_filename`), leading ``/`` and
    ``..`` traversal segments handled for every format
    (:func:`s3_archive.paths.safe_member_key`). *size* is the
    archive's declared size; for streaming zips this is the size from
    the local file header and may be 0 for ``stream_unzip`` entries
    that don't carry it. The caller should hash the bytes if it needs
    the actual size.

    *integrity* is ``None`` until the member's bytes have been consumed
    to the end. It is then set only when the iterator was opened with
    ``tolerate_integrity_errors=True`` and the decoded bytes failed the
    archive's own CRC-32 / size check — every decoded byte has still been
    handed on (see :mod:`s3_archive.integrity`). Under the strict
    default the same failure raises instead, and *integrity* stays
    ``None``.

    Pull chunks via :meth:`chunks`, :meth:`read_all`, or :meth:`drain`.
    Each consumes the underlying iterator at most once; a second call
    yields no bytes (and is a no-op for :meth:`drain`).
    """

    name: str
    size: int
    # A plain iterator, or a generator whose return value is the member's
    # integrity mismatch (``None`` when clean) — see :meth:`chunks`.
    _chunks: Iterator[bytes] | Generator[bytes, None, IntegrityMismatch | None] = field(repr=False)
    _consumed: bool = field(default=False, repr=False)
    integrity: IntegrityMismatch | None = None

    def chunks(self) -> Iterator[bytes]:
        """Yield this member's bytes as chunks.

        Idempotent in that calling :meth:`read_all` / :meth:`drain` /
        a second :meth:`chunks` after exhaustion produces no bytes.
        Records the chunk source's integrity mismatch, if it returns one,
        on :attr:`integrity` once the last chunk has been handed on.
        """
        if self._consumed:
            return
        self._consumed = True
        mismatch = yield from self._chunks
        if mismatch is not None:
            self.integrity = mismatch

    def read_all(self) -> bytes:
        """Concatenate all of this member's bytes and return them.

        Use only for files that fit comfortably in memory — e.g. BagIt
        tag files. Payload-shaped members should use :meth:`drain` or
        a custom :meth:`chunks` loop.
        """
        return b"".join(self.chunks())

    def drain(self) -> None:
        """Consume any remaining bytes without buffering them."""
        for _ in self.chunks():
            pass


def _iter_tar_chunks(fobj) -> Iterator[bytes]:
    while True:
        chunk = fobj.read(CHUNK_SIZE)
        if not chunk:
            break
        yield chunk


def _iter_tar_members(fileobj, archive_format: str) -> Iterator[ArchiveMember]:
    """Yield :class:`ArchiveMember` for each regular-file tar member.

    Auto-drains a member when the caller advances without consuming
    it. Tarfile in streaming mode (``r|...``) demands the previous
    member's bytes be consumed before the iterator advances — otherwise
    advancing tries to seek backwards through a non-seekable source —
    so the drain MUST happen before ``next()`` on the tarfile iterator.
    """
    with open_tar_stream(fileobj, archive_format) as tar:
        tar_iter = iter(tar)
        previous: ArchiveMember | None = None
        while True:
            if previous is not None:
                previous.drain()
                previous = None
            try:
                tar_member = next(tar_iter)
            except StopIteration:
                return
            if not tar_member.isfile():
                continue
            member_fileobj = tar.extractfile(tar_member)
            if member_fileobj is None:
                continue
            member = ArchiveMember(
                name=tar_member.name,
                size=tar_member.size,
                _chunks=_iter_tar_chunks(member_fileobj),
            )
            previous = member
            yield member


class _ZipContinuation:
    """The seekable central-directory side of a streaming zip walk, opened on first need.

    A streaming walk needs the central directory for two things, both
    mid-walk: the handoff when the stream can't go on (an unstreamable
    member, or a member that failed its integrity check under tolerance),
    and the expected CRC-32 / size of that failed member. One ranged-GET
    view serves both, opened at most once per walk and never for a zip
    that streams clean.
    """

    def __init__(self, client, bucket: str, key: str) -> None:
        self._client = client
        self._bucket = bucket
        self._key = key
        self._fileobj = None
        self._opened: tuple[zipfile.ZipFile, list[zipfile.ZipInfo]] | None = None

    def _open(self) -> tuple[zipfile.ZipFile, list[zipfile.ZipInfo]]:
        """Open the central directory (raising :class:`zipfile.BadZipFile` if unusable)."""
        if self._opened is None:
            # Lazy import: seekable depends on this module (ArchiveMember).
            from s3_archive.seekable import (  # noqa: PLC0415
                open_seekable,
                zip_entries_in_offset_order,
            )

            self._fileobj = open_seekable(self._client, self._bucket, self._key)
            zf = zipfile.ZipFile(self._fileobj)
            self._opened = (zf, zip_entries_in_offset_order(zf))
        return self._opened

    def entry(self, index: int) -> zipfile.ZipInfo:
        """The offset-ordered central-directory entry *index* (raw, directories counted)."""
        _zf, entries = self._open()
        if index >= len(entries):
            raise ArchiveReadError(
                f"zip stream reached entry {index}, but the central directory "
                f"lists only {len(entries)} entries"
            )
        return entries[index]

    def members_from(self, start_entry: int, *, tolerate: bool) -> Iterator[ArchiveMember]:
        """Yield the members from offset-ordered entry *start_entry* onward."""
        from s3_archive.seekable import iter_zip_entries  # noqa: PLC0415

        zf, entries = self._open()
        yield from iter_zip_entries(zf, entries[start_entry:], tolerate_integrity_errors=tolerate)

    def close(self) -> None:
        """Release the ranged-GET view, if one was opened."""
        if self._opened is not None:
            self._opened[0].close()
        if self._fileobj is not None:
            self._fileobj.close()


class _StreamingZipWalk:
    """One forward-only ``stream_unzip`` walk, yielding :class:`ArchiveMember` per file entry.

    Auto-drains the previous entry before advancing — ``stream_unzip``
    raises ``UnfinishedIterationError`` if its consumer doesn't drain
    each entry's chunk iterator before pulling the next.

    :attr:`entries_consumed` is a one-element list whose slot is kept
    equal to the number of raw entries (directory entries included) whose
    header parse succeeded (the bookkeeping lives in
    :func:`s3_archive.stream_decode.iter_zip_file_entries`). Because
    advancing drains the previous entry first, at the moment an advance
    *raises* the counter equals the number of fully-consumed entries —
    which is the offset-ordered index of the offending entry, i.e.
    exactly the ``start_entry`` the seekable continuation should resume
    from.

    Under *tolerate*, a member that fails ``stream_unzip``'s CRC-32 / size
    check has already had every decoded byte yielded (the check runs after
    the last one). Its chunk generator returns the mismatch — expected
    values from the central directory entry it occupies — and the walk
    stops there with :attr:`ended_early` set, since ``stream_unzip``
    cannot go on past a raise. The counter then already includes the
    failed member, so it is again exactly where the continuation resumes:
    the failed member is not re-read.
    """

    def __init__(self, chunks: Iterable[bytes], continuation: _ZipContinuation, *, tolerate: bool):
        self._chunks = chunks
        self._continuation = continuation
        self._tolerate = tolerate
        self.entries_consumed = [0]
        self.ended_early = False

    def members(self) -> Iterator[ArchiveMember]:
        zip_iter = iter_zip_file_entries(self._chunks, entry_counter=self.entries_consumed)
        previous: ArchiveMember | None = None
        while True:
            if previous is not None:
                previous.drain()
                previous = None
            if self.ended_early:
                return
            try:
                file_name, _size, chunks = next(zip_iter)
            except StopIteration:
                return
            size = _size if isinstance(_size, int) else 0
            member = ArchiveMember(
                name=file_name,
                size=size,
                _chunks=self._checked(file_name, chunks) if self._tolerate else iter(chunks),
            )
            previous = member
            yield member

    def _checked(
        self, name: str, chunks: Iterable[bytes]
    ) -> Generator[bytes, None, IntegrityMismatch | None]:
        counter = Crc32Counter()
        try:
            yield from counter.tap(chunks)
        except _STREAM_INTEGRITY_ERRORS as exc:
            self.ended_early = True
            check = _STREAM_INTEGRITY_CHECKS[type(exc)]
            try:
                info = self._continuation.entry(self.entries_consumed[0] - 1)
            except zipfile.BadZipFile as cd_exc:
                raise ArchiveReadError(
                    f"zip member {name!r} failed its {check} check, and the central "
                    f"directory needed to continue past it is unreadable: {cd_exc}",
                    cause=cd_exc,
                ) from cd_exc
            return IntegrityMismatch(
                check=check,
                expected_crc32=info.CRC,
                actual_crc32=counter.crc32,
                expected_size=info.file_size,
                actual_size=counter.size,
            )
        return None


def _iter_zip_members_with_fallback(
    client, bucket: str, key: str, chunks, *, tolerate_integrity_errors: bool
) -> Iterator[ArchiveMember]:
    """Stream-walk a zip; where the stream can't go on, continue via ranged GETs.

    Some valid zips can't be walked forward-only: on-the-fly generators
    (SwissTransfer, Google Drive) write stored members with data
    descriptors and no sizes in the local file headers (see
    :class:`s3_archive.exceptions.ZipNotStreamableError`). Streaming is
    still attempted first — it's a single sequential GET, optimal for
    every normal zip — and on the raise this hands off to the seekable
    central-directory walk *from the exact failure point*: the streaming
    walk visits entries in local-header offset order, so "N raw entries
    fully consumed" resumes at offset-ordered CD index N. Members
    already yielded are never re-yielded; total transfer stays ~one
    archive read even when the raise happens deep into a mixed zip.

    Under *tolerate_integrity_errors* the same handoff follows a member
    that failed its integrity check (see :class:`_StreamingZipWalk`),
    from the entry after it.

    The continuation is still streaming in the sense that matters —
    bounded memory, ranged GETs, nothing on local disk.
    """
    continuation = _ZipContinuation(client, bucket, key)
    walk = _StreamingZipWalk(chunks, continuation, tolerate=tolerate_integrity_errors)
    try:
        try:
            yield from walk.members()
        except NotStreamUnzippable as exc:
            log.info(
                "zip member %r is stored with a data descriptor and no size in its "
                "local header (not forward-streamable; the zip is not corrupt) — "
                "continuing from entry %d via the central directory over ranged GETs",
                decode_zip_filename(exc.args[0]),
                walk.entries_consumed[0],
            )
        else:
            if not walk.ended_early:
                return
            log.info(
                "zip stream stopped after a member failed its integrity check — "
                "continuing from entry %d via the central directory over ranged GETs",
                walk.entries_consumed[0],
            )
        # Reached only via a handoff: release the abandoned sequential GET,
        # then continue. (Kept outside the handler so a failure in the
        # continuation isn't misreported as caused by the expected trigger.)
        with contextlib.suppress(Exception):
            chunks.close()
        yield from continuation.members_from(
            walk.entries_consumed[0], tolerate=tolerate_integrity_errors
        )
    finally:
        continuation.close()


def _apply_safe_keys(
    members: Iterator[ArchiveMember], *, fix_unsafe_paths: bool
) -> Iterator[ArchiveMember]:
    """Rewrite each member's ``name`` into a safe relative S3 key as it's yielded.

    The single chokepoint that covers tar, zip, and 7z — and every
    external caller (s3-bagit, and other library users). Raises
    :class:`s3_archive.exceptions.UnsafeArchiveMemberError` when a member
    has a ``..`` segment and *fix_unsafe_paths* is false; because the
    walk is single-pass, that's raised when the member is reached, after
    earlier members have already been yielded.
    """
    for member in members:
        member.name = safe_member_key(member.name, fix_unsafe=fix_unsafe_paths)
        yield member


def iter_archive_members(
    client,
    bucket: str,
    key: str,
    fmt: str,
    *,
    fix_unsafe_paths: bool = False,
    tolerate_integrity_errors: bool = False,
    retry_delay_s: float = DEFAULT_RETRY_DELAY_S,
    retry_max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS,
    on_bytes: Callable[[int], object] | None = None,
) -> Iterator[ArchiveMember]:
    """GET ``s3://bucket/key`` and yield one :class:`ArchiveMember` per file entry.

    *fmt* is one of the strings returned by :func:`s3_archive.url.detect_format`:
    ``"tar"``, ``"tar.gz"``, ``"tar.bz2"``, ``"tar.xz"``, ``"tar.zst"``,
    ``"zip"``, or ``"7z"``.

    The body is streamed — no full-archive download. Members are
    yielded in archive order; the caller drives consumption per-member
    via the :class:`ArchiveMember` API. Forgotten members are
    auto-drained when the next one is requested.

    Every yielded member's ``name`` is normalized into a safe relative
    S3 key (see :func:`s3_archive.paths.safe_member_key`). A ``..``
    traversal segment raises
    :class:`s3_archive.exceptions.UnsafeArchiveMemberError` by default;
    pass *fix_unsafe_paths=True* to safely collapse it instead.

    For the sequential tar/zip path, the underlying byte stream is
    resumable: a transient connection drop is retried via a ranged GET
    from the byte offset already consumed (up to ``retry_max_attempts``
    *consecutive* failures, with exponential backoff between tries) —
    see :func:`s3_archive.retry.resumable_body_chunks`. *on_bytes*, if
    supplied, is called with the length of each archive chunk read from
    S3 (compressed bytes of forward progress), suitable for a
    ``tqdm.update`` bar sized against the archive's ``ContentLength``.
    ``"7z"`` cannot be decoded forward-only and uses a seekable-S3
    adapter with its own equivalent ranged-GET retry (and does not report
    *on_bytes*) — see :mod:`s3_archive.seven_z`.

    Zips that a forward-only reader can't walk (stored members with
    data descriptors and no local-header sizes — SwissTransfer /
    Drive-style generators) are handled transparently: the walk starts
    streaming and, at the first unstreamable member, continues from
    that exact entry via a ranged-GET central-directory walk (see
    :func:`_iter_zip_members_with_fallback`). No member is re-yielded
    and total transfer stays ~one archive read; like the 7z path, the
    continuation does not report *on_bytes*.

    *tolerate_integrity_errors* (default off) changes what a zip or 7z
    member that fails its CRC-32 / size check does. Off, the walk raises, as it
    always has. On, every decoded byte of that member is still handed on,
    the mismatch is recorded on :attr:`ArchiveMember.integrity` once the
    member is consumed, and the walk continues with the next member —
    Info-ZIP ``unzip`` behavior. On a streaming walk the stream cannot go
    on past the failure, so it hands off to the same ranged-GET
    central-directory continuation, from the entry after the failed one.
    For 7z see :func:`s3_archive.seven_z.iter_seven_z_members`. Tar-family
    formats have no per-member checksum, so the flag changes nothing for
    them: a stream-level failure (truncation, a gzip / xz CRC) is always
    fatal.
    """
    check_archive_format(fmt)

    if fmt == "7z":
        # Lazy import: seven_z depends on this module (ArchiveMember), so
        # eager-importing it at the top of this file would be circular.
        from s3_archive.seven_z import iter_seven_z_members  # noqa: PLC0415

        yield from _apply_safe_keys(
            iter_seven_z_members(
                client, bucket, key, tolerate_integrity_errors=tolerate_integrity_errors
            ),
            fix_unsafe_paths=fix_unsafe_paths,
        )
        return

    chunks = resumable_body_chunks(
        client,
        bucket,
        key,
        retry_delay_s=retry_delay_s,
        retry_max_attempts=retry_max_attempts,
        on_bytes=on_bytes,
    )

    # Wrap the decoder-native exceptions in ArchiveReadError so every
    # caller of iter_archive_members sees one exception type for "the
    # archive bytes are bad," regardless of which decoder noticed.
    try:
        if fmt in TAR_FAMILY_FORMATS:
            # IterableFileobj re-exposes the chunk stream as a sized-read()
            # file object for tarfile.open(fileobj=…) (and the tar.zst
            # zstandard stream_reader wrapper).
            yield from _apply_safe_keys(
                _iter_tar_members(IterableFileobj(chunks), fmt),
                fix_unsafe_paths=fix_unsafe_paths,
            )
            return
        yield from _apply_safe_keys(
            _iter_zip_members_with_fallback(
                client, bucket, key, chunks, tolerate_integrity_errors=tolerate_integrity_errors
            ),
            fix_unsafe_paths=fix_unsafe_paths,
        )
    except _DECODE_ERRORS as exc:
        prefix = next(p for kind, p in _DECODE_ERROR_PREFIXES if isinstance(exc, kind))
        raise ArchiveReadError(f"{prefix}: {exc}", cause=exc) from exc
