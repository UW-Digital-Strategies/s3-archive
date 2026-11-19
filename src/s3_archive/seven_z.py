"""Streaming .7z read support.

``.7z`` cannot be decoded forward-only — the StartHeader at the front
points at metadata at the tail, and the body decoder pipeline lives in
that tail header. So unlike tar/zip, this module needs a *seekable*
view of the archive. :class:`SeekableS3Object` provides one over
``client.get_object(Range=...)``, with a single tail prefetch so the
header parse doesn't burn dozens of small range GETs.

`.7z` create is intentionally not supported: the SignatureHeader
references a NextHeaderOffset/Size/CRC only known after the body and
trailing header are written, and S3 multipart's 5 MB minimum part size
makes "patch the first 32 bytes at the end" impractical. See
``docs/ARCHITECTURE.md`` § ".7z — the exception that proves the rule".

The iterator bridges py7zr's push-style ``WriterFactory`` API onto the
project's pull-style :class:`ArchiveMember` contract by spawning a
worker thread that drives ``SevenZipFile.extractall`` and writes each
member's bytes into a per-member ``os.pipe()``. The main generator
pulls metadata off a queue and yields one ``ArchiveMember`` per pipe.
"""

from __future__ import annotations

import contextlib
import lzma
import os
import queue
import struct
import threading
from collections.abc import Collection, Generator, Iterator
from dataclasses import dataclass

import py7zr
import py7zr.compressor
from py7zr.exceptions import ArchiveError, UnsupportedCompressionMethodError
from py7zr.io import Py7zIO, WriterFactory

from s3_archive.constants import CHUNK_SIZE
from s3_archive.exceptions import ArchiveReadError
from s3_archive.integrity import Crc32Counter, IntegrityMismatch
from s3_archive.log_config import get_logger
from s3_archive.members import ArchiveMember
from s3_archive.native_decoders import build_native_decoder

# SeekableS3Object (the ranged-GET file-object adapter) now lives in its
# own module so the zip/tar ``--resume`` path can reuse it without
# dragging py7zr — and this module's load-time monkeypatch — into an
# import that only wants zip/tar. ``open_seekable`` is its canonical
# buffered constructor (tuned BufferedReader size).
from s3_archive.seekable import open_seekable

log = get_logger(__name__)


# Sentinel attribute on the patched method so re-imports don't double-wrap.
_PATCH_MARKER = "_s3_archive_native_decoder_patched"


def _install_native_decoder_fallback() -> None:
    """Wrap py7zr's ``_get_lzma_decompressor`` with a native-decoder fallback.

    py7zr translates 7z coder chains into a stdlib ``lzma.LZMADecompressor``
    in ``FORMAT_RAW`` mode, which requires the chain to end with an
    LZMA1/LZMA2 filter. Real-world archives produced by 7-Zip with
    ``-mx=0 -mf=Delta:2`` violate that — Copy + Delta has no terminal
    compression filter — so the call raises ``_lzma.LZMAError`` before
    any byte is decoded. The patch tries py7zr's path first (so the
    common LZMA-terminated case is unchanged), and on either an
    ``LZMAError`` or an ``UnsupportedCompressionMethodError`` it asks
    :func:`s3_archive.native_decoders.build_native_decoder` to construct
    a replacement chain. If we can't build one either, the original
    exception is re-raised so the operator sees the underlying reason.

    Applied module-level: importing :mod:`s3_archive.seven_z` patches
    py7zr globally for this process. The patch is idempotent (a marker
    attribute prevents double-wrapping on re-import).
    """
    cls = py7zr.compressor.SevenZipDecompressor
    orig = cls._get_lzma_decompressor
    if getattr(orig, _PATCH_MARKER, False):
        return

    def _get_lzma_decompressor_with_fallback(self, coders, unpacksize):
        try:
            return orig(self, coders, unpacksize)
        except (lzma.LZMAError, UnsupportedCompressionMethodError):
            chain = build_native_decoder(coders)
            if chain is None:
                # We can't help — let py7zr's original exception propagate
                # so the operator sees stdlib lzma's actual complaint.
                raise
            log.debug(
                "Using s3_archive native decoder for coder chain %r "
                "(py7zr/stdlib-lzma rejected it)",
                [c.get("method") for c in coders],
            )
            return chain

    setattr(_get_lzma_decompressor_with_fallback, _PATCH_MARKER, True)
    # py7zr declares this private method as returning its two concrete lzma
    # decompressors; returning _NativeChain for coder chains stdlib lzma
    # rejects *is* the patch, so no annotation can make the assignment type-check.
    cls._get_lzma_decompressor = _get_lzma_decompressor_with_fallback  # pyright: ignore[reportAttributeAccessIssue]


_install_native_decoder_fallback()


def _install_corrupt_body_guards() -> None:
    """Make py7zr's per-Folder decoder fail loudly on a corrupt compressed body.

    Two wraps on :class:`py7zr.compressor.SevenZipDecompressor`, both
    raising :class:`ArchiveReadError` from the worker thread (which
    :func:`_reraise_worker_error` passes through unchanged):

    - ``_decompress(data, max_length)`` runs the codec chain over bytes
      already in memory — the packed-stream read is ``_read_data(fp)``, a
      separate method. So any exception it raises is the codec rejecting
      the body, whatever the codec: ``LZMAError``, ``zlib.error``, bz2's
      bare ``OSError``, inflate64's ``ValueError``, ``ZstdError``,
      ``brotli.error``, py7zr's own ``EOFError`` on overlong output. A
      real I/O failure on the S3 read raises from ``_read_data`` and never
      passes through here. ``MemoryError`` is left alone — a genuine
      allocation failure must not be reported as a bad archive — except
      from a PPMd chain: pyppmd allocates its model when the decoder is
      built (``Ppmd7_Alloc`` in ``Ppmd7Decoder.__init__``), leaving only
      chunk-sized buffers to ``decode``, and a corrupt PPMd body raises a
      bare ``MemoryError`` there. (It can also segfault the process, which
      nothing in-process can catch — OPERATIONS.md, "Failure modes".)
    - ``decompress(fp, max_length)`` is wrapped to detect a stall. py7zr's
      callers loop ``while remaining > 0`` on it; a corrupt Deflate body
      can end the stream early, after which every call returns ``b""``
      and the loop spins at 100% CPU forever. An empty return with the
      Folder's packed bytes all consumed and nothing buffered can never
      be followed by output, so the first one raises.

    Applied module-level and idempotent, like
    :func:`_install_native_decoder_fallback`.
    """
    cls = py7zr.compressor.SevenZipDecompressor
    orig_decompress_chain = cls._decompress
    if getattr(orig_decompress_chain, _PATCH_MARKER, False):
        return
    orig_decompress = cls.decompress

    def _decompress_or_raise_corrupt(self, data, max_length: int):
        try:
            return orig_decompress_chain(self, data, max_length)
        except MemoryError as exc:
            if not any(isinstance(d, py7zr.compressor.PpmdDecompressor) for d in self.chain):
                raise
            raise ArchiveReadError(
                "7z decode failed (corrupt compressed body): PPMd decoder raised MemoryError",
                cause=exc,
            ) from exc
        except Exception as exc:
            raise ArchiveReadError(
                f"7z decode failed (corrupt compressed body): {type(exc).__name__}: {exc}",
                cause=exc,
            ) from exc

    def _decompress_or_raise_stalled(self, fp, max_length: int = -1) -> bytes:
        out = orig_decompress(self, fp, max_length)
        if (
            not out
            and max_length != 0
            and self.consumed >= self.input_size
            and not self._unused
            and self._pos >= len(self._buf)
        ):
            raise ArchiveReadError(
                f"7z decode stalled (corrupt compressed body): all {self.input_size} "
                "packed bytes of the Folder are consumed and the decoder yields no "
                "more output"
            )
        return out

    setattr(_decompress_or_raise_corrupt, _PATCH_MARKER, True)
    cls._decompress = _decompress_or_raise_corrupt
    cls.decompress = _decompress_or_raise_stalled


_install_corrupt_body_guards()


class _PipeSink(Py7zIO):
    """Py7zIO that forwards writes to an ``os.pipe()`` write-end.

    py7zr's contract here is narrow:

    - ``write(chunk)`` is called for each decoded chunk of one member.
    - ``seek(0, 0)`` is called once after writing finishes (py7zr
      computes CRC inline during ``decompress``; the seek isn't a real
      seek, it's just leftover from the BytesIO-style interface). We
      no-op it — the pipe consumer has already moved on.
    - ``read`` / ``flush`` / ``size`` are required by the ABC but the
      decode path doesn't read back what it wrote.

    The pipe write-end is closed by the :class:`_WriterFactory`, not by
    this class — see :meth:`_WriterFactory.create`.
    """

    def __init__(self, write_fd: int) -> None:
        self._write_fd = write_fd
        self._size = 0
        self._closed = False

    def write(self, s: bytes | bytearray) -> int:
        data = bytes(s)
        os.write(self._write_fd, data)
        self._size += len(data)
        return len(data)

    def read(self, size: int | None = None) -> bytes:  # noqa: ARG002
        return b""

    def seek(self, offset: int, whence: int = 0) -> int:  # noqa: ARG002
        return 0

    def flush(self) -> None:
        pass

    def size(self) -> int:
        return self._size

    def close_write_end(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(OSError):
            os.close(self._write_fd)


# Sentinel value pushed onto the metadata queue by the worker thread when
# extractall has finished (or raised). The main generator uses identity
# comparison to distinguish it from real ``(filename, read_fd)`` tuples.
_SENTINEL = object()


class _WriterFactory(WriterFactory):
    """Build one :class:`_PipeSink` per archive member.

    py7zr calls ``create(filename)`` exactly once per non-directory
    member in archive order. For each call we make a fresh ``os.pipe()``,
    push ``(filename, read_fd)`` onto the metadata queue, and return a
    sink that writes into the write-end. The previous member's pipe is
    closed at that point so its consumer sees EOF — py7zr does not call
    any ``close`` method on the Py7zIO, so this is the only signal we
    have that the previous member is done.

    Empty members are handled identically: ``create`` is called, no
    ``write`` arrives, and the next ``create`` (or the worker's
    ``finally``) closes the write-end → consumer sees an immediate EOF.
    """

    def __init__(self, meta_queue: queue.Queue) -> None:
        self._queue = meta_queue
        self._current: _PipeSink | None = None

    def create(self, filename: str) -> Py7zIO:
        if self._current is not None:
            self._current.close_write_end()
        read_fd, write_fd = os.pipe()
        sink = _PipeSink(write_fd)
        self._current = sink
        self._queue.put((filename, read_fd))
        return sink

    def close_current(self) -> None:
        if self._current is not None:
            self._current.close_write_end()


def _pipe_chunks(read_fd: int) -> Iterator[bytes]:
    """Yield chunks from *read_fd* until EOF.

    Does NOT close the fd — :func:`iter_seven_z_members` owns the read-fd
    lifecycle so that exactly one party closes each pipe end. Closing it
    in both this generator's ``finally`` and in the main loop's
    auto-drain creates an fd-reuse race: between the two closes,
    ``os.pipe()`` for the next member can hand back the same integer,
    and the second close shuts the wrong pipe.
    """
    while True:
        chunk = os.read(read_fd, CHUNK_SIZE)
        if not chunk:
            return
        yield chunk


def _checked_pipe_chunks(
    read_fd: int, *, expected_crc32: int, size: int
) -> Generator[bytes, None, IntegrityMismatch | None]:
    """:func:`_pipe_chunks`, returning the member's CRC-32 mismatch if it has one.

    The tolerant counterpart of py7zr's own per-member check, which
    :func:`_take_member_crcs` disabled. py7zr decodes exactly the declared
    uncompressed size (a decoder that runs dry first is a stall, raised by
    :func:`_install_corrupt_body_guards`), so for 7z only the CRC can
    mismatch; *size* is reported as both expected and actual.
    """
    counter = Crc32Counter()
    yield from counter.tap(_pipe_chunks(read_fd))
    if counter.crc32 == expected_crc32:
        return None
    return counter.mismatch("CRC-32", expected_crc32=expected_crc32, expected_size=size)


def _take_member_crcs(sz: py7zr.SevenZipFile) -> dict[str, int]:
    """Remove py7zr's per-member CRC-32s from *sz* and return them by member name.

    py7zr compares each member's CRC *after* writing all of its bytes to
    the writer factory — so the bytes are kept — but then raises
    ``CrcError``, which ends the whole extraction. With the ``digest``
    gone from a member's header record (``ArchiveFile.crc32`` reads it),
    py7zr skips that comparison, and :func:`_checked_pipe_chunks` makes it
    instead without stopping. py7zr internals, so the shape is guarded
    the way ``_filePassed`` is: a changed shape fails loudly rather than
    silently leaving py7zr's check (and its abort) in place.

    A folder-level CRC (``Folder.crc``) is left alone and stays fatal:
    neither 7-Zip nor py7zr writes one alongside per-member CRCs.
    """
    files_list = getattr(sz.files, "files_list", None)
    if not isinstance(files_list, list) or not all(isinstance(f, dict) for f in files_list):
        raise RuntimeError(
            "py7zr's SevenZipFile.files.files_list is no longer a list of dicts; "
            "tolerating per-member CRC failures depends on it."
        )
    crcs: dict[str, int] = {}
    for file_info in files_list:
        digest = file_info.pop("digest", None)
        if digest is not None:
            crcs[file_info["filename"]] = digest
    return crcs


def _drain_and_close(read_fd: int) -> None:
    """Read *read_fd* to EOF, then close it. Tolerates already-closed fds."""
    with contextlib.suppress(OSError):
        while os.read(read_fd, CHUNK_SIZE):
            pass
    with contextlib.suppress(OSError):
        os.close(read_fd)


def _open_seven_z(
    client, bucket: str, key: str, *, if_match: str | None = None
) -> py7zr.SevenZipFile:
    """Open ``s3://bucket/key`` as a :class:`py7zr.SevenZipFile`.

    Wraps a :class:`SeekableS3Object` (ranged-GET view, tail-prefetched)
    in an :class:`io.BufferedReader` and hands it to py7zr. The returned
    file's ``fp`` is that BufferedReader; :meth:`SevenZipFile.close`
    releases it, and closing a py7zr file built from a passed-in fileobj
    does not close the fileobj itself, so the caller owns the lifecycle.

    *if_match* pins every ranged GET (and the opening HEAD) to that
    object generation — see :class:`SeekableS3Object`.

    py7zr's signature-header parse can fail two ways on bad bytes:
    ``Bad7zFile`` (parent: :class:`py7zr.exceptions.ArchiveError`) if the
    format is recognized but malformed, ``struct.error`` if the file is
    too short for ``struct.unpack`` to even reach ``Bad7zFile``. Both are
    translated into :class:`ArchiveReadError` so callers don't have to
    know py7zr internals.
    """
    buffered = open_seekable(client, bucket, key, if_match=if_match)
    try:
        return py7zr.SevenZipFile(buffered, mode="r")
    except ArchiveError as exc:
        raise ArchiveReadError(f"7z header parse failed: {exc}", cause=exc) from exc
    except struct.error as exc:
        raise ArchiveReadError(
            f"7z header parse failed (truncated input): {exc}", cause=exc
        ) from exc


def list_seven_z_entries(client, bucket: str, key: str) -> list[tuple[str, int]]:
    """Return ``(raw_name, size)`` per file entry, reading the header only.

    The 7z trailing header carries every member's name and uncompressed
    size, so listing needs no member bytes at all — the tail prefetch on
    open covers it. Directory entries are excluded, matching
    :func:`iter_seven_z_members`. Names are raw (pre-safety-pass);
    callers that show them next to extract keys should apply
    :func:`s3_archive.paths.safe_member_key`, as ``_apply_safe_keys``
    does for the member walk.
    """
    sz = _open_seven_z(client, bucket, key)
    try:
        return [(f.filename, f.uncompressed) for f in sz.files if not f.is_directory]
    finally:
        sz.close()


@dataclass(frozen=True)
class SevenZResumeInfo:
    """What ``extract --resume`` needs to know about a .7z before resuming.

    - *resumable* — whether py7zr can seek to an arbitrary member cheaply.
      True iff the archive has ≥2 compression Folders; a fully-solid
      archive (one Folder holding every file) decodes from the block
      start for *any* member, so resume buys nothing and is refused.
    - *numfolders* — the parsed Folder count (0 when the archive has no
      main pack stream, e.g. an all-empty-files archive), kept for the
      refuse message and tests.
    - *members* — ``(raw_filename, uncompressed_size)`` for every
      non-directory entry, in archive order. The raw (pre-safety-key)
      name is what py7zr's ``targets=`` set matches on.
    """

    resumable: bool
    numfolders: int
    members: list[tuple[str, int]]


def probe_seven_z_resume(client, bucket: str, key: str) -> SevenZResumeInfo:
    """Read a .7z header and report whether ``--resume`` can seek per-member.

    Header-only and tail-prefetched (one ranged GET region), so it's cheap
    next to the transfer. Raises :class:`ArchiveReadError` on bad bytes
    (same wrapping as :func:`iter_seven_z_members`) — a corrupt archive is
    not the same as "unsupported for resume."
    """
    sz = _open_seven_z(client, bucket, key)
    try:
        # ``main_streams`` is None when there is no packed body (e.g. an
        # archive of only empty files / directories) → 0 Folders → not
        # resumable, which is the correct answer (nothing to seek to).
        # ``main_streams.unpackinfo`` is independently optional — py7zr
        # leaves it None when the header carries no UnpackInfo property and
        # guards it the same way internally (archiveinfo.StreamsInfo) — so
        # read it through the same 0-Folder answer rather than assuming a
        # packinfo implies an unpackinfo.
        main_streams = sz.header.main_streams
        unpackinfo = main_streams.unpackinfo if main_streams is not None else None
        numfolders = unpackinfo.numfolders if unpackinfo is not None else 0
        members = [(f.filename, f.uncompressed) for f in sz.files if not f.is_directory]
    finally:
        with contextlib.suppress(Exception):
            sz.close()
    return SevenZResumeInfo(
        resumable=numfolders >= 2,
        numfolders=numfolders,
        members=members,
    )


def _run_extract_worker(
    sz: py7zr.SevenZipFile,
    factory: _WriterFactory,
    meta_queue: queue.Queue,
    target_set: set[str] | None,
    worker_error: list[BaseException],
) -> None:
    """Drive py7zr extraction, then close the last pipe and push the sentinel.

    Runs on the worker thread. Any exception is captured into
    *worker_error* rather than raised, so the thread always reaches the
    ``finally`` that unblocks the consumer — :func:`_drain_members`
    re-raises it on the caller's thread once the sentinel arrives.
    """
    try:
        if target_set is None:
            sz.extractall(factory=factory)
        else:
            # ``extract`` with ``skip_notarget=True`` (the default) skips
            # whole non-target Folders and seeks to each target Folder's
            # pack offset — the per-member seek that makes resume cheap.
            sz.extract(targets=target_set, factory=factory)
    except BaseException as exc:  # noqa: BLE001
        worker_error.append(exc)
    finally:
        factory.close_current()
        meta_queue.put(_SENTINEL)


def _reraise_worker_error(worker_error: list[BaseException]) -> None:
    """Re-raise the worker thread's exception, if it had one.

    py7zr's own decode failures (``ArchiveError`` — CRC, bad header — and
    ``struct.error``) are mapped to
    :class:`~s3_archive.exceptions.ArchiveReadError`. Codec errors and
    stalls arrive already as ``ArchiveReadError``, raised inside py7zr by
    :func:`_install_corrupt_body_guards`; they, and anything else,
    propagate unchanged.
    """
    if not worker_error:
        return
    err = worker_error[0]
    if isinstance(err, (ArchiveError, struct.error)):
        raise ArchiveReadError(f"7z decode failed: {err}", cause=err) from err
    raise err


def _abandon_extract(
    meta_queue: queue.Queue,
    worker_thread: threading.Thread,
    prev_read_fd: int | None,
) -> None:
    """Unblock and wind down a worker whose consumer stopped early.

    Called when the caller closed the generator (or an exception
    propagated through a ``yield``). Closing the open pipe gives the
    worker a ``BrokenPipeError`` on its next write; draining the rest of
    the queue lets it finish closing its current pipe and push the
    sentinel. Every read fd taken off the queue here is closed, since no
    :class:`ArchiveMember` will ever be handed out for it.
    """
    if prev_read_fd is not None:
        with contextlib.suppress(OSError):
            os.close(prev_read_fd)
    if not worker_thread.is_alive():
        return
    while True:
        try:
            item = meta_queue.get(timeout=30)
        except queue.Empty:
            break
        if item is _SENTINEL:
            break
        _, leftover_fd = item
        with contextlib.suppress(OSError):
            os.close(leftover_fd)
    worker_thread.join(timeout=30)


def _drain_members(
    meta_queue: queue.Queue,
    name_to_size: dict[str, int],
    worker_error: list[BaseException],
    worker_thread: threading.Thread,
    expected_crcs: dict[str, int],
) -> Generator[ArchiveMember, None, None]:
    """Yield one :class:`ArchiveMember` per ``(filename, read_fd)`` on the queue.

    Runs until the worker's sentinel arrives, then joins the thread and
    re-raises whatever it captured. Owns the read-fd lifecycle: each
    member's fd is drained and closed when the *next* member is pulled,
    and any fd still open when the consumer walks away is closed by
    :func:`_abandon_extract`.

    *expected_crcs* is non-empty only on a tolerant walk (see
    :func:`_take_member_crcs`); a member listed there checks its own CRC.
    """
    prev_read_fd: int | None = None
    clean_exit = False
    try:
        while True:
            if prev_read_fd is not None:
                _drain_and_close(prev_read_fd)
                prev_read_fd = None

            item = meta_queue.get()
            if item is _SENTINEL:
                clean_exit = True
                break

            filename, read_fd = item
            prev_read_fd = read_fd
            size = name_to_size.get(filename, 0)
            expected_crc32 = expected_crcs.get(filename)
            yield ArchiveMember(
                name=filename,
                size=size,
                _chunks=(
                    _pipe_chunks(read_fd)
                    if expected_crc32 is None
                    else _checked_pipe_chunks(read_fd, expected_crc32=expected_crc32, size=size)
                ),
            )
            # _pipe_chunks does NOT close the fd; the next loop's
            # _drain_and_close call owns the close. Exactly one party
            # closes each read fd — see _pipe_chunks for the fd-reuse
            # race that motivates this.

        worker_thread.join()
        _reraise_worker_error(worker_error)
    finally:
        if not clean_exit:
            _abandon_extract(meta_queue, worker_thread, prev_read_fd)


def iter_seven_z_members(
    client,
    bucket: str,
    key: str,
    *,
    targets: Collection[str] | None = None,
    if_match: str | None = None,
    tolerate_integrity_errors: bool = False,
) -> Generator[ArchiveMember, None, None]:
    """GET ``s3://bucket/key`` and yield one :class:`ArchiveMember` per file entry.

    Unlike the tar/zip iterators in :mod:`s3_archive.members` this opens
    a seekable view of the archive — see module docstring for why.
    Members are yielded in archive order; the caller drives consumption
    per-member via the :class:`ArchiveMember` API.

    *targets*, when given, restricts extraction to those raw member names
    (``--resume``): py7zr seeks to just those members' Folders and fires
    the writer factory only for them, so the generator yields exactly the
    requested members (still in archive order). ``None`` extracts every
    member. Requires a non-solid / multi-Folder archive to be cheap — see
    :func:`probe_seven_z_resume`.

    *if_match* (quoted or quote-stripped ETag) pins the opening HEAD and
    every ranged GET to that object generation; a concurrent overwrite
    raises :class:`s3_archive.exceptions.ETagMismatchError` (up front)
    or a 412 ``ClientError`` (mid-walk) instead of mixing bytes from two
    generations across range requests.

    *tolerate_integrity_errors* (default off): a member whose decoded
    bytes fail its CRC-32 is handed on whole, the mismatch is recorded on
    :attr:`ArchiveMember.integrity` once it is consumed, and extraction
    continues — instead of py7zr's ``CrcError`` ending the walk as
    :class:`ArchiveReadError`. A codec that rejects its input (corrupt
    LZMA, a stalled body) is fatal either way: there are no decoded bytes
    to hand on.
    """
    sz = _open_seven_z(client, bucket, key, if_match=if_match)
    # Load-bearing: ``parallel`` is gated on ``not _filePassed`` inside
    # py7zr, and we depend on sequential single-thread extraction so the
    # writer-factory pipe handoff stays in order. Passing an io.IOBase
    # subclass (which BufferedReader is) sets _filePassed=True; the
    # guard catches py7zr quietly changing that behavior.
    if not sz._filePassed:
        sz.close()
        raise RuntimeError(
            "py7zr did not set _filePassed=True for a BufferedReader; "
            "the streaming extract path depends on parallel=False."
        )

    try:
        name_to_size: dict[str, int] = {
            f.filename: f.uncompressed for f in sz.files if not f.is_directory
        }
        expected_crcs = _take_member_crcs(sz) if tolerate_integrity_errors else {}
        meta_queue: queue.Queue = queue.Queue()
        worker_error: list[BaseException] = []
        factory = _WriterFactory(meta_queue)
        target_set = set(targets) if targets is not None else None

        worker_thread = threading.Thread(
            target=_run_extract_worker,
            args=(sz, factory, meta_queue, target_set, worker_error),
            daemon=True,
        )
        worker_thread.start()

        yield from _drain_members(
            meta_queue, name_to_size, worker_error, worker_thread, expected_crcs
        )
    finally:
        # Close py7zr's SevenZipFile; suppress secondary errors so a
        # primary worker exception isn't masked.
        with contextlib.suppress(Exception):
            sz.close()
