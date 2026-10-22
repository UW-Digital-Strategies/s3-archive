"""Forward-only decoder entry points shared by the manifest builders and the member walk.

:mod:`s3_archive.manifest` (per-entry hashing) and
:mod:`s3_archive.members` (per-member iteration) both decode an archive
front-to-back from a non-seekable byte source. The two steps they share
live here, once, so the two walks cannot drift:

- :func:`open_tar_stream` — the one format-string → streaming
  :class:`tarfile.TarFile` opener, over the one tar-mode table, with the
  one ``tar.zst`` branch. :data:`TAR_FAMILY_FORMATS` is derived from that
  table, so "which formats are tar-family" and "which formats this opener
  accepts" are the same fact.
- :func:`iter_zip_file_entries` — the one forward-only ``stream_unzip``
  walk that decodes entry names and drains-and-skips directory entries.

The format strings are the ones :func:`s3_archive.url.detect_format`
returns. That module owns *detection* (extension → format string) and
stays free of decoder imports; this one owns *opening a decoder* for a
detected format.
"""

import tarfile
from collections.abc import Iterable, Iterator
from typing import IO, Literal, Protocol, cast

import zstandard
from stream_unzip import stream_unzip

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.paths import decode_zip_filename

# Maps a tarfile-native format string to the ``tarfile.open`` streaming
# mode that decodes it. The value type is the literal set ``tarfile.open``
# accepts, so a typo in a mode string is caught by the type checker here
# rather than at the open() call. ``tar.zst`` is absent on purpose:
# tarfile has no zstd mode, so :func:`open_tar_stream` wraps the source
# in a zstandard stream reader and opens the result as plain ``r|``.
_TAR_MODES: dict[str, Literal["r|", "r|gz", "r|bz2", "r|xz"]] = {
    "tar": "r|",
    "tar.gz": "r|gz",
    "tar.bz2": "r|bz2",
    "tar.xz": "r|xz",
}

_TAR_ZST = "tar.zst"


class ReadableBytes(Protocol):
    """The whole interface a forward-only decoder needs from its byte source."""

    def read(self, size: int, /) -> bytes: ...


TAR_FAMILY_FORMATS: tuple[str, ...] = (*_TAR_MODES, _TAR_ZST)
"""Formats decoded via the :mod:`tarfile` family (with optional decompression).

Exactly the formats :func:`open_tar_stream` accepts."""


def open_tar_stream(fileobj: ReadableBytes, archive_format: str) -> tarfile.TarFile:
    """Open a streaming (``r|``-family) :class:`tarfile.TarFile` over *fileobj*.

    *archive_format* is one of :data:`TAR_FAMILY_FORMATS`. ``"tar.zst"``
    wraps *fileobj* in a :class:`zstandard.ZstdDecompressor` stream
    reader first; the rest are tarfile-native modes. *fileobj* need only
    support ``read(n)`` (an S3 GET body, an
    :class:`~s3_archive.iter.IterableFileobj`, a
    :class:`~s3_archive.hashing.HashingTap`) — it is never seeked.

    Returns:
        An open streaming ``TarFile``; the caller closes it (use it as a
        context manager).

    Raises:
        UnsupportedArchiveFormatError: *archive_format* is not a
            tar-family format. Same exception every public entry point
            raises for an unsupported format, so a caller that passes a
            non-tar format straight through sees the one contract.
    """
    # Both library boundaries below are typed wider than they use: typeshed
    # gives every tarfile.open ``fileobj`` the read/write/seek superset and
    # zstandard's stubs say ``IO[bytes]``, but the ``r|`` stream modes
    # (tarfile's ``_Stream``) and ``stream_reader`` only ever call
    # ``read()`` — which is all *fileobj* promises. The casts say so once.
    if archive_format == _TAR_ZST:
        decompressor = zstandard.ZstdDecompressor()
        return tarfile.open(fileobj=decompressor.stream_reader(cast(IO[bytes], fileobj)), mode="r|")

    mode = _TAR_MODES.get(archive_format)
    if mode is None:
        raise UnsupportedArchiveFormatError(
            f"not a tar-family format: {archive_format!r} "
            f"(expected one of {', '.join(TAR_FAMILY_FORMATS)})"
        )
    return tarfile.open(fileobj=cast(IO[bytes], fileobj), mode=mode)


def iter_zip_file_entries(
    chunks_iter: Iterable[bytes], *, entry_counter: list[int] | None = None
) -> Iterator[tuple[str, int | None, Iterator[bytes]]]:
    """Walk a zip forward-only with ``stream_unzip``, yielding only its file entries.

    Yields ``(name, size, chunks)`` per file entry, in archive order:
    *name* decoded and separator-normalized by
    :func:`s3_archive.paths.decode_zip_filename`, *size* as
    ``stream_unzip`` reports it (``None`` when the local header doesn't
    carry one), *chunks* the entry's decoded bytes. The caller must
    consume *chunks* before advancing — ``stream_unzip``'s own rule.

    A directory entry (decoded name ending in ``/``, which after
    separator normalization also catches a Windows-style ``dir\\``
    entry) carries no file bytes, but ``stream_unzip`` still requires its
    chunk iterator consumed before it yields the next entry — so it is
    drained here and never yielded.

    *entry_counter*, when given, is a one-element list whose slot is
    incremented once per raw entry (directory entries included) whose
    header parse succeeded — see
    :func:`s3_archive.members._iter_zip_members` for why that count is
    the resume point of the seekable continuation.

    Raises:
        Nothing of its own: ``stream_unzip``'s exceptions
        (``NotStreamUnzippable``, ``UnzipError`` and friends) propagate
        unchanged, raised lazily at the entry that provokes them.
    """
    for raw_name, size, chunks in stream_unzip(chunks_iter):
        if entry_counter is not None:
            entry_counter[0] += 1
        name = decode_zip_filename(raw_name)
        if name.endswith("/"):
            for _ in chunks:
                pass
            continue
        yield name, size, chunks
