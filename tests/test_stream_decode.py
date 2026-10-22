"""Tests for s3_archive.stream_decode — the one tar-stream opener and the one zip walk.

Plus the caller-visible half of the 8-3 merge: every public entry point
that reaches the opener refuses a non-tar format with
``UnsupportedArchiveFormatError`` (the old manifest copy raised
``ValueError``).
"""

import io
import zipfile

import pytest
import zstandard

from s3_archive.exceptions import UnsupportedArchiveFormatError
from s3_archive.iter import IterableFileobj
from s3_archive.manifest import build_manifest_tar_fileobj
from s3_archive.stream_decode import TAR_FAMILY_FORMATS, iter_zip_file_entries, open_tar_stream
from s3_archive.url import detect_format

from .conftest import build_tar

_FILES = {"a.txt": b"alpha", "sub/b.txt": b"bravo"}

_BUILD_MODE = {"tar": "w", "tar.gz": "w:gz", "tar.bz2": "w:bz2", "tar.xz": "w:xz"}


def _tar_bytes(fmt: str) -> bytes:
    if fmt == "tar.zst":
        return zstandard.ZstdCompressor().compress(build_tar(_FILES, mode="w"))
    return build_tar(_FILES, mode=_BUILD_MODE[fmt])


class TestOpenTarStream:
    @pytest.mark.parametrize("fmt", TAR_FAMILY_FORMATS)
    def test_opens_every_tar_family_format_forward_only(self, fmt):
        # IterableFileobj has read() and nothing else — proves the opener
        # never asks the source for seek/tell.
        source = IterableFileobj(iter([_tar_bytes(fmt)]))
        with open_tar_stream(source, fmt) as tar:
            got = {}
            for member in tar:
                fobj = tar.extractfile(member)
                assert fobj is not None
                got[member.name] = fobj.read()
        assert got == _FILES

    @pytest.mark.parametrize("fmt", ["zip", "7z", "rar", ""])
    def test_non_tar_format_raises_unsupported_not_value_error(self, fmt):
        with pytest.raises(UnsupportedArchiveFormatError, match="not a tar-family format") as exc:
            open_tar_stream(io.BytesIO(b""), fmt)
        assert not isinstance(exc.value, ValueError)

    def test_tar_family_formats_is_every_tar_format_detect_format_returns(self):
        """Drift guard: detection (url.py) and decoding (stream_decode) agree."""
        detected = {
            detect_format(f"s3://b/x{ext}")
            for ext in (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")
            + (".tar.zst", ".zip", ".7z")
        }
        assert {f for f in detected if f.startswith("tar")} == set(TAR_FAMILY_FORMATS)


def _zip_with_dirs() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("top/", b"")
        zf.writestr("top/a.txt", b"alpha")
        zf.writestr("win\\", b"")  # Windows-separator directory entry
        zf.writestr("win\\b.txt", b"bravo")
    return buf.getvalue()


class TestIterZipFileEntries:
    def test_yields_file_entries_only_with_normalized_names(self):
        got = {
            name: b"".join(chunks)
            for name, _size, chunks in iter_zip_file_entries([_zip_with_dirs()])
        }
        assert got == {"top/a.txt": b"alpha", "win/b.txt": b"bravo"}

    def test_entry_counter_counts_directory_entries_too(self):
        counter = [0]
        for _name, _size, chunks in iter_zip_file_entries(
            [_zip_with_dirs()], entry_counter=counter
        ):
            for _ in chunks:
                pass
        assert counter == [4]


def test_build_manifest_tar_fileobj_refuses_non_tar_format():
    """The one public entry point that hands its format straight to the opener.

    Formerly ``ValueError`` from manifest's private copy. The other public
    entry points filter the format first and already raised
    ``UnsupportedArchiveFormatError``: ``build_manifest_from_tap``
    (``test_manifest.py`` ``test_unknown_format_raises``) and
    ``iter_archive_members`` (``test_members.py``
    ``test_unsupported_format_raises``).
    """
    with pytest.raises(UnsupportedArchiveFormatError, match="not a tar-family format"):
        build_manifest_tar_fileobj(io.BytesIO(b""), "zip")
