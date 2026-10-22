"""Tests for streaming .7z read support."""

import contextlib
import lzma
import os
import queue
import random
import threading
import time
import zlib
from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock

import botocore.exceptions
import py7zr
import py7zr.compressor
import pytest
from py7zr.exceptions import ArchiveError, UnsupportedCompressionMethodError

from s3_archive.constants import CHUNK_SIZE
from s3_archive.exceptions import ArchiveReadError, ETagMismatchError
from s3_archive.integrity import IntegrityMismatch
from s3_archive.members import ArchiveMember, iter_archive_members
from s3_archive.seekable import SeekableS3Object
from s3_archive.seven_z import (
    _SENTINEL,
    SevenZResumeInfo,
    _abandon_extract,
    _drain_members,
    _install_native_decoder_fallback,
    _PipeSink,
    iter_seven_z_members,
    list_seven_z_entries,
    probe_seven_z_resume,
)

from .conftest import (
    PY7ZR_FLAVORS,
    SEVEN_Z_FLAVORS,
    build_7z,
    build_7z_py7zr,
    incompressible_bytes,
)

# Every (builder, flavor) pair. The 7-Zip-CLI builds skip when no CLI is on
# PATH; the py7zr builds never do, so the reader is exercised either way.
# What py7zr can and cannot reproduce is recorded at PY7ZR_FLAVORS.
_EVERY_BUILD = [
    *(pytest.param(build_7z, flavor, id=f"cli-{flavor}") for flavor in sorted(SEVEN_Z_FLAVORS)),
    *(
        pytest.param(build_7z_py7zr, flavor, id=f"py7zr-{flavor}")
        for flavor in sorted(PY7ZR_FLAVORS)
    ),
]
# One builder of each kind, for tests whose flavor is fixed and where what
# varies is only whether the archive came from 7-Zip or from py7zr.
_BUILDERS = [pytest.param(build_7z, id="cli"), pytest.param(build_7z_py7zr, id="py7zr")]

_FILES = {
    "alpha.txt": b"alpha contents\n",
    "empty.txt": b"",
    "nested/beta.bin": b"\x00\x01\x02\x03\x04" * 100,
    "gamma.txt": b"gamma " * 50,
}

# Non-empty-only variant: with ``-ms=off`` each *non-empty* file gets its
# own Folder (empty files carry no pack stream), so numfolders == len(files)
# holds cleanly only when no member is empty.
_NONEMPTY_FILES = {
    "alpha.txt": b"alpha contents\n",
    "nested/beta.bin": b"\x00\x01\x02\x03\x04" * 100,
    "gamma.txt": b"gamma " * 50,
}


def _upload(client, key: str, body: bytes) -> None:
    client.put_object(Bucket="src-bucket", Key=key, Body=body)


@pytest.mark.parametrize(("builder", "flavor"), _EVERY_BUILD)
def test_round_trip_via_iter_archive_members(s3_client, builder, flavor):
    """Every flavor extracts byte-for-byte through the public dispatch path."""
    _upload(s3_client, "archive.7z", builder(_FILES, flavor=flavor))

    seen: dict[str, bytes] = {}
    for member in iter_archive_members(s3_client, "src-bucket", "archive.7z", "7z"):
        assert isinstance(member, ArchiveMember)
        seen[member.name] = member.read_all()
    assert seen == _FILES


@pytest.mark.parametrize(("builder", "flavor"), _EVERY_BUILD)
def test_sizes_match_uncompressed_lengths(s3_client, builder, flavor):
    """``ArchiveMember.size`` is the uncompressed length from the 7z header."""
    _upload(s3_client, "archive.7z", builder(_FILES, flavor=flavor))

    for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z"):
        assert member.size == len(_FILES[member.name])
        member.drain()


@pytest.mark.parametrize("builder", _BUILDERS)
def test_auto_drain_on_next_yield(s3_client, builder):
    """Forgetting to consume a member must not corrupt the next one."""
    _upload(s3_client, "archive.7z", builder(_FILES))

    names: list[str] = []
    for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z"):
        names.append(member.name)
        # Deliberately do NOT consume member.chunks().

    # Members from the iterator should match the input set even when the
    # caller never reads their bodies — the main loop auto-drains.
    assert set(names) == set(_FILES)


@pytest.mark.parametrize("builder", _BUILDERS)
def test_empty_member_yields_zero_bytes(s3_client, builder):
    """Zero-byte members should yield an immediately-empty chunk iterator.

    Both builders matter here: 7-Zip writes the member as an empty stream
    and py7zr as a zero-length substream, and py7zr's reader takes a
    different path for each.
    """
    _upload(s3_client, "archive.7z", builder(_FILES))

    for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z"):
        if member.name == "empty.txt":
            assert member.size == 0
            assert member.read_all() == b""
        else:
            member.drain()


@pytest.mark.parametrize("builder", _BUILDERS)
def test_iterator_close_mid_stream(s3_client, builder):
    """Closing the iterator early should clean up without raising or hanging."""
    _upload(s3_client, "archive.7z", builder(_FILES))

    it = iter_seven_z_members(s3_client, "src-bucket", "archive.7z")
    first = next(it)
    first.drain()
    # Abandon the generator — its finally block must close pipes, drain the
    # metadata queue, and join the worker thread without raising.
    it.close()


@pytest.mark.parametrize("builder", _BUILDERS)
def test_header_parse_error_surfaces_as_archive_read_error(s3_client, builder):
    """A truncated archive surfaces as ArchiveReadError, not a hang or Bad7zFile."""
    full = builder(_FILES)
    # 32 bytes is the SignatureHeader — enough for py7zr to recognize the
    # magic and then choke on the missing NextHeader. Wrapped into
    # ArchiveReadError so callers see one exception type for "bad bytes."
    _upload(s3_client, "archive.7z", full[:32])

    with pytest.raises(ArchiveReadError) as exc_info:
        list(iter_seven_z_members(s3_client, "src-bucket", "archive.7z"))
    # Original py7zr exception preserved on both __cause__ and .cause.
    assert isinstance(exc_info.value.__cause__, py7zr.Bad7zFile)
    assert isinstance(exc_info.value.cause, py7zr.Bad7zFile)


def test_very_short_input_surfaces_as_archive_read_error(s3_client):
    """An archive too short for py7zr to even reach Bad7zFile (struct.error path)."""
    # 6 bytes is enough for the magic check but struct.unpack on the
    # SignatureHeader fields fails with struct.error.
    _upload(s3_client, "archive.7z", b"\x37\x7a\xbc\xaf\x27\x1c")

    with pytest.raises(ArchiveReadError) as exc_info:
        list(iter_seven_z_members(s3_client, "src-bucket", "archive.7z"))
    assert exc_info.value.cause is not None


@pytest.mark.parametrize("builder", _BUILDERS)
def test_iter_archive_members_yields_seven_z_in_order(s3_client, builder):
    """The dispatch from iter_archive_members yields members in archive order."""
    _upload(s3_client, "archive.7z", builder(_FILES))

    names = [m.name for m in iter_archive_members(s3_client, "src-bucket", "archive.7z", "7z")]
    # py7zr orders members per its internal scan; verify the set matches and
    # that the iterator yields each member exactly once.
    assert sorted(names) == sorted(_FILES)
    assert len(names) == len(_FILES)


class TestProbeSevenZResume:
    """probe_seven_z_resume reports solidity + the member index for resume."""

    @pytest.mark.parametrize("builder", _BUILDERS)
    def test_nonsolid_is_resumable_one_folder_per_file(self, s3_client, builder):
        _upload(s3_client, "archive.7z", builder(_NONEMPTY_FILES, flavor="nonsolid"))

        info = probe_seven_z_resume(s3_client, "src-bucket", "archive.7z")
        assert isinstance(info, SevenZResumeInfo)
        assert info.resumable is True
        # -ms=off → one Folder per non-empty file.
        assert info.numfolders == len(_NONEMPTY_FILES)
        assert dict(info.members) == {name: len(body) for name, body in _NONEMPTY_FILES.items()}

    @pytest.mark.parametrize("builder", _BUILDERS)
    def test_solid_is_not_resumable_single_folder(self, s3_client, builder):
        _upload(s3_client, "archive.7z", builder(_NONEMPTY_FILES, flavor="solid"))

        info = probe_seven_z_resume(s3_client, "src-bucket", "archive.7z")
        assert info.resumable is False
        assert info.numfolders == 1
        # Members are still enumerated even when resume is refused.
        assert dict(info.members) == {name: len(body) for name, body in _NONEMPTY_FILES.items()}

    @pytest.mark.parametrize("builder", _BUILDERS)
    def test_corrupt_archive_surfaces_archive_read_error(self, s3_client, builder):
        _upload(s3_client, "archive.7z", builder(_FILES)[:32])

        with pytest.raises(ArchiveReadError):
            probe_seven_z_resume(s3_client, "src-bucket", "archive.7z")

    @pytest.mark.parametrize("unpackinfo", [None, SimpleNamespace(numfolders=3)])
    def test_header_without_unpackinfo_reports_zero_folders(self, monkeypatch, unpackinfo):
        """py7zr leaves ``main_streams.unpackinfo`` None on some headers.

        Reading ``.numfolders`` straight through raised AttributeError there
        instead of answering "not resumable". Built with stubs rather than a
        real archive: the 7z CLI can't be asked to emit a packinfo-without-
        unpackinfo header, and the shape is what's under test.
        """
        closed = []
        sz = SimpleNamespace(
            header=SimpleNamespace(main_streams=SimpleNamespace(unpackinfo=unpackinfo)),
            files=[],
            close=lambda: closed.append(True),
        )
        monkeypatch.setattr("s3_archive.seven_z._open_seven_z", lambda *a, **kw: sz)

        info = probe_seven_z_resume(MagicMock(), "src-bucket", "archive.7z")

        expected = 0 if unpackinfo is None else unpackinfo.numfolders
        assert info.numfolders == expected
        assert info.resumable is (expected >= 2)
        assert info.members == []
        assert closed == [True]


class TestSevenZTargets:
    """iter_seven_z_members(targets=…) yields exactly the requested members."""

    @pytest.mark.parametrize("builder", _BUILDERS)
    def test_targets_subset_yields_only_those_in_order(self, s3_client, builder):
        _upload(s3_client, "archive.7z", builder(_FILES, flavor="nonsolid"))

        targets = ["alpha.txt", "gamma.txt"]
        seen: list[tuple[str, bytes]] = []
        for member in iter_seven_z_members(s3_client, "src-bucket", "archive.7z", targets=targets):
            seen.append((member.name, member.read_all()))

        # Exactly the targets, correct bytes, in archive order (the order the
        # members appear in the archive, which py7zr preserves).
        assert [name for name, _ in seen] == ["alpha.txt", "gamma.txt"]
        assert dict(seen) == {name: _FILES[name] for name in targets}

    @pytest.mark.parametrize("builder", _BUILDERS)
    def test_empty_targets_yields_nothing(self, s3_client, builder):
        _upload(s3_client, "archive.7z", builder(_FILES, flavor="nonsolid"))

        members = list(iter_seven_z_members(s3_client, "src-bucket", "archive.7z", targets=[]))
        assert members == []

    @pytest.mark.parametrize("builder", _BUILDERS)
    def test_none_targets_yields_everything(self, s3_client, builder):
        _upload(s3_client, "archive.7z", builder(_FILES, flavor="nonsolid"))

        names = {m.name for m in iter_seven_z_members(s3_client, "src-bucket", "archive.7z")}
        assert names == set(_FILES)


@pytest.mark.parametrize("builder", _BUILDERS)
def test_list_entries_reads_names_and_sizes_skipping_directories(s3_client, builder):
    _upload(s3_client, "archive.7z", builder(_FILES))

    entries = list_seven_z_entries(s3_client, "src-bucket", "archive.7z")

    # "nested/" is implied by nested/beta.bin; neither builder's directory
    # entry (if any) may appear as a listed file.
    assert dict(entries) == {name: len(body) for name, body in _FILES.items()}


def _ranged_client(body: bytes) -> MagicMock:
    """A client serving *body* over HEAD + ranged GET, with no sockets.

    The lifecycle tests count open fds; moto's in-process server would add
    its own, so they read the archive through this stub instead.
    """
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": len(body), "ETag": '"etag"'}

    def get_object(**kwargs):
        start, end = kwargs["Range"].removeprefix("bytes=").split("-")
        response = MagicMock()
        response.read.return_value = body[int(start) : int(end) + 1]
        return {"Body": response}

    client.get_object.side_effect = get_object
    return client


def _open_fds() -> int:
    return len(os.listdir("/dev/fd"))


@contextlib.contextmanager
def _no_fd_or_thread_leak() -> Iterator[None]:
    """Assert the block leaves no pipe fd open and no worker thread alive.

    Each member is an ``os.pipe()`` pair and each walk a worker thread, so
    a lifecycle bug shows up as an fd or thread count that does not come
    back down — often long before it shows up as a hang.
    """
    fds, threads = _open_fds(), threading.active_count()
    yield
    assert _open_fds() == fds
    assert threading.active_count() == threads


class _ConsumerError(Exception):
    """Stands in for anything the caller raises mid-walk (an S3 put, a hash)."""


class TestGeneratorLifecycle:
    """``iter_seven_z_members`` winds down cleanly however the walk ends.

    The fragile part of the module: a worker thread pushing decoded bytes
    into one pipe per member while the caller pulls. Driven on py7zr-built
    archives, so it runs with no 7-Zip CLI installed. Members are larger
    than both CHUNK_SIZE and a pipe buffer, so a member left half-read has
    the worker genuinely blocked in a write.
    """

    _BIG = {f"m{i}.bin": incompressible_bytes(3 * CHUNK_SIZE, seed=i) for i in range(3)}

    def _walk(self, flavor: str = "solid"):
        body = build_7z_py7zr(self._BIG, flavor=flavor)
        return iter_seven_z_members(_ranged_client(body), "b", "k.7z")

    def test_close_after_partial_read_of_first_member(self):
        with _no_fd_or_thread_leak():
            walk = self._walk()
            first = next(walk)
            assert next(first.chunks()) == self._BIG[first.name][:CHUNK_SIZE]
            walk.close()

    def test_consumer_exception_propagates_unchanged(self):
        """An exception thrown in at the yield is the one the caller sees.

        Cleanup runs on the way out (``_abandon_extract``) and must neither
        mask the caller's exception nor leave the worker writing.
        """
        with _no_fd_or_thread_leak():
            walk = self._walk()
            next(walk)
            with pytest.raises(_ConsumerError):
                walk.throw(_ConsumerError("put_object failed"))

    @pytest.mark.parametrize("flavor", ["solid", "nonsolid"])
    def test_partial_reads_leave_every_later_member_intact(self, flavor):
        """Reading one chunk of each member, then moving on, loses nothing.

        The auto-drain on the next ``next()`` must consume exactly the
        rest of the member, so each member's first chunk is its own.
        """
        with _no_fd_or_thread_leak():
            heads = {m.name: next(m.chunks()) for m in self._walk(flavor)}
        assert heads == {name: body[:CHUNK_SIZE] for name, body in self._BIG.items()}

    def test_corrupt_body_is_archive_read_error_and_winds_down(self):
        """A body that fails its CRC surfaces as ArchiveReadError, not a hang.

        ``copy`` stores the members raw, so flipping bytes inside the
        second member's data is a deterministic CRC failure at a known
        member (a mangled LZMA stream fails however the decoder likes).
        """
        body = bytearray(build_7z_py7zr(self._BIG, flavor="copy"))
        # Pack stream starts right after the 32-byte signature header; the
        # members are stored back to back in archive order.
        second = 32 + len(self._BIG["m0.bin"]) + 100
        body[second : second + 8] = bytes(b ^ 0xFF for b in body[second : second + 8])

        with _no_fd_or_thread_leak():
            walk = iter_seven_z_members(_ranged_client(bytes(body)), "b", "k.7z")
            first = next(walk)
            assert first.read_all() == self._BIG["m0.bin"]
            with pytest.raises(ArchiveReadError) as exc_info:
                for member in walk:
                    member.drain()
        assert isinstance(exc_info.value.cause, py7zr.exceptions.CrcError)

    def test_refuses_when_py7zr_would_extract_in_parallel(self, monkeypatch):
        """The ``_filePassed`` guard fails fast, and closes what it opened.

        The pipe handoff depends on py7zr extracting one Folder at a time,
        which py7zr only guarantees when it was handed a file object.
        """
        closed = []
        sz = SimpleNamespace(_filePassed=False, close=lambda: closed.append(True))
        monkeypatch.setattr("s3_archive.seven_z._open_seven_z", lambda *a, **kw: sz)

        with pytest.raises(RuntimeError, match="parallel=False"):
            next(iter_seven_z_members(MagicMock(), "b", "k.7z"))
        assert closed == [True]


def _wordy_text(n_words: int) -> bytes:
    """Deterministic, compressible text: every codec gets a real stream to mangle."""
    rnd = random.Random(0)  # noqa: S311 - test fixture, not crypto
    words = [bytes(rnd.choice(b"abcdefghij") for _ in range(rnd.randint(2, 9))) for _ in range(300)]
    return b" ".join(rnd.choice(words) for _ in range(n_words))


class TestCorruptCompressedBody:
    """A mangled compressed body is ArchiveReadError for every codec, never a raw error or a hang.

    The corruption is pinned, not random: 16 bytes flipped at a fixed
    fraction of the packed stream, which on this text gives each codec's
    own "bad data" error (``_FIRST_TENTH``) or the Deflate end-of-stream
    stall (``_MIDDLE``). Pinned deps keep that stable; if a codec upgrade
    moves it, re-pick the fraction, don't loosen the assertion.
    """

    _TEXT = {"a.txt": _wordy_text(20000)}
    _FIRST_TENTH = 1
    _MIDDLE = 5

    def _corrupt(self, flavor: str, tenth: int) -> bytes:
        body = bytearray(build_7z_py7zr(self._TEXT, flavor=flavor))
        # Skip the 32-byte signature header; stay clear of the trailing header.
        at = 32 + (len(body) - 32 - 200) * tenth // 10
        body[at : at + 16] = bytes(b ^ 0xFF for b in body[at : at + 16])
        return bytes(body)

    def _walk(self, body: bytes) -> None:
        for member in iter_seven_z_members(_ranged_client(body), "b", "k.7z"):
            member.drain()

    @pytest.mark.parametrize(
        ("flavor", "codec_error"),
        [
            ("solid", lzma.LZMAError),
            ("lzma1", lzma.LZMAError),
            ("deflate", zlib.error),
            ("deflate64", ValueError),
            # bz2 raises a bare OSError — the reason the wrap sits on the
            # in-memory decode step, not on exception type.
            ("bzip2", OSError),
            ("zstd", py7zr.compressor.zstd.ZstdError),
            ("brotli", py7zr.compressor.brotli.error),
        ],
    )
    def test_codec_error_is_archive_read_error(self, flavor, codec_error):
        with _no_fd_or_thread_leak(), pytest.raises(ArchiveReadError) as exc_info:
            self._walk(self._corrupt(flavor, self._FIRST_TENTH))
        assert type(exc_info.value.cause) is codec_error
        assert "corrupt compressed body" in str(exc_info.value)

    def test_deflate_stall_is_archive_read_error_not_a_hang(self):
        """py7zr alone spins at 100% CPU forever on this body."""
        started = time.monotonic()
        with _no_fd_or_thread_leak(), pytest.raises(ArchiveReadError, match="stalled"):
            self._walk(self._corrupt("deflate", self._MIDDLE))
        assert time.monotonic() - started < 10

    def test_read_side_oserror_propagates_unwrapped(self, monkeypatch):
        """An I/O failure reading the packed stream is not "a corrupt archive".

        Same type and message bz2 uses for bad data, so only *where* it is
        raised tells them apart.
        """

        def failing_read(self, fp):
            raise OSError("Invalid data stream")

        monkeypatch.setattr(py7zr.compressor.SevenZipDecompressor, "_read_data", failing_read)
        with (
            _no_fd_or_thread_leak(),
            pytest.raises(OSError, match="Invalid data stream") as exc_info,
        ):
            self._walk(build_7z_py7zr(self._TEXT, flavor="zstd"))
        assert not isinstance(exc_info.value, ArchiveReadError)

    def test_memory_error_propagates_unwrapped(self, monkeypatch):
        """A real allocation failure must not be reported as a bad archive."""

        def out_of_memory(self, data, max_length=-1):
            raise MemoryError

        monkeypatch.setattr(py7zr.compressor.ZstdDecompressor, "decompress", out_of_memory)
        with _no_fd_or_thread_leak(), pytest.raises(MemoryError):
            self._walk(build_7z_py7zr(self._TEXT, flavor="zstd"))

    def test_ppmd_memory_error_is_archive_read_error(self, monkeypatch):
        """pyppmd's model is allocated at build time; a MemoryError from decode is the body.

        Simulated rather than a real corrupt body: pyppmd can also segfault
        on corrupt input, which would take the test run down with it.
        """

        def corrupt_body(self, data, max_length=-1):
            raise MemoryError

        monkeypatch.setattr(py7zr.compressor.PpmdDecompressor, "decompress", corrupt_body)
        with _no_fd_or_thread_leak(), pytest.raises(ArchiveReadError, match="PPMd") as exc_info:
            self._walk(build_7z_py7zr(self._TEXT, flavor="ppmd"))
        assert type(exc_info.value.cause) is MemoryError


class TestPipeSink:
    """The parts of the Py7zIO ABC py7zr's decode path never calls."""

    def test_readback_methods_are_inert(self):
        read_fd, write_fd = os.pipe()
        try:
            sink = _PipeSink(write_fd)
            assert sink.write(b"abc") == 3
            assert sink.size() == 3
            assert sink.read() == b""
            assert sink.seek(0) == 0
            sink.flush()
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_close_write_end_is_idempotent(self):
        read_fd, write_fd = os.pipe()
        sink = _PipeSink(write_fd)
        sink.close_write_end()
        # A second close must not close whatever fd number got reused.
        reused_read, reused_write = os.pipe()
        try:
            sink.close_write_end()
            assert not _fd_closed(reused_read)
            assert not _fd_closed(reused_write)
        finally:
            for fd in (read_fd, reused_read, reused_write):
                os.close(fd)


# A Delta(1) coder alone (property byte = dist - 1): no terminal LZMA filter,
# so stdlib lzma refuses it and the native decoder is what makes it
# decodable at all. Delta(1) decodes to a running sum.
_DELTA_ONLY = [{"method": b"\x03", "numinstreams": 1, "numoutstreams": 1, "properties": b"\x00"}]
# A two-input coder: neither py7zr's lzma translation nor the native
# builder handles one, so the original error must surface.
_MULTI_STREAM = [{"method": b"\x03", "numinstreams": 2, "numoutstreams": 1}]


class TestNativeDecoderFallback:
    """The load-time wrapper around py7zr's ``_get_lzma_decompressor``.

    The end-to-end case (a real Copy + Delta archive) needs 7-Zip to build
    — py7zr cannot write that chain — so it lives in the ``cli-copy_delta``
    round trip. These pin the wrapper's three outcomes without the CLI.
    """

    _get = staticmethod(py7zr.compressor.SevenZipDecompressor._get_lzma_decompressor)

    def test_install_is_idempotent(self):
        before = py7zr.compressor.SevenZipDecompressor._get_lzma_decompressor
        _install_native_decoder_fallback()
        assert py7zr.compressor.SevenZipDecompressor._get_lzma_decompressor is before

    def test_chain_stdlib_rejects_gets_the_native_decoder(self):
        chain = self._get(None, _DELTA_ONLY, 4)
        assert chain.decompress(b"\x01\x01\x01\x01") == b"\x01\x02\x03\x04"

    def test_chain_nobody_can_decode_reraises_the_original(self):
        with pytest.raises(UnsupportedCompressionMethodError):
            self._get(None, _MULTI_STREAM, 4)


class TestSeekableS3ObjectRetry:
    """SeekableS3Object._ranged_get retries on transient errors.

    Real failures we want to survive: one stalled ranged GET out of
    ~3000 on a 3 GB .7z walk shouldn't drop the whole archive. The
    retry covers ReadTimeoutError and the connection-class botocore
    exceptions.
    """

    def _client_with_failure_schedule(self, body: bytes, *, failures_per_call: list[int]):
        """A mocked client whose get_object follows a per-call failure schedule.

        Each entry in *failures_per_call* is the number of transient
        failures that must occur before that GET succeeds. So
        ``[2, 0]`` means: the first ``get_object`` is preceded by 2
        scheduled failures (3rd attempt succeeds), the second is
        unconditional success.

        head_object is unconditional so SeekableS3Object's __init__ can
        size the object before any retry behavior is exercised.
        """
        client = MagicMock()
        client.head_object.return_value = {"ContentLength": len(body)}
        remaining_failures = [failures_per_call.copy() if failures_per_call else []]
        current_budget = [0]

        def get_object(*, Bucket, Key, Range):  # noqa: N803
            # Start of a new logical GET: refill the budget from the schedule.
            if current_budget[0] == 0 and remaining_failures[0]:
                current_budget[0] = remaining_failures[0].pop(0)
            if current_budget[0] > 0:
                current_budget[0] -= 1
                raise botocore.exceptions.ReadTimeoutError(
                    endpoint_url="https://test.example/" + Key
                )
            start, end = Range.removeprefix("bytes=").split("-")
            slice_bytes = body[int(start) : int(end) + 1]
            mock_resp = MagicMock()
            mock_resp.read.return_value = slice_bytes
            return {"Body": mock_resp}

        client.get_object.side_effect = get_object
        return client

    @pytest.mark.parametrize(
        ("body", "failures_per_call", "tail_prefetch_bytes", "read_back", "expected_sleeps"),
        [
            (
                b"abcdefghij" * 1000,  # 10 KB
                [2],
                # tail_prefetch_bytes=0 so init does no GETs and the test's
                # failure budget is fully consumed by the explicit _fetch.
                # First 2 attempts fail; the 3rd succeeds → two sleeps.
                0,
                lambda obj, body: (obj._fetch(0, len(body)), body),
                [0, 0],
            ),
            (
                b"y" * 5000,
                [1],
                # The init-time tail prefetch is the same shape of ranged GET
                # and equally vulnerable, so the retry must cover it too. It is
                # the FIRST get_object call: fail once, succeed on the retry.
                1000,
                lambda obj, body: (obj._tail_bytes, body[-1000:]),
                [0],
            ),
        ],
        ids=["explicit_fetch", "tail_prefetch"],
    )
    def test_recovers_after_transient_read_timeouts(
        self, monkeypatch, body, failures_per_call, tail_prefetch_bytes, read_back, expected_sleeps
    ):
        # Skip the real sleep — we just need to assert it was called the
        # right number of times.
        sleep_calls: list[float] = []
        monkeypatch.setattr("s3_archive.seekable.time.sleep", sleep_calls.append)

        client = self._client_with_failure_schedule(body, failures_per_call=failures_per_call)
        obj = SeekableS3Object(
            client, "b", "k", tail_prefetch_bytes=tail_prefetch_bytes, retry_delay_s=0
        )

        got, want = read_back(obj, body)
        assert got == want
        # One sleep per retry.
        assert sleep_calls == expected_sleeps

    def test_propagates_after_max_attempts(self, monkeypatch):
        monkeypatch.setattr("s3_archive.seekable.time.sleep", lambda _s: None)
        body = b"x" * 100
        # Failure budget exhausted: 3 attempts default, 5 scheduled
        # failures — every attempt fails.
        client = self._client_with_failure_schedule(body, failures_per_call=[5])
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=0, retry_delay_s=0)
        with pytest.raises(botocore.exceptions.ReadTimeoutError):
            obj._fetch(0, len(body))

    def test_non_transient_error_propagates_immediately(self, monkeypatch):
        sleep_calls: list[float] = []
        monkeypatch.setattr("s3_archive.seekable.time.sleep", sleep_calls.append)

        body = b"x" * 100
        client = MagicMock()
        client.head_object.return_value = {"ContentLength": len(body)}
        client.get_object.side_effect = botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "nope"}},
            operation_name="GetObject",
        )
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=0, retry_delay_s=0)
        with pytest.raises(botocore.exceptions.ClientError):
            obj._fetch(0, len(body))
        # No sleep — non-transient errors shouldn't burn time in retries.
        assert sleep_calls == []


class TestSeekableS3ObjectIfMatch:
    """if_match pins the HEAD + every ranged GET to one object generation."""

    def _client(self, body: bytes, etag: str = '"live-etag"') -> MagicMock:
        client = MagicMock()
        client.head_object.return_value = {"ContentLength": len(body), "ETag": etag}

        def get_object(**kwargs):
            start, end = kwargs["Range"].removeprefix("bytes=").split("-")
            mock_resp = MagicMock()
            mock_resp.read.return_value = body[int(start) : int(end) + 1]
            return {"Body": mock_resp}

        client.get_object.side_effect = get_object
        return client

    def test_head_etag_mismatch_raises_before_any_get(self):
        client = self._client(b"x" * 100, etag='"current"')
        with pytest.raises(ETagMismatchError, match="stale"):
            SeekableS3Object(client, "b", "k", if_match="stale")
        client.get_object.assert_not_called()

    def test_ranged_gets_carry_quoted_if_match(self):
        body = b"z" * 5000
        client = self._client(body, etag='"live-etag"')
        # Stripped form in, quoted form on the wire.
        obj = SeekableS3Object(client, "b", "k", if_match="live-etag", tail_prefetch_bytes=1000)
        assert obj._tail_bytes == body[-1000:]
        obj._fetch(0, 100)
        for call in client.get_object.call_args_list:
            assert call.kwargs["IfMatch"] == '"live-etag"'

    def test_no_if_match_sends_no_header(self):
        client = self._client(b"z" * 100)
        obj = SeekableS3Object(client, "b", "k", tail_prefetch_bytes=10)
        obj._fetch(0, 10)
        for call in client.get_object.call_args_list:
            assert "IfMatch" not in call.kwargs

    def test_iter_seven_z_members_threads_if_match_to_head_check(self):
        """iter_seven_z_members(if_match=...) fails fast on a changed object.

        The mismatch surfaces on first next() (generator body starts
        there), before any member bytes are pulled.
        """
        client = self._client(b"7z-ish bytes", etag='"current"')
        with pytest.raises(ETagMismatchError):
            next(iter_seven_z_members(client, "b", "k.7z", if_match="stale"))
        client.get_object.assert_not_called()


def _fd_closed(fd: int) -> bool:
    """True when *fd* is no longer an open file descriptor."""
    try:
        os.fstat(fd)
    except OSError:
        return True
    return False


def _finished_thread() -> threading.Thread:
    """A thread that has already run to completion (``is_alive()`` False)."""
    thread = threading.Thread(target=lambda: None)
    thread.start()
    thread.join()
    return thread


def _queue_member(meta_queue: queue.Queue, name: str, payload: bytes) -> int:
    """Push one ``(name, read_fd)`` pair carrying *payload*, as the factory would."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, payload)
    os.close(write_fd)
    meta_queue.put((name, read_fd))
    return read_fd


class TestDrainMembers:
    """The queue-consumer half of ``iter_seven_z_members``.

    These drive :func:`_drain_members` and :func:`_abandon_extract` off a
    hand-fed metadata queue instead of a real archive, so the pipe-fd and
    worker-error contract stays covered on machines without the ``7z``
    CLI that the archive-fixture tests above need.
    """

    def test_yields_members_and_closes_each_fd_in_turn(self):
        meta_queue: queue.Queue = queue.Queue()
        fd_a = _queue_member(meta_queue, "a.txt", b"alpha")
        fd_b = _queue_member(meta_queue, "b.txt", b"bravo")
        meta_queue.put(_SENTINEL)

        members = _drain_members(meta_queue, {"a.txt": 5, "b.txt": 5}, [], _finished_thread(), {})

        first = next(members)
        assert (first.name, first.size) == ("a.txt", 5)
        assert first.read_all() == b"alpha"
        assert not _fd_closed(fd_a)

        second = next(members)
        # Pulling the next member is what closes the previous read fd.
        assert _fd_closed(fd_a)
        assert second.read_all() == b"bravo"

        with pytest.raises(StopIteration):
            next(members)
        assert _fd_closed(fd_b)

    def test_size_falls_back_to_zero_for_unknown_member(self):
        meta_queue: queue.Queue = queue.Queue()
        _queue_member(meta_queue, "surprise.txt", b"")
        meta_queue.put(_SENTINEL)

        (member,) = list(_drain_members(meta_queue, {}, [], _finished_thread(), {}))
        assert member.size == 0

    def test_decode_error_from_worker_becomes_archive_read_error(self):
        meta_queue: queue.Queue = queue.Queue()
        meta_queue.put(_SENTINEL)
        cause = ArchiveError("bad folder")

        with pytest.raises(ArchiveReadError) as exc_info:
            list(_drain_members(meta_queue, {}, [cause], _finished_thread(), {}))
        assert exc_info.value.cause is cause
        assert exc_info.value.__cause__ is cause

    def test_other_worker_error_propagates_unwrapped(self):
        meta_queue: queue.Queue = queue.Queue()
        meta_queue.put(_SENTINEL)

        with pytest.raises(RuntimeError, match="worker blew up"):
            list(
                _drain_members(
                    meta_queue, {}, [RuntimeError("worker blew up")], _finished_thread(), {}
                )
            )

    def test_abandon_closes_open_fd_when_worker_already_finished(self):
        read_fd, write_fd = os.pipe()
        os.close(write_fd)

        _abandon_extract(queue.Queue(), _finished_thread(), read_fd)
        assert _fd_closed(read_fd)

    def test_abandon_drains_queued_fds_and_joins_a_live_worker(self):
        meta_queue: queue.Queue = queue.Queue()
        # The fd of the member already handed to the caller.
        open_fd, write_fd = os.pipe()
        os.close(write_fd)
        # A member the factory queued that will never be yielded.
        leftover_fd = _queue_member(meta_queue, "leftover.txt", b"never read")

        def _worker() -> None:
            meta_queue.put(_SENTINEL)
            # Stand in for py7zr still writing: stay alive until the
            # consumer has drained everything, so _abandon_extract takes
            # its live-worker path.
            while not meta_queue.empty():
                time.sleep(0.01)

        worker_thread = threading.Thread(target=_worker, daemon=True)
        worker_thread.start()

        _abandon_extract(meta_queue, worker_thread, open_fd)

        assert _fd_closed(open_fd)
        assert _fd_closed(leftover_fd)
        assert not worker_thread.is_alive()


class TestCrcTolerance:
    """A member whose bytes fail its CRC-32: strict raises ArchiveReadError
    as before; tolerant hands on the decoded bytes, records the mismatch,
    and keeps extracting — including later members of the same Folder."""

    _BAD = incompressible_bytes(4000, seed=7)
    _FILES = {"a.txt": b"alpha " * 300, "bad.bin": _BAD, "z.txt": b"zulu " * 300}

    def _corrupt(self, flavor: str) -> tuple[bytes, bytes]:
        blob = bytearray(build_7z_py7zr(self._FILES, flavor=flavor))
        at = blob.index(self._BAD[100:140])
        blob[at] ^= 0xFF
        decoded = bytearray(self._BAD)
        decoded[100] ^= 0xFF
        return bytes(blob), bytes(decoded)

    def test_strict_raises(self, s3_client):
        blob, _ = self._corrupt("copy")
        s3_client.put_object(Bucket="src-bucket", Key="bad.7z", Body=blob)
        with pytest.raises(ArchiveReadError, match="7z decode failed"):
            for member in iter_archive_members(s3_client, "src-bucket", "bad.7z", "7z"):
                member.drain()

    def test_tolerant_keeps_bytes_and_continues_in_the_same_folder(self, s3_client):
        blob, decoded = self._corrupt("copy")  # one Folder holds all three members
        s3_client.put_object(Bucket="src-bucket", Key="bad.7z", Body=blob)
        bodies, integrity = {}, {}
        for member in iter_archive_members(
            s3_client, "src-bucket", "bad.7z", "7z", tolerate_integrity_errors=True
        ):
            bodies[member.name] = member.read_all()
            integrity[member.name] = member.integrity
        assert bodies == {**self._FILES, "bad.bin": decoded}
        assert integrity == {
            "a.txt": None,
            "bad.bin": IntegrityMismatch(
                check="CRC-32",
                expected_crc32=zlib.crc32(self._BAD),
                actual_crc32=zlib.crc32(decoded),
                expected_size=len(self._BAD),
                actual_size=len(self._BAD),
            ),
            "z.txt": None,
        }

    def test_tolerant_codec_failure_stays_fatal(self, s3_client):
        """Corrupt LZMA2 has no decoded bytes to hand on: fatal either way."""
        blob = bytearray(build_7z_py7zr(self._FILES, flavor="solid"))
        blob[32 + 200] ^= (
            0xFF  # inside the packed stream (it starts after the 32-byte signature header)
        )
        s3_client.put_object(Bucket="src-bucket", Key="bad.7z", Body=bytes(blob))
        with pytest.raises(ArchiveReadError):
            for member in iter_archive_members(
                s3_client, "src-bucket", "bad.7z", "7z", tolerate_integrity_errors=True
            ):
                member.drain()
