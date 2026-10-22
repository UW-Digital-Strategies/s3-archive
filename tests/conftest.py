"""Shared pytest fixtures: in-memory S3 (moto) + archive-builder helpers."""

import io
import random
import shutil
import struct
import subprocess
import tarfile
import tempfile
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path

import boto3
import py7zr
import pytest
from moto import mock_aws
from moto.server import ThreadedMotoServer
from py7zr.properties import FILTER_DEFLATE64

from s3_archive.s3_client import _reset_client_cache


@pytest.fixture(autouse=True)
# An `autouse` fixture is never requested by name, and the check never reads
# the decorator — false positive; evidence and re-evaluate-by in the docstring.
# pysmelly: ignore[orphaned-test-helpers]  # noqa: ERA001
def _clear_client_cache():
    """Drop any cached boto3 clients between tests.

    `s3_archive.s3_client` keeps a module-level dict of profile → client
    for the lifetime of the process. Without this fixture a client
    built against one test's moto context would leak into the next,
    pointing at a torn-down endpoint and producing baffling failures.

    **pysmelly false positive** (`orphaned-test-helpers`). That check
    decides a fixture is unused purely by whether its name appears as a
    parameter of some function in a test file; it never reads the
    decorator's `autouse` argument. An `autouse=True` fixture is never
    requested by name — that is the whole point of autouse. Evidence:
    `pytest tests/test_paths.py --setup-show` prints
    `SETUP F _clear_client_cache` / `TEARDOWN F _clear_client_cache`
    for all 21 collected tests in that module.
    re-evaluate-by: 2026-11 review.
    """
    _reset_client_cache()
    yield
    _reset_client_cache()


@pytest.fixture
def aws_creds(monkeypatch):
    """Set bogus AWS credentials so moto + boto3 don't try the real chain."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_client(aws_creds):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="src-bucket")
        client.create_bucket(Bucket="dest-bucket")
        yield client


@pytest.fixture
def cross_env_real_endpoints(tmp_path, monkeypatch):
    """Two real moto-server endpoints + two `~/.s3cfg-*` files.

    Exercises the real two-client path end-to-end: each profile resolves
    via `~/.s3cfg-<name>` to a distinct boto3 client pointed at a
    different `ThreadedMotoServer` instance. Considerably slower than the
    plain :func:`s3_client` moto fixture (two extra HTTP servers per test)
    — use sparingly, one acceptance test per module is enough. Modules
    that need a cheaper cross-endpoint stand-in patch `client_for`
    locally; see `patched_client` in test_cli.py.

    Yields a dict::

        {
            "src": {"profile": "src-env", "client": <boto3>, "bucket": "src-bucket"},
            "dst": {"profile": "dst-env", "client": <boto3>, "bucket": "dst-bucket"},
        }

    The two endpoints have *distinct* bucket namespaces so a test that
    accidentally talks to the wrong endpoint fails loudly (NoSuchBucket).
    """
    # Bind to ephemeral ports so concurrent test runs don't collide.
    src_server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    dst_server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    src_server.start()
    dst_server.start()
    try:
        src_port = src_server._server.socket.getsockname()[1]
        dst_port = dst_server._server.socket.getsockname()[1]
        src_endpoint = f"http://127.0.0.1:{src_port}"
        dst_endpoint = f"http://127.0.0.1:{dst_port}"

        # HOME → tmp so we don't touch the developer's real config.
        # We bypass ~/.s3cfg-<name> entirely by patching client_for to
        # a static dict of pre-built clients; that's enough to exercise
        # the dual-endpoint code path. End-to-end resolver coverage
        # lives in test_s3_client.py.
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)

        src_client = boto3.client(
            "s3",
            endpoint_url=src_endpoint,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        dst_client = boto3.client(
            "s3",
            endpoint_url=dst_endpoint,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )

        # Create distinct buckets on each side so a cross-talk bug
        # (uploading to the wrong endpoint) fails with NoSuchBucket.
        src_client.create_bucket(Bucket="src-bucket")
        dst_client.create_bucket(Bucket="dst-bucket")

        _reset_client_cache()
        clients_by_profile = {"src-env": src_client, "dst-env": dst_client}

        def _client_for(profile):
            key = profile if profile is not None else "default"
            if key not in clients_by_profile:
                raise KeyError(f"unexpected profile {key!r} in test")
            return clients_by_profile[key]

        monkeypatch.setattr("s3_archive.s3_client.client_for", _client_for)
        monkeypatch.setattr("s3_archive.cli.client_for", _client_for)

        yield {
            "src": {"profile": "src-env", "client": src_client, "bucket": "src-bucket"},
            "dst": {"profile": "dst-env", "client": dst_client, "bucket": "dst-bucket"},
        }
    finally:
        src_server.stop()
        dst_server.stop()


# ---------------------------------------------------------------------------
# Destination inspection
# ---------------------------------------------------------------------------


def _extracted_keys(s3, bucket, prefix):
    """Every key under *prefix*, sorted — what an extract run actually wrote.

    Lives here because test_cli.py and test_extract.py had byte-identical
    private copies. Paginated because a resume test can write more objects
    than a single ListObjectsV2 page returns.
    """
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


# ---------------------------------------------------------------------------
# Archive construction helpers
# ---------------------------------------------------------------------------


def build_tar(
    files: dict[str, bytes],
    *,
    mode: str = "w:gz",
    wrap_prefix: str = "",
) -> bytes:
    """Serialize *files* as a tar archive in memory.

    *mode* is passed directly to :func:`tarfile.open` and selects the
    compression — ``"w"`` for plain tar, ``"w:gz"`` / ``"w:bz2"`` / ``"w:xz"``
    for the compressed variants.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode=mode) as tar:
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            info = tarfile.TarInfo(name=member_name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def build_tar_gz(files: dict[str, bytes], *, wrap_prefix: str = "") -> bytes:
    """Shortcut for :func:`build_tar` with ``mode="w:gz"``."""
    return build_tar(files, mode="w:gz", wrap_prefix=wrap_prefix)


def build_tar_xz_multiblock(
    files: dict[str, bytes], *, block_size: str = "1MiB", wrap_prefix: str = ""
) -> bytes:
    """Serialize *files* as a **multi-block** ``.tar.xz`` via the ``xz`` CLI.

    stdlib ``lzma`` (``build_tar(mode="w:xz")``) emits a *single* xz block,
    which resume refuses — so the resumable-xz tests need a multi-block
    archive. ``xz --block-size`` splits the stream into independently
    seekable blocks; feed it enough incompressible data (see
    :func:`incompressible_bytes`) that the split actually happens. Skipped
    when the ``xz`` CLI isn't on PATH.
    """
    if shutil.which("xz") is None:
        pytest.skip("xz CLI not installed; skipping multi-block .tar.xz fixture")
    inner = build_tar(files, mode="w", wrap_prefix=wrap_prefix)  # uncompressed tar
    # cmd is built from constants; not user input.
    cmd = ["xz", "-z", f"--block-size={block_size}", "-c", "-"]
    proc = subprocess.run(cmd, input=inner, capture_output=True, check=True)  # noqa: S603
    return proc.stdout


def incompressible_bytes(n: int, *, seed: int) -> bytes:
    """*n* deterministic pseudo-random bytes that gzip can't shrink.

    Used by the gzip ``--resume`` tests: they need archives whose
    *compressed* size exceeds :class:`SeekableS3Object`'s tail prefetch, so
    that seeking past already-done members demonstrably avoids re-reading
    the early source. Seeded so a run is reproducible.
    """
    return random.Random(seed).randbytes(n)  # noqa: S311 - test fixture, not crypto


def build_zip(files: dict[str, bytes], *, wrap_prefix: str = "") -> bytes:
    """Serialize *files* as a zip archive in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            zf.writestr(member_name, content)
    return buf.getvalue()


def build_zip_with_corrupt_member(
    files: dict[str, bytes], corrupt: str, *, declared_size_delta: int = 0
) -> bytes:
    """A deflated zip in which member *corrupt* fails its integrity check.

    By default *corrupt* is written stored (so a damaged byte still
    decodes) and its first data byte is XOR-flipped afterwards, leaving
    the CRC-32 in both the local header and the central directory
    describing the original bytes: the shape of a bit-rotted member in an
    otherwise good archive. The expected CRC is ``zlib.crc32(files[corrupt])``;
    the decoded one is that of the flipped bytes.

    *declared_size_delta*, when non-zero, leaves the bytes alone and adds
    the delta to the member's declared uncompressed size in both headers
    instead (the member is deflated, so its compressed framing still
    holds): a size-only mismatch.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            stored = name == corrupt and declared_size_delta == 0
            zf.writestr(name, content, compress_type=zipfile.ZIP_STORED if stored else None)
    blob = bytearray(buf.getvalue())
    with zipfile.ZipFile(io.BytesIO(bytes(blob))) as zf:
        info = zf.getinfo(corrupt)
        infos = zf.infolist()
    lfh = info.header_offset
    if declared_size_delta == 0:
        name_len, extra_len = struct.unpack("<HH", blob[lfh + 26 : lfh + 30])
        blob[lfh + 30 + name_len + extra_len] ^= 0xFF
        return bytes(blob)
    new_size = info.file_size + declared_size_delta
    struct.pack_into("<I", blob, lfh + 22, new_size)  # local header uncompressed size
    # Walk the central directory (start offset from the comment-less EOCD).
    cd_at = struct.unpack("<I", blob[-6:-2])[0]
    for entry in infos:
        name_len, extra_len, comment_len = struct.unpack("<HHH", blob[cd_at + 28 : cd_at + 34])
        if entry.filename == corrupt:
            struct.pack_into("<I", blob, cd_at + 24, new_size)  # CD uncompressed size
            break
        cd_at += 46 + name_len + extra_len + comment_len
    return bytes(blob)


def build_stored_dd_zip(
    members: dict[str, bytes],
    *,
    dd_names: set[str] | None = None,
    dos_datetime: tuple[int, int, int, int, int, int] | None = None,
) -> bytes:
    """Hand-pack a zip with *stored* members, optionally data-descriptor-shaped.

    Members in *dd_names* get flag bit 3 (data descriptor) and ZERO
    sizes in the local file header — the shape SwissTransfer /
    Drive-style on-the-fly generators produce, which a forward-only
    reader cannot walk. ``dd_names=None`` means ALL file members get
    that shape. Members not in *dd_names* are written as plain stored
    entries with real local-header sizes (forward-streamable), so a
    mixed dict exercises the mid-archive streaming→seekable handoff.
    Names ending in ``/`` become directory entries (empty, never DD).

    *dos_datetime*, when given as ``(year, month, day, hour, minute,
    second)``, is stamped into every member's mod time/date fields
    (2-second DOS granularity — use an even second). The default zeros
    mean "no mtime", matching generators that don't write one.

    Names are written as raw UTF-8 with flag bit 11 UNSET — the
    flagless-UTF-8 shape many zip tools produce, which exercises the
    CD/LFH name-decode parity path for non-ASCII names.

    Stdlib ``zipfile`` always writes real LFH sizes, hence the hand
    ``struct.pack``. Everything is stored (compression=0) to keep the
    packing simple; the central directory carries the true sizes either
    way.
    """
    if dos_datetime is None:
        dos_time, dos_date = 0, 0
    else:
        year, month, day, hour, minute, second = dos_datetime
        dos_time = (hour << 11) | (minute << 5) | (second // 2)
        dos_date = ((year - 1980) << 9) | (month << 5) | day
    buf = io.BytesIO()
    cd_records = []
    for name, raw_body in members.items():
        name_b = name.encode("utf-8")
        is_dir = name.endswith("/")
        body = b"" if is_dir else raw_body
        with_dd = not is_dir and (dd_names is None or name in dd_names)
        offset = buf.tell()
        crc = zlib.crc32(body) & 0xFFFFFFFF
        flags = 0x0008 if with_dd else 0
        lfh_crc = 0 if with_dd else crc
        lfh_size = 0 if with_dd else len(body)
        buf.write(
            struct.pack(
                "<IHHHHHIIIHH",
                0x04034B50,  # local file header signature
                20,  # version needed
                flags,
                0,  # compression: stored
                dos_time,
                dos_date,
                lfh_crc,
                lfh_size,  # compressed size (0 under DD — the unstreamable part)
                lfh_size,  # uncompressed size
                len(name_b),
                0,  # extra length
            )
        )
        buf.write(name_b)
        buf.write(body)
        if with_dd:
            # Data descriptor (with the optional PK\x07\x08 signature).
            buf.write(struct.pack("<IIII", 0x08074B50, crc, len(body), len(body)))
        cd_records.append((name_b, flags, crc, len(body), offset))

    cd_start = buf.tell()
    for name_b, flags, crc, size, offset in cd_records:
        buf.write(
            struct.pack(
                "<IHHHHHHIIIHHHHHII",
                0x02014B50,  # central directory header signature
                20,  # version made by
                20,  # version needed
                flags,
                0,  # compression: stored
                dos_time,
                dos_date,
                crc,
                size,  # compressed size — the CD has the truth
                size,  # uncompressed size
                len(name_b),
                0,  # extra length
                0,  # comment length
                0,  # disk number
                0,  # internal attrs
                0,  # external attrs
                offset,
            )
        )
        buf.write(name_b)
    cd_size = buf.tell() - cd_start
    buf.write(
        struct.pack(
            "<IHHHHIIH",
            0x06054B50,  # end of central directory signature
            0,
            0,
            len(cd_records),
            len(cd_records),
            cd_size,
            cd_start,
            0,  # comment length
        )
    )
    return buf.getvalue()


# The 7-Zip CLI ships under two names: Homebrew's `sevenzip` formula (the
# install CLAUDE.md names) and 7-Zip's own Linux/macOS builds install `7zz`;
# p7zip and most Linux distro packages install `7z`. Prefer `7zz` — it is
# the current upstream binary — and accept `7z`. Resolved once at import.
SEVEN_Z_CLI_NAMES: tuple[str, ...] = ("7zz", "7z")
SEVEN_Z_CLI: str | None = next(
    (name for name in SEVEN_Z_CLI_NAMES if shutil.which(name) is not None), None
)


# 7z archive flavors that exercise different code paths in any prospective
# streaming reader. See docs/ARCHITECTURE.md § ".7z — the exception that
# proves the rule" for the design context.
SEVEN_Z_FLAVORS: dict[str, list[str]] = {
    "solid": [],
    "nonsolid": ["-ms=off"],
    "plain_header": ["-mhc=off"],
    "solid_bcj": ["-m0=BCJ", "-m1=LZMA2"],
    # `Copy + Delta` chain: no terminal compression filter, so stdlib lzma
    # rejects py7zr's translated FORMAT_RAW chain. Exercises the
    # native-decoder fallback in s3_archive.native_decoders.
    "copy_delta": ["-mx=0", "-mf=Delta:2"],
}


def build_7z(
    files: dict[str, bytes],
    *,
    flavor: str = "solid",
    wrap_prefix: str = "",
) -> bytes:
    """Serialize *files* as a .7z archive by shelling out to the 7-Zip CLI.

    *flavor* selects a preset flag set keyed in :data:`SEVEN_Z_FLAVORS`:

    - ``"solid"`` — default (solid + LZMA2 + encoded header); the common
      Preservation case.
    - ``"nonsolid"`` — ``-ms=off``; one Folder per file.
    - ``"plain_header"`` — ``-mhc=off``; non-encoded header (simpler
      parse path for comparison).
    - ``"solid_bcj"`` — ``-m0=BCJ -m1=LZMA2``; filter chain with
      interleaved pack streams.

    The 7z format can't be serialized in memory the way tar and zip can
    (the StartHeader at the front references a header at the end), so
    this helper writes the members to a temp dir, invokes ``7zz a`` (or
    ``7z a`` — see :data:`SEVEN_Z_CLI`), and returns the resulting
    archive bytes. Tests that call it are skipped if neither CLI name is
    on ``PATH``.
    """
    if SEVEN_Z_CLI is None:
        pytest.skip("7-Zip CLI (7zz or 7z) not installed; skipping .7z fixture")
    if flavor not in SEVEN_Z_FLAVORS:
        raise ValueError(f"Unknown 7z flavor {flavor!r}; expected one of {sorted(SEVEN_Z_FLAVORS)}")

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        src = tmpdir / "src"
        src.mkdir()
        member_names: list[str] = []
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            target = src / member_name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            member_names.append(member_name)

        archive = tmpdir / "out.7z"
        cmd = [SEVEN_Z_CLI, "a", *SEVEN_Z_FLAVORS[flavor], str(archive), *member_names]
        # cmd is built from constants and dict keys controlled by the test;
        # not user input.
        subprocess.run(cmd, check=True, capture_output=True, cwd=src)  # noqa: S603
        return archive.read_bytes()


@dataclass(frozen=True)
class Py7zrFlavor:
    """How :func:`build_7z_py7zr` writes one flavor with py7zr's own writer.

    - *filters* — the coder chain, in py7zr's ``filters=`` form. Always
      given explicitly: py7zr's default is BCJ + LZMA2, which is not what
      the CLI picks for these members.
    - *folder_per_member* — write each member in its own append session,
      which py7zr closes into its own Folder (its writer has no
      non-solid switch; ``mode="a"`` is the only way to get >1 Folder).
    - *encoded_header* — False writes the header plain (``-mhc=off``).
    """

    filters: list[dict[str, int]]
    folder_per_member: bool = False
    encoded_header: bool = True


_PY7ZR_LZMA2: list[dict[str, int]] = [{"id": py7zr.FILTER_LZMA2}]

# The SEVEN_Z_FLAVORS py7zr can write with no CLI present, keyed the same
# so a test can parametrize over both builders. Checked structurally
# against 7zz's output (Folder count, coder method IDs, bind pairs, header
# encoding): solid, nonsolid, plain_header and solid_bcj match. Two
# deliberate gaps, both reasons the CLI builder stays:
#
# - ``copy_delta`` is absent. py7zr's writer refuses a Delta chain with no
#   terminal compressor (``UnsupportedCompressionMethodError: Unknown
#   combination of methods``), so the archive that drives the
#   native-decoder fallback end to end can only come from the CLI.
# - py7zr writes an empty member as a zero-length *substream*; 7-Zip marks
#   it an empty stream (no pack stream at all), which py7zr's reader
#   extracts on a separate path. So ``nonsolid`` here has a Folder for the
#   empty member too, and only the CLI builds the empty-stream shape.
#
# ``copy`` has no CLI counterpart in the table: stored bytes, so a test can
# corrupt a member's body at a known offset and get a CRC failure rather
# than whatever a mangled LZMA stream happens to do. ``copy_nonsolid`` is
# the same with a Folder per member, which ``--resume`` requires. Nor do the per-codec
# flavors below it: one per compression method py7zr can write, so every
# codec's decoder is exercised on a round trip and on a corrupt body.
PY7ZR_FLAVORS: dict[str, Py7zrFlavor] = {
    "solid": Py7zrFlavor(_PY7ZR_LZMA2),
    "nonsolid": Py7zrFlavor(_PY7ZR_LZMA2, folder_per_member=True),
    "plain_header": Py7zrFlavor(_PY7ZR_LZMA2, encoded_header=False),
    "solid_bcj": Py7zrFlavor([{"id": py7zr.FILTER_X86}, {"id": py7zr.FILTER_LZMA2}]),
    "copy": Py7zrFlavor([{"id": py7zr.FILTER_COPY}]),
    "copy_nonsolid": Py7zrFlavor([{"id": py7zr.FILTER_COPY}], folder_per_member=True),
    "lzma1": Py7zrFlavor([{"id": py7zr.FILTER_LZMA}]),
    "deflate": Py7zrFlavor([{"id": py7zr.FILTER_DEFLATE}]),
    "deflate64": Py7zrFlavor([{"id": FILTER_DEFLATE64}]),
    "bzip2": Py7zrFlavor([{"id": py7zr.FILTER_BZIP2}]),
    "zstd": Py7zrFlavor([{"id": py7zr.FILTER_ZSTD}]),
    "brotli": Py7zrFlavor([{"id": py7zr.FILTER_BROTLI}]),
    "ppmd": Py7zrFlavor([{"id": py7zr.FILTER_PPMD}]),
}


def build_7z_py7zr(
    files: dict[str, bytes],
    *,
    flavor: str = "solid",
    wrap_prefix: str = "",
) -> bytes:
    """Serialize *files* as a .7z archive in memory with py7zr's writer.

    The no-CLI counterpart of :func:`build_7z` — same signature, *flavor*
    keyed in :data:`PY7ZR_FLAVORS` (see there for what it can and cannot
    reproduce). Never skips, so the 7z reader is exercised on a machine
    with no 7-Zip installed.
    """
    if flavor not in PY7ZR_FLAVORS:
        raise ValueError(
            f"Unknown py7zr flavor {flavor!r}; expected one of {sorted(PY7ZR_FLAVORS)}"
        )
    spec = PY7ZR_FLAVORS[flavor]
    members = [
        (f"{wrap_prefix}/{name}" if wrap_prefix else name, body) for name, body in files.items()
    ]
    # One session writes every member into one Folder; folder_per_member
    # opens a fresh append session per member instead.
    sessions = [[member] for member in members] if spec.folder_per_member else [members]

    buf = io.BytesIO()
    for index, session in enumerate(sessions):
        buf.seek(0)
        with py7zr.SevenZipFile(buf, "a" if index else "w", filters=spec.filters) as sz:
            if not spec.encoded_header:
                sz.set_encoded_header_mode(False)
            for name, body in session:
                sz.writestr(body, name)
    return buf.getvalue()
