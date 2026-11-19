"""Tests for streaming extract from S3 to S3."""

import io
import json
import logging
import zlib

import pytest
import zstandard

from s3_archive import resume
from s3_archive.exceptions import (
    ArchiveReadError,
    ResumeUnsupportedError,
    UnsafeArchiveMemberError,
    UnsupportedArchiveFormatError,
)
from s3_archive.extract import ExtractEvent, extract, extract_with_report
from s3_archive.gzip_seek import export_index_bytes, open_tar_gz_seekable
from s3_archive.seekable import _BUFFER_SIZE, SeekableS3Object

from .conftest import (
    SEVEN_Z_FLAVORS,
    _extracted_keys,
    build_7z,
    build_7z_py7zr,
    build_tar,
    build_tar_gz,
    build_tar_xz_multiblock,
    build_zip,
    build_zip_with_corrupt_member,
    incompressible_bytes,
)


@pytest.fixture
def sample_files():
    return {"a.txt": b"hello\n", "sub/b.txt": b"world\n"}


def _body(s3, bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


class TestExtractTarGz:
    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.tar.gz", "dest-bucket", "out/", "tar.gz"
        )

        assert set(members) == set(sample_files)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/a.txt", "out/sub/b.txt"]
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"

    def test_dry_run_uploads_nothing(self, s3_client, sample_files):
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        members = extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            dry_run=True,
        )

        assert set(members) == set(sample_files)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []

    def test_empty_prefix(self, s3_client, sample_files):
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="archive.tar.gz", Body=archive)

        extract(s3_client, s3_client, "src-bucket", "archive.tar.gz", "dest-bucket", "", "tar.gz")
        keys = _extracted_keys(s3_client, "dest-bucket", "")
        assert "a.txt" in keys


class TestExtractTar:
    """Plain (uncompressed) tar — tarfile.open mode 'r|'."""

    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.tar", "dest-bucket", "out/", "tar"
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"


class TestExtractTarBz2:
    """bzip2-compressed tar — exercises the dispatch into mode 'r|bz2'."""

    def test_round_trip(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w:bz2")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.bz2", Body=archive)

        members = extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.bz2",
            "dest-bucket",
            "out/",
            "tar.bz2",
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"


class TestExtractTarZst:
    """zstandard-compressed tar — wired via members.py's ZstdDecompressor path."""

    def test_round_trip(self, s3_client, sample_files):
        inner = build_tar(sample_files, mode="w")
        archive = zstandard.ZstdCompressor().compress(inner)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.zst", Body=archive)

        members = extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.zst",
            "dest-bucket",
            "out/",
            "tar.zst",
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"


class TestExtractZip:
    def test_round_trip(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.zip", "dest-bucket", "out/", "zip"
        )

        assert set(members) == set(sample_files)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/a.txt", "out/sub/b.txt"]
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"

    def test_dry_run_uploads_nothing(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        members = extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            dry_run=True,
        )
        assert set(members) == set(sample_files)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []


class TestExtractPathNormalization:
    """End-to-end: normalized member names drive the destination S3 keys."""

    def test_windows_zip_dest_keys_are_forward_slashed(self, s3_client):
        archive = build_zip({"Image repository\\UW26509z.tif": b"tiff"})
        s3_client.put_object(Bucket="src-bucket", Key="in/win.zip", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/win.zip", "dest-bucket", "out/", "zip"
        )

        assert members == ["Image repository/UW26509z.tif"]
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert keys == ["out/Image repository/UW26509z.tif"]
        # No literal backslash survives into the destination key.
        assert not any("\\" in k for k in keys)

    def test_dotdot_member_raises(self, s3_client):
        archive = build_tar({"../evil.txt": b"x"}, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/evil.tar", Body=archive)

        with pytest.raises(UnsafeArchiveMemberError):
            extract(s3_client, s3_client, "src-bucket", "in/evil.tar", "dest-bucket", "out/", "tar")

    def test_dotdot_member_collapses_with_fix_unsafe_paths(self, s3_client):
        archive = build_tar({"a/../safe.txt": b"payload"}, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/fix.tar", Body=archive)

        members = extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/fix.tar",
            "dest-bucket",
            "out/",
            "tar",
            fix_unsafe_paths=True,
        )

        assert members == ["safe.txt"]
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/safe.txt"]


class TestExtract7z:
    """py7zr-backed extract — see :mod:`s3_archive.seven_z`."""

    @pytest.mark.parametrize("flavor", sorted(SEVEN_Z_FLAVORS))
    def test_round_trip(self, s3_client, sample_files, flavor):
        archive = build_7z(sample_files, flavor=flavor)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.7z", Body=archive)

        members = extract(
            s3_client, s3_client, "src-bucket", "in/archive.7z", "dest-bucket", "out/", "7z"
        )

        assert set(members) == set(sample_files)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert "out/sub/b.txt" in keys
        assert _body(s3_client, "dest-bucket", "out/a.txt") == b"hello\n"
        assert _body(s3_client, "dest-bucket", "out/sub/b.txt") == b"world\n"


def test_unsupported_format_raises(s3_client):
    with pytest.raises(UnsupportedArchiveFormatError, match="Unsupported format"):
        extract(s3_client, s3_client, "src-bucket", "x", "dest-bucket", "", "rar")


class TestExtractProgressCallback:
    """The on_progress callback is invoked with structured events."""

    def test_emits_boundary_event_per_member_with_member_metadata(self, s3_client, sample_files):
        # Use a tar.gz so members come from the tar path (not 7z's
        # pipe-thread model) — gives a simple deterministic sequence.
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        events: list[ExtractEvent] = []
        extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            on_progress=events.append,
        )

        # One boundary event per member, in archive order.
        boundary = [e for e in events if e.bytes_transferred == 0]
        assert [e.member for e in boundary] == list(sample_files)
        # member_index counts up from 0
        assert [e.member_index for e in boundary] == list(range(len(sample_files)))
        # member_size carries the known uncompressed size when the archive
        # exposes it (tar does)
        for ev in boundary:
            assert ev.member_size == len(sample_files[ev.member])

    def test_byte_events_sum_to_each_member_size(self, s3_client):
        # Pick a member large enough that boto3's multipart machinery
        # emits at least one Callback invocation per upload.
        files = {"big.bin": b"x" * (8 * 1024 * 1024)}
        archive = build_tar_gz(files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        events: list[ExtractEvent] = []
        extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            on_progress=events.append,
        )

        # Sum of byte-transfer events per member equals that member's size.
        for member, content in files.items():
            transferred = sum(e.bytes_transferred for e in events if e.member == member)
            assert transferred == len(content)

    def test_dry_run_still_emits_boundary_events(self, s3_client, sample_files):
        # In dry_run mode no upload happens, but operators still benefit
        # from seeing what *would* be written; boundary events let the
        # UI render that list incrementally without buffering.
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar.gz", Body=archive)

        events: list[ExtractEvent] = []
        extract(
            s3_client,
            s3_client,
            "src-bucket",
            "in/archive.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            dry_run=True,
            on_progress=events.append,
        )

        # Only boundary events — no byte-transfer events when nothing is uploaded.
        assert all(e.bytes_transferred == 0 for e in events)
        assert {e.member for e in events} == set(sample_files)


class TestExtractDualEndpoint:
    """Real two-endpoint wiring via `cross_env_real_endpoints` (moto-server)."""

    def test_extracts_across_endpoints(self, cross_env_real_endpoints):
        src = cross_env_real_endpoints["src"]
        dst = cross_env_real_endpoints["dst"]

        files = {"a.txt": b"alpha\n", "sub/b.txt": b"beta\n"}
        src["client"].put_object(
            Bucket=src["bucket"], Key="in/archive.tar.gz", Body=build_tar_gz(files)
        )

        members = extract(
            src["client"],
            dst["client"],
            src["bucket"],
            "in/archive.tar.gz",
            dst["bucket"],
            "out/",
            "tar.gz",
        )

        assert set(members) == set(files)
        # Members land in the *destination* endpoint's bucket and not
        # the source endpoint — verify both ways to catch cross-talk.
        assert _extracted_keys(dst["client"], dst["bucket"], "out/") == [
            "out/a.txt",
            "out/sub/b.txt",
        ]
        assert _extracted_keys(src["client"], src["bucket"], "out/") == []


class _DyingUploadClient:
    """Delegate to a real client, but raise on the first ``upload_fileobj``.

    Simulates a run that dies partway (network drop, SIGKILL's moral
    equivalent) *after* the resume control marker was written, so a test can
    read the marker the interrupted run left behind.
    """

    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def upload_fileobj(self, fileobj, bucket, key, **kwargs):
        raise RuntimeError("connection died mid-upload")


class _UploadSpyClient:
    """Delegate to a real client, recording the Key of each upload_fileobj call.

    Lets a resume test assert *which* members were actually re-transferred
    (vs. skipped as already-present) without inspecting S3 timing.
    """

    def __init__(self, real):
        self._real = real
        self.uploaded_keys: list[str] = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def upload_fileobj(self, fileobj, bucket, key, **kwargs):
        self.uploaded_keys.append(key)
        return self._real.upload_fileobj(fileobj, bucket, key, **kwargs)


def _src_etag(s3, bucket, key):
    return s3.head_object(Bucket=bucket, Key=key)["ETag"]


def _write_matching_control(s3, *, src_bucket, src_key, dest_bucket, dest_prefix, fmt):
    """Write a control marker for the *actual* source ETag (a resumable run)."""
    etag = _src_etag(s3, src_bucket, src_key)
    ckey = resume.control_key(dest_prefix, etag)
    resume.write_control_file(
        s3,
        dest_bucket,
        ckey,
        source_etag=etag,
        source_size=1,
        fmt=fmt,
        now_iso="2026-07-01T00:00:00+00:00",
    )
    return ckey


class _SourceReadSpyClient:
    """Delegate to a real client, tallying bytes returned by ``get_object``.

    The gzip resume path reads the compressed source through ranged
    ``get_object`` calls (:class:`SeekableS3Object`). Summing the bytes they
    return lets a test prove a resume *seeked past* the early source instead
    of re-downloading it.
    """

    def __init__(self, real):
        self._real = real
        self.get_bytes = 0

    def __getattr__(self, name):
        return getattr(self._real, name)

    def get_object(self, **kwargs):
        resp = self._real.get_object(**kwargs)
        body = resp["Body"].read()
        self.get_bytes += len(body)
        # Re-wrap so the caller still reads the bytes it asked for.
        resp["Body"] = io.BytesIO(body)
        return resp


class _PutKeySpyClient:
    """Delegate to a real client, recording the Key of each ``put_object`` call.

    Lets a test assert the ``.idx`` seek-index companion was actually
    written mid-run (it's deleted on clean completion, so it can't be
    observed afterward).
    """

    def __init__(self, real):
        self._real = real
        self.put_keys: list[str] = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def put_object(self, **kwargs):
        self.put_keys.append(kwargs["Key"])
        return self._real.put_object(**kwargs)


def _build_and_put_gzip_index(s3, *, src_bucket, src_key, dest_bucket, dest_prefix, spacing):
    """Build a real seek index for the source .tar.gz and PUT it as the ``.idx``.

    Mimics the companion an *interrupted* resume run would have left behind:
    a full forward pass (draining every member in small chunks) accrues seek
    points, which we export and store under the ETag-named idx key.
    """
    raw = SeekableS3Object(s3, src_bucket, src_key)
    buffered = io.BufferedReader(raw, buffer_size=_BUFFER_SIZE)
    igzf, members = open_tar_gz_seekable(buffered, spacing=spacing)
    for member in members:
        member.drain()
    etag = _src_etag(s3, src_bucket, src_key)
    ikey = resume.index_key(dest_prefix, etag)
    resume.write_index_object(s3, dest_bucket, ikey, export_index_bytes(igzf))
    return ikey


def _resume_after_partial_run(s3, *, src_key, fmt, done_member, missing_member, files):
    """Resume a run that died after writing *done_member*, and assert the rest.

    Seeds the destination with *done_member* at its correct bytes plus a
    matching control marker — exactly the debris an interrupted run leaves —
    then resumes and asserts that only *missing_member* was transferred and
    the marker was cleaned up. The zip and .tar.gz resume paths reach this
    same state by different routes, so they share the assertion tail.
    """
    s3.put_object(Bucket="dest-bucket", Key=f"out/{done_member}", Body=files[done_member])
    ckey = _write_matching_control(
        s3,
        src_bucket="src-bucket",
        src_key=src_key,
        dest_bucket="dest-bucket",
        dest_prefix="out/",
        fmt=fmt,
    )

    spy = _UploadSpyClient(s3)
    extract(s3, spy, "src-bucket", src_key, "dest-bucket", "out/", fmt, resume=True)

    assert spy.uploaded_keys == [f"out/{missing_member}"]
    assert _body(s3, "dest-bucket", f"out/{missing_member}") == files[missing_member]
    assert resume.read_control_file(s3, "dest-bucket", ckey) is None


def _resume_with_source_spy(s3, *, src_key, fmt, dest_prefix, missing, expected_body):
    """Resume with spies on both sides; assert only *missing* was re-uploaded.

    Returns the source-side spy so the caller can go on to assert how much of
    the *compressed* source the run had to read — which is the whole point of
    the gzip seek index and the xz in-file block index.
    """
    spy = _UploadSpyClient(s3)
    src_spy = _SourceReadSpyClient(s3)
    extract(src_spy, spy, "src-bucket", src_key, "dest-bucket", dest_prefix, fmt, resume=True)

    assert spy.uploaded_keys == [f"{dest_prefix}{missing}"]
    assert _body(s3, "dest-bucket", f"{dest_prefix}{missing}") == expected_body
    return src_spy


class TestExtractResumeZip:
    """--resume over a zip: skip already-written members, transfer the rest."""

    def test_skip_present_reuploads_only_the_missing_member(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        # Full extract, then knock out one destination object to simulate a
        # run that died just before finishing it.
        extract(s3_client, s3_client, "src-bucket", "in/archive.zip", "dest-bucket", "out/", "zip")
        s3_client.delete_object(Bucket="dest-bucket", Key="out/a.txt")
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/archive.zip",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="zip",
        )

        spy = _UploadSpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        # Only the dropped member was written this run — the return list is
        # "written this run", and the skipped member's presence is asserted
        # separately below via the destination contents + upload spy.
        assert members == ["a.txt"]
        assert spy.uploaded_keys == ["out/a.txt"]
        # Both members present; the control marker is gone on clean finish.
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/a.txt", "out/sub/b.txt"]
        assert resume.read_control_file(s3_client, "dest-bucket", ckey) is None

    def test_interrupt_then_resume_transfers_only_missing(self, s3_client, sample_files):
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        # Pre-populate the destination with a subset (as a prior run would
        # have) at the correct size, plus the control marker.
        _resume_after_partial_run(
            s3_client,
            src_key="in/archive.zip",
            fmt="zip",
            done_member="sub/b.txt",
            missing_member="a.txt",
            files=sample_files,
        )

    def test_no_control_marker_means_fresh_run_writes_everything(self, s3_client, sample_files):
        # A pre-existing object with no matching control marker is NOT
        # vouched for: a fresh --resume run must not trust it, so it writes
        # every member (and the marker it creates is cleaned up at the end).
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        s3_client.put_object(Bucket="dest-bucket", Key="out/a.txt", Body=b"stale")

        spy = _UploadSpyClient(s3_client)
        extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        assert sorted(spy.uploaded_keys) == ["out/a.txt", "out/sub/b.txt"]
        # The stale object was overwritten with the real member bytes.
        assert _body(s3_client, "dest-bucket", "out/a.txt") == sample_files["a.txt"]
        etag = _src_etag(s3_client, "src-bucket", "in/archive.zip")
        assert (
            resume.read_control_file(s3_client, "dest-bucket", resume.control_key("out/", etag))
            is None
        )

    def test_identity_guard_wrong_etag_marker_skips_nothing(self, s3_client, sample_files):
        # A marker left by a DIFFERENT source (wrong ETag) must not make us
        # treat this source's destination objects as done.
        archive = build_zip(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.zip", Body=archive)
        # Destination already fully populated, but only under a foreign marker.
        for name, body in sample_files.items():
            s3_client.put_object(Bucket="dest-bucket", Key=f"out/{name}", Body=body)
        foreign_key = resume.control_key("out/", '"some-other-archive-etag"')
        resume.write_control_file(
            s3_client,
            "dest-bucket",
            foreign_key,
            source_etag='"some-other-archive-etag"',
            source_size=1,
            fmt="zip",
            now_iso="2026-07-01T00:00:00+00:00",
        )

        spy = _UploadSpyClient(s3_client)
        extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )

        # Nothing was skipped — every member re-uploaded despite being present.
        assert sorted(spy.uploaded_keys) == ["out/a.txt", "out/sub/b.txt"]
        # The foreign marker is untouched (we only ever manage our own).
        assert resume.read_control_file(s3_client, "dest-bucket", foreign_key) is not None

    def test_interrupted_fresh_run_marker_records_source_etag_and_size(
        self, s3_client, sample_files
    ):
        # The marker an interrupted fresh run leaves behind must carry the
        # real source identity — ETag *and* size, both read off the source
        # object. The ETag is what a later run matches on; the size is what
        # an operator browsing the destination reads. Nothing else in the
        # suite asserts the size half of the pair end-to-end.
        s3_client.put_object(
            Bucket="src-bucket", Key="in/archive.zip", Body=build_zip(sample_files)
        )
        head = s3_client.head_object(Bucket="src-bucket", Key="in/archive.zip")

        with pytest.raises(RuntimeError, match="connection died"):
            extract(
                s3_client,
                _DyingUploadClient(s3_client),
                "src-bucket",
                "in/archive.zip",
                "dest-bucket",
                "out/",
                "zip",
                resume=True,
            )

        ckey = resume.control_key("out/", head["ETag"])
        body = json.loads(s3_client.get_object(Bucket="dest-bucket", Key=ckey)["Body"].read())
        assert body["source_etag"] == head["ETag"]
        assert body["source_size"] == head["ContentLength"]
        assert body["format"] == "zip"

    def test_no_central_directory_refuses(self, s3_client):
        # A .zip that isn't a real zip (no usable central directory) can't
        # be walked per-member → refuse, without leaving a control marker.
        s3_client.put_object(Bucket="src-bucket", Key="in/bad.zip", Body=b"not a zip at all")

        with pytest.raises(ResumeUnsupportedError):
            extract(
                s3_client,
                s3_client,
                "src-bucket",
                "in/bad.zip",
                "dest-bucket",
                "out/",
                "zip",
                resume=True,
            )
        assert _no_control_markers(s3_client, "dest-bucket")


class TestExtractResumeTar:
    """--resume over an uncompressed tar mirrors the zip skip behavior."""

    def test_skip_present_reuploads_only_the_missing_member(self, s3_client, sample_files):
        archive = build_tar(sample_files, mode="w")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.tar", Body=archive)
        extract(s3_client, s3_client, "src-bucket", "in/archive.tar", "dest-bucket", "out/", "tar")
        s3_client.delete_object(Bucket="dest-bucket", Key="out/sub/b.txt")
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/archive.tar",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="tar",
        )

        spy = _UploadSpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.tar",
            "dest-bucket",
            "out/",
            "tar",
            resume=True,
        )

        # "written this run" — only the dropped member; the other's presence
        # is confirmed via _extracted implicitly (it was never deleted).
        assert members == ["sub/b.txt"]
        assert spy.uploaded_keys == ["out/sub/b.txt"]
        assert _body(s3_client, "dest-bucket", "out/sub/b.txt") == sample_files["sub/b.txt"]
        assert resume.read_control_file(s3_client, "dest-bucket", ckey) is None


class TestExtractResume7z:
    """--resume over a non-solid .7z (v2): per-member seek via py7zr targets."""

    def test_skip_present_reuploads_only_the_missing_member(self, s3_client, sample_files):
        archive = build_7z(sample_files, flavor="nonsolid")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.7z", Body=archive)
        # Full extract, then knock out one destination object to simulate a
        # run that died just before finishing it.
        extract(s3_client, s3_client, "src-bucket", "in/archive.7z", "dest-bucket", "out/", "7z")
        s3_client.delete_object(Bucket="dest-bucket", Key="out/a.txt")
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/archive.7z",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="7z",
        )

        spy = _UploadSpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/archive.7z",
            "dest-bucket",
            "out/",
            "7z",
            resume=True,
        )

        # Only the dropped member is written this run (7z yields just the
        # undone targets); the other's presence is confirmed via dest keys.
        assert members == ["a.txt"]
        assert spy.uploaded_keys == ["out/a.txt"]
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == ["out/a.txt", "out/sub/b.txt"]
        assert _body(s3_client, "dest-bucket", "out/a.txt") == sample_files["a.txt"]
        assert resume.read_control_file(s3_client, "dest-bucket", ckey) is None

    def test_interrupt_then_resume_transfers_only_missing(self, s3_client, sample_files):
        archive = build_7z(sample_files, flavor="nonsolid")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.7z", Body=archive)
        # Pre-populate the destination with a subset at the correct size,
        # plus the control marker (as an interrupted prior run would leave).
        _resume_after_partial_run(
            s3_client,
            src_key="in/archive.7z",
            fmt="7z",
            done_member="sub/b.txt",
            missing_member="a.txt",
            files=sample_files,
        )

    def test_solid_7z_refuses_without_marker(self, s3_client, sample_files):
        # A solid archive (single compression block) can't be seeked into
        # per-member → refuse up front, before any control marker is written.
        archive = build_7z(sample_files, flavor="solid")
        s3_client.put_object(Bucket="src-bucket", Key="in/archive.7z", Body=archive)

        with pytest.raises(ResumeUnsupportedError, match="solid"):
            extract(
                s3_client,
                s3_client,
                "src-bucket",
                "in/archive.7z",
                "dest-bucket",
                "out/",
                "7z",
                resume=True,
            )

        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []
        assert _no_control_markers(s3_client, "dest-bucket")


class TestExtractResumeGzip:
    """--resume over a .tar.gz (v3): seek past done members via an indexed_gzip index.

    Uses a tiny ``DEFAULT_INDEX_SPACING`` (monkeypatched) so a few-MB fixture
    forces multiple seek points / checkpoints. Members are incompressible so
    the *compressed* archive exceeds SeekableS3Object's tail prefetch — that's
    what makes a seek past early members measurably cheaper than a re-read.
    """

    _SPACING = 131072

    def _big_files(self):
        # 4 x 2 MB incompressible → ~8 MB compressed, comfortably above the
        # 4 MB tail prefetch so seeking past early members avoids real GETs.
        return {f"m{i}.bin": incompressible_bytes(2 * 1024 * 1024, seed=i) for i in range(4)}

    def test_fresh_run_writes_then_deletes_marker_and_index(self, s3_client, monkeypatch):
        monkeypatch.setattr("s3_archive.gzip_seek.DEFAULT_INDEX_SPACING", self._SPACING)
        files = self._big_files()
        s3_client.put_object(Bucket="src-bucket", Key="in/a.tar.gz", Body=build_tar_gz(files))

        spy = _PutKeySpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/a.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            resume=True,
        )

        assert sorted(members) == [f"m{i}.bin" for i in range(4)]
        # The .idx companion was PUT at least once during the run (checkpoint).
        assert any(k.endswith(".idx") for k in spy.put_keys)
        # All members present; both the .json marker and the .idx are gone.
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert keys == [f"out/m{i}.bin" for i in range(4)]
        assert not any(resume.is_control_key(k) for k in keys)

    def test_interrupted_run_keeps_marker_and_index_until_clean_completion(
        self, s3_client, monkeypatch
    ):
        # Only a *clean* completion drops the resume artifacts: a run that dies
        # mid-walk leaves both the .json marker and the .idx checkpoint, and
        # the resume that then finishes removes both.
        monkeypatch.setattr("s3_archive.gzip_seek.DEFAULT_INDEX_SPACING", self._SPACING)
        files = self._big_files()
        s3_client.put_object(Bucket="src-bucket", Key="in/a.tar.gz", Body=build_tar_gz(files))
        etag = _src_etag(s3_client, "src-bucket", "in/a.tar.gz")
        artifacts = {resume.control_key("out/", etag), resume.index_key("out/", etag)}
        args = ("src-bucket", "in/a.tar.gz", "dest-bucket", "out/", "tar.gz")

        with pytest.raises(RuntimeError, match="connection died"):
            extract(s3_client, _DiesAfterUploads(s3_client, survive=2), *args, resume=True)
        assert artifacts <= set(_extracted_keys(s3_client, "dest-bucket", "out/"))

        extract(s3_client, s3_client, *args, resume=True)
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == [
            f"out/m{i}.bin" for i in range(4)
        ]

    def _resume_reading_only_last(self, s3_client, *, dest_prefix, files, with_index):
        """Set up an interrupted run (all but m3 done) and resume; return the source spy.

        With *with_index* a valid seek index is left behind; without it the
        marker is present but no ``.idx`` (the death-before-first-checkpoint
        case). Both must resume correctly — the difference is only how much
        compressed source gets re-read.
        """
        for name, body in files.items():
            if name != "m3.bin":
                s3_client.put_object(Bucket="dest-bucket", Key=f"{dest_prefix}{name}", Body=body)
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/a.tar.gz",
            dest_bucket="dest-bucket",
            dest_prefix=dest_prefix,
            fmt="tar.gz",
        )
        if with_index:
            _build_and_put_gzip_index(
                s3_client,
                src_bucket="src-bucket",
                src_key="in/a.tar.gz",
                dest_bucket="dest-bucket",
                dest_prefix=dest_prefix,
                spacing=self._SPACING,
            )

        # Only the missing member is re-uploaded, at the right bytes; marker
        # cleaned up on completion.
        src_spy = _resume_with_source_spy(
            s3_client,
            src_key="in/a.tar.gz",
            fmt="tar.gz",
            dest_prefix=dest_prefix,
            missing="m3.bin",
            expected_body=files["m3.bin"],
        )
        assert resume.read_control_file(s3_client, "dest-bucket", ckey) is None
        return src_spy

    def test_interrupt_then_resume_bounds_source_reads(self, s3_client, monkeypatch):
        monkeypatch.setattr("s3_archive.gzip_seek.DEFAULT_INDEX_SPACING", self._SPACING)
        files = self._big_files()
        archive = build_tar_gz(files)
        s3_client.put_object(Bucket="src-bucket", Key="in/a.tar.gz", Body=archive)

        # Same interrupted-run scenario resumed two ways: with a valid seek
        # index vs. without one (which forces a full forward re-decode to skip
        # the done members). Isolating the two proves the index — not just the
        # done-set skip — is what avoids re-reading the source.
        with_index = self._resume_reading_only_last(
            s3_client, dest_prefix="with/", files=files, with_index=True
        )
        without_index = self._resume_reading_only_last(
            s3_client, dest_prefix="without/", files=files, with_index=False
        )

        # The seek index jumped past the done members instead of decoding
        # their bodies: strictly fewer source bytes read...
        assert with_index.get_bytes < without_index.get_bytes
        # ...and less than a full download of the compressed source (the v3
        # "don't re-download everything" guarantee).
        assert with_index.get_bytes < len(archive)

    def test_missing_index_still_resumes_correctly(self, s3_client, sample_files):
        # An interrupted run that died before its first checkpoint leaves a
        # marker but no .idx. Resume must still work (forward decode, done-set
        # skip) — the index is a pure optimization.
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/a.tar.gz", Body=archive)
        _resume_after_partial_run(
            s3_client,
            src_key="in/a.tar.gz",
            fmt="tar.gz",
            done_member="sub/b.txt",
            missing_member="a.txt",
            files=sample_files,
        )

    def test_corrupt_index_still_resumes_correctly(self, s3_client, sample_files, caplog):
        # A garbage .idx must degrade gracefully to forward decode, not break
        # the run (or produce wrong output).
        archive = build_tar_gz(sample_files)
        s3_client.put_object(Bucket="src-bucket", Key="in/a.tar.gz", Body=archive)
        s3_client.put_object(
            Bucket="dest-bucket", Key="out/sub/b.txt", Body=sample_files["sub/b.txt"]
        )
        etag = _src_etag(s3_client, "src-bucket", "in/a.tar.gz")
        resume.write_index_object(
            s3_client, "dest-bucket", resume.index_key("out/", etag), b"garbage-not-an-index"
        )
        _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/a.tar.gz",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="tar.gz",
        )

        spy = _UploadSpyClient(s3_client)
        extract(
            s3_client,
            spy,
            "src-bucket",
            "in/a.tar.gz",
            "dest-bucket",
            "out/",
            "tar.gz",
            resume=True,
        )

        assert spy.uploaded_keys == ["out/a.txt"]
        assert _body(s3_client, "dest-bucket", "out/a.txt") == sample_files["a.txt"]
        assert any("failed to import" in r.message for r in caplog.records)


class TestExtractResumeXz:
    """--resume over a multi-block .tar.xz (v4): seek via the in-file block index.

    xz keeps its block index in the file footer, so there is **no companion
    ``.idx``** — resume state is just the destination ledger. A single-block
    .tar.xz refuses (see :class:`TestExtractResumeRefuse`).
    """

    def _big_files(self):
        return {f"m{i}.bin": incompressible_bytes(2 * 1024 * 1024, seed=i) for i in range(4)}

    def test_fresh_run_writes_marker_no_index_then_cleans_up(self, s3_client):
        files = self._big_files()
        s3_client.put_object(
            Bucket="src-bucket", Key="in/a.tar.xz", Body=build_tar_xz_multiblock(files)
        )

        spy = _PutKeySpyClient(s3_client)
        members = extract(
            s3_client,
            spy,
            "src-bucket",
            "in/a.tar.xz",
            "dest-bucket",
            "out/",
            "tar.xz",
            resume=True,
        )

        assert sorted(members) == [f"m{i}.bin" for i in range(4)]
        # xz persists no seek index — only the .json marker is ever written.
        assert not any(k.endswith(".idx") for k in spy.put_keys)
        keys = _extracted_keys(s3_client, "dest-bucket", "out/")
        assert keys == [f"out/m{i}.bin" for i in range(4)]
        assert not any(resume.is_control_key(k) for k in keys)

    def test_interrupt_then_resume_bounds_source_reads(self, s3_client):
        files = self._big_files()
        archive = build_tar_xz_multiblock(files)
        s3_client.put_object(Bucket="src-bucket", Key="in/a.tar.xz", Body=archive)

        for name, body in files.items():
            if name != "m3.bin":
                s3_client.put_object(Bucket="dest-bucket", Key=f"out/{name}", Body=body)
        ckey = _write_matching_control(
            s3_client,
            src_bucket="src-bucket",
            src_key="in/a.tar.xz",
            dest_bucket="dest-bucket",
            dest_prefix="out/",
            fmt="tar.xz",
        )

        src_spy = _resume_with_source_spy(
            s3_client,
            src_key="in/a.tar.xz",
            fmt="tar.xz",
            dest_prefix="out/",
            missing="m3.bin",
            expected_body=files["m3.bin"],
        )

        # The in-file block index let us seek to the tail member instead of
        # decoding the whole source.
        assert src_spy.get_bytes < len(archive)
        assert resume.read_control_file(s3_client, "dest-bucket", ckey) is None

    def test_single_block_refuses_without_marker(self, s3_client):
        # `tar -J` / stdlib lzma default: one block, no interior seek point.
        s3_client.put_object(
            Bucket="src-bucket", Key="in/a.tar.xz", Body=build_tar({"a.txt": b"x"}, mode="w:xz")
        )
        with pytest.raises(ResumeUnsupportedError, match="single xz block"):
            extract(
                s3_client,
                s3_client,
                "src-bucket",
                "in/a.tar.xz",
                "dest-bucket",
                "out/",
                "tar.xz",
                resume=True,
            )
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []
        assert _no_control_markers(s3_client, "dest-bucket")


class TestExtractResumeRefuse:
    """Non-seekable formats refuse up front and write no control marker.

    7z is *not* here — it's conditionally supported (non-solid resumes,
    solid refuses; see :class:`TestExtractResume7z`).
    """

    @pytest.mark.parametrize(
        ("fmt", "key", "body_factory"),
        [
            # tar.gz + multi-block tar.xz are resumable (see the per-format
            # resume tests). What refuses: tar.bz2 (indexed_bzip2 can't seek
            # the tar walk — see the "Resumable extract for .tar.bz2 (and
            # other seek-hostile formats)" idea in docs/someday-maybe/), a
            # *single-block* tar.xz (stdlib lzma / `tar -J` default — no
            # interior seek point), and tar.zst (no fileobj-friendly seek
            # lib).
            ("tar.bz2", "in/a.tar.bz2", lambda: build_tar({"a.txt": b"x"}, mode="w:bz2")),
            ("tar.xz", "in/a.tar.xz", lambda: build_tar({"a.txt": b"x"}, mode="w:xz")),
            ("tar.zst", "in/a.tar.zst", lambda: zstandard.ZstdCompressor().compress(b"x")),
        ],
    )
    def test_refuse_writes_no_control_file(self, s3_client, fmt, key, body_factory):
        s3_client.put_object(Bucket="src-bucket", Key=key, Body=body_factory())

        with pytest.raises(ResumeUnsupportedError):
            extract(
                s3_client, s3_client, "src-bucket", key, "dest-bucket", "out/", fmt, resume=True
            )

        # Fail-fast contract: nothing extracted, no marker left behind.
        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []
        assert _no_control_markers(s3_client, "dest-bucket")

    def test_corrupt_7z_surfaces_archive_read_error_no_marker(self, s3_client):
        # A .7z whose bytes are bad is a corrupt archive, not an
        # unsupported-for-resume one: the resume probe opens the header and
        # surfaces ArchiveReadError (distinct from ResumeUnsupportedError),
        # still before any control marker is written.
        s3_client.put_object(Bucket="src-bucket", Key="in/a.7z", Body=b"7z placeholder")

        with pytest.raises(ArchiveReadError):
            extract(
                s3_client,
                s3_client,
                "src-bucket",
                "in/a.7z",
                "dest-bucket",
                "out/",
                "7z",
                resume=True,
            )

        assert _extracted_keys(s3_client, "dest-bucket", "out/") == []
        assert _no_control_markers(s3_client, "dest-bucket")


def _no_control_markers(s3, bucket):
    return not any(resume.is_control_key(k) for k in _extracted_keys(s3, bucket, ""))


_GOOD_A = b"alpha\n" * 50
_BAD = b"thumbs " * 200
_GOOD_Z = b"zulu\n" * 50
_INTEGRITY_FILES = {"a.txt": _GOOD_A, "Diary 37/Thumbs.db": _BAD, "z.txt": _GOOD_Z}


def _flipped(body: bytes, at: int = 0) -> bytes:
    out = bytearray(body)
    out[at] ^= 0xFF
    return bytes(out)


class TestExtractIntegrityWarnings:
    """A member that fails its integrity check is written as decoded, logged
    as a WARNING, reported, and the extract carries on to the end."""

    def _put_corrupt_zip(self, s3):
        s3.put_object(
            Bucket="src-bucket",
            Key="bad.zip",
            Body=build_zip_with_corrupt_member(_INTEGRITY_FILES, "Diary 37/Thumbs.db"),
        )

    def test_zip_writes_every_member_and_reports_the_bad_one(self, s3_client, caplog):
        self._put_corrupt_zip(s3_client)
        with caplog.at_level(logging.WARNING, logger="s3_archive"):
            report = extract_with_report(
                s3_client, s3_client, "src-bucket", "bad.zip", "dest-bucket", "out/", "zip"
            )
        assert report.written == list(_INTEGRITY_FILES)
        assert _body(s3_client, "dest-bucket", "out/a.txt") == _GOOD_A
        assert _body(s3_client, "dest-bucket", "out/Diary 37/Thumbs.db") == _flipped(_BAD)
        assert _body(s3_client, "dest-bucket", "out/z.txt") == _GOOD_Z
        assert list(report.integrity_warnings) == ["Diary 37/Thumbs.db"]
        mismatch = report.integrity_warnings["Diary 37/Thumbs.db"]
        assert mismatch.expected_crc32 == zlib.crc32(_BAD)
        assert mismatch.actual_crc32 == zlib.crc32(_flipped(_BAD))
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "Diary 37/Thumbs.db" in warnings[0]
        assert "written as decoded" in warnings[0]

    def test_extract_keeps_returning_the_member_list(self, s3_client):
        """extract() (s3-bagit's entry point) keeps its list[str] contract."""
        self._put_corrupt_zip(s3_client)
        written = extract(
            s3_client, s3_client, "src-bucket", "bad.zip", "dest-bucket", "out/", "zip"
        )
        assert written == list(_INTEGRITY_FILES)

    def test_dry_run_reports_without_writing(self, s3_client, caplog):
        self._put_corrupt_zip(s3_client)
        with caplog.at_level(logging.WARNING, logger="s3_archive"):
            report = extract_with_report(
                s3_client,
                s3_client,
                "src-bucket",
                "bad.zip",
                "dest-bucket",
                "out/",
                "zip",
                dry_run=True,
            )
        assert list(report.integrity_warnings) == ["Diary 37/Thumbs.db"]
        assert _extracted_keys(s3_client, "dest-bucket", "") == []
        assert any("would be written as decoded" in r.getMessage() for r in caplog.records)

    def test_resume_zip_tolerates_too(self, s3_client):
        self._put_corrupt_zip(s3_client)
        report = extract_with_report(
            s3_client,
            s3_client,
            "src-bucket",
            "bad.zip",
            "dest-bucket",
            "out/",
            "zip",
            resume=True,
        )
        assert report.written == list(_INTEGRITY_FILES)
        assert list(report.integrity_warnings) == ["Diary 37/Thumbs.db"]
        assert _body(s3_client, "dest-bucket", "out/Diary 37/Thumbs.db") == _flipped(_BAD)

    def test_7z(self, s3_client):
        bad = incompressible_bytes(4000, seed=3)
        files = {"a.txt": _GOOD_A, "bad.bin": bad, "z.txt": _GOOD_Z}
        blob = bytearray(build_7z_py7zr(files, flavor="copy"))
        blob[blob.index(bad[:64])] ^= 0xFF
        s3_client.put_object(Bucket="src-bucket", Key="bad.7z", Body=bytes(blob))
        report = extract_with_report(
            s3_client, s3_client, "src-bucket", "bad.7z", "dest-bucket", "out/", "7z"
        )
        assert sorted(report.written) == sorted(files)
        assert list(report.integrity_warnings) == ["bad.bin"]
        assert _body(s3_client, "dest-bucket", "out/bad.bin") == _flipped(bad)
        assert _body(s3_client, "dest-bucket", "out/z.txt") == _GOOD_Z

    def test_clean_archive_has_no_warnings(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="ok.zip", Body=build_zip(_INTEGRITY_FILES))
        report = extract_with_report(
            s3_client, s3_client, "src-bucket", "ok.zip", "dest-bucket", "out/", "zip"
        )
        assert report.integrity_warnings == {}


class _DiesAfterUploads:
    """Delegate to a real client; let *survive* uploads through, then die.

    With *read_first*, the dying upload reads its body to EOF before raising
    — a run that dies after the member's bytes were all consumed but before
    its object completed, the window the marker rewrite must already cover.
    """

    def __init__(self, real, *, survive: int, read_first: bool = False):
        self._real = real
        self._left = survive
        self._read_first = read_first

    def __getattr__(self, name):
        return getattr(self._real, name)

    def upload_fileobj(self, fileobj, bucket, key, **kwargs):
        if self._left == 0:
            if self._read_first:
                while fileobj.read(64 * 1024):
                    pass
            raise RuntimeError("connection died mid-upload")
        self._left -= 1
        return self._real.upload_fileobj(fileobj, bucket, key, **kwargs)


def _marker(s3, src_key):
    etag = _src_etag(s3, "src-bucket", src_key)
    return resume.read_control_file(s3, "dest-bucket", resume.control_key("out/", etag))


class TestResumeCarriesIntegrityWarnings:
    """Warnings accumulate in the resume marker across an interruption: the
    final report and .log cover the whole extract, and a warned member counts
    as done at the size it was written."""

    def _interrupt_then_resume(self, s3, key, body, fmt, *, survive, read_first=False):
        s3.put_object(Bucket="src-bucket", Key=key, Body=body)
        with pytest.raises(RuntimeError, match="connection died"):
            extract_with_report(
                s3,
                _DiesAfterUploads(s3, survive=survive, read_first=read_first),
                "src-bucket",
                key,
                "dest-bucket",
                "out/",
                fmt,
                resume=True,
            )
        interrupted_marker = _marker(s3, key)
        spy = _UploadSpyClient(s3)
        report = extract_with_report(
            s3, spy, "src-bucket", key, "dest-bucket", "out/", fmt, resume=True
        )
        return interrupted_marker, spy, report

    def test_zip_warning_survives_the_interruption(self, s3_client):
        body = build_zip_with_corrupt_member(_INTEGRITY_FILES, "Diary 37/Thumbs.db")
        marker, spy, report = self._interrupt_then_resume(
            s3_client, "in/bad.zip", body, "zip", survive=2
        )
        # The interrupted run recorded the warning in its marker...
        assert marker is not None
        assert list(resume.marker_integrity_warnings(marker)) == ["Diary 37/Thumbs.db"]
        # ...the resumed run wrote only what was missing...
        assert spy.uploaded_keys == ["out/z.txt"]
        assert report.already_present == 2
        # ...and still reports, and logs, the whole extract's warning.
        assert list(report.integrity_warnings) == ["Diary 37/Thumbs.db"]
        log_text = _body(s3_client, "dest-bucket", "out.log").decode("utf-8")
        assert "resumed: yes" in log_text
        assert "WARNING Diary 37/Thumbs.db:" in log_text
        assert _marker(s3_client, "in/bad.zip") is None  # cleaned up on completion

    def test_warned_member_with_a_size_mismatch_counts_as_done(self, s3_client):
        """Decoded size != declared size: without the carried warning, the
        done-check (declared size) would re-extract it on every resume."""
        body = build_zip_with_corrupt_member(
            _INTEGRITY_FILES, "Diary 37/Thumbs.db", declared_size_delta=7
        )
        _, spy, report = self._interrupt_then_resume(
            s3_client, "in/bad.zip", body, "zip", survive=2
        )
        assert spy.uploaded_keys == ["out/z.txt"]
        mismatch = report.integrity_warnings["Diary 37/Thumbs.db"]
        assert mismatch.actual_size == len(_BAD)
        assert mismatch.expected_size == len(_BAD) + 7

    def test_warning_is_recorded_before_the_object_completes(self, s3_client):
        """A run that dies after reading the bad member to EOF but before its
        object exists has already recorded the warning; the resume rewrites
        the member and warns again, once."""
        body = build_zip_with_corrupt_member(_INTEGRITY_FILES, "Diary 37/Thumbs.db")
        marker, spy, report = self._interrupt_then_resume(
            s3_client, "in/bad.zip", body, "zip", survive=1, read_first=True
        )
        assert marker is not None
        assert list(resume.marker_integrity_warnings(marker)) == ["Diary 37/Thumbs.db"]
        assert spy.uploaded_keys == ["out/Diary 37/Thumbs.db", "out/z.txt"]
        assert list(report.integrity_warnings) == ["Diary 37/Thumbs.db"]

    def test_7z_warning_survives_the_interruption(self, s3_client):
        bad = incompressible_bytes(4000, seed=5)
        files = {"a.txt": _GOOD_A, "bad.bin": bad, "z.txt": _GOOD_Z}
        blob = bytearray(build_7z_py7zr(files, flavor="copy_nonsolid"))
        blob[blob.index(bad[:64])] ^= 0xFF
        marker, spy, report = self._interrupt_then_resume(
            s3_client, "in/bad.7z", bytes(blob), "7z", survive=2
        )
        assert marker is not None
        assert list(resume.marker_integrity_warnings(marker)) == ["bad.bin"]
        assert spy.uploaded_keys == ["out/z.txt"]
        assert list(report.integrity_warnings) == ["bad.bin"]
        assert _body(s3_client, "dest-bucket", "out/bad.bin") == _flipped(bad)
