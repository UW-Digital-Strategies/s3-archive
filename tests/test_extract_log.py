"""Tests for the ``<dir>.log`` extract sidecar: key derivation, text, and the write."""

import logging
import zlib
from datetime import UTC, datetime

import pytest

from s3_archive.extract import extract_with_report
from s3_archive.extract_log import ExtractLogRecord, log_key, render
from s3_archive.integrity import IntegrityMismatch

from .conftest import _extracted_keys, build_zip, build_zip_with_corrupt_member

_FILES = {"a.txt": b"alpha\n" * 20, "Diary 37/Thumbs.db": b"thumbs " * 100, "z.txt": b"zulu\n"}


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("Iraq/foo/", "Iraq/foo.log"),
        ("Iraq/foo", "Iraq/foo.log"),
        ("Iraq/", "Iraq.log"),
        ("a//", "a.log"),
        ("", None),
        ("/", None),
    ],
)
def test_log_key(prefix, expected):
    assert log_key(prefix) == expected


def test_render_names_each_warning_on_its_own_line():
    mismatch = IntegrityMismatch(
        check="CRC-32",
        expected_crc32=0x1234ABCD,
        actual_crc32=0x0BADF00D,
        expected_size=10,
        actual_size=10,
    )
    text = render(
        ExtractLogRecord(
            version="9.9.9",
            source_url="s3://b/in/x.zip",
            source_etag='"abc-2"',
            fmt="zip",
            destination_url="s3://d/Iraq/",
            resumed=False,
            started=datetime(2026, 10, 16, 1, 2, 3, tzinfo=UTC),
            finished=datetime(2026, 10, 16, 1, 3, 4, tzinfo=UTC),
            written=3,
            already_present=0,
            integrity_warnings={"Diary 37/Thumbs.db": mismatch},
        )
    )
    assert text.endswith("\n")
    lines = text.splitlines()
    assert "source: s3://b/in/x.zip" in lines
    assert 'source-etag: "abc-2"' in lines
    assert "started: 2026-10-16T01:02:03+00:00" in lines
    assert "members-written-this-run: 3" in lines
    assert "integrity-warnings: 1" in lines
    assert lines[-1] == (
        "WARNING Diary 37/Thumbs.db: CRC-32 check failed: expected CRC-32 1234abcd size 10, "
        "decoded CRC-32 0badf00d size 10; written as decoded"
    )


def _read(s3, key):
    obj = s3.get_object(Bucket="dest-bucket", Key=key)
    return obj["Body"].read().decode("utf-8"), obj["ContentType"]


def _extract(s3, key, prefix, **kwargs):
    return extract_with_report(s3, s3, "src-bucket", key, "dest-bucket", prefix, "zip", **kwargs)


class TestSidecarWrite:
    def test_clean_extract_writes_a_log_beside_the_folder(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="in/ok.zip", Body=build_zip(_FILES))
        etag = s3_client.head_object(Bucket="src-bucket", Key="in/ok.zip")["ETag"]

        report = _extract(s3_client, "in/ok.zip", "Iraq/foo/")

        assert report.log_key == "Iraq/foo.log"
        text, content_type = _read(s3_client, "Iraq/foo.log")
        assert content_type == "text/plain; charset=utf-8"
        lines = text.splitlines()
        assert "source: s3://src-bucket/in/ok.zip" in lines
        assert f"source-etag: {etag}" in lines
        assert "destination: s3://dest-bucket/Iraq/foo/" in lines
        assert "members-written-this-run: 3" in lines
        assert "integrity-warnings: 0" in lines
        assert not any(line.startswith("WARNING") for line in lines)
        # Beside the folder, never inside it: the folder mirrors the archive.
        assert _extracted_keys(s3_client, "dest-bucket", "Iraq/foo/") == sorted(
            f"Iraq/foo/{n}" for n in _FILES
        )

    def test_integrity_warning_is_named_in_the_log(self, s3_client):
        s3_client.put_object(
            Bucket="src-bucket",
            Key="in/bad.zip",
            Body=build_zip_with_corrupt_member(_FILES, "Diary 37/Thumbs.db"),
        )
        _extract(s3_client, "in/bad.zip", "Iraq/")
        text, _ = _read(s3_client, "Iraq.log")
        warnings = [line for line in text.splitlines() if line.startswith("WARNING")]
        body = _FILES["Diary 37/Thumbs.db"]
        assert warnings == [
            f"WARNING Diary 37/Thumbs.db: CRC-32 check failed: expected CRC-32 "
            f"{zlib.crc32(body):08x} size {len(body)}, decoded CRC-32 "
            f"{zlib.crc32(bytes([body[0] ^ 0xFF]) + body[1:]):08x} size {len(body)}; "
            f"written as decoded"
        ]

    def test_clean_rerun_overwrites_a_stale_warning(self, s3_client):
        s3_client.put_object(
            Bucket="src-bucket",
            Key="in/x.zip",
            Body=build_zip_with_corrupt_member(_FILES, "Diary 37/Thumbs.db"),
        )
        _extract(s3_client, "in/x.zip", "out/")
        assert "integrity-warnings: 1" in _read(s3_client, "out.log")[0]

        s3_client.put_object(Bucket="src-bucket", Key="in/x.zip", Body=build_zip(_FILES))
        _extract(s3_client, "in/x.zip", "out/")
        assert "integrity-warnings: 0" in _read(s3_client, "out.log")[0]

    def test_dry_run_writes_no_log(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="in/ok.zip", Body=build_zip(_FILES))
        report = _extract(s3_client, "in/ok.zip", "out/", dry_run=True)
        assert report.log_key is None
        assert _extracted_keys(s3_client, "dest-bucket", "") == []

    def test_bucket_root_writes_no_log_and_warns(self, s3_client, caplog):
        s3_client.put_object(Bucket="src-bucket", Key="in/ok.zip", Body=build_zip(_FILES))
        with caplog.at_level(logging.WARNING, logger="s3_archive"):
            report = _extract(s3_client, "in/ok.zip", "")
        assert report.log_key is None
        assert _extracted_keys(s3_client, "dest-bucket", "") == sorted(_FILES)
        assert any("bucket root" in r.getMessage() for r in caplog.records)
