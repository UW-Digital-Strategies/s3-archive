"""Unit tests for the format-agnostic resume core (:mod:`s3_archive.resume`)."""

import json

import botocore.exceptions
import pytest
from botocore.stub import Stubber

from s3_archive import resume
from s3_archive.integrity import IntegrityMismatch

from .fixture_values import EMPTY_MD5


class TestControlKey:
    """Control-object key construction + ETag sanitization."""

    def test_strips_surrounding_quotes(self):
        # S3/RGW hand back ETags wrapped in literal double quotes.
        assert (
            resume.control_key("out/", f'"{EMPTY_MD5}"')
            == f"out/.s3-archive-resume.{EMPTY_MD5}.json"
        )

    def test_keeps_multipart_etag_suffix(self):
        # Multipart ETags look like "<hex>-<partcount>"; the hyphen is
        # allowed so the suffix survives sanitization.
        assert (
            resume.control_key("out/", '"abc123def456-42"')
            == "out/.s3-archive-resume.abc123def456-42.json"
        )

    def test_drops_disallowed_characters(self):
        # Anything outside [A-Za-z0-9._-] is dropped, never substituted, so
        # the result is always a single well-formed path segment.
        assert resume.control_key("out/", 'a/b c:d"e') == "out/.s3-archive-resume.abcde.json"

    def test_empty_prefix(self):
        assert resume.control_key("", '"tag"') == ".s3-archive-resume.tag.json"

    def test_prefix_without_trailing_slash(self):
        assert resume.control_key("out", '"tag"') == "out/.s3-archive-resume.tag.json"


class TestIsControlKey:
    def test_matches_full_key(self):
        assert resume.is_control_key("out/.s3-archive-resume.tag.json")

    def test_matches_bare_relative_path(self):
        # build_done_set sees prefix-stripped RelativePaths, so a bare name
        # (no leading slash) must match too.
        assert resume.is_control_key(".s3-archive-resume.tag.json")

    def test_rejects_ordinary_member(self):
        assert not resume.is_control_key("out/a.txt")
        assert not resume.is_control_key("out/notes.s3-archive-resume.txt")


class TestControlFileRoundTrip:
    """write → exists → delete against moto, plus the body contract."""

    def test_write_then_exists_then_delete(self, s3_client):
        key = resume.control_key("out/", '"etag-1"')
        assert resume.read_control_file(s3_client, "dest-bucket", key) is None

        resume.write_control_file(
            s3_client,
            "dest-bucket",
            key,
            source_etag='"etag-1"',
            source_size=123,
            fmt="zip",
            now_iso="2026-07-01T00:00:00+00:00",
        )
        assert resume.read_control_file(s3_client, "dest-bucket", key) is not None

        resume.delete_control_file(s3_client, "dest-bucket", key)
        assert resume.read_control_file(s3_client, "dest-bucket", key) is None

    def test_body_records_provenance_but_no_bucket_or_key(self, s3_client):
        key = resume.control_key("out/", '"etag-1"')
        resume.write_control_file(
            s3_client,
            "dest-bucket",
            key,
            source_etag='"etag-1"',
            source_size=456,
            fmt="tar",
            now_iso="2026-07-01T12:00:00+00:00",
        )
        body = json.loads(s3_client.get_object(Bucket="dest-bucket", Key=key)["Body"].read())
        assert body == {
            "schema_version": resume.SCHEMA_VERSION,
            "source_etag": '"etag-1"',
            "source_size": 456,
            "format": "tar",
            "created_at": "2026-07-01T12:00:00+00:00",
            "integrity_warnings": {},
        }
        # Deliberately no bucket/key anywhere in the marker (no leak).
        assert "bucket" not in body
        assert "key" not in body


class TestMarkerIntegrityWarnings:
    """The marker carries the warnings a run has recorded, across a re-run."""

    _MISMATCH = IntegrityMismatch(
        check="CRC-32", expected_crc32=1, actual_crc32=2, expected_size=10, actual_size=9
    )

    def _write(self, s3, key, warnings):
        resume.write_control_file(
            s3,
            "dest-bucket",
            key,
            source_etag='"e"',
            source_size=1,
            fmt="zip",
            now_iso="2026-10-16T00:00:00+00:00",
            integrity_warnings=warnings,
        )

    def test_round_trip(self, s3_client):
        key = resume.control_key("out/", '"e"')
        self._write(s3_client, key, {"Diary 37/Thumbs.db": self._MISMATCH})
        marker = resume.read_control_file(s3_client, "dest-bucket", key)
        assert marker is not None
        assert resume.marker_integrity_warnings(marker) == {"Diary 37/Thumbs.db": self._MISMATCH}

    def test_marker_from_before_the_field_has_none(self):
        """Older versions stopped at the first integrity failure, so a
        marker they wrote has genuinely nothing to carry."""
        assert resume.marker_integrity_warnings({"schema_version": 1}) == {}

    def test_malformed_warning_raises(self):
        with pytest.raises(KeyError):
            resume.marker_integrity_warnings({"integrity_warnings": {"x": {"check": "CRC-32"}}})

    def test_non_object_marker_raises(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="out/m.json", Body=b"[1, 2]")
        with pytest.raises(ValueError, match="not a JSON object"):
            resume.read_control_file(s3_client, "dest-bucket", "out/m.json")


# The two resume reads whose sentinel means *absence* (PYTHON §19): each
# case is (function, the absence sentinel).
_ABSENCE_READS = [
    pytest.param(resume.read_control_file, None, id="read_control_file"),
    pytest.param(resume.read_index_object, None, id="read_index_object"),
]


class TestAbsenceVersusFailure:
    """A missing key returns the sentinel; any other failure raises.

    Absence and "could not look" must never read the same: a swallowed 403
    or missing bucket would turn a resume into a fresh run (marker) or hide
    a broken destination (index).
    """

    @pytest.mark.parametrize(("read", "absent"), _ABSENCE_READS)
    def test_missing_key_returns_absence_sentinel(self, s3_client, read, absent):
        assert read(s3_client, "dest-bucket", "out/.s3-archive-resume.x.json") is absent

    @pytest.mark.parametrize(("read", "absent"), _ABSENCE_READS)
    def test_missing_bucket_raises_not_absent(self, s3_client, read, absent):
        # On real S3 a HEAD here is a body-less 404, indistinguishable from
        # a missing key; the GET carries NoSuchBucket, which must raise.
        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            read(s3_client, "no-such-bucket", "out/.s3-archive-resume.x.json")
        assert excinfo.value.response["Error"]["Code"] == "NoSuchBucket"

    @pytest.mark.parametrize(("read", "absent"), _ABSENCE_READS)
    @pytest.mark.parametrize(
        ("code", "status"),
        [("AccessDenied", 403), ("403", 403), ("404", 404), ("SlowDown", 503)],
    )
    def test_non_nosuchkey_client_error_propagates(self, s3_client, read, absent, code, status):
        # "404" is the bare status a body-less response yields — ambiguous
        # between key and bucket, so it is a failure, not an absence.
        with Stubber(s3_client) as stub:
            stub.add_client_error("get_object", service_error_code=code, http_status_code=status)
            with pytest.raises(botocore.exceptions.ClientError) as excinfo:
                read(s3_client, "dest-bucket", "out/.s3-archive-resume.x.json")
        assert excinfo.value.response["Error"]["Code"] == code

    def test_read_index_object_returns_stored_bytes(self, s3_client):
        key = resume.index_key("out/", '"etag-1"')
        resume.write_index_object(s3_client, "dest-bucket", key, b"\x00idx-bytes")
        assert resume.read_index_object(s3_client, "dest-bucket", key) == b"\x00idx-bytes"


class TestBuildDoneSet:
    def test_maps_relative_path_to_size(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="out/a.txt", Body=b"hello")
        s3_client.put_object(Bucket="dest-bucket", Key="out/sub/b.txt", Body=b"worldish")

        done = resume.build_done_set(s3_client, "dest-bucket", "out/")
        assert done == {"a.txt": 5, "sub/b.txt": 8}

    def test_excludes_the_control_object(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="out/a.txt", Body=b"hello")
        key = resume.control_key("out/", '"etag-1"')
        resume.write_control_file(
            s3_client,
            "dest-bucket",
            key,
            source_etag='"etag-1"',
            source_size=1,
            fmt="zip",
            now_iso="2026-07-01T00:00:00+00:00",
        )

        done = resume.build_done_set(s3_client, "dest-bucket", "out/")
        # Only the real member — the marker is filtered out.
        assert done == {"a.txt": 5}

    def test_empty_prefix(self, s3_client):
        s3_client.put_object(Bucket="dest-bucket", Key="a.txt", Body=b"hi")
        done = resume.build_done_set(s3_client, "dest-bucket", "")
        assert done == {"a.txt": 2}


@pytest.mark.parametrize(
    ("done_size", "member_size", "should_skip"),
    [
        (5, 5, True),  # present at expected size → done
        (3, 5, False),  # present but wrong size → not done (partial/mismatch)
        (0, 0, True),  # empty member present as empty → done
    ],
)
def test_skip_rule(done_size, member_size, should_skip):
    """The skip predicate used by extract: done iff size matches exactly."""
    done = {"a.txt": done_size}
    assert (done.get("a.txt") == member_size) is should_skip


def test_skip_rule_absent_member_never_skips():
    done: dict[str, int] = {}
    assert (done.get("a.txt") == 0) is False
