"""Tests for s3_archive.zlib_verify.

Every case asserts the pass-through contract (the wrapped chunks come out
byte-identical, chunk for chunk) alongside the verdict. The bomb case
observes each ``decompress`` call's output through a recording wrapper,
so the per-call bound is asserted, not assumed.
"""

import json
import random
import zlib

import pytest

from s3_archive import zlib_verify
from s3_archive.zlib_verify import (
    MAX_INPUT_PER_CALL,
    MAX_OUTPUT_PER_CALL,
    ZlibFailure,
    ZlibOutcome,
    ZlibVerdict,
    ZlibVerifyTap,
    is_zlib_header,
)

from .conftest import incompressible_bytes


def _waveform_json() -> bytes:
    """An audiowaveform-shaped JSON document: header fields plus a long min/max sample list."""
    rng = random.Random(1950)  # noqa: S311 - test fixture, not crypto
    samples = []
    for _ in range(20_000):
        low = rng.randint(-128, 0)
        samples += [low, rng.randint(low, 127)]
    doc = {
        "version": 2,
        "channels": 1,
        "sample_rate": 44100,
        "samples_per_pixel": 256,
        "bits": 8,
        "length": len(samples) // 2,
        "data": samples,
    }
    return json.dumps(doc).encode("utf-8")


def _chunked(data: bytes, sizes: list[int]) -> list[bytes]:
    """Split *data* into chunks cycling through *sizes*."""
    chunks, pos, i = [], 0, 0
    while pos < len(data):
        size = sizes[i % len(sizes)]
        chunks.append(data[pos : pos + size])
        pos += size
        i += 1
    return chunks


def _run(chunks: list[bytes]) -> ZlibVerdict:
    """Pass *chunks* through a fresh tap; assert they come out unchanged; return the verdict."""
    tap = ZlibVerifyTap()
    assert list(tap.wrap(chunks)) == chunks
    return tap.verdict


def _invalid(failure: ZlibFailure) -> ZlibVerdict:
    return ZlibVerdict(ZlibOutcome.INVALID, failure)


_WAVEFORM_Z = zlib.compress(_waveform_json())


class TestIsZlibHeader:
    @pytest.mark.parametrize("head", [b"\x78\x9c", b"\x78\x01", b"\x78\xda", b"\x08\x1d"])
    def test_common_headers(self, head):
        assert is_zlib_header(head)

    @pytest.mark.parametrize(
        ("head", "why"),
        [
            (b"\x79\x9c", "CM is not 8"),
            (b"\x88\x98", "CINFO is 8"),
            (b"\x78\x9d", "check bits wrong"),
            (b"\x78\xbb", "FDICT set"),
            (b"\x78", "too short"),
            (b"", "empty"),
        ],
    )
    def test_rejects(self, head, why):
        assert not is_zlib_header(head), why


class TestValid:
    @pytest.mark.parametrize("sizes", [[1], [7, 3, 1013], [1, 1, 65536], [len(_WAVEFORM_Z)]])
    def test_real_shape_stream_in_odd_chunks(self, sizes):
        assert _run(_chunked(_WAVEFORM_Z, sizes)) == ZlibVerdict(ZlibOutcome.VALID)

    def test_empty_chunks_in_the_stream_are_harmless(self):
        chunks = [b"", _WAVEFORM_Z[:1], b"", _WAVEFORM_Z[1:], b""]
        assert _run(chunks).outcome is ZlibOutcome.VALID

    def test_bomb_is_valid_with_bounded_output_per_call(self, monkeypatch):
        outputs: list[int] = []
        real_decompressobj = zlib.decompressobj

        class RecordingDecompress:
            def __init__(self):
                self._inner = real_decompressobj()

            def decompress(self, data, max_length=0):
                out = self._inner.decompress(data, max_length)
                outputs.append(len(out))
                return out

            def __getattr__(self, name):
                return getattr(self._inner, name)

        monkeypatch.setattr(zlib_verify.zlib, "decompressobj", RecordingDecompress)

        compressor = zlib.compressobj()
        zeros = bytes(1024 * 1024)
        bomb = b"".join(compressor.compress(zeros) for _ in range(200)) + compressor.flush()
        assert len(bomb) < 1024 * 1024  # 200 MiB inflated from well under 1 MiB

        assert _run(_chunked(bomb, [65536])) == ZlibVerdict(ZlibOutcome.VALID)
        assert sum(outputs) == 200 * 1024 * 1024
        assert max(outputs) <= MAX_OUTPUT_PER_CALL

    def test_a_large_chunk_is_fed_in_bounded_slices(self, monkeypatch):
        inputs: list[int] = []
        real_decompressobj = zlib.decompressobj

        class RecordingDecompress:
            def __init__(self):
                self._inner = real_decompressobj()

            def decompress(self, data, max_length=0):
                inputs.append(len(data))
                return self._inner.decompress(data, max_length)

            def __getattr__(self, name):
                return getattr(self._inner, name)

        monkeypatch.setattr(zlib_verify.zlib, "decompressobj", RecordingDecompress)

        # Incompressible payload: the stream is about as long as its output,
        # so one 4 MiB chunk would otherwise be re-copied on every capped call.
        stream = zlib.compress(incompressible_bytes(4 * 1024 * 1024, seed=47))
        assert _run([stream]) == ZlibVerdict(ZlibOutcome.VALID)
        assert max(inputs) <= MAX_INPUT_PER_CALL


class TestInvalid:
    def test_forged_header_on_random_bytes(self):
        body = incompressible_bytes(64 * 1024, seed=31)
        verdict = _run(_chunked(b"\x78\x9c" + body, [4096]))
        assert verdict.outcome is ZlibOutcome.INVALID
        assert verdict.failure is ZlibFailure.INFLATE_ERROR
        assert verdict.detail

    def test_bad_adler32_is_an_inflate_error(self):
        corrupt = _WAVEFORM_Z[:-1] + bytes([_WAVEFORM_Z[-1] ^ 0xFF])
        verdict = _run(_chunked(corrupt, [1000]))
        assert verdict.failure is ZlibFailure.INFLATE_ERROR
        assert "incorrect data check" in verdict.detail

    @pytest.mark.parametrize("cut", [2, 3, 100, -4, -1])
    def test_truncated(self, cut):
        assert _run(_chunked(_WAVEFORM_Z[:cut], [997])) == _invalid(ZlibFailure.TRUNCATED)

    def test_trailing_bytes_in_the_final_chunk(self):
        verdict = _run([_WAVEFORM_Z + b"\x00"])
        assert verdict == _invalid(ZlibFailure.TRAILING_BYTES)

    def test_trailing_bytes_in_a_later_chunk(self):
        verdict = _run([_WAVEFORM_Z, b"", b"junk"])
        assert verdict == _invalid(ZlibFailure.TRAILING_BYTES)

    def test_trailing_bytes_one_byte_at_a_time(self):
        verdict = _run(_chunked(_WAVEFORM_Z + b"tail", [1]))
        assert verdict == _invalid(ZlibFailure.TRAILING_BYTES)


class TestNotZlib:
    def test_non_zlib_head_passes_through_untouched(self, monkeypatch):
        def no_decompress():
            raise AssertionError("an inert tap must not build a decompressor")

        monkeypatch.setattr(zlib_verify.zlib, "decompressobj", no_decompress)
        chunks = [b"PK\x03\x04", b"rest of a zip", b"\x78\x9c later is irrelevant"]
        assert _run(chunks) == ZlibVerdict(ZlibOutcome.NOT_ZLIB)

    def test_header_split_across_one_byte_chunks(self):
        assert _run([b"\x78", b"\x9d", b"xyz"]).outcome is ZlibOutcome.NOT_ZLIB

    def test_empty_stream(self):
        assert _run([]) == ZlibVerdict(ZlibOutcome.NOT_ZLIB)

    def test_one_byte_stream(self):
        assert _run([b"\x78"]) == ZlibVerdict(ZlibOutcome.NOT_ZLIB)


class TestContract:
    def test_verdict_before_exhaustion_raises(self):
        tap = ZlibVerifyTap()
        stream = tap.wrap([_WAVEFORM_Z[:10], _WAVEFORM_Z[10:]])
        with pytest.raises(RuntimeError, match="before the wrapped stream was exhausted"):
            _ = tap.verdict
        next(stream)
        with pytest.raises(RuntimeError):
            _ = tap.verdict

    def test_wrap_twice_raises(self):
        tap = ZlibVerifyTap()
        tap.wrap([])
        with pytest.raises(RuntimeError, match="wrap"):
            tap.wrap([])

    def test_verdict_rejects_inconsistent_fields(self):
        with pytest.raises(ValueError, match="INVALID"):
            ZlibVerdict(ZlibOutcome.INVALID)
        with pytest.raises(ValueError, match="INVALID"):
            ZlibVerdict(ZlibOutcome.VALID, ZlibFailure.TRUNCATED)

    def test_describe(self):
        assert ZlibVerdict(ZlibOutcome.VALID).describe() == "valid"
        assert ZlibVerdict(ZlibOutcome.NOT_ZLIB).describe() == "not zlib"
        assert _invalid(ZlibFailure.TRUNCATED).describe() == "invalid: truncated"
        inflate = ZlibVerdict(ZlibOutcome.INVALID, ZlibFailure.INFLATE_ERROR, "bad")
        assert inflate.describe() == "invalid: inflate error (bad)"
