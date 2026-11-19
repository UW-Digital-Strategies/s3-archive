"""Tests for s3_archive.hashing primitives.

Known-vector coverage for :func:`triple_hash`, :func:`multi_hash`, and
:func:`stream_hash_object`; behavioral coverage for
:class:`HashingTap`'s ``read(n)`` / ``drain`` contract — drained tap
output must equal a straight :func:`triple_hash` of the same bytes.
"""

import hashlib
import io

from s3_archive.constants import CHUNK_SIZE
from s3_archive.hashing import (
    HashingTap,
    TripleHash,
    body_chunks,
    multi_hash,
    stream_hash_object,
    triple_hash,
)

from .fixture_values import (
    EMPTY_MD5,
    EMPTY_SHA1,
    EMPTY_SHA256,
    HELLO,
    HELLO_MD5,
    HELLO_SHA1,
    HELLO_SHA256,
)


class TestTripleHash:
    def test_empty(self):
        result = triple_hash(iter([]))
        assert result == TripleHash(
            md5=EMPTY_MD5,
            sha1=EMPTY_SHA1,
            sha256=EMPTY_SHA256,
            size=0,
        )

    def test_known_vector_single_chunk(self):
        result = triple_hash([HELLO])
        assert result.size == 5
        assert result.md5 == HELLO_MD5
        assert result.sha1 == HELLO_SHA1
        assert result.sha256 == HELLO_SHA256

    def test_chunked_equals_single_blob(self):
        data = b"the quick brown fox jumps over the lazy dog"
        a = triple_hash([data])
        b = triple_hash([data[:5], data[5:20], data[20:]])
        assert a == b

    def test_large_synthetic(self):
        chunk = b"A" * 1024
        n_chunks = 16 * 1024  # 16 MiB total
        result = triple_hash(chunk for _ in range(n_chunks))
        assert result.size == 16 * 1024 * 1024
        assert result.sha256 == hashlib.sha256(b"A" * result.size).hexdigest()


class TestMultiHash:
    def test_empty_algorithms(self):
        # No algorithms requested => no work done, no bytes read.
        consumed = []

        def gen():
            for chunk in [b"abc", b"def"]:
                consumed.append(chunk)
                yield chunk

        result = multi_hash(gen(), [])
        assert result == {}
        assert consumed == []

    def test_single_algorithm(self):
        result = multi_hash([HELLO], ["sha256"])
        assert result == {"sha256": HELLO_SHA256}

    def test_multiple_algorithms_single_pass(self):
        # md5 + sha256 over "hello"
        result = multi_hash([HELLO], ["md5", "sha256"])
        assert result == {"md5": HELLO_MD5, "sha256": HELLO_SHA256}

    def test_sha512_supported(self):
        result = multi_hash([HELLO], ["sha512"])
        assert result == {"sha512": hashlib.sha512(HELLO).hexdigest()}


class TestBodyChunks:
    def test_emits_chunks_until_eof(self):
        body = io.BytesIO(b"X" * (CHUNK_SIZE + 100))
        chunks = list(body_chunks(body))
        assert len(chunks) == 2
        assert len(chunks[0]) == CHUNK_SIZE
        assert len(chunks[1]) == 100

    def test_custom_chunk_size(self):
        body = io.BytesIO(b"abcdefgh")
        chunks = list(body_chunks(body, chunk_size=3))
        assert chunks == [b"abc", b"def", b"gh"]


class TestStreamHashObject:
    def test_round_trip(self, s3_client):
        s3_client.put_object(Bucket="src-bucket", Key="obj", Body=HELLO)
        digests = stream_hash_object(s3_client, "src-bucket", "obj", ["md5", "sha256"])
        assert digests == {"md5": HELLO_MD5, "sha256": HELLO_SHA256}


class TestHashingTap:
    """The HashingTap is the load-bearing primitive for inline archive walks.

    Its drained output must match a straight :func:`triple_hash` of the
    same bytes, even when the consumer reads less than the whole body
    before drain().
    """

    def test_drain_full_bytes_matches_triple_hash(self):
        data = b"This is the parent archive's bytes. " * 1000
        chunks = [data[i : i + 64] for i in range(0, len(data), 64)]

        tap = HashingTap(iter(chunks))
        # Consume some bytes through read() to exercise the tap path.
        first = tap.read(100)
        assert len(first) == 100
        # Drain the rest.
        tap.drain()

        digests = tap.hexdigests()
        expected = triple_hash([data])
        assert digests["md5"] == expected.md5
        assert digests["sha1"] == expected.sha1
        assert digests["sha256"] == expected.sha256
        assert tap.size == len(data)

    def test_drain_after_partial_buffer(self):
        # Buffer holds bytes pulled but not returned; drain() must
        # hash those before consuming the iterator.
        chunks = [b"AAAA", b"BBBB", b"CCCC", b"DDDD"]
        tap = HashingTap(iter(chunks))
        # n=2 leaves "AA" in the buffer's tail
        first = tap.read(2)
        assert first == b"AA"
        tap.drain()
        expected = triple_hash([b"AAAABBBBCCCCDDDD"])
        assert tap.hexdigests()["sha256"] == expected.sha256
        assert tap.size == 16

    def test_read_n_returns_exactly_n_bytes_until_eof(self):
        """tarfile reads exactly info.size bytes; short reads would corrupt the tar."""
        # Upstream yields 7-byte chunks; the consumer asks for 10 at a time.
        body = b"X" * 100
        chunks = [body[i : i + 7] for i in range(0, len(body), 7)]
        tap = HashingTap(iter(chunks))
        collected = []
        while True:
            data = tap.read(10)
            if not data:
                break
            collected.append(data)
        assert b"".join(collected) == body
        # All but possibly the last read returned exactly 10 bytes.
        full_reads = collected[:-1]
        assert all(len(c) == 10 for c in full_reads)

    def test_read_all_with_negative_n(self):
        chunks = [b"abc", b"def", b"ghi"]
        tap = HashingTap(iter(chunks))
        assert tap.read(-1) == b"abcdefghi"
        assert tap.hexdigests()["sha256"] == hashlib.sha256(b"abcdefghi").hexdigest()

    def test_custom_algorithms(self):
        tap = HashingTap(iter([HELLO]), algorithms=("sha256",))
        tap.drain()
        assert tap.hexdigests() == {"sha256": HELLO_SHA256}

    def test_finalize_triple(self):
        tap = HashingTap(iter([HELLO]))
        tap.drain()
        result = tap.finalize_triple()
        assert result == TripleHash(
            md5=HELLO_MD5,
            sha1=HELLO_SHA1,
            sha256=HELLO_SHA256,
            size=5,
        )

    def test_readable_seekable(self):
        tap = HashingTap(iter([b"x"]))
        assert tap.readable() is True
        assert tap.seekable() is False
