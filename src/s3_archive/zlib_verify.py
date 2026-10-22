"""Forward-only verify tap for a bare zlib (RFC 1950) stream.

Some objects only *sniff* as zlib: libmagic's ``application/zlib`` rule
is two header bytes, which one in a few thousand random (or encrypted)
objects satisfies by chance. Whether the object really is zlib is
whether it inflates to the end of the stream, passes zlib's own Adler-32
check, and leaves nothing after that end. :class:`ZlibVerifyTap` answers
that in the same pass as whatever else reads the bytes — hashing, upload
— without re-reading the object.

:meth:`ZlibVerifyTap.wrap` yields every chunk unchanged. The tap engages
only when the first two bytes form a valid zlib header; otherwise it is
inert and does no decompression work. Engaged, it feeds a
``zlib.decompressobj`` and discards the output, bounded per call by
:data:`MAX_OUTPUT_PER_CALL`, so a deflate bomb costs CPU, never memory.
Input is fed in slices of at most :data:`MAX_INPUT_PER_CALL`, so the
``unconsumed_tail`` each capped call hands back, and copies, stays small
however large the caller's chunks are. Once the source is exhausted,
:attr:`ZlibVerifyTap.verdict` holds a :class:`ZlibVerdict`.

Pure stdlib: no S3, no payload output. Decoding a payload for the
consumer is a different job from verifying one.
"""

import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import Enum

from s3_archive.constants import CHUNK_SIZE

# Upper bound on the decompressed bytes one ``decompress`` call may return.
# Each call's output is dropped before the next, so this is the tap's peak
# output memory however large the stream inflates: one chunk, the same
# per-stream footprint as every other streaming primitive here.
MAX_OUTPUT_PER_CALL = CHUNK_SIZE

# Upper bound on the compressed bytes handed to one ``decompress`` call. A
# capped call returns the input it did not reach as a fresh
# ``unconsumed_tail`` copy, so feeding a whole large chunk at once would copy
# its remainder on every call: quadratic in the chunk size.
MAX_INPUT_PER_CALL = CHUNK_SIZE

_HEADER_LEN = 2


class ZlibOutcome(Enum):
    """What the tap concluded about the stream."""

    NOT_ZLIB = "not zlib"
    VALID = "valid"
    INVALID = "invalid"


class ZlibFailure(Enum):
    """Why an engaged stream is :attr:`ZlibOutcome.INVALID`."""

    INFLATE_ERROR = "inflate error"
    TRUNCATED = "truncated"
    TRAILING_BYTES = "trailing bytes"


@dataclass(frozen=True, slots=True)
class ZlibVerdict:
    """The tap's conclusion: an outcome, and for INVALID the failure and zlib's detail.

    *failure* is set exactly when *outcome* is INVALID. *detail* is zlib's
    own message for an inflate error (``"incorrect data check"`` is a bad
    Adler-32) and empty otherwise.
    """

    outcome: ZlibOutcome
    failure: ZlibFailure | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        if (self.outcome is ZlibOutcome.INVALID) != (self.failure is not None):
            raise ValueError(f"failure must be set exactly for INVALID, got {self!r}")

    def describe(self) -> str:
        """One line for a log record: ``valid``, ``not zlib``, ``invalid: truncated``..."""
        if self.failure is None:
            return self.outcome.value
        suffix = f" ({self.detail})" if self.detail else ""
        return f"{self.outcome.value}: {self.failure.value}{suffix}"


def is_zlib_header(head: bytes) -> bool:
    """True when the first two bytes of *head* are a valid zlib header with no preset dictionary.

    CM must be 8 (deflate), CINFO at most 7 (a window of 32 KiB or less),
    ``CMF*256 + FLG`` a multiple of 31, and FDICT unset: a stream that
    needs a preset dictionary cannot be verified without it.
    """
    if len(head) < _HEADER_LEN:
        return False
    cmf, flg = head[0], head[1]
    return cmf & 0x0F == 8 and cmf >> 4 <= 7 and ((cmf << 8) | flg) % 31 == 0 and not flg & 0x20


class ZlibVerifyTap:
    """Pass-through tap that verifies a zlib stream as its chunks go past.

    One tap verifies one stream: call :meth:`wrap` once, consume the
    returned iterator to the end, then read :attr:`verdict`. "To the end"
    means the iterator itself raises StopIteration: an outer wrapper that
    stops pulling once it holds every byte (``islice``, a size-limited
    generator) leaves the verdict unset, and reading it raises.
    """

    def __init__(self) -> None:
        self._wrapped = False
        self._head = b""
        self._decomp: zlib._Decompress | None = None
        self._failure: ZlibFailure | None = None
        self._detail = ""
        self._verdict: ZlibVerdict | None = None

    @property
    def verdict(self) -> ZlibVerdict:
        """The verdict; reading it before the wrapped stream is exhausted raises."""
        if self._verdict is None:
            raise RuntimeError("ZlibVerifyTap.verdict read before the wrapped stream was exhausted")
        return self._verdict

    def wrap(self, chunks: Iterable[bytes]) -> Iterator[bytes]:
        """Yield every chunk of *chunks* unchanged, verifying them as zlib if the head says so."""
        if self._wrapped:
            raise RuntimeError("a ZlibVerifyTap verifies one stream; wrap() was called twice")
        self._wrapped = True
        return self._pass_through(chunks)

    def _pass_through(self, chunks: Iterable[bytes]) -> Iterator[bytes]:
        for chunk in chunks:
            self._observe(chunk)
            yield chunk
        self._verdict = self._finish()

    def _observe(self, chunk: bytes) -> None:
        if self._decomp is None:
            chunk = self._sniff(chunk)
        if self._decomp is not None and self._failure is None:
            self._inflate(self._decomp, chunk)

    def _sniff(self, chunk: bytes) -> bytes:
        """Accumulate the header; engage on a zlib head and return the bytes to inflate.

        The head may arrive split across chunks, so bytes are held until two
        are in hand. Only the first two are kept afterwards, engaged or inert.
        """
        if len(self._head) >= _HEADER_LEN:
            return b""  # Head already seen and it was not zlib: inert.
        pending = self._head + chunk
        self._head = pending[:_HEADER_LEN]
        if is_zlib_header(self._head):
            self._decomp = zlib.decompressobj()
        return pending

    def _inflate(self, decomp: "zlib._Decompress", data: bytes) -> None:
        view = memoryview(data)
        for start in range(0, len(view), MAX_INPUT_PER_CALL):
            self._inflate_slice(decomp, view[start : start + MAX_INPUT_PER_CALL])
            if self._failure is not None:
                return

    def _inflate_slice(self, decomp: "zlib._Decompress", data: memoryview) -> None:
        if decomp.eof:
            if data:
                self._failure = ZlibFailure.TRAILING_BYTES
            return
        try:
            decomp.decompress(data, MAX_OUTPUT_PER_CALL)
            while decomp.unconsumed_tail and not decomp.eof:
                decomp.decompress(decomp.unconsumed_tail, MAX_OUTPUT_PER_CALL)
        except zlib.error as exc:
            self._failure = ZlibFailure.INFLATE_ERROR
            self._detail = str(exc)
            return
        if decomp.unused_data:
            self._failure = ZlibFailure.TRAILING_BYTES

    def _finish(self) -> ZlibVerdict:
        decomp = self._decomp
        if decomp is None:
            return ZlibVerdict(ZlibOutcome.NOT_ZLIB)
        if self._failure is None and not decomp.eof:
            # Output zlib still holds back (a max_length-capped call can spend
            # its input with output pending) never needs draining here: eof
            # comes only after the Adler-32 trailer, which zlib reads after
            # the last output byte, so a source that ends before eof is
            # truncated whatever is pending.
            self._failure = ZlibFailure.TRUNCATED
        if self._failure is not None:
            return ZlibVerdict(ZlibOutcome.INVALID, self._failure, self._detail)
        return ZlibVerdict(ZlibOutcome.VALID)
