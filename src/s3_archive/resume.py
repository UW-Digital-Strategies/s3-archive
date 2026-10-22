"""Format-agnostic core for ``extract --resume``.

Resume answers one question cheaply: *which members of this archive are
already durably written to the destination, so a re-run can skip them?*
The design uses two S3 artifacts, with a deliberate split of duties:

- **The control object is the identity guard.** A tiny JSON marker at
  ``{dest_prefix}/.s3-archive-resume.<etag>.json`` whose mere existence
  says "a ``--resume`` run for *this exact source* began writing here."
  Naming it by the source ``ETag`` makes the identity check fall out of
  the filename: same source → same ETag → same marker → resume; a changed
  source → a different ETag → no marker → a clean fresh run, automatically.
  The name carries no bucket/key (nothing to leak in an ``ls``), and the
  body deliberately stores no bucket/key either.

- **The destination objects are the progress ledger.** Each finished
  member is exactly one destination object, and ``upload_fileobj`` is
  all-or-nothing (s3transfer aborts the multipart on failure — never a
  half-written object). So "which members are done" is re-derived from a
  single ``LIST`` of the destination prefix, never a stored cursor that
  could drift. The control object's own key is excluded from that ledger.

The control object is written at the start of a fresh resume run and
deleted on clean completion, so only an *interrupted* run leaves one
behind — which is exactly the provenance a later re-run needs to trust
the destination objects it finds.

It is also where a member written as decoded despite failing its integrity
check is remembered across runs: the marker is rewritten with the
accumulated ``integrity_warnings`` before that member's object completes.
A re-run reads them back, so the final ``<dir>.log`` covers the whole
extract, and a warned member counts as done at the size it was actually
written rather than the size the archive declares.
"""

from __future__ import annotations

import json
import re

import botocore.exceptions

from s3_archive.integrity import IntegrityMismatch
from s3_archive.list import list_objects

# Bumped only if the on-disk control-file shape changes incompatibly.
SCHEMA_VERSION = 1

# Recognizes a resume *artifact* key — the ``.json`` control marker or the
# ``.idx`` seek-index companion (gzip resume) — so :func:`build_done_set`
# never counts either as an extracted member. Anchored to a path boundary
# (start or ``/``) so it matches both a full key and the prefix-stripped
# RelativePath the ledger walk sees.
RESUME_KEY_RE = re.compile(r"(?:^|/)\.s3-archive-resume\.[A-Za-z0-9._-]+\.(?:json|idx)$")

# Everything NOT allowed in a sanitized ETag. AWS/RGW ETags are hex + a
# ``-<n>`` multipart suffix, wrapped in quotes; stripping to this set
# keeps the marker key safe and predictable without inventing our own id.
_ETAG_UNSAFE_RE = re.compile(r"[^A-Za-z0-9._-]")


def is_control_key(key: str) -> bool:
    """True if *key* is a resume artifact — the ``.json`` marker or ``.idx`` index.

    *key* may be a full key or a prefix-stripped RelativePath. Used to keep
    both artifacts out of the destination-as-ledger done-set.
    """
    return RESUME_KEY_RE.search(key) is not None


def _marker_key(dest_prefix: str, source_etag: str, suffix: str) -> str:
    """Build a resume-artifact key ``.s3-archive-resume.<etag>.<suffix>`` under *dest_prefix*.

    Shared by :func:`control_key` (``suffix="json"``) and :func:`index_key`
    (``suffix="idx"``) so both artifacts land beside the extracted members
    under one ETag-sanitized name. Mirrors
    :func:`s3_archive.extract._dest_key`'s prefix-join rules. The ETag is
    sanitized (surrounding quotes stripped, then restricted to
    ``[A-Za-z0-9._-]``) so it's always a well-formed single path segment.
    """
    safe_etag = _ETAG_UNSAFE_RE.sub("", source_etag.strip('"'))
    name = f".s3-archive-resume.{safe_etag}.{suffix}"
    if not dest_prefix:
        return name
    if dest_prefix.endswith("/"):
        return dest_prefix + name
    return dest_prefix + "/" + name


def control_key(dest_prefix: str, source_etag: str) -> str:
    """Build the control marker's key (``.json``) under *dest_prefix* for *source_etag*."""
    return _marker_key(dest_prefix, source_etag, "json")


def index_key(dest_prefix: str, source_etag: str) -> str:
    """Build the seek-index companion's key (``.idx``) for *source_etag*.

    The gzip ``--resume`` path persists an :mod:`indexed_gzip` seek index
    here so a re-run can jump past already-done members without
    re-downloading (and re-decoding) the compressed source. ETag-named
    exactly like :func:`control_key`, so it shares the marker's identity
    guard: a changed source → different ETag → no matching index → a clean
    rebuild. It's a pure optimization — a missing / stale / corrupt
    ``.idx`` only costs more forward re-decode, never correctness.
    """
    return _marker_key(dest_prefix, source_etag, "idx")


def write_control_file(
    dst_client,
    bucket: str,
    key: str,
    *,
    source_etag: str,
    source_size: int,
    fmt: str,
    now_iso: str,
    integrity_warnings: dict[str, IntegrityMismatch] | None = None,
) -> None:
    """``put_object`` the control marker JSON at ``s3://bucket/key``.

    The body records enough to reason about a stalled run — the source
    ETag it belongs to, the source size, the format, and when it started —
    but deliberately **no bucket/key**, so an operator browsing the
    destination learns nothing about where the archive lives.

    *integrity_warnings* (member name → mismatch) is everything the run
    has so far written as decoded despite a failed integrity check; a
    rewrite passes the original *now_iso* so ``created_at`` keeps saying
    when the extract began.
    """
    warnings = integrity_warnings or {}
    body = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "source_etag": source_etag,
            "source_size": source_size,
            "format": fmt,
            "created_at": now_iso,
            "integrity_warnings": {name: m.to_json() for name, m in warnings.items()},
        },
        indent=2,
        # The marker is read by a human browsing the destination; the ETag
        # comes off the wire, so don't let a non-ASCII byte become \uXXXX.
        ensure_ascii=False,
    ).encode("utf-8")
    dst_client.put_object(Bucket=bucket, Key=key, Body=body)


def _is_no_such_key(exc: botocore.exceptions.ClientError) -> bool:
    """True if *exc* is S3's "this key does not exist" answer to a GET.

    The one absence signal for the resume artifacts. Keyed on the
    ``NoSuchKey`` error code, which a GET carries in its XML error body on
    AWS S3, Ceph RadosGW and MinIO. Every other code is a failure and must
    not read as absence: ``NoSuchBucket`` (wrong destination), ``AccessDenied``
    / ``403`` (and AWS answers a missing key with 403 when the caller lacks
    ``s3:ListBucket``), throttling, 5xx.

    Why GET and not HEAD: a HEAD response has no body, so botocore can only
    report the bare status ``"404"``, and a HEAD against a missing *bucket*
    returns the same body-less 404 on real S3. HEAD cannot tell "no marker"
    from "no bucket"; GET can.
    """
    return exc.response.get("Error", {}).get("Code", "") == "NoSuchKey"


def read_control_file(dst_client, bucket: str, key: str) -> dict | None:
    """The control marker at ``s3://bucket/key``, parsed, or ``None`` if there is none.

    Existence is the provenance signal: a prior ``--resume`` run for this
    exact source began writing here, so the destination objects can be
    trusted as a progress ledger. A missing marker means a fresh run —
    pre-existing objects, if any, aren't vouched for and are not skipped.

    A GET rather than a HEAD (the marker is a few hundred bytes) both
    returns the body and keeps absence distinguishable from failure — see
    :func:`_is_no_such_key`.

    Returns:
        The marker's JSON object, or None if the key does not exist.
        Absence is a normal answer (every fresh run).

    Raises:
        botocore.exceptions.ClientError: for any error other than
            ``NoSuchKey`` — a missing bucket, a permissions failure,
            throttling. A failure to look is never reported as "no marker",
            which would silently turn a resume into a fresh run.
        botocore.exceptions.BotoCoreError: for transport failures
            (connection, timeout), unhandled here.
        ValueError: the body is not a JSON object — a damaged marker
            stops the run rather than being mistaken for a fresh start.
    """
    try:
        resp = dst_client.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        if _is_no_such_key(exc):
            return None
        raise
    body = json.loads(resp["Body"].read().decode("utf-8"))
    if not isinstance(body, dict):
        raise ValueError(f"resume control marker {key!r} is not a JSON object")
    return body


def marker_integrity_warnings(marker: dict) -> dict[str, IntegrityMismatch]:
    """The ``integrity_warnings`` a control marker carries, by member name.

    A marker written before this field existed has none, and that is the
    truth rather than a default: the versions that wrote such markers
    stopped at the first member that failed its integrity check, so no
    member was ever written as decoded under them. A field that is present
    but malformed raises.
    """
    return {
        name: IntegrityMismatch.from_json(data)
        for name, data in marker.get("integrity_warnings", {}).items()
    }


def delete_control_file(dst_client, bucket: str, key: str) -> None:
    """Delete the control marker at ``s3://bucket/key`` (clean-completion path)."""
    dst_client.delete_object(Bucket=bucket, Key=key)


def write_index_object(dst_client, bucket: str, key: str, data: bytes) -> None:
    """``put_object`` the gzip seek index *data* at ``s3://bucket/key``.

    A single PUT is atomic (no half-written object), so a periodic
    checkpoint either lands whole or not at all. The index is a pure
    optimization, so the caller may swallow a failed PUT — see
    :func:`s3_archive.resume.index_key`.
    """
    dst_client.put_object(Bucket=bucket, Key=key, Body=data)


def read_index_object(dst_client, bucket: str, key: str) -> bytes | None:
    """Return the seek-index bytes at ``s3://bucket/key``, or ``None`` if absent.

    A missing index is the normal case on a *fresh* resume run (no
    checkpoint written yet) and on a changed source (different ETag → no
    matching ``.idx``): both degrade gracefully to forward re-decode.

    Returns:
        The index bytes, or None if the key does not exist. Absence is a
        normal answer.

    Raises:
        botocore.exceptions.ClientError: for any error other than
            ``NoSuchKey`` (see :func:`_is_no_such_key`). The index is an
            optimization, but a failure to read it is still a failure, not
            a miss: the run reads the same destination for its marker and
            its done-set, so a 403 or a missing bucket here means the run
            cannot resume correctly, and it stops before any write.
        botocore.exceptions.BotoCoreError: for transport failures,
            unhandled here.
    """
    try:
        resp = dst_client.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        if _is_no_such_key(exc):
            return None
        raise
    return resp["Body"].read()


def delete_index_object(dst_client, bucket: str, key: str) -> None:
    """Delete the seek-index companion at ``s3://bucket/key`` (clean-completion path)."""
    dst_client.delete_object(Bucket=bucket, Key=key)


def build_done_set(dst_client, bucket: str, dest_prefix: str) -> dict[str, int]:
    """Return ``{RelativePath: Size}`` for every extracted member under *dest_prefix*.

    One paginated ``LIST`` — far cheaper than a HEAD per member — reusing
    :func:`s3_archive.list.list_objects` (which already skips directory
    markers). The control object is excluded so it's never mistaken for a
    member. The returned relative paths equal ``ArchiveMember.name``
    post-normalization, so the extract loop can match members directly.
    """
    done: dict[str, int] = {}
    for obj in list_objects(dst_client, bucket, dest_prefix):
        rel = obj["RelativePath"]
        if not rel or is_control_key(rel):
            continue
        done[rel] = obj["Size"]
    return done
