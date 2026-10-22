"""Streaming archive extract from S3 to S3.

``extract`` streams the archive object out of S3 (via *src_client*),
decodes it via :func:`s3_archive.members.iter_archive_members`, and
``upload_fileobj`` each member back to S3 (via *dst_client*) — nothing
is staged on local disk. A 500 GB archive does not need 500 GB of free
space anywhere.

The two clients may be the same boto3 client (single-endpoint workflows)
or two clients pointed at different endpoints (e.g. archive at AWS,
extracted tree at Ceph/RGW). The streaming model is identical either
way — see docs/ARCHITECTURE.md.
"""

import contextlib
import functools
import tarfile
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime

from s3_archive import extract_log
from s3_archive import resume as resume_mod
from s3_archive.exceptions import ResumeUnsupportedError
from s3_archive.integrity import IntegrityMismatch
from s3_archive.iter import IterableFileobj
from s3_archive.log_config import get_logger
from s3_archive.members import (
    ArchiveMember,
    _apply_safe_keys,
    check_archive_format,
    iter_archive_members,
)
from s3_archive.seekable import (
    iter_tar_members_seekable,
    iter_zip_members_seekable,
    open_seekable,
)

log = get_logger(__name__)

# Fileobj-driven resume iterators: zip (central directory) and
# uncompressed tar (512-byte-aligned headers) are walked per-member
# through a SeekableS3Object file object. 7z (v2), tar.gz (v3) and
# multi-block tar.xz (v4) are resumable too but take different paths —
# py7zr push-style targeted extraction, and a seekable decompressor over
# the decoded tar — so they're not in this table; see
# ``_begin_resume_seven_z`` / ``_begin_resume_gzip`` / ``_begin_resume_xz``.
_RESUME_ITERATORS: dict[str, Callable[[object], Iterator[ArchiveMember]]] = {
    # extract always tolerates a member that fails its integrity check.
    "zip": functools.partial(iter_zip_members_seekable, tolerate_integrity_errors=True),
    "tar": iter_tar_members_seekable,
}
# The one canonical set of formats ``--resume`` accepts. Non-solid 7z is
# conditionally supported (solid refuses at probe time); a single-block
# .tar.xz (no interior seek point) refuses at open time; every format not
# here — including .tar.bz2 and .tar.zst — refuses up front. See
# docs/RESUMABLE-EXTRACT.md (and the "Resumable extract for .tar.bz2 (and
# other seek-hostile formats)" idea in docs/someday-maybe/ for why bz2 is
# out).
RESUMABLE_FORMATS = frozenset({*_RESUME_ITERATORS, "7z", "tar.gz", "tar.xz"})


@dataclass(frozen=True)
class ExtractEvent:
    """One progress event emitted during :func:`extract`.

    Designed to map cleanly onto ``tqdm.update(bytes_transferred)`` plus
    ``tqdm.set_postfix(file=member)`` without the callback needing to
    track state itself. Events come in two shapes:

    - **Byte-transfer events** during an upload (``bytes_transferred > 0``):
      ``member`` and ``member_index`` identify the file in flight,
      ``bytes_transferred`` is the delta since the last event (raw from
      boto3's per-chunk ``Callback=``), and ``member_size`` is the file's
      known uncompressed size (``None`` if the archive format doesn't
      expose it before extraction, e.g. for streamed tar entries).

    - **Member-boundary events** (``bytes_transferred == 0``): emitted
      once before each upload begins, so a UI can switch its "current
      file" indicator without waiting for the first chunk.

    Frozen dataclass: the event is shared by reference into a callback
    that may run on boto3's transfer threadpool, so it must not mutate
    after construction.
    """

    # Read by s3-bagit's progress bar (src/s3_bagit/cli.py), outside this repo.
    # re-evaluate-by: next comprehensive review.
    # pysmelly: ignore[write-only-attributes]  # noqa: ERA001
    member: str
    bytes_transferred: int
    member_index: int
    member_size: int | None = None


# The return value is ignored, so the alias accepts any return type: the CLI
# hands these hooks straight to tqdm, whose ``update`` returns bool | None.
ProgressCallback = Callable[[ExtractEvent], object]


@dataclass
class ExtractReport:
    """What one :func:`extract_with_report` run did.

    *written* is the member names written (or, in dry-run, that would
    be) by this run, relative to the destination prefix — what
    :func:`extract` returns. *already_present* counts the members a
    ``--resume`` run skipped because a prior run wrote them (always 0
    for a 7z resume, whose iterator never yields done members).

    *integrity_warnings* maps each member that failed the archive's own
    CRC-32 / size check — and was written as decoded anyway — to its
    mismatch. A caller that turns the run into an exit status should
    treat a non-empty map as "completed with integrity warnings", not as
    a clean success.
    """

    written: list[str] = field(default_factory=list)
    already_present: int = 0
    integrity_warnings: dict[str, IntegrityMismatch] = field(default_factory=dict)
    # Where the ``<dir>.log`` sidecar was written (see
    # :mod:`s3_archive.extract_log`); ``None`` in dry-run and for a
    # bucket-root destination.
    log_key: str | None = None


def _member_chunks_then(
    member: ArchiveMember, on_consumed: Callable[[ArchiveMember], None]
) -> Iterator[bytes]:
    """Yield *member*'s chunks, then call *on_consumed* before signalling EOF.

    ``upload_fileobj`` only completes the destination object after it
    reads EOF, so whatever *on_consumed* records about the member (its
    integrity mismatch) is recorded strictly before the member's object
    exists — and an exception there aborts the upload instead of leaving
    an object whose warning was never recorded.
    """
    yield from member.chunks()
    on_consumed(member)


def _dest_key(prefix: str, member_name: str) -> str:
    """Join *prefix* (may be ``""`` or end-with-``/``) and *member_name* into an S3 key."""
    if not prefix:
        return member_name
    if prefix.endswith("/"):
        return prefix + member_name
    return prefix + "/" + member_name


def _make_upload_callback(
    on_progress: ProgressCallback,
    member_name: str,
    member_index: int,
    member_size: int | None,
) -> Callable[[int], None]:
    """Build a one-arg ``Callback=`` for ``boto3.upload_fileobj``.

    Extracted out of the extract loop so the closure doesn't capture
    loop variables (ruff B023). boto3 calls this with a single int:
    bytes transferred since the previous invocation.
    """

    def _cb(bytes_transferred: int) -> None:
        on_progress(
            ExtractEvent(
                member=member_name,
                bytes_transferred=bytes_transferred,
                member_index=member_index,
                member_size=member_size,
            )
        )

    return _cb


def extract(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    fix_unsafe_paths: bool = False,
    resume: bool = False,
    on_progress: ProgressCallback | None = None,
    on_read: Callable[[int], object] | None = None,
) -> list[str]:
    """Stream an archive from S3 and upload each member back to S3.

    Returns the list of member names that were (or would be) written,
    relative to *dest_prefix*. This is :func:`extract_with_report` with
    only the member list kept; a caller that needs to know whether any
    member was written despite failing its integrity check (the CLI's
    exit 3) calls that instead. Every argument is documented there.
    """
    return extract_with_report(
        src_client,
        dst_client,
        archive_bucket,
        archive_key,
        dest_bucket,
        dest_prefix,
        fmt,
        dry_run=dry_run,
        verbose=verbose,
        fix_unsafe_paths=fix_unsafe_paths,
        resume=resume,
        on_progress=on_progress,
        on_read=on_read,
    ).written


def _record_integrity(member: ArchiveMember, report: ExtractReport, *, dry_run: bool) -> bool:
    """Log and record *member*'s integrity mismatch, if its bytes had one; True if so."""
    mismatch = member.integrity
    if mismatch is None:
        return False
    report.integrity_warnings[member.name] = mismatch
    log.warning(
        "%s: %s — %s",
        member.name,
        mismatch.describe(),
        "would be written as decoded" if dry_run else "written as decoded",
    )
    return True


def _upload_member(
    dst_client,
    member: ArchiveMember,
    dest_bucket: str,
    dest_key: str,
    *,
    on_consumed: Callable[[ArchiveMember], None],
    callback: Callable[[int], None] | None,
) -> None:
    """``upload_fileobj`` one member, calling *on_consumed* before its object completes."""
    body = IterableFileobj(_member_chunks_then(member, on_consumed))
    if callback is None:
        dst_client.upload_fileobj(body, dest_bucket, dest_key)
    else:
        dst_client.upload_fileobj(body, dest_bucket, dest_key, Callback=callback)


# Complexity 13: the format dispatch, the --resume/--dry-run modes, and
# the streaming→seekable fallback each add branches to one linear walk.
# Parked at onboarding (2026-08-13); re-evaluate-by: 2026-11 review.
# Ledger: docs/HOWTO-SIMPLIFY.md.
def extract_with_report(  # noqa: C901
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
    fix_unsafe_paths: bool = False,
    resume: bool = False,
    on_progress: ProgressCallback | None = None,
    on_read: Callable[[int], object] | None = None,
) -> ExtractReport:
    """Stream an archive from S3 and upload each member back to S3.

    *src_client* reads the archive object; *dst_client* writes the
    extracted members. They may be the same boto3 client. *fmt* is one
    of the strings returned by :func:`s3_archive.url.detect_format`.
    Returns an :class:`ExtractReport`.

    A zip or 7z member whose decoded bytes fail the archive's own CRC-32
    / size check is written as decoded, logged as a WARNING, recorded in
    :attr:`ExtractReport.integrity_warnings`, and the extract continues —
    what Info-ZIP ``unzip`` and 7-Zip do. A codec that rejects its input
    (there are no decoded bytes to write), and any tar-family stream
    error, is still fatal; see docs/DECISIONS.md.

    *on_progress*, if supplied, is invoked for byte-level *upload*
    progress events (uncompressed member bytes written to the
    destination). See :class:`ExtractEvent`. The callback may run on
    boto3's transfer threadpool, so it must be thread-safe (tqdm.update
    is).

    *on_read*, if supplied, is called with the length of each archive
    chunk *read* from the source (compressed bytes). Sized against the
    archive object's ``ContentLength`` it drives a "how far through the
    archive" progress bar. Not reported for ``7z`` (random-access
    decode). Distinct from *on_progress*, which counts uncompressed
    upload bytes and has no clean total for streamed tar entries.

    *fix_unsafe_paths* is threaded into
    :func:`s3_archive.members.iter_archive_members`: by default a member
    with a ``..`` traversal segment raises
    :class:`s3_archive.exceptions.UnsafeArchiveMemberError` (after
    earlier members are already written — the walk is single-pass);
    ``True`` safely collapses it instead.

    *resume* (opt-in, default off) continues an interrupted extract
    instead of re-processing from byte 0. It requires a per-member-
    seekable source: ``zip``, uncompressed ``tar``, ``tar.gz`` (via an
    :mod:`indexed_gzip` seek index persisted alongside the marker) and
    multi-block ``tar.xz`` (whose block index lives in the file, so no
    companion index is needed) — both of which let a re-run seek past done
    members instead of re-downloading and re-decoding the whole source —
    and non-solid ``7z`` (a solid 7z refuses — its single compression block
    can't be seeked into per-member). A single-block ``tar.xz``,
    ``tar.bz2`` (see the "Resumable extract for .tar.bz2 (and other
    seek-hostile formats)" idea in docs/someday-maybe/), and
    every other format raise
    :class:`s3_archive.exceptions.ResumeUnsupportedError` up front, before
    any object is written. A member already present at the destination at
    its expected size is skipped without re-transfer (the destination is
    the progress ledger; see :mod:`s3_archive.resume`). With *resume*,
    *on_read* is not called — the seekable walk doesn't read the archive
    end-to-end — so a progress bar should be driven off *on_progress*
    instead.

    Every run that is not a dry-run ends by writing a ``<dir>.log``
    sidecar beside the destination folder — source URL and ETag, run
    times, member counts, one line per integrity warning — overwriting
    the previous run's (see :mod:`s3_archive.extract_log`). It is written
    only on completion: a run that raises leaves the previous one in
    place. A bucket-root destination has no folder name to derive it
    from, so none is written there and a WARNING says so.
    """
    log.info(
        "Extracting %s%s s3://%s/%s -> s3://%s/%s",
        "(resume) " if resume else "",
        fmt,
        archive_bucket,
        archive_key,
        dest_bucket,
        dest_prefix,
    )

    # Before any S3 call: an unsupported format is an invocation error.
    check_archive_format(fmt)
    started = datetime.now(UTC)
    # Read before any member: the ETag names the resume marker, and the
    # sidecar records the generation this run started from.
    source = _source_identity(src_client, archive_bucket, archive_key)
    report = ExtractReport()
    if resume:
        walk = _begin_resume(
            src_client,
            dst_client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            fmt,
            fix_unsafe_paths=fix_unsafe_paths,
            dry_run=dry_run,
            source=source,
        )
    else:
        walk = _Walk(
            members=iter_archive_members(
                src_client,
                archive_bucket,
                archive_key,
                fmt,
                fix_unsafe_paths=fix_unsafe_paths,
                tolerate_integrity_errors=True,
                on_bytes=on_read,
            ),
            ledger=_ResumeLedger(),
        )
    # Warnings a prior --resume run recorded belong to this extract too.
    report.integrity_warnings.update(walk.ledger.carried)

    def on_consumed(member: ArchiveMember) -> None:
        recorded = _record_integrity(member, report, dry_run=dry_run)
        if recorded and walk.ledger.marker is not None:
            # Before the member's object completes (see _member_chunks_then),
            # so an interruption can never leave a warned member on the
            # destination with its warning unrecorded.
            walk.ledger.marker.write(dst_client, dest_bucket, report.integrity_warnings)

    for idx, member in enumerate(walk.members):
        # Resume skip: a member already written at its expected size is
        # done (upload_fileobj is all-or-nothing). The ledger is empty on the
        # non-resume path, so this never fires there. Nothing is read from
        # the source for a skipped member — the lazy chunk generator is
        # simply never started. The append comes *after* the skip so the
        # returned list is "members written this run" for every format —
        # uniform, and it matches the docstring. (The 7z resume path yields
        # only undone members, so it can't report the skipped ones anyway.)
        if walk.ledger.is_done(member.name, member.size):
            report.already_present += 1
            if verbose:
                log.info("  skip (already present) %s", member.name)
            continue
        report.written.append(member.name)
        if on_progress is not None:
            # Boundary event so the UI can switch "current file" before
            # any bytes arrive.
            on_progress(
                ExtractEvent(
                    member=member.name,
                    bytes_transferred=0,
                    member_index=idx,
                    member_size=member.size if member.size else None,
                )
            )
        if dry_run:
            member.drain()
            on_consumed(member)
            if verbose:
                if member.size:
                    log.info("  would write %s (%d bytes)", member.name, member.size)
                else:
                    log.info("  would write %s", member.name)
            continue
        dest_key = _dest_key(dest_prefix, member.name)
        if verbose:
            log.info("  %s -> s3://%s/%s", member.name, dest_bucket, dest_key)

        _upload_member(
            dst_client,
            member,
            dest_bucket,
            dest_key,
            on_consumed=on_consumed,
            callback=(
                None
                if on_progress is None
                else _make_upload_callback(
                    on_progress, member.name, idx, member.size if member.size else None
                )
            ),
        )

    # The sidecar goes before the marker is dropped, so a failed PUT leaves
    # the run resumable rather than complete-but-unrecorded.
    if not dry_run:
        report.log_key = _write_extract_log(
            dst_client,
            dest_bucket,
            dest_prefix,
            source_url=f"s3://{archive_bucket}/{archive_key}",
            source=source,
            fmt=fmt,
            resumed=resume,
            started=started,
            report=report,
        )

    # Clean completion of a resume run: drop the control marker (and, for
    # gzip, its seek-index companion) so only an *interrupted* run leaves
    # artifacts behind. Both keys are None on the non-resume path and in
    # dry-run, so nothing is deleted there.
    if walk.ledger.marker is not None:
        resume_mod.delete_control_file(dst_client, dest_bucket, walk.ledger.marker.key)
    if walk.index_key is not None:
        resume_mod.delete_index_object(dst_client, dest_bucket, walk.index_key)

    log.info(
        "extract %s: %d files%s",
        "(dry-run)" if dry_run else "complete",
        len(report.written),
        (
            f", {len(report.integrity_warnings)} with integrity warnings"
            if report.integrity_warnings
            else ""
        ),
    )
    return report


def _write_extract_log(
    dst_client,
    dest_bucket: str,
    dest_prefix: str,
    *,
    source_url: str,
    source: "SourceIdentity",
    fmt: str,
    resumed: bool,
    started: datetime,
    report: ExtractReport,
) -> str | None:
    """PUT the ``<dir>.log`` sidecar for this run; return its key (``None`` at bucket root)."""
    key = extract_log.log_key(dest_prefix)
    if key is None:
        log.warning(
            "destination s3://%s/ is the bucket root: there is no folder name to name "
            "a <dir>.log after, so no extract log is written",
            dest_bucket,
        )
        return None
    # Lazy import: the package __init__ imports this module.
    from s3_archive import __version__  # noqa: PLC0415

    text = extract_log.render(
        extract_log.ExtractLogRecord(
            version=__version__,
            source_url=source_url,
            source_etag=source.etag,
            fmt=fmt,
            destination_url=f"s3://{dest_bucket}/{dest_prefix}",
            resumed=resumed,
            started=started,
            finished=datetime.now(UTC),
            written=len(report.written),
            already_present=report.already_present,
            integrity_warnings=report.integrity_warnings,
        )
    )
    dst_client.put_object(
        Bucket=dest_bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )
    log.info("extract log written to s3://%s/%s", dest_bucket, key)
    return key


@dataclass(frozen=True)
class SourceIdentity:
    """The source archive's resume identity, read once per resume run.

    The ETag names the control marker (and gzip's seek-index companion), so
    a source that changed under us can never resume against a stale ledger;
    the size is recorded in the marker for the operator's benefit. The two
    always travel together — every resumable format reads both up front and
    hands them straight to :func:`_resume_control_and_done`.
    """

    etag: str
    size: int


def _source_identity(src_client, archive_bucket: str, archive_key: str) -> SourceIdentity:
    """Read the resume identity of the archive at *archive_bucket*/*archive_key*."""
    head = src_client.head_object(Bucket=archive_bucket, Key=archive_key)
    return SourceIdentity(etag=head["ETag"], size=head["ContentLength"])


@dataclass(frozen=True)
class _ResumeMarker:
    """The control marker of a resume run that writes (i.e. not a dry-run).

    Rewritten whenever a member is written as decoded despite an integrity
    failure, so the accumulated warnings survive an interruption; deleted on
    clean completion. *created_at* stays the time the extract first began.
    """

    key: str
    source: SourceIdentity
    fmt: str
    created_at: str

    def write(
        self, dst_client, dest_bucket: str, integrity_warnings: dict[str, IntegrityMismatch]
    ) -> None:
        resume_mod.write_control_file(
            dst_client,
            dest_bucket,
            self.key,
            source_etag=self.source.etag,
            source_size=self.source.size,
            fmt=self.fmt,
            now_iso=self.created_at,
            integrity_warnings=integrity_warnings,
        )


@dataclass(frozen=True)
class _ResumeLedger:
    """What a run knows about prior ``--resume`` progress; empty on a fresh or non-resume run.

    *done* is the destination-as-ledger (``RelativePath → Size``); *carried*
    the integrity warnings earlier runs recorded in the marker; *marker*
    the marker this run rewrites and finally deletes (``None`` when it
    writes none: dry-run, or no ``--resume``).
    """

    done: dict[str, int] = field(default_factory=dict)
    carried: dict[str, IntegrityMismatch] = field(default_factory=dict)
    marker: _ResumeMarker | None = None

    def is_done(self, name: str, declared_size: int) -> bool:
        """True if *name* is already written at the size this extract writes it at.

        That is the archive's declared size, except for a member a prior
        run wrote as decoded despite an integrity failure: its object holds
        the decoded size, which may differ, and it counts as done at that
        size — otherwise every resume would re-extract it.
        """
        carried = self.carried.get(name)
        expected = declared_size if carried is None else carried.actual_size
        return self.done.get(name) == expected


@dataclass(frozen=True)
class _Walk:
    """The member iterator one extract run consumes, plus its resume state."""

    members: Iterator[ArchiveMember]
    ledger: _ResumeLedger
    # gzip's seek-index companion, deleted on clean completion; None otherwise.
    index_key: str | None = None


def _begin_resume(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    fmt: str,
    *,
    fix_unsafe_paths: bool,
    dry_run: bool,
    source: SourceIdentity,
) -> "_Walk":
    """Prepare a resumable extract: the member walk plus its resume ledger.

    Refuses (:class:`ResumeUnsupportedError`) before any write when *fmt*
    isn't resumable, or when the archive turns out to lack the per-member
    index resume needs (no zip central directory / unreadable tar /
    non-gzip tar.gz / solid 7z). Otherwise returns the safe-keyed member
    iterator, the ledger to skip against (done-set, carried warnings, the
    control marker to rewrite and delete), and the seek-index key to delete
    on clean completion, and the seek-index key to delete alongside it
    (the latter only for gzip; ``None`` for the other formats and whenever
    nothing should be deleted — i.e. in dry-run).
    """
    if fmt not in RESUMABLE_FORMATS:
        raise ResumeUnsupportedError(
            f"--resume is not supported for {fmt!r} archives "
            f"(only zip, uncompressed tar, .tar.gz, multi-block .tar.xz, and "
            f"non-solid 7z are per-member seekable); re-run without --resume."
        )

    if fmt == "7z":
        return _begin_resume_seven_z(
            src_client,
            dst_client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            fix_unsafe_paths=fix_unsafe_paths,
            dry_run=dry_run,
            source=source,
        )

    if fmt in _COMPRESSED_TAR_RESUME:
        return _COMPRESSED_TAR_RESUME[fmt](
            src_client,
            dst_client,
            archive_bucket,
            archive_key,
            dest_bucket,
            dest_prefix,
            fix_unsafe_paths=fix_unsafe_paths,
            dry_run=dry_run,
            source=source,
        )

    iterator_factory = _RESUME_ITERATORS[fmt]
    # Build the member iterator (which opens + validates the archive)
    # *before* writing any control file, so a genuinely unseekable archive
    # refuses without leaving an orphan marker at the destination.
    buffered = open_seekable(src_client, archive_bucket, archive_key)
    try:
        raw_members = iterator_factory(buffered)
    except (zipfile.BadZipFile, tarfile.TarError) as exc:
        raise ResumeUnsupportedError(
            f"{fmt} archive at s3://{archive_bucket}/{archive_key} is not "
            f"per-member seekable ({exc}); re-run without --resume."
        ) from exc
    members = _apply_safe_keys(raw_members, fix_unsafe_paths=fix_unsafe_paths)

    ledger = _resume_control_and_done(
        dst_client,
        dest_bucket,
        dest_prefix,
        source=source,
        fmt=fmt,
        dry_run=dry_run,
    )
    # zip / uncompressed tar carry no seek index (natively per-member
    # seekable) → no idx key to clean up.
    return _Walk(members=members, ledger=ledger)


def _resume_control_and_done(
    dst_client,
    dest_bucket: str,
    dest_prefix: str,
    *,
    source: SourceIdentity,
    fmt: str,
    dry_run: bool,
) -> _ResumeLedger:
    """Resolve the control marker, done-set and carried warnings for a resume run.

    Shared by every resumable format (the marker + destination-ledger
    machinery is format-agnostic — see :mod:`s3_archive.resume`). The
    returned ledger's *marker* is ``None`` in dry-run (no marker written,
    none rewritten, nothing to delete on completion).
    """
    ckey = resume_mod.control_key(dest_prefix, source.etag)
    existing = resume_mod.read_control_file(dst_client, dest_bucket, ckey)
    if existing is not None:
        # A prior --resume run for this exact source (same ETag) began
        # here: trust the destination objects as the progress ledger, and
        # the warnings it recorded as part of this extract.
        done = resume_mod.build_done_set(dst_client, dest_bucket, dest_prefix)
        carried = resume_mod.marker_integrity_warnings(existing)
        created_at = str(existing["created_at"])
    else:
        # Fresh run — don't trust un-vouched pre-existing objects.
        done, carried = {}, {}
        created_at = datetime.now(UTC).isoformat()
    marker = _ResumeMarker(key=ckey, source=source, fmt=fmt, created_at=created_at)
    if dry_run:
        # A preview must leave no S3 side effects: no marker written,
        # rewritten or deleted.
        return _ResumeLedger(done=done, carried=carried)
    if existing is None:
        # Written now so an interruption partway through is itself resumable.
        marker.write(dst_client, dest_bucket, {})
    return _ResumeLedger(done=done, carried=carried, marker=marker)


def _begin_resume_seven_z(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    fix_unsafe_paths: bool,
    dry_run: bool,
    source: SourceIdentity,
) -> "_Walk":
    """Prepare a resumable 7z extract (v2): the undone members plus the resume ledger.

    7z can't be walked through a fileobj like zip/tar: py7zr drives
    extraction push-style through a :class:`WriterFactory`, so instead of
    skipping done members inside the loop we compute the *undone* set up
    front and hand it to py7zr as ``targets``. The returned iterator
    therefore yields only members that still need writing.

    Refuses (:class:`ResumeUnsupportedError`) before any write when the
    archive is solid (single compression Folder) — extracting any member
    then decodes from the block start, so resume buys nothing. The probe
    precedes the control-marker write, so a refusal leaves no orphan marker
    (mirroring the zip BadZipFile ordering).
    """
    # Lazy import: seven_z pulls in py7zr (and its load-time monkeypatch);
    # keep that off the zip/tar resume path, matching members.py's dispatch.
    from s3_archive.paths import safe_member_key  # noqa: PLC0415
    from s3_archive.seven_z import (  # noqa: PLC0415
        iter_seven_z_members,
        probe_seven_z_resume,
    )

    # Probe the header (cheap, tail-prefetched) *before* writing any control
    # marker, so a solid archive refuses without leaving an orphan behind.
    info = probe_seven_z_resume(src_client, archive_bucket, archive_key)
    if not info.resumable:
        raise ResumeUnsupportedError(
            f"the .7z at s3://{archive_bucket}/{archive_key} is solid "
            f"(single compression block), so resume can't seek to an "
            f"individual member; re-create it non-solid (e.g. "
            f"`7z a -ms=off …`), or re-run without --resume."
        )

    ledger = _resume_control_and_done(
        dst_client,
        dest_bucket,
        dest_prefix,
        source=source,
        fmt="7z",
        dry_run=dry_run,
    )

    # Compute the undone target set on the *raw* member names py7zr matches:
    # a member is done iff a destination object at its safe key already has
    # the expected size (the uncompressed size, or for a member a prior run
    # wrote despite an integrity warning, the size it was written at — the
    # ledger decides both). safe_member_key mirrors the loop's
    # _apply_safe_keys, so the done-set lookup lines up with the ledger. A
    # ``..`` member raises UnsafeArchiveMemberError here (fail-fast) — it
    # could never have been written anyway.
    targets: list[str] = []
    for raw_name, size in info.members:
        safe = safe_member_key(raw_name, fix_unsafe=fix_unsafe_paths)
        if not ledger.is_done(safe, size):
            targets.append(raw_name)

    members = _apply_safe_keys(
        iter_seven_z_members(
            src_client,
            archive_bucket,
            archive_key,
            targets=targets,
            tolerate_integrity_errors=True,
        ),
        fix_unsafe_paths=fix_unsafe_paths,
    )
    # ``targets`` already excludes done members, so the loop never skips —
    # but the ledger goes through anyway for a defensive belt-and-suspenders
    # match (harmless: no member the iterator yields is done). 7z seeks
    # natively (no decompressor index), so there's no idx key to clean up.
    return _Walk(members=members, ledger=ledger)


def _close_decoder(decoder) -> None:
    """Close a seekable decompressor (gzip / xz), suppressing errors.

    Hygiene — releases the decoder's resources once the member walk ends
    rather than waiting on GC. Called from both the checkpointing and the
    plain closing member wrappers.
    """
    with contextlib.suppress(Exception):
        decoder.close()


def _closing_members(members: Iterator[ArchiveMember], decoder) -> Iterator[ArchiveMember]:
    """Yield *members*, then close *decoder* — no index work (dry-run / xz)."""
    try:
        yield from members
    finally:
        _close_decoder(decoder)


def _checkpointing_indexed_members(
    members: Iterator[ArchiveMember],
    *,
    decoder,
    export_index: Callable[[object], bytes],
    dst_client,
    dest_bucket: str,
    index_key: str,
    interval: int,
) -> Iterator[ArchiveMember]:
    """Yield *members*, persisting *decoder*'s seek index every ~*interval* bytes.

    Used by the gzip resume path (the codec supplies *export_index*; kept
    generic so a future index-persisting format could reuse it). The
    checkpoint snaps to a member boundary (atomicity — a member is uploaded
    by one all-or-nothing ``upload_fileobj``) and fires at most once per
    *interval* uncompressed bytes (so millions of tiny files don't each
    trigger a PUT).
    ``decoder.tell()`` is the uncompressed offset of the member about to be
    yielded — everything before it has been decoded (or seeked past on a
    resume), so the accrued index covers it.

    The index is a pure optimization: an export / PUT failure is logged and
    swallowed so it can never kill a multi-hour extract or corrupt a later
    resume. The decoder is always closed when the walk ends.
    """
    last_pos = 0
    try:
        for member in members:
            pos = decoder.tell()
            if pos - last_pos >= interval:
                _put_index(decoder, export_index, dst_client, dest_bucket, index_key)
                last_pos = pos
            yield member
    finally:
        _close_decoder(decoder)


def _put_index(
    decoder,
    export_index: Callable[[object], bytes],
    dst_client,
    dest_bucket: str,
    index_key: str,
) -> None:
    """Export *decoder*'s seek index and PUT it, swallowing any failure.

    Deliberately broad ``except``: the index is a pure optimization, so a
    transient PUT error (or an export hiccup) must degrade to "more
    re-decode on the next resume," never abort the run. ``Exception``
    (not ``BaseException``) still lets a ``KeyboardInterrupt`` through.
    """
    try:
        data = export_index(decoder)
        resume_mod.write_index_object(dst_client, dest_bucket, index_key, data)
    except Exception as exc:  # noqa: BLE001 - index is a pure optimization
        log.warning("resume: failed to persist seek index (%s); continuing", exc)


def _begin_resume_gzip(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    fix_unsafe_paths: bool,
    dry_run: bool,
    source: SourceIdentity,
) -> "_Walk":
    """Prepare a resumable ``.tar.gz`` extract (v3): members, resume ledger, seek-index key.

    gzip isn't natively per-member seekable, so this opens the archive through
    an :mod:`indexed_gzip` zran seek index (see :mod:`s3_archive.gzip_seek`)
    that presents a decoded tar to the existing
    :func:`iter_tar_members_seekable`; the loop's done-set skip applies
    unchanged. The companion ``.idx`` is persisted alongside the marker so a
    re-run seeks to a late member re-decoding ≤ ~1 GB rather than
    re-downloading the whole source. ``open_tar_gz_seekable`` opens + validates
    (refusing a non-gzip/non-tar body before any marker is written).

    The returned iterator self-checkpoints (except in dry-run, which persists
    nothing → ``ikey`` is ``None``) and always closes the decoder when the
    walk ends.

    (This is the only index-persisting resume format: bzip2 was evaluated for
    v4 and dropped — ``indexed_bzip2`` decodes *through* the tar walk's short
    forward seeks, so a block-offset index wouldn't avoid the re-download /
    re-decode that is the point; see the "Resumable extract for .tar.bz2
    (and other seek-hostile formats)" idea in docs/someday-maybe/.)
    """
    # Lazy import: keep the compiled indexed_gzip extension off every other
    # resume/extract path (matches members.py / seven_z.py dispatch). The
    # spacing is read at call time so a test's monkeypatch is honored.
    from s3_archive import gzip_seek  # noqa: PLC0415

    spacing = gzip_seek.DEFAULT_INDEX_SPACING

    buffered = open_seekable(src_client, archive_bucket, archive_key)

    # Load any previously-persisted seek index for this exact source. The
    # idx is ETag-named like the marker, so a changed source (different
    # ETag) or a fresh run (nothing written yet) both return None here — a
    # miss just means more forward re-decode, never wrong output.
    ikey = resume_mod.index_key(dest_prefix, source.etag)
    index_bytes = resume_mod.read_index_object(dst_client, dest_bucket, ikey)

    # Open + validate the archive *before* writing any control marker, so a
    # non-gzip / non-tar body refuses without leaving an orphan marker.
    igzf, raw_members = gzip_seek.open_tar_gz_seekable(
        buffered, index_bytes=index_bytes, spacing=spacing
    )
    members = _apply_safe_keys(raw_members, fix_unsafe_paths=fix_unsafe_paths)

    ledger = _resume_control_and_done(
        dst_client,
        dest_bucket,
        dest_prefix,
        source=source,
        fmt="tar.gz",
        dry_run=dry_run,
    )

    # A preview must leave no S3 side effects: skip index checkpointing (and
    # report no idx to delete) in dry-run — but still close the decoder.
    if dry_run:
        return _Walk(members=_closing_members(members, igzf), ledger=ledger)

    members = _checkpointing_indexed_members(
        members,
        decoder=igzf,
        export_index=gzip_seek.export_index_bytes,
        dst_client=dst_client,
        dest_bucket=dest_bucket,
        index_key=ikey,
        interval=spacing,
    )
    return _Walk(members=members, ledger=ledger, index_key=ikey)


def _begin_resume_xz(
    src_client,
    dst_client,
    archive_bucket: str,
    archive_key: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    fix_unsafe_paths: bool,
    dry_run: bool,
    source: SourceIdentity,
) -> "_Walk":
    """Prepare a resumable ``.tar.xz`` extract (v4): members plus the resume ledger.

    xz carries its block index *in the file* (the stream footer), so unlike
    gzip there is **no companion ``.idx``**: :mod:`s3_archive.xz_seek`
    reads the footer on open and seeks from there, and the destination ledger
    is the only resume state (hence ``ikey`` is always ``None``). A
    single-block ``.tar.xz`` (no interior seek point) or a non-xz/non-tar body
    refuses inside :func:`s3_archive.xz_seek.open_tar_xz_seekable`, before any
    marker is written. The decoder is closed when the walk ends.
    """
    from s3_archive import xz_seek  # noqa: PLC0415

    buffered = open_seekable(src_client, archive_bucket, archive_key)

    # Open + validate (and enforce multi-block) before writing any marker.
    xzf, raw_members = xz_seek.open_tar_xz_seekable(buffered)
    members = _apply_safe_keys(raw_members, fix_unsafe_paths=fix_unsafe_paths)

    ledger = _resume_control_and_done(
        dst_client,
        dest_bucket,
        dest_prefix,
        source=source,
        fmt="tar.xz",
        dry_run=dry_run,
    )
    return _Walk(members=_closing_members(members, xzf), ledger=ledger)


# fmt → resume-begin function for the compressed-tar variants that decode
# through a seekable decompressor (gzip v3, multi-block xz v4). 7z and
# zip/tar take their own branches in ``_begin_resume``; this table keeps the
# dispatch flat.
_COMPRESSED_TAR_RESUME: dict[str, Callable[..., _Walk]] = {
    "tar.gz": _begin_resume_gzip,
    "tar.xz": _begin_resume_xz,
}
