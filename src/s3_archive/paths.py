"""Pure member-name → S3-key normalization helpers.

Two concerns, deliberately kept separate:

- **Zip quirks** (:func:`normalize_zip_separators`) — the zip format
  mandates forward slashes (PKWARE APPNOTE §4.4.17.1), but Windows
  archivers routinely store ``\\`` separators and even a ``C:`` drive
  prefix. Info-ZIP ``unzip`` and every mainstream extractor rewrite
  those on the way out; we do the same. This layer is **zip-only** —
  in tar / 7z a backslash is a legal filename byte (a Unix file
  literally named ``a\\b``) and ``C:`` is a legal directory name, so we
  must not touch them.

- **S3-key safety** (:func:`safe_member_key`) — format-agnostic. A
  stored member name that begins with ``/`` or contains a ``..``
  traversal segment is never a valid relative destination key; GNU tar
  and Info-ZIP both strip the leading slash, and a ``..`` that escapes
  the extraction root is a classic archive-traversal attack. This layer
  applies to **every** format.

Both are pure functions over ``str`` — no I/O, no S3. Dependency-wise
this module imports only :mod:`s3_archive.exceptions`, so it sits at the
bottom of the import graph (``manifest`` and ``members`` both import it
without a cycle).
"""

import re

from s3_archive.exceptions import UnsafeArchiveMemberError

# A leading Windows drive specifier: a single ASCII letter, a colon, and
# any run of (already-normalized) separators — e.g. ``C:/`` in ``C:/x/y``.
# Applied only after "\\" -> "/", so we match "/" here, not "\\".
_DRIVE_PREFIX_RE = re.compile(r"^[A-Za-z]:/*")


def normalize_zip_separators(name: str) -> str:
    """Rewrite Windows-isms in a zip member name. ZIP-ONLY; pure; never raises.

    Converts every ``\\`` to ``/`` and strips a leading drive specifier
    (``C:\\`` → ``""``). Matches Info-ZIP ``unzip`` behavior.

    A Unix file genuinely named ``a\\b`` zipped conformantly is
    indistinguishable from a Windows-separator ``a\\b`` here — both
    become ``a/b``. Converting (as Info-ZIP does) is the correct
    pragmatic choice; the conformant-``\\``-in-a-filename case is
    vanishingly rare and non-portable anyway.
    """
    name = name.replace("\\", "/")
    return _DRIVE_PREFIX_RE.sub("", name, count=1)


def decode_zip_filename(name: bytes | str) -> str:
    """Decode a zip member name to ``str`` and normalize its separators.

    Zips written before general-purpose bit 11 (the "language encoding
    flag") became conventional carry CP437-encoded names; only bit-11
    entries are UTF-8. Neither ``stream_unzip`` nor our central-directory
    parser surfaces that flag, so we try UTF-8 first and fall back to
    CP437 — a no-fail decoder, so this never raises on the decode.

    Both the local-file-header path (the forward-only walk,
    :func:`s3_archive.stream_decode.iter_zip_file_entries`) and the
    central-directory path (the readers in :mod:`s3_archive.manifest`)
    route through here, so their keys move together — the LFH→CD mtime
    lookup keys stay byte-identical after normalization.

    Returns:
        The decoded, separator-normalized member name.

    Raises:
        Nothing. CP437 maps all 256 byte values, so the fallback decode
        cannot fail, and separator normalization is a pure string
        rewrite. Callers need no handler for this call. Where a call
        site sits inside a ``try``, that handler is catching
        ``stream_unzip``'s ``NotStreamUnzippable`` from the surrounding
        zip walk, not anything this function raises. (The pysmelly
        ``inconsistent-error-handling`` finding this caller split used
        to mint stopped printing on 2026-09-22, when plan item 8-3 moved
        the two in-walk calls into the one shared zip walk; see
        ``docs/PYSMELLY.md``.)
    """
    if isinstance(name, bytes):
        try:
            decoded = name.decode("utf-8")
        except UnicodeDecodeError:
            decoded = name.decode("cp437")
    else:
        decoded = name
    return normalize_zip_separators(decoded)


def safe_member_key(name: str, *, fix_unsafe: bool = False) -> str:
    """Turn a stored member name into a safe relative S3 key. Format-agnostic.

    Applies three rules that hold for every archive format when the
    output is an S3 key:

    - a leading ``/`` (or run of them) is stripped — never a valid
      relative member; matches GNU tar / Info-ZIP;
    - a ``.`` segment is dropped (current-directory no-op);
    - a ``..`` traversal segment raises :class:`UnsafeArchiveMemberError`
      by default. When *fix_unsafe* is true it instead pops the parent
      segment, collapsing the path without ever escaping the root (a
      ``..`` already at the root is simply dropped).

    Interior empty segments (from ``//``) collapse away too.

    Returns:
        The safe relative key (``""`` when every segment collapsed away).

    Raises:
        UnsafeArchiveMemberError: if *name* has a ``..`` segment and
            *fix_unsafe* is false. This is the only exception. No caller
            in ``src/`` handles it, on purpose: an unsafe member aborts
            the operation, and the CLI boundary (``cli.main()``) maps it
            to "Unsafe archive member" and exit 1. Library callers
            (such as s3-bagit) get the exception. The pysmelly
            ``inconsistent-error-handling`` finding on this function
            (six callers, three "catch") is a caller-count artifact: the
            three guarded call sites sit inside, or are reached from
            inside, ``try`` blocks that catch ``stream_unzip``'s
            ``NotStreamUnzippable``, not this exception. The split is
            deliberate and the finding stands (ratified in
            ``docs/PYSMELLY.md``).
    """
    result: list[str] = []
    for segment in name.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if not fix_unsafe:
                raise UnsafeArchiveMemberError(name)
            if result:
                result.pop()
            continue
        result.append(segment)
    return "/".join(result)
