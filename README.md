# s3-archive

Streaming S3 archive operations against any S3-compatible object
storage. Nothing ever touches local disk — a 500 GB archive does not
need 500 GB of free space anywhere. Works against AWS S3, Ceph RadosGW,
MinIO, DigitalOcean Spaces, Backblaze B2, Wasabi — anything that
speaks S3.

```
s3-archive extract <archive_url> <dest_prefix_url>   # archive in S3 → members at an S3 prefix
s3-archive create  <src_prefix_url> <archive_url>    # S3 prefix → archive in S3
s3-archive ls      <archive_url>                     # peek inside an archive without extracting
s3-archive config  [--profile NAME]                  # interactive S3 credentials setup
```

URLs accept an optional `profile:` prefix
(`profile_name:s3://bucket/key`) for multi-provider workflows — see
[Multiple S3 providers](#multiple-s3-providers) below.

Supported formats: `tar`, `tar.gz` / `tgz`, `tar.bz2` / `tbz2`,
`tar.xz` / `txz`, `tar.zst`, `zip`, `7z` (extract / ls only — `.7z`
create is not supported; use `.tar.gz` or `.zip`).

## Quick start

### Install [uv](https://docs.astral.sh/uv/)

```
curl -LsSf https://astral.sh/uv/install.sh | sh        # macOS / Linux
irm https://astral.sh/uv/install.ps1 | iex             # Windows PowerShell
```

### Run it from the published git URL

```
uvx --from git+https://github.com/borwickatuw/s3-archive s3-archive --help
```

Or install as a `uv` tool:

```
uv tool install git+https://github.com/borwickatuw/s3-archive
s3-archive --help
```

### Credentials

The easy path is `s3-archive config` — interactive prompts for
endpoint URL, access key, and secret key, then writes them to a
standard `~/.s3cfg` file (s3cmd-compatible, `0600` perms). Press
Enter on the endpoint prompt for AWS S3 defaults.

If you'd rather wire it up manually, pick one:

- **s3cmd config** — if `~/.s3cfg` already targets your endpoint,
  nothing else to do. Set `$S3CMD_CONFIG` to point at a non-default
  path.
- **AWS-style env vars** — `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`.
  For non-AWS endpoints, also set `S3_ENDPOINT_URL=https://...`.
- **boto3 default chain** — `~/.aws/credentials`, IAM role, AWS SSO,
  etc.

See [`.env.example`](.env.example) for the full resolution order.

### Logging

`-v` turns on per-file DEBUG output; without it the tool logs at INFO.
`LOG_LEVEL` sets the same thing from the environment (`-v` wins if both
are given). Third-party chatter (botocore, urllib3, s3transfer) follows
a second variable, `LOG_LEVEL_ROOT` (default WARNING, untouched by
`-v`); `LOG_LEVEL_ROOT=DEBUG` is the wire-level view. An unrecognized
name in either exits 2 rather than quietly logging at the default.

### Multiple S3 providers

If you work across more than one S3-compatible provider — say, AWS for
archival and Ceph/RGW for working copies — give each provider
a **profile** and prefix URLs with the profile name. One interactive
`config` run per profile:

```bash
s3-archive config                          # default profile → ~/.s3cfg
s3-archive config --profile ceph-server    # → ~/.s3cfg-ceph-server
s3-archive config --profile aws-prsv       # → ~/.s3cfg-aws-prsv
```

Then use the profile in URLs. The two URLs in a command don't have to
share a profile — that's the whole point of cross-endpoint workflows:

```bash
# Extract a bag from AWS-Preservation into the Ceph cluster:
s3-archive extract aws-prsv:s3://bags/x.tar.gz ceph-server:s3://extracted/

# `ls` and `create` accept the same syntax:
s3-archive ls aws-prsv:s3://bags/x.tar.gz
s3-archive create ceph-server:s3://src/ aws-prsv:s3://archives/snapshot.tar.gz
```

A bare `s3://...` URL uses the `default` profile. Profile names must
match `[A-Za-z0-9_-]+` (letters, digits, underscore, hyphen). Each
`~/.s3cfg-<name>` file is a complete s3cmd INI, so `s3cmd` itself can
read one with `s3cmd --config=~/.s3cfg-ceph-server`.

Named profiles read **only** `~/.s3cfg-<name>` — `$S3CMD_CONFIG` and
the boto3 default chain are part of the *default profile's*
resolution only and are intentionally ignored for named profiles, so
an unrelated env var can't accidentally hijack one. If `~/.s3cfg-<name>`
is missing, the error message includes the exact command to fix it
(`s3-archive config --profile <name>`).

## Common tasks

### Extract an archive

```
s3-archive extract s3://my-bucket/incoming/snapshot.tar.gz s3://my-bucket/extracted/
```

Streams the archive out of S3, decodes on the fly, and writes each
member back to the destination prefix.

### Resume an interrupted extract

A large restore that dies partway through (crash, reboot, Ctrl-C,
network out) can be continued instead of restarted:

```
s3-archive extract --resume s3://my-bucket/incoming/big.zip s3://my-bucket/extracted/
```

`--resume` is **opt-in** (default off, so existing callers are
unaffected). On a re-run it lists the destination prefix, skips every
member already written at its expected size, and transfers only the
rest — re-processing at most one member. It's distinct from the built-in
mid-stream reconnect (which already survives transient *connection* drops
within a single run); `--resume` survives the whole *process* dying.

How it works: a tiny control marker
(`.s3-archive-resume.<source-etag>.json`) is written at the destination
prefix when a resumable run starts and deleted on clean completion, so
only an *interrupted* run leaves one behind. Naming it by the source
object's ETag is the identity guard — the same source resumes, a changed
source starts fresh automatically. The destination objects themselves are
the authoritative progress ledger (each finished member is one
all-or-nothing upload), so nothing can drift.

**Per-format support:**

| Format                | `--resume`                                                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `zip`                 | ✅ supported                                                                                                                                  |
| `.tar` (uncompressed) | ✅ supported                                                                                                                                  |
| `.tar.gz`             | ✅ supported (persisted seek index)                                                                                                           |
| `.tar.xz`             | ✅ multi-block only (single-block refused)                                                                                                    |
| `7z`                  | ✅ non-solid only (solid refused)                                                                                                             |
| `.tar.bz2`            | ❌ refused (see the "Resumable extract for `.tar.bz2` (and other seek-hostile formats)" idea in [`docs/someday-maybe/`](docs/someday-maybe/)) |
| `.tar.zst`            | ❌ refused (not resumable in the streaming model)                                                                                             |

Resume needs per-member random access into the source. The natively
seekable formats offer it directly — zip's central directory, uncompressed
tar's aligned headers, 7z's per-Folder pack offsets — and `.tar.gz` /
multi-block `.tar.xz` get it from a decompressor seek index: `.tar.gz` via a
small companion index object persisted alongside the marker (so a re-run
seeks past done members without re-downloading or re-decoding the whole
source), and `.tar.xz` via the block index xz already stores in the file
(no companion object needed).

Some encodings can't be seeked and **fail fast up front** — before writing
anything and without creating a control marker — with a clear message
telling you to re-run without `--resume`: a **solid** `.7z` (create it
non-solid with `7z a -ms=off …`), a **single-block** `.tar.xz` (the
`xz` / `tar -J` default — re-compress with `xz --block-size=…` or `xz -T0`
for a seekable multi-block file), `.tar.bz2` (the `indexed_bzip2` decoder
can't seek the member-walk cheaply, so resume would re-download and
re-decode everything anyway — the whole point is to avoid that), and
`.tar.zst` (no seek library accepts our streaming file object). The design
and the compressed-tar rationale live in
[`docs/RESUMABLE-EXTRACT.md`](docs/RESUMABLE-EXTRACT.md); deferred formats
are the "Resumable extract for `.tar.bz2` (and other seek-hostile formats)"
idea in [`docs/someday-maybe/`](docs/someday-maybe/).

### List an archive's contents

```
s3-archive ls s3://my-bucket/incoming/snapshot.tar.gz
```

Useful as a sanity check before a multi-GB extract. For `zip` and
`7z`, `ls` reads only the archive's index (the central directory /
trailing header) via ranged GETs — a 1 TB zip lists in seconds without
fetching any member bytes. The tar family has no index, so tar
listings stream the archive front-to-back (still disk-free; inherent
to the format).

### Create an archive

```
s3-archive create s3://my-bucket/source-dir/ s3://my-bucket/archives/snapshot.tar.gz
```

The destination URL's extension determines the format (`.tar.gz`,
`.tgz`, or `.zip`).

### Progress display

Interactive (TTY) `extract` and `create` runs show a byte progress bar
with percent complete, ETA, and transfer rate. It's automatically
suppressed when output isn't a terminal (piped / redirected), so logs
stay clean. Add `-v` for per-file lines alongside the bar.

> **Known issue (cosmetic, Windows):** the bar is drawn with Unicode
> block-drawing characters. Some Windows consoles/fonts lack the
> *partial*-block glyphs (`▏▎▍▌▋▊▉`) and render the bar's leading edge as
> a missing-glyph box (`□`); the solid `█` cells and all the numbers are
> unaffected and correct. This is a terminal/font limitation, not a
> transfer problem — use a terminal + font with full Unicode block
> support (e.g. Windows Terminal with Cascadia Mono/Code) if it bothers
> you.

## Library use

`s3-archive` is also a Python library:

```python
import boto3
from s3_archive import extract, list_archive, detect_format, parse_s3_url

client = boto3.client("s3")
extract(client, "src-bucket", "incoming/snapshot.tar.gz", "dest-bucket", "extracted/", "tar.gz")
list_archive(client, "src-bucket", "incoming/snapshot.tar.gz", "tar.gz")
```

`extract` returns the member names it wrote; `extract_with_report` returns
an `ExtractReport` that also carries the integrity warnings (members written
as decoded despite a failed CRC-32 / size check) — what the CLI turns into
exit 3. `iter_archive_members` raises on such a member by default; pass
`tolerate_integrity_errors=True` to get every decoded byte and the mismatch
on `ArchiveMember.integrity` instead.

The streaming-extract / streaming-list primitives, the
`IterableFileobj` / `NonSeekableReader` adapters, and the
`s3_archive.manifest` per-entry hashing primitives (phase 3) are
designed to be reusable from downstream code — `s3-bagit` is one
known consumer, and UW Libraries' internal storage tooling is another.

`ZlibVerifyTap` checks whether an object really is a bare zlib stream in
the same pass as whatever else reads it:

```python
from s3_archive import ZlibOutcome, ZlibVerifyTap, body_chunks, triple_hash

tap = ZlibVerifyTap()
hashes = triple_hash(tap.wrap(body_chunks(client.get_object(Bucket=b, Key=k)["Body"])))
if tap.verdict.outcome is ZlibOutcome.INVALID:
    print(tap.verdict.describe())  # e.g. "invalid: inflate error (... incorrect data check)"
```

## Data safety

s3-archive never prompts before writing, and the destination prefix is
treated as a plain namespace:

- **`extract` overwrites any destination object whose key it is about to
  write.** There is no skip-if-exists and no confirmation, so extracting a
  second archive into a prefix that already holds objects silently replaces
  the colliding keys. `--resume` is the one exception, and only for members
  already present at their expected size.
- **`create` overwrites the destination archive key** the same way.
- **Every extract also writes one object outside the destination prefix:**
  a plain-text `<dir>.log` named for the prefix without its trailing slash
  (extracting to `Iraq/foo/` writes `Iraq/foo.log`), overwriting whatever is
  at that key. It records the source URL and ETag, run times, member counts,
  and one line per member written despite failing its integrity check (see
  below). Dry-runs write none, and neither does a bucket-root destination
  (no folder name to derive it from; a WARNING says so).
- **A zip or 7z member that fails its CRC-32 / size check is still
  written**, with every byte that decoded — what Info-ZIP `unzip` and 7-Zip
  do. A WARNING names it, the `.log` lists it, the rest of the archive
  extracts, and the run exits 3 instead of 0. Tar-family archives carry no
  per-member checksum, so a damaged tar / gzip / xz stream is still a fatal
  error (exit 1). See [DECISIONS 006](docs/DECISIONS.md#006-a-member-that-fails-its-integrity-check-is-written-as-decoded-warned-and-the-extract-continues).
- **The only objects it deletes are its own resume artifacts** — the
  `.s3-archive-resume.<source-etag>.json` marker and its `.idx` companion,
  and only on clean completion. Nothing else at the destination is ever
  deleted.

There is no undo. Recovery from an unwanted overwrite depends entirely on
the destination bucket's versioning and lifecycle configuration, which
s3-archive neither sets, inspects, nor reports — configure it on the bucket
before pointing a restore at a prefix holding anything you care about.

An interrupted run leaves the members written so far in place (plus the
marker and `.idx`, if `--resume` was used). Re-running with `--resume`
continues from there; re-running without it rewrites from the beginning.

## Gotchas

Traps we've hit in production — worth knowing before you do.

### Some valid zips can't be streamed forward-only

On-the-fly zip generators (SwissTransfer, Google Drive downloads)
write *stored* (uncompressed) members with a data descriptor and zero
sizes in the local file headers. A forward-only reader can't tell
where such a member ends, **even though the zip is not corrupt** —
the central directory at the end has the true sizes, and any seekable
reader handles the file fine.

- **CLI: handled transparently.** `extract`, `ls`, and
  `extract --resume` all work on these zips. `ls` reads the central
  directory anyway; `extract` starts streaming (optimal for every
  normal zip) and, at the first unstreamable member, continues from
  that exact entry via a ranged-GET central-directory walk — no
  member is re-processed and total transfer stays ~one archive read.
- **Library:** the same transparency applies to
  `iter_archive_members`. Only the pure-transducer manifest builders
  can't fall back themselves (they have no S3 client):
  `build_manifest_zip_chunks` / `build_manifest_from_tap` raise
  `ZipNotStreamableError`; catch it and retry with
  `build_manifest_zip_seekable(open_seekable(...))`. Two things to
  reset on that retry: a `HashingTap` feeding the failed streaming
  pass holds an *incomplete* parent-archive hash (the seekable
  retry's ranged GETs bypass the tap — compute the parent hash
  separately), and an `entry_observer` may already have seen members
  from the failed walk (discard its state; the retry re-observes
  everything from scratch).

### Wrap `SeekableS3Object` via `open_seekable()`, not by hand

A bare `io.BufferedReader(SeekableS3Object(...))` gets BufferedReader's
default 8 KiB buffer — one ranged GET per 8 KiB of body. On a 105 MB
zip that was ~13,000 requests and 270 s, 40-70× slower than a plain
download of the same bytes. `s3_archive.seekable.open_seekable(client, bucket, key, if_match=etag)` is the canonical constructor: same
object, tuned 1 MiB buffer, ~100 requests, within ~2× of a plain
download.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the streaming
model and the rationale behind the `os.pipe()` writer-thread shape, and
[`docs/DECISIONS.md`](docs/DECISIONS.md) for the numbered, dated design
decisions (credential chain, Ceph workaround, no `.7z` create, named
profiles, overwrite with no undo).

## Operations

[`docs/OPERATIONS.md`](docs/OPERATIONS.md) is the entry point when a run
goes wrong: failure modes, the exit-code contract a wrapper script should
branch on, and the index of every runbook here.

## License

BSD 3-Clause — see [`LICENSE`](LICENSE).

### Copyleft in the runtime dependency tree

s3-archive stays BSD-3-Clause. Its runtime dependency tree includes six
weak-copyleft packages. All six are accepted as unmodified runtime
dependencies:

| Package           | License           | Pulled in by                     |
| ----------------- | ----------------- | -------------------------------- |
| `py7zr`           | LGPL-2.1-or-later | direct dependency (`.7z` reader) |
| `pybcj`           | LGPL-2.1-or-later | `py7zr`                          |
| `pyppmd`          | LGPL-2.1-or-later | `py7zr`                          |
| `inflate64`       | LGPLv2+           | `py7zr`                          |
| `multivolumefile` | LGPLv2+           | `py7zr`                          |
| `tqdm`            | MPL-2.0 AND MIT   | direct dependency (progress bar) |

The five LGPL packages are accepted under the maintainer's licensing
rule for separately installed dependencies; `tqdm`'s MPL-2.0 arm was
ratified by the operator on 2026-09-22 on the same reasoning. They are
acceptable because each one is a separately installed PyPI
wheel. None is vendored into this repo, statically linked, or modified.
Using an LGPL or MPL library that way puts no obligation on a
BSD-3-Clause distribution that it cannot meet. The py7zr chain is the
worked example of that posture. Two
things would reverse it: modifying one of these libraries, or bundling
one into a shipped artifact (vendored source, a static link, a frozen
binary). Either calls for a new review, not an edit to the exemption
list.

`make licenses` enforces this on the runtime closure only (the dev and
test groups never ship). It exempts the six packages above by name, not
by license string, so the next copyleft dependency fails the gate until
someone makes a deliberate decision. To re-derive the tree, run
`uv tree --no-default-groups`. `--no-dev` is not enough here, because
`test` is also a default group.
