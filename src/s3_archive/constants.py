"""Tuning constants shared across the streaming modules.

This module imports nothing from :mod:`s3_archive`, so every module in
the package can import it without creating a cycle.
"""

# Canonical read size for every streaming byte pull in this library: S3
# body reads, tar/zip per-member reads, and the 7z pipe drain. One value
# so the chunking behaviour of a hash pass matches the extract path it
# shares a stream with; callers that need a different size pass it
# explicitly (see :func:`s3_archive.hashing.body_chunks`).
CHUNK_SIZE = 64 * 1024
