"""Logging setup for s3-archive.

One module-level logger per file. Console handler is attached lazily by
the CLI entry point so library callers can configure logging themselves.
"""

import logging
import os
import sys

from tqdm import tqdm

from s3_archive.exceptions import ConfigError

_console_initialized = False

#: Level used when neither ``-v`` nor ``$LOG_LEVEL`` says otherwise.
_DEFAULT_LEVEL = logging.INFO

#: Root-logger level (botocore, urllib3, s3transfer) when ``$LOG_LEVEL_ROOT``
#: is unset: the tool's own progress without third-party chatter.
_DEFAULT_ROOT_LEVEL = logging.WARNING

#: Named in the error message only; `logging.getLevelNamesMapping()` is what
#: actually resolves a name, so aliases like WARN keep working.
_LEVEL_NAMES = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


class _TqdmLoggingHandler(logging.Handler):
    """Route log records through tqdm.write so they don't clobber an active progress bar."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tqdm.write(msg, file=sys.stderr)
        except Exception:  # noqa: BLE001 - defensive, matches stdlib StreamHandler
            self.handleError(record)


def setup_console(level: int, root_level: int) -> None:
    """Attach a console handler and set both log levels. Idempotent.

    The ``s3_archive`` package logger follows *level*; the root logger —
    and so every third-party library — follows *root_level*. Keeping them
    apart is what lets ``-v`` (DEBUG) surface our per-file progress without
    unleashing botocore / urllib3 / s3transfer debug chatter, while
    ``LOG_LEVEL_ROOT=DEBUG`` still reaches that chatter when it is wanted.
    """
    global _console_initialized  # noqa: PLW0603 - module-level once-flag
    if _console_initialized:
        return
    _console_initialized = True

    # The handler passes everything through; per-logger levels do the gating.
    handler = _TqdmLoggingHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))

    # Root (and thus third-party libraries) follows $LOG_LEVEL_ROOT, never -v.
    root = logging.getLogger()
    root.setLevel(root_level)
    root.addHandler(handler)

    # Our package alone honors the requested level (INFO, or DEBUG under -v).
    logging.getLogger("s3_archive").setLevel(level)


def resolve_level(*, verbose: bool) -> int:
    """Return the level for the ``s3_archive`` logger, fail-fast on a bad name.

    ``-v`` is shorthand for the DEBUG level and wins outright; otherwise
    ``$LOG_LEVEL`` names the level, and INFO applies when it is unset or
    empty.
    An unrecognized name raises :class:`~s3_archive.exceptions.ConfigError`
    rather than silently falling back, so a typo in a cron environment is
    visible on the first run instead of quietly downgrading a long extract's
    logs.
    """
    if verbose:
        return logging.DEBUG
    return _level_from_env("LOG_LEVEL", _DEFAULT_LEVEL)


def resolve_root_level() -> int:
    """Return the root-logger level, fail-fast on a bad name.

    ``$LOG_LEVEL_ROOT`` names the level for everything that is not
    ``s3_archive`` — botocore, urllib3, s3transfer — and WARNING applies
    when it is unset or empty. ``-v`` does not touch it: per-file progress
    and boto's wire-level DEBUG are separate asks. An unrecognized name raises
    :class:`~s3_archive.exceptions.ConfigError`, as for ``$LOG_LEVEL``.
    """
    return _level_from_env("LOG_LEVEL_ROOT", _DEFAULT_ROOT_LEVEL)


def _level_from_env(var: str, default: int) -> int:
    """The level named by environment variable *var*, or *default* if unset/blank."""
    requested = os.environ.get(var, "").strip()
    if not requested:
        return default

    level = logging.getLevelNamesMapping().get(requested.upper())
    if level is None:
        raise ConfigError(
            f"{var}={requested!r} is not a log level name "
            f"(expected one of: {', '.join(_LEVEL_NAMES)})"
        )
    return level


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
