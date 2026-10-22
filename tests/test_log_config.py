"""Tests for console logging setup.

The key behavioral contract: ``-v`` (DEBUG) must surface s3_archive's own
per-file progress without turning on botocore / urllib3 debug chatter.
"""

import logging

import pytest

from s3_archive import log_config
from s3_archive.exceptions import ConfigError


@pytest.fixture
def reset_logging():
    """Reset the once-flag and restore logger/handler state around each test.

    ``setup_console`` is deliberately idempotent via a module-level flag and
    mutates the process-global root logger, so tests must isolate themselves.
    """
    root = logging.getLogger()
    pkg = logging.getLogger("s3_archive")

    saved_flag = log_config._console_initialized
    saved_root_level = root.level
    saved_root_handlers = root.handlers[:]
    saved_pkg_level = pkg.level

    log_config._console_initialized = False

    try:
        yield
    finally:
        log_config._console_initialized = saved_flag
        root.setLevel(saved_root_level)
        root.handlers[:] = saved_root_handlers
        pkg.setLevel(saved_pkg_level)


def test_verbose_enables_package_debug_but_not_third_party(reset_logging):
    """-v (DEBUG) lowers the s3_archive logger to DEBUG while root/botocore stay quiet."""
    log_config.setup_console(logging.DEBUG, logging.WARNING)

    pkg = logging.getLogger("s3_archive")
    assert pkg.isEnabledFor(logging.DEBUG)

    # Root — and therefore third-party loggers — must not drop to DEBUG.
    assert logging.getLogger().level == logging.WARNING
    assert not logging.getLogger("botocore").isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("botocore").isEnabledFor(logging.INFO)


def test_non_verbose_leaves_package_at_info(reset_logging):
    """Without -v the package logs at INFO, and third-party stays at WARNING."""
    log_config.setup_console(logging.INFO, logging.WARNING)

    pkg = logging.getLogger("s3_archive")
    assert pkg.isEnabledFor(logging.INFO)
    assert not pkg.isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("botocore").isEnabledFor(logging.INFO)


def test_setup_console_is_idempotent(reset_logging):
    """A second call must not stack a second handler on root."""
    log_config.setup_console(logging.INFO, logging.WARNING)
    handler_count = len(logging.getLogger().handlers)

    log_config.setup_console(logging.DEBUG, logging.WARNING)
    assert len(logging.getLogger().handlers) == handler_count


def test_root_level_reaches_third_party_but_not_package(reset_logging):
    """root_level (LOG_LEVEL_ROOT) opens botocore DEBUG; the package keeps its own level."""
    log_config.setup_console(logging.INFO, logging.DEBUG)

    assert logging.getLogger().level == logging.DEBUG
    assert logging.getLogger("botocore").isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("s3_archive").isEnabledFor(logging.DEBUG)


def test_resolve_level_defaults_to_info(monkeypatch):
    """Unset LOG_LEVEL, no -v: INFO."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    assert log_config.resolve_level(verbose=False) == logging.INFO


def test_resolve_level_reads_log_level_env(monkeypatch):
    """LOG_LEVEL names the level, case-insensitively."""
    monkeypatch.setenv("LOG_LEVEL", "warning")
    assert log_config.resolve_level(verbose=False) == logging.WARNING


def test_verbose_beats_log_level(monkeypatch):
    """-v is shorthand for DEBUG and outranks a quieter LOG_LEVEL."""
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    assert log_config.resolve_level(verbose=True) == logging.DEBUG


def test_blank_log_level_is_treated_as_unset(monkeypatch):
    """An exported-but-empty LOG_LEVEL is not a typo; it means 'default'."""
    monkeypatch.setenv("LOG_LEVEL", "   ")
    assert log_config.resolve_level(verbose=False) == logging.INFO


def test_unknown_log_level_fails_fast(monkeypatch):
    """A typo raises ConfigError instead of silently logging at INFO."""
    monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        log_config.resolve_level(verbose=False)


def test_resolve_root_level_defaults_to_warning(monkeypatch):
    """Unset LOG_LEVEL_ROOT: third-party loggers at WARNING."""
    monkeypatch.delenv("LOG_LEVEL_ROOT", raising=False)
    assert log_config.resolve_root_level() == logging.WARNING


def test_blank_log_level_root_is_treated_as_unset(monkeypatch):
    """An exported-but-empty LOG_LEVEL_ROOT means 'default', not a typo."""
    monkeypatch.setenv("LOG_LEVEL_ROOT", "")
    assert log_config.resolve_root_level() == logging.WARNING


def test_resolve_root_level_reads_env(monkeypatch):
    """LOG_LEVEL_ROOT names the root level, case-insensitively."""
    monkeypatch.setenv("LOG_LEVEL_ROOT", "debug")
    assert log_config.resolve_root_level() == logging.DEBUG


def test_unknown_log_level_root_fails_fast(monkeypatch):
    """A typo raises ConfigError naming LOG_LEVEL_ROOT, not a silent WARNING."""
    monkeypatch.setenv("LOG_LEVEL_ROOT", "CHATTY")
    with pytest.raises(ConfigError, match=r"LOG_LEVEL_ROOT='CHATTY'"):
        log_config.resolve_root_level()
