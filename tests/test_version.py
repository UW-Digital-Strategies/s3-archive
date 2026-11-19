"""Tests for the package version fallback in ``s3_archive/__init__.py``."""

import importlib
import sys

import s3_archive
from s3_archive import _version


def test_version_comes_from_build_hook():
    assert s3_archive.__version__ == _version.__version__


def test_source_tree_without_build_hook_still_imports(monkeypatch):
    """With no hatch-vcs `_version.py`, the package imports with a sentinel."""
    monkeypatch.setitem(sys.modules, "s3_archive._version", None)
    try:
        importlib.reload(s3_archive)
        assert s3_archive.__version__ == "0.0.0+unknown"
    finally:
        monkeypatch.undo()
        importlib.reload(s3_archive)
    assert s3_archive.__version__ != "0.0.0+unknown"
