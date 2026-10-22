"""Source-tree hygiene guards that no runtime test would catch."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SKIP_DIR_NAMES = frozenset({".venv", ".git", "__pycache__", "build", "dist"})

# Assembled at runtime so this file is not itself an apparent offender.
DIRECTIVE = "# pysmelly" + ": ignore"


def _tracked_python_files():
    for path in sorted(REPO_ROOT.rglob("*.py")):
        if SKIP_DIR_NAMES.isdisjoint(path.parts):
            yield path


def test_pysmelly_directives_own_their_line():
    """Suppression directives must be full-line comments, never trailing.

    A directive trailing a code line rides on a line black may re-wrap.
    The re-wrap moves the comment off the line pysmelly inspects, so the
    suppression dies silently while still looking present in the source.
    The maintainer's pysmelly review guide calls this
    "Suppression-comment placement".
    """
    offenders = []
    for path in _tracked_python_files():
        lines = path.read_text(encoding="utf-8").splitlines()
        for lineno, line in enumerate(lines, start=1):
            if DIRECTIVE in line and not line.lstrip().startswith("#"):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not offenders, f"trailing suppression directives (black-unstable): {offenders}"


def test_hygiene_guard_sees_the_source_tree():
    """Guard the guard: a bad glob would make the check above vacuous."""
    names = {path.name for path in _tracked_python_files()}
    assert "conftest.py" in names
    assert "s3_client.py" in names
