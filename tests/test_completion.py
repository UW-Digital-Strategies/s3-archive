"""Tests for the bash completion script: drift from the parser, and what Tab offers."""

from __future__ import annotations

import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from s3_archive.cli import _build_parser
from s3_archive.completion import render_bash

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "completions" / "s3-archive.bash"


def test_shipped_script_matches_parser() -> None:
    assert SCRIPT.read_text(encoding="utf-8") == render_bash(_build_parser()), (
        "completions/s3-archive.bash is stale; run `make completions`"
    )


def test_wheel_ships_script_where_bash_completion_looks() -> None:
    with open(REPO / "pyproject.toml", "rb") as fh:
        shared = tomllib.load(fh)["tool"]["hatch"]["build"]["targets"]["wheel"]["shared-data"]
    assert shared == {
        "completions/s3-archive.bash": "share/bash-completion/completions/s3-archive.bash"
    }


def _bash() -> str | None:
    """A bash new enough for the script (mapfile is bash 4+), or None."""
    bash = shutil.which("bash")
    if bash is None:
        return None
    major = subprocess.run(  # noqa: S603
        [bash, "-c", "echo ${BASH_VERSINFO[0]}"],
        capture_output=True,
        encoding="utf-8",
        check=True,
    ).stdout.strip()
    return bash if int(major) >= 4 else None


BASH = _bash()


def _tab(*words: str) -> list[str]:
    """What Tab offers with the cursor on the last of ``s3-archive WORDS``."""
    assert BASH is not None
    words_literal = " ".join(f"'{w}'" for w in ("s3-archive", *words))
    program = (
        f"source '{SCRIPT}'\n"
        f"COMP_WORDS=({words_literal}); COMP_CWORD={len(words)}\n"
        "_s3_archive; printf '%s\\n' \"${COMPREPLY[@]}\"\n"
    )
    out = subprocess.run(  # noqa: S603
        [BASH, "-c", program], capture_output=True, encoding="utf-8", check=True
    ).stdout
    return [line for line in out.splitlines() if line]


needs_bash = pytest.mark.skipif(BASH is None, reason="needs bash 4 or later")


SUBCOMMANDS = ["extract", "create", "ls", "config"]


@needs_bash
@pytest.mark.parametrize(
    ("words", "expected"),
    [
        ([""], SUBCOMMANDS),
        (["c"], ["create", "config"]),
        (["config", "--"], ["--help", "--profile"]),
        (["config", "--profile", ""], []),  # a profile name is free text
        (["-v", ""], SUBCOMMANDS),
    ],
)
def test_tab_offers(words: list[str], expected: list[str]) -> None:
    assert _tab(*words) == expected


@needs_bash
@pytest.mark.parametrize("command", ["extract", "create", "ls"])
def test_each_subcommand_offers_its_options(command: str) -> None:
    offered = _tab(command, "--")
    assert "--help" in offered
    assert "--version" not in offered


@needs_bash
def test_top_level_options_include_version() -> None:
    assert "--version" in _tab("--")
    assert "--version" not in _tab("ls", "--")
