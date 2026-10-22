"""End-to-end CLI smoke tests (with a patched S3 client + moto)."""

from unittest.mock import patch

import pytest

from s3_archive import resume
from s3_archive.cli import _build_parser, _CLI_ERRORS, main

from .conftest import (
    _extracted_keys,
    build_7z,
    build_tar,
    build_tar_gz,
    build_tar_xz_multiblock,
    build_zip,
    build_zip_with_corrupt_member,
    incompressible_bytes,
)

# Member sets reused across the format-parametrized cases below.
_SMOKE_FILES = {"a.txt": b"alpha\n", "b.txt": b"beta\n"}
_RESUME_FILES = {"a.txt": b"alpha\n", "sub/b.txt": b"beta\n"}
# Big enough (and incompressible enough) that `xz --block-size=1MiB` really
# splits — a single-block .tar.xz is refused, so the fixture must be genuinely
# multi-block for the resume case to mean anything.
_XZ_RESUME_FILES = {f"m{i}.bin": incompressible_bytes(512 * 1024, seed=i) for i in range(3)}


@pytest.fixture
def patched_client(s3_client):
    """Make every `client_for(profile)` call in the CLI return the moto client.

    This serves single-endpoint tests, where source and destination share
    one client. Tests that need genuinely distinct clients per side use
    the `cross_env_real_endpoints` fixture in conftest.py instead, which
    resolves each profile to its own `ThreadedMotoServer`.
    """
    with patch("s3_archive.cli.client_for", return_value=s3_client):
        yield s3_client


def _put_archive(s3, bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body)


class TestExtractCommand:
    @pytest.mark.parametrize(
        ("ext", "build"),
        [("tar.gz", build_tar_gz), ("7z", build_7z)],
        ids=["tar_gz", "7z"],
    )
    def test_extract_writes_every_member(self, patched_client, capsys, ext, build):
        _put_archive(patched_client, "src-bucket", f"in/archive.{ext}", build(_SMOKE_FILES))

        rc = main(["extract", f"s3://src-bucket/in/archive.{ext}", "s3://dest-bucket/out/"])

        assert rc == 0, capsys.readouterr().err
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == ["out/a.txt", "out/b.txt"]

    def test_extract_zip_dry_run(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.zip", build_zip(files))

        rc = main(
            [
                "extract",
                "--dry-run",
                "s3://src-bucket/in/archive.zip",
                "s3://dest-bucket/out/",
            ]
        )

        assert rc == 0, capsys.readouterr().err
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == []


class TestResumeCommand:
    @pytest.mark.parametrize(
        ("ext", "build_body", "expected"),
        [
            # zip and non-solid .7z seek per-member; .tar.gz (v3) resumes via an
            # indexed_gzip seek index written as a .idx companion; multi-block
            # .tar.xz (v4) uses the in-file block index and writes no .idx. All
            # four must finish clean and leave no debris of any kind behind.
            ("zip", lambda: build_zip(_RESUME_FILES), ["out/a.txt", "out/sub/b.txt"]),
            (
                "7z",
                lambda: build_7z(_RESUME_FILES, flavor="nonsolid"),
                ["out/a.txt", "out/sub/b.txt"],
            ),
            ("tar.gz", lambda: build_tar_gz(_RESUME_FILES), ["out/a.txt", "out/sub/b.txt"]),
            (
                "tar.xz",
                lambda: build_tar_xz_multiblock(_XZ_RESUME_FILES),
                [f"out/m{i}.bin" for i in range(3)],
            ),
        ],
        ids=["zip", "nonsolid_7z", "tar_gz", "multiblock_tar_xz"],
    )
    def test_resume_happy_path_leaves_no_debris(
        self, patched_client, capsys, ext, build_body, expected
    ):
        _put_archive(patched_client, "src-bucket", f"in/archive.{ext}", build_body())

        rc = main(
            ["extract", "--resume", f"s3://src-bucket/in/archive.{ext}", "s3://dest-bucket/out/"]
        )

        assert rc == 0, capsys.readouterr().err
        keys = _extracted_keys(patched_client, "dest-bucket", "out/")
        assert keys == expected
        # Clean completion removes the control marker (and, for .tar.gz, the
        # .idx companion) — no resume debris.
        assert not any(resume.is_control_key(k) for k in keys)

    @pytest.mark.parametrize(
        ("ext", "build_body", "reason"),
        [
            # A solid .7z has no per-member entry point, and stdlib lzma /
            # `tar -J` emit a single xz block with no interior seek point.
            ("7z", lambda: build_7z({"a.txt": b"x"}, flavor="solid"), "solid"),
            ("tar.xz", lambda: build_tar({"a.txt": b"x"}, mode="w:xz"), "single xz block"),
        ],
        ids=["solid_7z", "single_block_tar_xz"],
    )
    def test_resume_refused_writes_nothing(self, patched_client, capsys, ext, build_body, reason):
        _put_archive(patched_client, "src-bucket", f"in/archive.{ext}", build_body())

        rc = main(
            ["extract", "--resume", f"s3://src-bucket/in/archive.{ext}", "s3://dest-bucket/out/"]
        )

        assert rc == 2
        err = capsys.readouterr().err
        assert "Resume not supported" in err
        assert reason in err
        # Fail-fast: nothing written, no control marker.
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == []


class TestUnsafePaths:
    """`--fix-unsafe-paths` wiring and the UnsafeArchiveMemberError → stderr map."""

    def test_windows_zip_extract_dest_keys_forward_slashed(self, patched_client, capsys):
        _put_archive(
            patched_client,
            "src-bucket",
            "in/win.zip",
            build_zip({"Image repository\\UW.tif": b"tiff"}),
        )

        rc = main(["extract", "s3://src-bucket/in/win.zip", "s3://dest-bucket/out/"])

        assert rc == 0, capsys.readouterr().err
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == [
            "out/Image repository/UW.tif"
        ]

    def test_dotdot_member_maps_to_clean_stderr_and_exit_1(self, patched_client, capsys):
        _put_archive(
            patched_client, "src-bucket", "in/evil.tar", build_tar({"../evil": b"x"}, mode="w")
        )

        rc = main(["extract", "s3://src-bucket/in/evil.tar", "s3://dest-bucket/out/"])

        err = capsys.readouterr().err
        assert rc == 1
        assert "Unsafe archive member" in err
        assert "--fix-unsafe-paths" in err

    def test_fix_unsafe_paths_flag_collapses_and_extracts(self, patched_client, capsys):
        _put_archive(
            patched_client,
            "src-bucket",
            "in/fix.tar",
            build_tar({"a/../safe.txt": b"x"}, mode="w"),
        )

        rc = main(
            [
                "extract",
                "--fix-unsafe-paths",
                "s3://src-bucket/in/fix.tar",
                "s3://dest-bucket/out/",
            ]
        )

        assert rc == 0, capsys.readouterr().err
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == ["out/safe.txt"]

    def test_ls_accepts_fix_unsafe_paths(self, patched_client, capsys):
        _put_archive(
            patched_client,
            "src-bucket",
            "in/fix.tar",
            build_tar({"a/../safe.txt": b"x"}, mode="w"),
        )

        rc = main(["ls", "--fix-unsafe-paths", "s3://src-bucket/in/fix.tar"])

        assert rc == 0, capsys.readouterr().err
        assert "safe.txt" in capsys.readouterr().out


class TestLsCommand:
    @pytest.mark.parametrize(
        ("ext", "build", "files", "expected"),
        [
            ("tar.gz", build_tar_gz, _SMOKE_FILES, ["a.txt", "b.txt", " files, "]),
            ("zip", build_zip, {"a.txt": b"alpha\n"}, ["a.txt"]),
            ("7z", build_7z, _SMOKE_FILES, ["a.txt", "b.txt", " files, "]),
        ],
        ids=["tar_gz", "zip", "7z"],
    )
    def test_ls_prints_members_and_summary(
        self, patched_client, capsys, ext, build, files, expected
    ):
        _put_archive(patched_client, "src-bucket", f"in/archive.{ext}", build(files))

        rc = main(["ls", f"s3://src-bucket/in/archive.{ext}"])

        out = capsys.readouterr().out
        assert rc == 0
        for fragment in expected:
            assert fragment in out

    def test_ls_rejects_unknown_format(self, patched_client, capsys):
        rc = main(["ls", "s3://src-bucket/file.rar"])
        assert rc == 2
        assert "Cannot detect archive format" in capsys.readouterr().err


class TestCreateCommand:
    def test_create_tar_gz(self, patched_client, capsys):
        patched_client.put_object(Bucket="src-bucket", Key="src/a.txt", Body=b"alpha\n")

        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.tar.gz",
            ]
        )
        assert rc == 0, capsys.readouterr().err
        # The archive object was written.
        head = patched_client.head_object(Bucket="dest-bucket", Key="out/archive.tar.gz")
        assert head["ContentLength"] > 0

    def test_create_zip(self, patched_client, capsys):
        patched_client.put_object(Bucket="src-bucket", Key="src/a.txt", Body=b"alpha\n")

        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.zip",
            ]
        )
        assert rc == 0, capsys.readouterr().err

    def test_create_dry_run(self, patched_client, capsys):
        patched_client.put_object(Bucket="src-bucket", Key="src/a.txt", Body=b"alpha\n")

        rc = main(
            [
                "create",
                "--dry-run",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.tar.gz",
            ]
        )
        assert rc == 0, capsys.readouterr().err
        # Dry-run does not upload.
        keys = []
        paginator = patched_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="dest-bucket", Prefix="out/"):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        assert keys == []

    def test_create_rejects_bad_extension(self, patched_client, capsys):
        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.rar",
            ]
        )
        assert rc == 2
        assert "must end with" in capsys.readouterr().err

    def test_create_rejects_7z(self, patched_client, capsys):
        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.7z",
            ]
        )
        assert rc == 2
        assert ".7z create is not supported" in capsys.readouterr().err


class TestErrorHandling:
    def test_bad_extract_url_exits_2(self, patched_client, capsys):
        rc = main(["extract", "s3://src-bucket/file.rar", "s3://dest-bucket/out/"])
        assert rc == 2
        assert "Cannot detect archive format" in capsys.readouterr().err

    def test_corrupt_archive_exits_1_with_clean_stderr(self, patched_client, capsys):
        # Corrupt bytes reach the CLI as ArchiveReadError; the operator gets
        # a one-line message and exit 1, not a decoder traceback.
        _put_archive(patched_client, "src-bucket", "in/bad.tar.gz", b"not a gzip header\x00" * 10)

        rc = main(["extract", "s3://src-bucket/in/bad.tar.gz", "s3://dest-bucket/out/"])

        err = capsys.readouterr().err
        assert rc == 1
        assert "Archive read error" in err
        assert "Traceback" not in err

    def test_integrity_failure_writes_everything_and_exits_3(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n" * 20, "bad.db": b"thumbs " * 100, "z.txt": b"zulu\n"}
        _put_archive(
            patched_client,
            "src-bucket",
            "in/bad.zip",
            build_zip_with_corrupt_member(files, "bad.db"),
        )

        rc = main(["extract", "s3://src-bucket/in/bad.zip", "s3://dest-bucket/out/"])

        err = capsys.readouterr().err
        assert rc == 3
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == [
            "out/a.txt",
            "out/bad.db",
            "out/z.txt",
        ]
        assert "bad.db" in err
        assert "integrity warning" in err
        assert "s3://dest-bucket/out.log" in err
        assert "Traceback" not in err

    def test_corrupt_archive_ls_exits_1_with_clean_stderr(self, patched_client, capsys):
        _put_archive(patched_client, "src-bucket", "in/bad.zip", b"PK\x03\x04 not a real zip")

        rc = main(["ls", "s3://src-bucket/in/bad.zip"])

        err = capsys.readouterr().err
        assert rc == 1
        assert "Archive read error" in err

    def test_missing_creds_exits_2(self, monkeypatch, tmp_path, capsys):
        # No patched_client — let load_client() run for real. Clear every
        # credential source so the in-memory s3-archive can't pick anything up.
        for k in (
            "S3CMD_CONFIG",
            "S3_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
        ):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        with patch("s3_archive.s3_client.boto3.Session") as mock_session:
            mock_session.return_value.get_credentials.return_value = None
            rc = main(["ls", "s3://x/y.tar.gz"])
        assert rc == 2
        assert "No S3 credentials configured" in capsys.readouterr().err

    def test_bad_log_level_exits_2_before_touching_s3(self, monkeypatch, capsys):
        # setup_console runs inside main's try, so a typo'd LOG_LEVEL is
        # reported as a configuration error rather than a traceback — and
        # it lands before any client is built.
        monkeypatch.setenv("LOG_LEVEL", "LOUD")
        with patch("s3_archive.cli.client_for") as mock_client:
            rc = main(["ls", "s3://src-bucket/a.tar.gz"])
        assert rc == 2
        assert not mock_client.called
        assert "LOG_LEVEL" in capsys.readouterr().err

    def test_bad_log_level_root_exits_2_before_touching_s3(self, monkeypatch, capsys):
        # LOG_LEVEL_ROOT is resolved alongside LOG_LEVEL, inside main's try.
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        monkeypatch.setenv("LOG_LEVEL_ROOT", "LOUD")
        with patch("s3_archive.cli.client_for") as mock_client:
            rc = main(["ls", "s3://src-bucket/a.tar.gz"])
        assert rc == 2
        assert not mock_client.called
        assert "LOG_LEVEL_ROOT='LOUD'" in capsys.readouterr().err

    @pytest.mark.parametrize(("exc_type", "prefix", "code"), _CLI_ERRORS)
    def test_cli_error_table_row_maps_to_prefix_and_exit_code(self, exc_type, prefix, code, capsys):
        # main() walks _CLI_ERRORS instead of carrying one handler per
        # exception, so every row needs its own proof: a wrong prefix or
        # exit code in the table is otherwise invisible until an operator
        # hits it.
        # Some of these classes compose their own message from the arg,
        # so compare against the instance's own str().
        exc = exc_type("boom")
        with patch("s3_archive.cli._cmd_ls", side_effect=exc):
            rc = main(["ls", "s3://src-bucket/a.tar.gz"])

        assert rc == code
        assert capsys.readouterr().err.strip() == f"{prefix}: {exc}"


class TestParserSurface:
    """The parser is assembled from one `_add_*_parser` helper per subcommand.

    Each helper owns its subcommand's flags, so nothing in the code
    proves the four helpers are actually wired in, nor that a flag kept
    its `dest`/default when it moved. These tests are that proof.
    """

    @pytest.mark.parametrize(
        ("argv", "expected"),
        [
            (
                ["extract", "s3://b/a.tar.gz", "s3://b/out/"],
                {
                    "command": "extract",
                    "archive_url": "s3://b/a.tar.gz",
                    "dest_url": "s3://b/out/",
                    "dry_run": False,
                    "fix_unsafe_paths": False,
                    "resume": False,
                    "verbose": False,
                },
            ),
            (
                ["-v", "extract", "s3://b/a.tar.gz", "s3://b/out/", "--dry-run", "--resume"],
                {
                    "command": "extract",
                    "archive_url": "s3://b/a.tar.gz",
                    "dest_url": "s3://b/out/",
                    "dry_run": True,
                    "fix_unsafe_paths": False,
                    "resume": True,
                    "verbose": True,
                },
            ),
            (
                ["create", "s3://b/src/", "s3://b/a.zip"],
                {
                    "command": "create",
                    "src_url": "s3://b/src/",
                    "dest_url": "s3://b/a.zip",
                    "dry_run": False,
                    "verbose": False,
                },
            ),
            (
                ["ls", "s3://b/a.tar.gz", "--fix-unsafe-paths"],
                {
                    "command": "ls",
                    "archive_url": "s3://b/a.tar.gz",
                    "fix_unsafe_paths": True,
                    "verbose": False,
                },
            ),
            (
                ["config"],
                {"command": "config", "profile": "default", "verbose": False},
            ),
            (
                ["config", "--profile", "work"],
                {"command": "config", "profile": "work", "verbose": False},
            ),
        ],
        ids=["extract", "extract_flags", "create", "ls", "config", "config_profile"],
    )
    def test_parses_to_expected_namespace(self, argv, expected):
        assert vars(_build_parser().parse_args(argv)) == expected

    @pytest.mark.parametrize("command", ["extract", "create", "ls", "config"])
    def test_subcommand_help_renders(self, command, capsys):
        with pytest.raises(SystemExit) as exc:
            _build_parser().parse_args([command, "--help"])

        assert exc.value.code == 0
        assert capsys.readouterr().out.startswith(f"usage: s3-archive {command}")

    def test_bad_profile_name_is_a_clean_argparse_error(self, capsys):
        with pytest.raises(SystemExit) as exc:
            _build_parser().parse_args(["config", "--profile", "bad/name"])

        assert exc.value.code == 2
        assert "--profile" in capsys.readouterr().err
