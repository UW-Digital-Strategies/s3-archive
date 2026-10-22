"""Tests for the interactive `s3-archive config` subcommand.

The production prompts go through `questionary`, which talks directly to
the controlling TTY. Tests patch the thin `_ask_text` / `_ask_password`
/ `_ask_confirm` helpers in `config_cmd` so we can script answers and
make assertions on the prompt text without spinning up a fake TTY.
"""

import configparser
from pathlib import Path

import pytest

from s3_archive import config_cmd
from s3_archive.exceptions import ConfigError

# A minimal but complete s3cmd INI, used wherever a test needs a
# pre-existing config on disk for the early-detect path to find.
_EXISTING_INI = "[default]\naccess_key = OLD\nsecret_key = OLD\nhost_base = {host}\n"


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    """Point $HOME at tmp_path and clear $S3CMD_CONFIG.

    Both matter: the early-detect check for an existing `~/.s3cfg` would
    otherwise see the developer's real config, and it prefers
    $S3CMD_CONFIG over `~/.s3cfg`, so a developer's environment would
    bleed in too.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("S3CMD_CONFIG", raising=False)
    return tmp_path


@pytest.fixture
def cfg_path(home) -> Path:
    # We deliberately pick a *non*-`.s3cfg` filename so writing to
    # `cfg_path` doesn't collide with the default-path early-detect
    # check; tests that exercise the early-detect prompt explicitly
    # create `home / ".s3cfg"`.
    return home / "custom-s3cfg"


def _drive_prompts(
    monkeypatch,
    *,
    text: list[str] = (),
    passwords: list[str] = (),
    confirms: list[bool] = (),
) -> list[str]:
    """Patch the three `_ask_*` helpers to consume scripted answers.

    Each fake echoes the question to stdout the way a real questionary
    prompt would render it, so capsys-based assertions on the prompt
    text (e.g. "already exists") continue to work.

    Returns the (initially empty) list of `default=` values `_ask_text`
    is called with, appended in call order — that's how tests assert on
    prompt prefills without hand-rolling their own fakes.
    """
    text_iter = iter(text)
    password_iter = iter(passwords)
    confirm_iter = iter(confirms)
    seen_text_defaults: list[str] = []

    def fake_text(question: str, default: str = "") -> str:
        print(question)
        seen_text_defaults.append(default)
        return next(text_iter)

    def fake_password(question: str) -> str:
        print(question)
        return next(password_iter)

    def fake_confirm(question: str, *, default: bool = False) -> bool:
        print(question)
        return next(confirm_iter)

    monkeypatch.setattr(config_cmd, "_ask_text", fake_text)
    monkeypatch.setattr(config_cmd, "_ask_password", fake_password)
    monkeypatch.setattr(config_cmd, "_ask_confirm", fake_confirm)
    return seen_text_defaults


def _stub_connect(monkeypatch, results: list[str | None]) -> None:
    """Patch the pre-write connection check to hand back scripted results.

    The real `_try_connect` reaches into boto3; unit tests script it
    instead. It returns ``None`` for "connection OK" and a one-line
    error string otherwise, so the default `(None,)` means every attempt
    succeeds and the prompt loop exits without asking "retry?". The last
    entry repeats, so a one-element sequence pins every attempt and a
    longer one walks a retry sequence.
    """
    pending = list(results)

    def fake_connect(*_a, **_kw) -> str | None:
        return pending.pop(0) if len(pending) > 1 else pending[0]

    monkeypatch.setattr(config_cmd, "_try_connect", fake_connect)


def _run_config_ok(
    monkeypatch,
    *,
    text: list[str] = (),
    passwords: list[str] = (),
    confirms: list[bool] = (),
    connect: list[str | None] = (None,),
    **run_kwargs,
) -> list[str]:
    """Script the prompts and the connection check, run, assert a clean exit.

    Every interactive path in this module ends in exit code 0 (the
    decline/cancel branches included), so the rc assertion lives here
    rather than being restated in each test. Returns `_drive_prompts`'
    captured `_ask_text` defaults.
    """
    seen_text_defaults = _drive_prompts(
        monkeypatch, text=text, passwords=passwords, confirms=confirms
    )
    _stub_connect(monkeypatch, connect)

    assert config_cmd.run_config(**run_kwargs) == 0

    return seen_text_defaults


def _read_default_section(path: Path) -> configparser.SectionProxy:
    """Return the `[default]` section of the s3cmd INI written at *path*."""
    parser = configparser.ConfigParser()
    parser.read(path)
    return parser["default"]


class TestValidateProfileName:
    @pytest.mark.parametrize(
        "name",
        ["default", "ceph-server", "aws-prsv", "team_a", "a", "ABC123", "x-y_z"],
    )
    def test_accepts_valid(self, name):
        assert config_cmd.validate_profile_name(name) == name

    @pytest.mark.parametrize(
        "name",
        ["", "bad name", "with/slash", "with.dot", "ééé", "has space"],
    )
    def test_rejects_invalid(self, name):
        with pytest.raises(ConfigError, match="Invalid profile name"):
            config_cmd.validate_profile_name(name)


class TestDefaultPathForProfile:
    def test_default_maps_to_s3cfg(self):
        assert config_cmd._default_path_for("default") == "~/.s3cfg"

    @pytest.mark.parametrize(
        "profile,expected",
        [
            ("ceph-server", "~/.s3cfg-ceph-server"),
            ("aws-prsv", "~/.s3cfg-aws-prsv"),
            ("team_a", "~/.s3cfg-team_a"),
        ],
    )
    def test_named_profile(self, profile, expected):
        assert config_cmd._default_path_for(profile) == expected


class TestCanonicalConfigPath:
    def test_default_honors_s3cmd_config(self, home, monkeypatch):
        explicit = home / "explicit.cfg"
        monkeypatch.setenv("S3CMD_CONFIG", str(explicit))

        assert config_cmd._canonical_config_path("default") == explicit

    def test_named_profile_ignores_s3cmd_config(self, home, monkeypatch):
        # The plan calls this out specifically: $S3CMD_CONFIG must NOT
        # apply when the operator names a profile. The named-profile
        # branch always reads/writes ~/.s3cfg-<name>.
        monkeypatch.setenv("S3CMD_CONFIG", "/tmp/bogus.cfg")  # noqa: S108 - test fixture path

        result = config_cmd._canonical_config_path("ceph-server")
        assert result == (home / ".s3cfg-ceph-server").absolute()


class TestRunConfigHappyPath:
    @pytest.mark.parametrize(
        "endpoint,expected_host_base",
        [
            pytest.param("https://s3.example.com", "s3.example.com", id="explicit-endpoint"),
            # Endpoint left blank → AWS S3, so no host_base is written.
            pytest.param("", None, id="blank-endpoint-means-aws"),
        ],
    )
    def test_writes_full_s3cmd_ini(
        self, monkeypatch, cfg_path, capsys, endpoint, expected_host_base
    ):
        _run_config_ok(
            monkeypatch,
            text=[endpoint, "ACCESS123", str(cfg_path)],
            passwords=["SECRET456"],
        )

        assert cfg_path.exists()
        section = _read_default_section(cfg_path)
        assert section["access_key"] == "ACCESS123"
        assert section["secret_key"] == "SECRET456"
        assert section.get("host_base") == expected_host_base
        # The AWS-defaults hint fires exactly when no host_base was written.
        aws_note_shown = "AWS S3 defaults" in capsys.readouterr().out
        assert aws_note_shown is (expected_host_base is None)


class TestRunConfigProfileFlag:
    """`--profile NAME` selects ~/.s3cfg-NAME for both reads and writes."""

    @pytest.mark.parametrize(
        "profile,s3cmd_config",
        [
            pytest.param("default", None, id="default-profile"),
            pytest.param("ceph-server", None, id="named-profile"),
            # `$S3CMD_CONFIG=/tmp/bogus` does not influence a named profile.
            pytest.param(
                "ceph-server",
                "/tmp/bogus.cfg",  # noqa: S108 - test fixture path
                id="named-ignores-s3cmd-config",
            ),
        ],
    )
    def test_target_path_default_per_profile(self, profile, s3cmd_config, home, monkeypatch):
        if s3cmd_config:
            monkeypatch.setenv("S3CMD_CONFIG", s3cmd_config)
        expected = home / (".s3cfg" if profile == "default" else f".s3cfg-{profile}")

        # Accept the prefilled path so we land on the canonical location.
        _run_config_ok(
            monkeypatch,
            text=["https://s3.example.com", "ACCESS", str(expected)],
            passwords=["SECRET"],
            profile=profile,
        )

        assert expected.exists()
        assert _read_default_section(expected)["access_key"] == "ACCESS"
        # Nothing was written at the bogus $S3CMD_CONFIG path.
        assert s3cmd_config is None or not Path(s3cmd_config).exists()

    def test_invalid_profile_raises_early(self):
        with pytest.raises(ConfigError, match="Invalid profile name"):
            config_cmd.run_config(profile="bad name")


class TestToolNameInPrompt:
    def test_default_tool_name_is_s3_archive(self, monkeypatch, cfg_path, capsys):
        _run_config_ok(
            monkeypatch,
            text=["", "ACCESS", str(cfg_path)],
            passwords=["SECRET"],
        )

        assert "Configure S3 credentials for s3-archive" in capsys.readouterr().out

    def test_tool_name_override(self, monkeypatch, cfg_path, capsys):
        _run_config_ok(
            monkeypatch,
            text=["", "ACCESS", str(cfg_path)],
            passwords=["SECRET"],
            tool_name="s3-bagit",
        )

        assert "Configure S3 credentials for s3-bagit" in capsys.readouterr().out


class TestRunConfigOverwriteGuard:
    def test_existing_file_overwrite_declined(self, monkeypatch, cfg_path, capsys):
        cfg_path.write_text("# original content\n")
        original_mtime = cfg_path.stat().st_mtime

        _run_config_ok(
            monkeypatch,
            text=["https://s3.example.com", "ACCESS", str(cfg_path)],
            passwords=["SECRET"],
            confirms=[False],  # declines the overwrite prompt
        )

        # File untouched.
        assert cfg_path.read_text() == "# original content\n"
        assert cfg_path.stat().st_mtime == original_mtime
        assert "Aborted" in capsys.readouterr().out

    def test_existing_file_overwrite_accepted(self, monkeypatch, cfg_path):
        cfg_path.write_text("# original content\n")

        _run_config_ok(
            monkeypatch,
            text=["https://s3.example.com", "ACCESS", str(cfg_path)],
            passwords=["SECRET"],
            confirms=[True],  # accepts overwrite
        )

        assert _read_default_section(cfg_path)["access_key"] == "ACCESS"


class TestEndpointToHostBase:
    @pytest.mark.parametrize(
        "endpoint,expected",
        [
            ("https://s3.example.com", "s3.example.com"),
            ("http://localhost:9000", "localhost:9000"),
            ("s3.example.com", "s3.example.com"),
            ("https://s3.example.com/", "s3.example.com"),
        ],
    )
    def test_strips_scheme_and_path(self, endpoint, expected):
        assert config_cmd._endpoint_to_host_base(endpoint) == expected


class TestEarlyDetectExistingConfig:
    """The fast-path: detect an existing ~/.s3cfg before prompting for anything."""

    @pytest.mark.parametrize(
        "filename,contents,profile,expected_host",
        [
            pytest.param(
                ".s3cfg",
                _EXISTING_INI.format(host="s3.example.com"),
                "default",
                "s3.example.com",
                id="default-profile",
            ),
            # Malformed-but-parseable INI: no [default] section, so there
            # is no host_base to quote back at the operator.
            pytest.param(
                ".s3cfg",
                "# half-broken file\n",
                "default",
                None,
                id="no-default-section",
            ),
            # A named profile reads ~/.s3cfg-<name>, not ~/.s3cfg.
            pytest.param(
                ".s3cfg-ceph-server",
                _EXISTING_INI.format(host="ceph-server.example"),
                "ceph-server",
                "ceph-server.example",
                id="named-profile",
            ),
        ],
    )
    def test_decline_keeps_existing_file(
        self, home, monkeypatch, capsys, filename, contents, profile, expected_host
    ):
        existing = home / filename
        existing.write_text(contents)
        original_mtime = existing.stat().st_mtime

        _run_config_ok(monkeypatch, confirms=[False], profile=profile)

        out = capsys.readouterr().out
        assert existing.read_text() == contents
        assert existing.stat().st_mtime == original_mtime
        assert "already exists" in out
        assert "Keeping existing configuration" in out
        if expected_host is None:
            # No URL claim made when we couldn't read host_base.
            assert "pointing to" not in out
        else:
            assert expected_host in out  # URL hint is shown.


class TestEarlyDetectRespectsS3CMDConfig:
    """The early-detect path mirrors s3_client.load_client's precedence (default profile only)."""

    def test_existing_s3cmd_config_takes_precedence_over_default(self, home, monkeypatch, capsys):
        # Both files exist; $S3CMD_CONFIG should win.
        (home / ".s3cfg").write_text(_EXISTING_INI.format(host="default.example"))
        explicit = home / "explicit.cfg"
        explicit.write_text(_EXISTING_INI.format(host="explicit.example"))
        monkeypatch.setenv("S3CMD_CONFIG", str(explicit))

        _run_config_ok(monkeypatch, confirms=[False])  # decline early-detect

        out = capsys.readouterr().out
        # The explicit path (and its host) is what's surfaced, not the default.
        assert str(explicit) in out
        assert "explicit.example" in out
        assert "default.example" not in out

    def test_missing_s3cmd_config_skips_early_detect(self, home, monkeypatch, capsys):
        # $S3CMD_CONFIG set but file doesn't exist: operator wants to write
        # a new config there. No early-detect; prompts proceed.
        target = home / "tmp-bobo"
        monkeypatch.setenv("S3CMD_CONFIG", str(target))
        assert not target.exists()
        # Even if ~/.s3cfg exists, the env-var path wins precedence and
        # the env-var path doesn't exist — so no early-detect prompt.
        (home / ".s3cfg").write_text(_EXISTING_INI.format(host="nope"))

        _run_config_ok(
            monkeypatch,
            # The third answer accepts the prefilled path.
            text=["https://s3.example.com", "ACCESS", str(target)],
            passwords=["SECRET"],
        )

        assert target.exists()
        # No "already exists" prompt fired — the env-var path was missing.
        assert "already exists" not in capsys.readouterr().out

    def test_env_var_path_is_prompt_default(self, home, monkeypatch):
        """The 'Config file path' prompt prefills $S3CMD_CONFIG when set."""
        target = home / "tmp-bobo"
        monkeypatch.setenv("S3CMD_CONFIG", str(target))

        seen_text_defaults = _run_config_ok(
            monkeypatch,
            text=["https://s3.example", "ACCESS", str(target)],
            passwords=["SECRET"],
        )

        # The third _ask_text call is for the config path. Its default
        # must be the $S3CMD_CONFIG value, not ~/.s3cfg.
        assert seen_text_defaults[2] == str(target)


class TestValidateEndpoint:
    @pytest.mark.parametrize(
        "endpoint",
        [
            "",  # blank = AWS defaults
            "https://s3.example.com",
            "http://localhost:9000",
            "s3.example.com",
            "localhost:9000",
        ],
    )
    def test_accepts_valid(self, endpoint):
        assert config_cmd._validate_endpoint(endpoint) is None

    @pytest.mark.parametrize(
        "endpoint,token",
        [
            ("blark dar dar", "spaces"),
            ("https:// s3.example.com", "spaces"),
            ("https://", "scheme but no host"),
            ("http://", "scheme but no host"),
        ],
    )
    def test_rejects_invalid(self, endpoint, token):
        msg = config_cmd._validate_endpoint(endpoint)
        assert msg is not None
        assert token in msg


class TestRunConfigEndpointRetries:
    def test_invalid_endpoint_reprompts_until_valid(self, monkeypatch, cfg_path, capsys):
        _run_config_ok(
            monkeypatch,
            text=[
                "blark dar dar",  # rejected
                "https://",  # rejected
                "https://s3.example.com",  # accepted
                "ACCESS",
                str(cfg_path),
            ],
            passwords=["SECRET"],
        )

        out = capsys.readouterr().out
        assert "Endpoint must not contain spaces" in out
        assert "scheme but no host" in out
        assert _read_default_section(cfg_path)["host_base"] == "s3.example.com"


class TestTryConnectSwallowsExceptions:
    """``_try_connect`` must never raise; it returns an error string instead."""

    def test_value_error_from_boto3_returned_as_string(self, monkeypatch):
        # A `host_base` with a space crashes boto3 with ValueError. The
        # helper must catch and surface it as a one-line string so the
        # interactive loop can show it and prompt for retry.
        for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE"):
            monkeypatch.delenv(k, raising=False)

        result = config_cmd._try_connect(
            access_key="X",
            secret_key="X",
            host_base="blark dar dar",
        )

        assert result is not None
        assert "ValueError" in result or "Invalid endpoint" in result


class TestPreWriteConnectionTest:
    """The new flow: connection test happens BEFORE writing the file."""

    def test_failed_then_successful_retry_writes_second_values(self, home, monkeypatch, capsys):
        target = home / "second-attempt.cfg"

        _run_config_ok(
            monkeypatch,
            text=[
                "https://broken.example",  # attempt 1 — fails
                "ACCESS1",
                "https://good.example",  # attempt 2 — succeeds
                "ACCESS2",
                str(target),  # path
            ],
            passwords=["SECRET1", "SECRET2"],
            confirms=[True],  # "Try again with different credentials?" → yes
            connect=["EndpointConnectionError: nope", None],  # first call fails, second succeeds
        )

        # File was only written after the SECOND (successful) attempt,
        # and it carries the second set of values.
        assert target.exists()
        section = _read_default_section(target)
        assert section["access_key"] == "ACCESS2"
        assert section["host_base"] == "good.example"
        out = capsys.readouterr().out
        assert "FAILED" in out
        assert "Configured." in out

    @pytest.mark.parametrize(
        "save_anyway,expected_notes,absent_notes",
        [
            # Offline-configuration case: operator explicitly opts in to
            # saving values that failed the connection test.
            pytest.param(
                True,
                ("Saving the values anyway.", "Configured."),
                (),
                id="save-anyway-writes-broken-config",
            ),
            # The safe default: declining both prompts cancels cleanly.
            pytest.param(
                False,
                ("Cancelled; no changes written.",),
                ("Configured.",),
                id="decline-both-exits-without-writing",
            ),
        ],
    )
    def test_connection_failure_save_prompt(
        self, home, monkeypatch, capsys, save_anyway, expected_notes, absent_notes
    ):
        target = home / "offline.cfg"
        # The config-path prompt is only reached on the save-anyway branch.
        answers = ["https://future.example", "ACCESS"]
        if save_anyway:
            answers.append(str(target))

        _run_config_ok(
            monkeypatch,
            text=answers,
            passwords=["SECRET"],
            # "Try again?" → no; "Save these values anyway?" → save_anyway.
            confirms=[False, save_anyway],
            connect=["EndpointConnectionError: not on the network"],
        )

        assert target.exists() is save_anyway
        out = capsys.readouterr().out
        for note in expected_notes:
            assert note in out
        for note in absent_notes:
            assert note not in out


class TestSummaryOrdering:
    """End-of-run summary structure: 'Configured.' then optional hints."""

    def test_non_default_path_note_appears_after_configured_line(self, home, monkeypatch, capsys):
        # Non-default path so the note fires.
        target = home / "non-default.cfg"

        _run_config_ok(
            monkeypatch,
            text=["https://s3.example.com", "ACCESS", str(target)],
            passwords=["SECRET"],
        )

        out = capsys.readouterr().out
        # The non-default-path note must come AFTER the "Configured." summary.
        configured_idx = out.find("Configured.")
        hint_idx = out.find("not the default location")
        assert configured_idx != -1
        assert hint_idx != -1
        # And it must not include the old shell-snippets we removed.
        assert "export S3CMD_CONFIG=" not in out
        assert "$env:S3CMD_CONFIG" not in out
        assert hint_idx > configured_idx


class TestResolvePathDoesNotFollowSymlinks:
    def test_symlink_displayed_as_typed(self, tmp_path):
        # ~/.s3cfg → /some/real/file is the surprising case the operator hit.
        real = tmp_path / "real-config.cfg"
        real.write_text("real")
        link = tmp_path / "symlinked-config"
        link.symlink_to(real)

        resolved = config_cmd._resolve_path(str(link))

        # We preserve the path the operator typed; we do NOT show them
        # /Users/.../some-other-place that the link happens to target.
        assert resolved == link
        assert resolved != real


class TestReprompts:
    def test_blank_access_key_reprompts(self, monkeypatch, cfg_path):
        _run_config_ok(
            monkeypatch,
            text=[
                "https://s3.example.com",
                "",  # first access-key attempt: blank, reprompts
                "ACCESS",
                str(cfg_path),
            ],
            passwords=["SECRET"],
        )

        assert _read_default_section(cfg_path)["access_key"] == "ACCESS"


class TestCtrlCSurfacesKeyboardInterrupt:
    """questionary returns None on Ctrl-C — the helpers must raise KeyboardInterrupt."""

    def test_text_helper_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_cmd.questionary,
            "text",
            lambda *_a, **_kw: _StubAsk(None),
        )
        with pytest.raises(KeyboardInterrupt):
            config_cmd._ask_text("anything")

    def test_password_helper_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_cmd.questionary,
            "password",
            lambda *_a, **_kw: _StubAsk(None),
        )
        with pytest.raises(KeyboardInterrupt):
            config_cmd._ask_password("anything")

    def test_confirm_helper_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_cmd.questionary,
            "confirm",
            lambda *_a, **_kw: _StubAsk(None),
        )
        with pytest.raises(KeyboardInterrupt):
            config_cmd._ask_confirm("anything")


class _StubAsk:
    """Mimic the `questionary.Question` interface our helpers consume."""

    def __init__(self, value):
        self._value = value

    def ask(self):
        return self._value
