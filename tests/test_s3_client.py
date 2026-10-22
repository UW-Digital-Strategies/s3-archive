"""Tests for s3_archive.s3_client — credential resolution + per-profile cache."""

from unittest.mock import patch

import pytest

from s3_archive import s3_client
from s3_archive.exceptions import ConfigError


@pytest.fixture
def home_tmp(tmp_path, monkeypatch):
    """Point HOME at tmp_path and clear bleed-in env vars."""
    monkeypatch.setenv("HOME", str(tmp_path))
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
    return tmp_path


def _write_s3cfg(path, *, access_key: str = "AK", secret_key: str = "SK", host_base: str = "h"):
    path.write_text(
        f"[default]\n"
        f"access_key = {access_key}\n"
        f"secret_key = {secret_key}\n"
        f"host_base = {host_base}\n"
    )


class TestLoadClientDefaultProfile:
    """The default branch is the unchanged historical chain (env > ~/.s3cfg > boto3)."""

    def test_reads_s3cmd_config_env(self, home_tmp, monkeypatch):
        path = home_tmp / "explicit.cfg"
        _write_s3cfg(path, access_key="EX", secret_key="EX", host_base="explicit.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(path))

        client = s3_client.load_client()

        # The smoke test: a client was built against the host_base we set.
        assert client.meta.endpoint_url == "https://explicit.example"

    def test_reads_default_s3cfg_when_env_unset(self, home_tmp):
        default = home_tmp / ".s3cfg"
        _write_s3cfg(default, host_base="default.example")

        client = s3_client.load_client()

        assert client.meta.endpoint_url == "https://default.example"

    def test_non_ascii_config_reads_as_utf8(self, home_tmp):
        # `config` writes ~/.s3cfg-<profile> as UTF-8; the reader must decode
        # it as UTF-8 too, not with whatever the host locale happens to be.
        # On a non-UTF-8-locale host, an accented comment or value used to
        # blow the whole file up with a UnicodeDecodeError.
        default = home_tmp / ".s3cfg"
        default.write_text(
            "[default]\n"
            "# notes: Ceph — 한국어 bucket\n"
            "access_key = AK\n"
            "secret_key = SK\n"
            "host_base = utf8.example\n"
            "bucket_location = Amérique\n",
            encoding="utf-8",
        )

        client = s3_client.load_client()

        assert client.meta.endpoint_url == "https://utf8.example"

    def test_no_creds_anywhere_raises(self, home_tmp):
        # No ~/.s3cfg, no AWS_*, no $S3CMD_CONFIG. boto3.Session returns None.
        with (
            patch("s3_archive.s3_client.boto3.Session") as mock_session,
            pytest.raises(ConfigError, match="No S3 credentials configured"),
        ):
            mock_session.return_value.get_credentials.return_value = None
            s3_client.load_client()


class TestLoadClientNamedProfile:
    """Named-profile branch — reads ~/.s3cfg-<name> exclusively."""

    def test_reads_profile_file(self, home_tmp):
        path = home_tmp / ".s3cfg-ceph-server"
        _write_s3cfg(path, host_base="ceph-server.example")

        client = s3_client.load_client(profile="ceph-server")

        assert client.meta.endpoint_url == "https://ceph-server.example"

    def test_missing_file_raises_with_hint(self, home_tmp):
        with pytest.raises(ConfigError, match="Profile 'ceph-server'") as excinfo:
            s3_client.load_client(profile="ceph-server")
        msg = str(excinfo.value)
        assert ".s3cfg-ceph-server" in msg
        assert "s3-archive config --profile ceph-server" in msg

    def test_ignores_s3cmd_config_env(self, home_tmp, monkeypatch):
        # $S3CMD_CONFIG pointing at a valid file does NOT influence the
        # named-profile branch. Only ~/.s3cfg-<name> matters.
        bogus = home_tmp / "would-have-worked.cfg"
        _write_s3cfg(bogus, host_base="bogus.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(bogus))

        # ~/.s3cfg-ceph-server doesn't exist → ConfigError, not silent fallback.
        with pytest.raises(ConfigError, match="Profile 'ceph-server'"):
            s3_client.load_client(profile="ceph-server")

    def test_does_not_fall_back_to_boto3(self, home_tmp, monkeypatch):
        # Even with AWS_* in the env, a missing profile file raises.
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
        with pytest.raises(ConfigError, match="Profile 'ceph-server'"):
            s3_client.load_client(profile="ceph-server")


class TestS3cmdConfigEnvPrecedence:
    """The two branches disagree about $S3CMD_CONFIG, deliberately.

    With no profile, $S3CMD_CONFIG overrides ~/.s3cfg. With a named
    profile, ~/.s3cfg-<name> is the only source and $S3CMD_CONFIG is
    ignored outright — no silent fallback to whatever the env points at.
    """

    @pytest.mark.parametrize(
        ("cfg_name", "profile", "expected_host"),
        [
            (".s3cfg", "default", "from-env.example"),
            (".s3cfg-ceph-server", "ceph-server", "on-disk.example"),
        ],
        ids=["env_wins_over_default", "profile_file_ignores_env"],
    )
    def test_env_config_precedence(self, home_tmp, monkeypatch, cfg_name, profile, expected_host):
        _write_s3cfg(home_tmp / cfg_name, host_base="on-disk.example")
        env_cfg = home_tmp / "from-env.cfg"
        _write_s3cfg(env_cfg, host_base="from-env.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(env_cfg))

        client = s3_client.load_client(profile=profile)

        assert client.meta.endpoint_url == f"https://{expected_host}"


class TestClientForCache:
    @pytest.mark.parametrize(
        ("cfg_name", "first", "second"),
        [
            (".s3cfg-ceph-server", "ceph-server", "ceph-server"),
            # None is canonicalised to "default", so both names hit one entry.
            (".s3cfg", None, "default"),
        ],
        ids=["same_profile", "none_is_default"],
    )
    def test_equivalent_profile_names_share_one_client(self, home_tmp, cfg_name, first, second):
        _write_s3cfg(home_tmp / cfg_name, host_base="cached.example")

        assert s3_client.client_for(first) is s3_client.client_for(second)

    def test_different_profiles_return_different_objects(self, home_tmp):
        default = home_tmp / ".s3cfg"
        _write_s3cfg(default, host_base="default.example")
        ceph_server = home_tmp / ".s3cfg-ceph-server"
        _write_s3cfg(ceph_server, host_base="ceph-server.example")

        c_default = s3_client.client_for("default")
        c_ceph_server = s3_client.client_for("ceph-server")
        assert c_default is not c_ceph_server
        assert c_default.meta.endpoint_url == "https://default.example"
        assert c_ceph_server.meta.endpoint_url == "https://ceph-server.example"

    def test_reset_clears_cache(self, home_tmp):
        path = home_tmp / ".s3cfg"
        _write_s3cfg(path, host_base="default.example")

        c1 = s3_client.client_for("default")
        s3_client._reset_client_cache()
        c2 = s3_client.client_for("default")
        assert c1 is not c2
