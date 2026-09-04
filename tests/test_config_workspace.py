import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dr_sync.config import DRSyncConfig
from dr_sync.exceptions import ConfigurationError
from dr_sync.workspace import create_client


def test_from_env_parses_profiles_and_values(monkeypatch):
    monkeypatch.setenv("DR_SYNC_SOURCE_PROFILE", "source")
    monkeypatch.setenv("DR_SYNC_TARGET_PROFILE", "target")
    monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "one, two, ,three")
    monkeypatch.setenv("DR_SYNC_NUM_EXEC", "8")
    monkeypatch.setenv("DR_SYNC_RESPONSE_BACKOFF", "1.25")
    monkeypatch.setenv("DR_SYNC_DRY_RUN", "yes")

    config = DRSyncConfig.from_env()

    assert config.source_profile == "source"
    assert config.target_profile == "target"
    assert config.catalogs_to_copy == ["one", "two", "three"]
    assert config.num_exec == 8
    assert config.response_backoff == 1.25
    assert config.dry_run is True


def test_from_common_supports_profiles_and_legacy_tokens(monkeypatch):
    common = SimpleNamespace(
        cloud_type="aws",
        source_profile="source",
        target_profile="target",
        source_pat="",
        target_pat="",
        catalogs_to_copy=["catalog"],
    )
    monkeypatch.setitem(sys.modules, "common", common)

    config = DRSyncConfig.from_common_module()

    assert config.cloud_type == "aws"
    assert config.source_profile == "source"
    assert config.target_profile == "target"


def test_validation_requires_safe_unambiguous_configuration():
    config = DRSyncConfig(
        cloud_type="other",
        source_token="secret",
        source_profile="source",
        target_token="secret",
        target_profile="target",
        catalogs_to_copy=[],
        num_exec=0,
    )

    errors = config.validate()

    assert "source_token and source_profile are mutually exclusive" in errors
    assert "target_token and target_profile are mutually exclusive" in errors
    assert "catalogs_to_copy must not be empty" in errors
    assert any("cloud_type" in error for error in errors)
    assert any("num_exec" in error for error in errors)


def test_validation_accepts_profile_without_pat_and_hides_tokens():
    config = DRSyncConfig(
        target_profile="target",
        target_token="",
        source_host="https://source.example.com",
        source_token="do-not-print",
        catalogs_to_copy=["c"],
    )
    assert config.validate() == []
    assert "do-not-print" not in repr(config)


def test_load_detects_profile_only_environment(monkeypatch):
    monkeypatch.setenv("DR_SYNC_SOURCE_PROFILE", "SOURCE")
    monkeypatch.setenv("DR_SYNC_TARGET_PROFILE", "TARGET")
    monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "catalog")

    config = DRSyncConfig.load()

    assert config.source_profile == "SOURCE"
    assert config.target_profile == "TARGET"


def test_load_fails_closed_on_invalid_environment(monkeypatch):
    monkeypatch.setenv("DR_SYNC_TARGET_PROFILE", "TARGET")
    monkeypatch.setenv("DR_SYNC_NUM_EXEC", "0")
    with pytest.raises(ConfigurationError, match=r"catalogs_to_copy.*num_exec"):
        DRSyncConfig.load()


@pytest.mark.parametrize(
    "host",
    [
        "http://workspace.example.com",
        "https://user:password@workspace.example.com",
        "https://workspace.example.com/api",
        "https://workspace.example.com?token=x",
        "not-a-url",
    ],
)
def test_client_rejects_unsafe_hosts(host):
    with pytest.raises(ConfigurationError):
        create_client(host=host)


def test_client_uses_profile_and_normalizes_host():
    expected = object()
    with patch("dr_sync.workspace.WorkspaceClient", return_value=expected) as workspace:
        assert create_client(host="https://workspace.example.com/", profile="TARGET") is expected
    workspace.assert_called_once_with(host="https://workspace.example.com", profile="TARGET")


def test_client_delegates_to_default_chain_without_credentials():
    with patch("dr_sync.workspace.WorkspaceClient") as workspace:
        create_client()
    workspace.assert_called_once_with()


def test_client_supports_explicit_legacy_pat_but_not_profile_combination():
    with patch("dr_sync.workspace.WorkspaceClient") as workspace:
        create_client(host="https://workspace.example.com", token="legacy")
    workspace.assert_called_once_with(host="https://workspace.example.com", token="legacy")
    with pytest.raises(ConfigurationError):
        create_client(token="legacy", profile="TARGET")
