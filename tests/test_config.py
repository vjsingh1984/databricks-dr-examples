"""Tests for dr_sync.config module."""

import pytest

from dr_sync.config import DRSyncConfig
from dr_sync.exceptions import ConfigurationError


class TestDRSyncConfig:
    """Tests for DRSyncConfig dataclass."""

    def test_from_env_with_all_vars(self, monkeypatch):
        """Test loading config from environment variables."""
        monkeypatch.setenv("DR_SYNC_SOURCE_HOST", "https://source.example.com")
        monkeypatch.setenv("DR_SYNC_SOURCE_TOKEN", "source-token")
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "cat1,cat2")
        monkeypatch.setenv("DR_SYNC_CLOUD_TYPE", "aws")
        monkeypatch.setenv("DR_SYNC_NUM_EXEC", "8")
        monkeypatch.setenv("DR_SYNC_WAREHOUSE_SIZE", "Medium")
        monkeypatch.setenv("DR_SYNC_RESPONSE_BACKOFF", "1.0")

        config = DRSyncConfig.from_env()

        assert config.source_host == "https://source.example.com"
        assert config.source_token == "source-token"
        assert config.target_host == "https://target.example.com"
        assert config.target_token == "target-token"
        assert config.catalogs_to_copy == ["cat1", "cat2"]
        assert config.cloud_type == "aws"
        assert config.num_exec == 8
        assert config.warehouse_size == "Medium"
        assert config.response_backoff == 1.0

    def test_from_env_with_defaults(self, monkeypatch):
        """Test loading config with default values."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")

        config = DRSyncConfig.from_env()

        assert config.cloud_type == "azure"
        assert config.num_exec == 4
        assert config.warehouse_size == "Small"
        assert config.response_backoff == 0.5
        assert config.dry_run is False

    def test_from_env_with_dry_run_true(self, monkeypatch):
        """Test dry_run parsing from environment."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")
        monkeypatch.setenv("DR_SYNC_DRY_RUN", "true")

        config = DRSyncConfig.from_env()
        assert config.dry_run is True

    def test_from_env_with_dry_run_1(self, monkeypatch):
        """Test dry_run parsing with '1'."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")
        monkeypatch.setenv("DR_SYNC_DRY_RUN", "1")

        config = DRSyncConfig.from_env()
        assert config.dry_run is True

    def test_from_common_module(self, monkeypatch, tmp_path):
        """Test loading config from common.py module."""
        # Create a temporary common.py file
        common_file = tmp_path / "common.py"
        common_file.write_text(
            """
cloud_type = "aws"
source_host = "https://source.example.com"
source_pat = "source-token"
target_host = "https://target.example.com"
target_pat = "target-token"
catalogs_to_copy = ["cat1", "cat2"]
num_exec = 8
"""
        )

        # Add tmp_path to sys.path so we can import common

        monkeypatch.syspath_prepend(str(tmp_path))

        config = DRSyncConfig.from_common_module()

        assert config.cloud_type == "aws"
        assert config.source_host == "https://source.example.com"
        assert config.source_token == "source-token"
        assert config.target_host == "https://target.example.com"
        assert config.target_token == "target-token"
        assert config.catalogs_to_copy == ["cat1", "cat2"]
        assert config.num_exec == 8

    def test_from_common_module_not_found(self, mocker):
        """Test error when common.py is not found."""
        # Patch the import statement inside from_common_module
        import sys
        import builtins

        # Temporarily remove common from sys.modules
        common_backup = sys.modules.pop("common", None)
        try:
            # Mock __import__ to raise ImportError for 'common' only
            original_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "common":
                    raise ImportError("No module named 'common'")
                return original_import(name, *args, **kwargs)

            mocker.patch("builtins.__import__", side_effect=mock_import)

            with pytest.raises(ConfigurationError, match="common.py not found"):
                DRSyncConfig.from_common_module()
        finally:
            # Restore common if it was there
            if common_backup:
                sys.modules["common"] = common_backup

    def test_validate_success(self, monkeypatch):
        """Test validation with valid config."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")

        config = DRSyncConfig.from_env()
        errors = config.validate()

        assert errors == []

    def test_validate_missing_target_host(self, monkeypatch):
        """Test validation fails when target_host is missing."""
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")
        # Don't set DR_SYNC_TARGET_HOST

        config = DRSyncConfig.from_env()
        errors = config.validate()

        assert len(errors) == 1
        assert "target_host" in errors[0].lower()

    def test_validate_empty_catalogs(self, monkeypatch):
        """Test validation fails when catalogs_to_copy is empty."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        # Don't set DR_SYNC_CATALOGS_TO_COPY

        config = DRSyncConfig.from_env()
        errors = config.validate()

        assert len(errors) == 1
        assert "catalog" in errors[0].lower()

    def test_validate_invalid_cloud_type(self, monkeypatch):
        """Test validation fails with invalid cloud_type."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")
        monkeypatch.setenv("DR_SYNC_CLOUD_TYPE", "invalid")

        config = DRSyncConfig.from_env()
        errors = config.validate()

        assert len(errors) == 1
        assert "cloud" in errors[0].lower()

    def test_validate_invalid_num_exec(self, monkeypatch):
        """Test validation fails with invalid num_exec."""
        monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
        monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
        monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")
        monkeypatch.setenv("DR_SYNC_NUM_EXEC", "0")

        config = DRSyncConfig.from_env()
        errors = config.validate()

        assert len(errors) == 1
        assert "num_exec" in errors[0].lower()
