from dr_sync.cli import configure_runtime
from dr_sync.config import DRSyncConfig


def test_runtime_flags_are_applied_before_operations():
    config = DRSyncConfig(dry_run=False)
    logger = configure_runtime(config, "test", ["--dry-run", "--log-level", "WARNING"])
    assert config.dry_run is True
    assert logger.level == 30


def test_environment_dry_run_cannot_be_disabled_by_cli_default():
    config = DRSyncConfig(dry_run=True)
    configure_runtime(config, "test", [])
    assert config.dry_run is True
