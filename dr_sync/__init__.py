"""DR Sync — shared utilities for Databricks Disaster Recovery scripts."""

from dr_sync.exceptions import (
    DRSyncError,
    ConfigurationError,
    MappingError,
    StatementError,
    WarehouseError,
    SyncError,
)
from dr_sync.sql_utils import (
    execute_statement_sync,
    managed_warehouse,
    drop_table_if_exists,
)
from dr_sync.workspace import create_client
from dr_sync.csv_mapping import load_mapping, lookup_value
from dr_sync.thread_utils import parallel_map, ProgressCounter
from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging
from dr_sync.retry import retry_with_backoff
from dr_sync.checkpoint import CheckpointManager, SyncCheckpoint
from dr_sync.filter import ResourceFilter, parse_filter_args
from dr_sync.registry import register_sync, get_registry, SyncRegistry, SyncModule

__all__ = [
    "DRSyncError",
    "ConfigurationError",
    "MappingError",
    "StatementError",
    "WarehouseError",
    "SyncError",
    "execute_statement_sync",
    "managed_warehouse",
    "drop_table_if_exists",
    "create_client",
    "load_mapping",
    "lookup_value",
    "parallel_map",
    "ProgressCounter",
    "DRSyncConfig",
    "setup_logging",
    "retry_with_backoff",
    "CheckpointManager",
    "SyncCheckpoint",
    "ResourceFilter",
    "parse_filter_args",
    "register_sync",
    "get_registry",
    "SyncRegistry",
    "SyncModule",
]
