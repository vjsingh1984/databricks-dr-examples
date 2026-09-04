"""DR Sync — shared utilities for Databricks Disaster Recovery scripts."""

from dr_sync.cli import configure_runtime
from dr_sync.config import DRSyncConfig
from dr_sync.csv_mapping import load_mapping, lookup_value
from dr_sync.exceptions import (
    ConfigurationError,
    DRSyncError,
    MappingError,
    StatementError,
    SyncError,
    WarehouseError,
)
from dr_sync.log import setup_logging
from dr_sync.sql_utils import (
    drop_table_if_exists,
    execute_statement_sync,
    managed_warehouse,
    qualified_identifier,
    quote_identifier,
    quote_string_literal,
)
from dr_sync.thread_utils import ProgressCounter, parallel_map
from dr_sync.workspace import create_client

__all__ = [
    "ConfigurationError",
    "DRSyncConfig",
    "DRSyncError",
    "MappingError",
    "ProgressCounter",
    "StatementError",
    "SyncError",
    "WarehouseError",
    "configure_runtime",
    "create_client",
    "drop_table_if_exists",
    "execute_statement_sync",
    "load_mapping",
    "lookup_value",
    "managed_warehouse",
    "parallel_map",
    "qualified_identifier",
    "quote_identifier",
    "quote_string_literal",
    "setup_logging",
]
