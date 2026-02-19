"""Custom exception hierarchy for DR sync operations."""


class DRSyncError(Exception):
    """Base exception for all DR sync errors."""


class ConfigurationError(DRSyncError):
    """Raised when configuration is invalid or missing."""


class MappingError(DRSyncError):
    """Raised when CSV mapping lookup fails."""

    def __init__(self, mapping_file, key_col, key_val, message=None):
        self.mapping_file = mapping_file
        self.key_col = key_col
        self.key_val = key_val
        msg = message or f"No mapping found for {key_col}={key_val!r} in {mapping_file}"
        super().__init__(msg)


class StatementError(DRSyncError):
    """Raised when a SQL statement execution fails."""

    def __init__(self, statement, message):
        self.statement = statement
        super().__init__(f"Statement failed: {message}\nSQL: {statement[:200]}")


class WarehouseError(DRSyncError):
    """Raised when warehouse operations fail."""


class SyncError(DRSyncError):
    """Raised when syncing a specific resource fails."""

    def __init__(self, resource_type, resource_name, message):
        self.resource_type = resource_type
        self.resource_name = resource_name
        super().__init__(f"Failed to sync {resource_type} {resource_name!r}: {message}")
