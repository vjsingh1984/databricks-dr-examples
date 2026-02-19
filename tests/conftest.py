"""Pytest configuration and fixtures for dr_sync tests."""

import os
import tempfile
from unittest.mock import MagicMock

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog, sql

from dr_sync.config import DRSyncConfig


@pytest.fixture
def mock_source_client():
    """Mock source WorkspaceClient."""
    client = MagicMock(spec=WorkspaceClient)
    client.host = "https://source.cloud.databricks.com"
    return client


@pytest.fixture
def mock_target_client():
    """Mock target WorkspaceClient."""
    client = MagicMock(spec=WorkspaceClient)
    client.host = "https://target.cloud.databricks.com"
    return client


@pytest.fixture
def mock_config(monkeypatch):
    """Mock DRSyncConfig with minimal valid settings."""
    monkeypatch.setenv("DR_SYNC_TARGET_HOST", "https://target.example.com")
    monkeypatch.setenv("DR_SYNC_TARGET_TOKEN", "target-token")
    monkeypatch.setenv("DR_SYNC_CATALOGS_TO_COPY", "test_catalog")
    return DRSyncConfig.from_env()


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("source,target\n")
        f.write("src1,tgt1\n")
        f.write("src2,tgt2\n")
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def sample_catalog():
    """Sample catalog object."""
    return catalog.CloudCatalog(
        name="test_catalog",
        comment="Test catalog",
    )


@pytest.fixture
def sample_schema():
    """Sample schema object."""
    return catalog.Schema(
        name="test_schema",
        catalog_name="test_catalog",
        comment="Test schema",
    )


@pytest.fixture
def mock_client():
    """Generic mock WorkspaceClient."""
    client = MagicMock(spec=WorkspaceClient)
    client.host = "https://test.cloud.databricks.com"
    return client


@pytest.fixture
def sample_warehouse_response():
    """Sample warehouse creation response."""
    return sql.CreateWarehouseResponse(id="warehouse-123")


@pytest.fixture
def sample_statement_response():
    """Sample statement execution response."""
    return sql.StatementResponse(
        statement_id="statement-123",
        status=sql.StatementStatus(state=sql.StatementState.SUCCEEDED),
        manifest={},
    )
