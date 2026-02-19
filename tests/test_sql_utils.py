"""Tests for dr_sync.sql_utils module."""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import sql

from dr_sync.sql_utils import execute_statement_sync, managed_warehouse, drop_table_if_exists
from dr_sync.exceptions import StatementError, WarehouseError


class TestExecuteStatementSync:
    """Tests for execute_statement_sync function."""

    def test_execute_statement_sync_success(self, mock_client, sample_statement_response):
        """Test successful statement execution."""
        mock_client.statement_execution.execute_statement.return_value = sample_statement_response

        result = execute_statement_sync(mock_client, "warehouse-123", "SELECT 1")

        assert result.status.state == sql.StatementState.SUCCEEDED
        mock_client.statement_execution.execute_statement.assert_called_once()
        # get_statement is NOT called when statement is already SUCCEEDED

    def test_execute_statement_sync_failure(self, mock_client):
        """Test statement execution with failure status."""
        response = MagicMock()
        response.statement_id = "stmt-123"
        response.status.state = sql.StatementState.FAILED
        response.status.error.message = "Syntax error"

        mock_client.statement_execution.execute_statement.return_value = response
        mock_client.statement_execution.get_statement.return_value = response

        with pytest.raises(StatementError, match="Syntax error"):
            execute_statement_sync(mock_client, "warehouse-123", "INVALID SQL")

    def test_execute_statement_sync_timeout(self, mock_client):
        """Test statement execution timeout."""
        pending_response = MagicMock()
        pending_response.statement_id = "stmt-123"
        pending_response.status.state = sql.StatementState.PENDING

        mock_client.statement_execution.execute_statement.return_value = pending_response
        mock_client.statement_execution.get_statement.return_value = pending_response
        mock_client.statement_execution.cancel_execution.return_value = None

        with pytest.raises(StatementError, match="Timed out"):
            execute_statement_sync(mock_client, "warehouse-123", "SELECT 1", timeout_seconds=0.1)


class TestManagedWarehouse:
    """Tests for managed_warehouse context manager."""

    def test_managed_warehouse_creates_and_deletes(self, mock_client, sample_warehouse_response):
        """Test warehouse is created and deleted."""
        mock_client.warehouses.create.return_value.result.return_value = sample_warehouse_response
        mock_client.warehouses.delete.return_value = None

        with managed_warehouse(mock_client) as warehouse_id:
            assert warehouse_id == "warehouse-123"
            mock_client.warehouses.create.assert_called_once()
            mock_client.warehouses.delete.assert_not_called()

        # After context, warehouse should be deleted
        mock_client.warehouses.delete.assert_called_once_with("warehouse-123")

    def test_managed_warehouse_deletes_on_exception(self, mock_client, sample_warehouse_response):
        """Test warehouse is deleted even when exception occurs."""
        mock_client.warehouses.create.return_value.result.return_value = sample_warehouse_response
        mock_client.warehouses.delete.return_value = None

        with pytest.raises(ValueError):
            with managed_warehouse(mock_client):
                raise ValueError("Test error")

        # Warehouse should still be deleted
        mock_client.warehouses.delete.assert_called_once_with("warehouse-123")

    def test_managed_warehouse_creation_failure(self, mock_client):
        """Test error when warehouse creation fails."""
        mock_client.warehouses.create.side_effect = Exception("Creation failed")

        with pytest.raises(WarehouseError, match="Failed to create warehouse"):
            with managed_warehouse(mock_client):
                pass


class TestDropTableIfExists:
    """Tests for drop_table_if_exists function."""

    def test_drop_table_if_exists_success(self, mock_client):
        """Test successful table drop."""
        mock_client.statement_execution.execute_statement.return_value = MagicMock(
            statement_id="stmt-123",
            status=MagicMock(state=sql.StatementState.SUCCEEDED),
        )
        mock_client.statement_execution.get_statement.return_value = MagicMock(
            status=MagicMock(state=sql.StatementState.SUCCEEDED),
        )

        result = drop_table_if_exists(mock_client, "warehouse-123", "cat", "schema", "table")

        assert result["status"] == 1
        assert result["catalog"] == "cat"
        assert result["schema"] == "schema"
        assert result["table_name"] == "table"

    def test_drop_table_if_exists_failure(self, mock_client):
        """Test table drop failure returns status 0."""
        mock_client.statement_execution.execute_statement.return_value = MagicMock(
            statement_id="stmt-123",
        )
        mock_client.statement_execution.get_statement.side_effect = Exception("Drop failed")

        result = drop_table_if_exists(mock_client, "warehouse-123", "cat", "schema", "table")

        assert result["status"] == 0
        assert result["catalog"] == "cat"
        assert result["schema"] == "schema"
        assert result["table_name"] == "table"
