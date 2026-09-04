from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.sql import StatementState

from dr_sync.exceptions import StatementError, WarehouseError
from dr_sync.sql_utils import (
    drop_table_if_exists,
    execute_statement_sync,
    managed_warehouse,
    qualified_identifier,
    quote_identifier,
    quote_string_literal,
)


def response(state, statement_id="statement", message=None):
    error = SimpleNamespace(message=message) if message else None
    return SimpleNamespace(
        statement_id=statement_id,
        status=SimpleNamespace(state=state, error=error),
    )


def test_sql_quoting_handles_reserved_names_and_injection_text():
    assert quote_identifier("a`b") == "`a``b`"
    assert qualified_identifier("catalog", "select", "a.b") == "`catalog`.`select`.`a.b`"
    assert quote_string_literal("x'; DROP TABLE y; --") == "'x''; DROP TABLE y; --'"
    with pytest.raises(ValueError):
        quote_identifier("")
    with pytest.raises(ValueError):
        quote_string_literal("bad\x00value")


def test_execute_statement_returns_successful_response():
    client = Mock()
    expected = response(StatementState.SUCCEEDED)
    client.statement_execution.execute_statement.return_value = expected

    assert execute_statement_sync(client, "warehouse", "SELECT 1") is expected
    client.statement_execution.get_statement.assert_not_called()


def test_execute_statement_polls_and_reports_remote_failure():
    client = Mock()
    client.statement_execution.execute_statement.return_value = response(StatementState.PENDING)
    client.statement_execution.get_statement.return_value = response(
        StatementState.FAILED, message="denied"
    )
    with patch("dr_sync.sql_utils.time.sleep"):
        with pytest.raises(StatementError, match="denied"):
            execute_statement_sync(client, "warehouse", "SELECT 1")


def test_execute_statement_cancels_on_timeout_even_if_cancel_fails():
    client = Mock()
    client.statement_execution.execute_statement.return_value = response(StatementState.RUNNING)
    client.statement_execution.cancel_execution.side_effect = RuntimeError("offline")
    with patch("dr_sync.sql_utils.time.monotonic", side_effect=[0, 2]):
        with pytest.raises(StatementError, match="Timed out"):
            execute_statement_sync(client, "warehouse", "SELECT 1", timeout_seconds=1)
    client.statement_execution.cancel_execution.assert_called_once_with("statement")


def test_managed_warehouse_deletes_after_success_and_body_failure():
    client = Mock()
    client.warehouses.create.return_value.result.return_value = SimpleNamespace(id="warehouse")
    with managed_warehouse(client) as warehouse_id:
        assert warehouse_id == "warehouse"
    client.warehouses.delete.assert_called_once_with("warehouse")

    client.warehouses.delete.reset_mock()
    with pytest.raises(RuntimeError, match="body"):
        with managed_warehouse(client):
            raise RuntimeError("body")
    client.warehouses.delete.assert_called_once_with("warehouse")


def test_managed_warehouse_wraps_creation_failure_and_tolerates_cleanup_failure():
    client = Mock()
    client.warehouses.create.side_effect = RuntimeError("no capacity")
    with pytest.raises(WarehouseError, match="no capacity"):
        with managed_warehouse(client):
            pass

    client = Mock()
    client.warehouses.create.return_value.result.return_value = SimpleNamespace(id="warehouse")
    client.warehouses.delete.side_effect = RuntimeError("already gone")
    with managed_warehouse(client) as warehouse_id:
        assert warehouse_id == "warehouse"


def test_drop_table_quotes_identifiers_and_returns_status():
    client = Mock()
    with patch("dr_sync.sql_utils.execute_statement_sync") as execute:
        result = drop_table_if_exists(client, "wh", "cat", "schema", "name`; DROP TABLE x")
    assert result["status"] == 1
    assert execute.call_args.args[2] == "DROP TABLE IF EXISTS `cat`.`schema`.`name``; DROP TABLE x`"

    with patch("dr_sync.sql_utils.execute_statement_sync", side_effect=StatementError("x", "bad")):
        result = drop_table_if_exists(client, "wh", "cat", "schema", "table")
    assert result["status"] == 0
