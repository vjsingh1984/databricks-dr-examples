"""SQL statement execution utilities and warehouse lifecycle management."""

import logging
import time
from contextlib import contextmanager

from databricks.sdk.service import sql as dbsql
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    StatementState,
)

from dr_sync.exceptions import StatementError, WarehouseError

logger = logging.getLogger("dr_sync")


def quote_identifier(value):
    """Return one safely quoted Spark/Databricks SQL identifier component."""
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("SQL identifier components must be non-empty strings without NUL")
    return f"`{value.replace('`', '``')}`"


def qualified_identifier(*parts):
    """Return a safely quoted multipart Spark/Databricks SQL identifier."""
    return ".".join(quote_identifier(part) for part in parts)


def quote_string_literal(value):
    """Return one safely quoted Spark/Databricks SQL string literal."""
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError("SQL string literals must be strings without NUL")
    return "'" + value.replace("'", "''") + "'"


def execute_statement_sync(client, warehouse_id, statement, backoff=0.5, timeout_seconds=3600):
    """Execute a SQL statement and poll until completion.

    Args:
        client: WorkspaceClient instance.
        warehouse_id: ID of the warehouse to execute on.
        statement: SQL statement string.
        backoff: Seconds between polling attempts.
        timeout_seconds: Maximum seconds to wait before cancelling.

    Returns:
        The final statement response object.

    Raises:
        StatementError: If the statement fails or times out.
    """
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        wait_timeout="0s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
        disposition=Disposition("EXTERNAL_LINKS"),
        statement=statement,
    )

    start = time.monotonic()
    while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
        if time.monotonic() - start > timeout_seconds:
            try:
                client.statement_execution.cancel_execution(resp.statement_id)
            except Exception as exc:  # noqa: BLE001 - preserve the timeout cause
                logger.warning(
                    "Could not cancel timed-out statement %s: %s", resp.statement_id, exc
                )
            raise StatementError(statement, f"Timed out after {timeout_seconds}s")
        time.sleep(backoff)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        error_msg = resp.status.error.message if resp.status.error else "Unknown error"
        raise StatementError(statement, error_msg)

    return resp


@contextmanager
def managed_warehouse(client, size="Small", name_prefix="sdk"):
    """Context manager that creates a serverless warehouse and deletes it on exit.

    Args:
        client: WorkspaceClient instance.
        size: Warehouse cluster size.
        name_prefix: Prefix for the warehouse name.

    Yields:
        The warehouse ID string.

    Raises:
        WarehouseError: If warehouse creation fails.
    """
    wh_type = CreateWarehouseRequestWarehouseType("PRO")

    try:
        wh = client.warehouses.create(
            name=f"{name_prefix}-{time.time_ns()}",
            cluster_size=size,
            max_num_clusters=1,
            auto_stop_mins=10,
            warehouse_type=wh_type,
            enable_serverless_compute=True,
            tags=dbsql.EndpointTags(
                custom_tags=[dbsql.EndpointTagPair(key="Owner", value="dr-sync-tool")]
            ),
        ).result()
    except Exception as e:
        raise WarehouseError(f"Failed to create warehouse: {e}") from e

    try:
        yield wh.id
    finally:
        try:
            client.warehouses.delete(wh.id)
            logger.info("Cleaned up warehouse %s", wh.id)
        except Exception as e:  # noqa: BLE001 - cleanup must not hide the body failure
            logger.warning("Could not delete warehouse %s: %s", wh.id, e)


def drop_table_if_exists(client, warehouse_id, catalog, schema, table_name, backoff=0.5):
    """Drop a table if it exists via SQL statement execution.

    Returns:
        dict with status (1=success, 0=failure) and table identifiers.
    """
    fqn = qualified_identifier(catalog, schema, table_name)
    logger.info("Dropping table %s...", fqn)

    try:
        execute_statement_sync(
            client,
            warehouse_id,
            f"DROP TABLE IF EXISTS {fqn}",
            backoff=backoff,
        )
        return {
            "status": 1,
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
        }
    except Exception as exc:  # noqa: BLE001 - return the documented status record
        logger.warning("Could not drop table %s: %s", fqn, exc)
        return {
            "status": 0,
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
        }
