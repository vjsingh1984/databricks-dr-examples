"""SQL statement execution utilities and warehouse lifecycle management."""

import time
from contextlib import contextmanager

from databricks.sdk.service.sql import (
    Disposition,
    StatementState,
    CreateWarehouseRequestWarehouseType,
    ExecuteStatementRequestOnWaitTimeout,
)
from databricks.sdk.service import sql as dbsql

from dr_sync.exceptions import StatementError, WarehouseError


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
            except Exception:
                pass
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
            print(f"Cleaned up warehouse {wh.id}")
        except Exception as e:
            print(f"Warning: could not delete warehouse {wh.id}: {e}")


def drop_table_if_exists(client, warehouse_id, catalog, schema, table_name, backoff=0.5):
    """Drop a table if it exists via SQL statement execution.

    Returns:
        dict with status (1=success, 0=failure) and table identifiers.
    """
    fqn = f"{catalog}.{schema}.{table_name}"
    print(f"Dropping table {fqn}...")

    try:
        execute_statement_sync(
            client, warehouse_id,
            f"DROP TABLE IF EXISTS {fqn}",
            backoff=backoff,
        )
        return {"status": 1, "catalog": catalog, "schema": schema, "table_name": table_name}
    except Exception:
        return {"status": 0, "catalog": catalog, "schema": schema, "table_name": table_name}
