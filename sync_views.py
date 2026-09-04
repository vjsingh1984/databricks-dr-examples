# sync_views.py
#
# *EXAMPLE* Script to sync views between workspaces. This will very likely need to be altered in your environment to
# match the use cases, syntax styles, etc. that you use. Please do NOT expect this to work directly.
#
# NOTE: This script must be run in the PRIMARY workspace. This simplifies and accelerates system table fetch and
# spark writes to the target bucket.
#
# This script will attempt to use DEEP CLONE on all tables within the specified catalog(s), and will then create those
# tables in the secondary metastore, within the same catalog and schema. The catalogs and schemas should already be
# created in the secondary metastore by using, i.e., sync_catalogs_and_schemas.py.
#
# Please note that this script uses Serverless compute by default to avoid waiting for classic warehouse startup times.
#
# Configuration is loaded from DR_SYNC_* environment variables or common.py. Use a
# TARGET unified-auth profile or workload identity; PATs are legacy-only. Set the
# landing zone, target workspace/profile, catalog list, worker count, and warehouse size.
#
# To improve throughput, this script uses ThreadPoolExecutor to parallelize submission of statements to the Databricks
# warehouse. Table load statuses will be written to the delta table at {landing_zone_url}/sync_status_{time.time_ns()}.


import time
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

import pandas as pd

from dr_sync.cli import configure_runtime
from dr_sync.config import DRSyncConfig
from dr_sync.exceptions import StatementError
from dr_sync.log import setup_logging
from dr_sync.sql_utils import execute_statement_sync, managed_warehouse, qualified_identifier
from dr_sync.workspace import create_client

config = DRSyncConfig.load()
logger = (
    configure_runtime(config, "Sync views between primary and secondary Databricks workspaces")
    if __name__ == "__main__"
    else setup_logging()
)
target_host = config.target_host
target_pat = config.target_token
catalogs_to_copy = config.catalogs_to_copy
num_exec = config.num_exec
landing_zone_url = config.landing_zone_url
warehouse_size = config.warehouse_size
response_backoff = config.response_backoff


# helper function to create a view
def create_view(w, catalog, schema, view_name, warehouse):

    try:
        view_stmt = spark.sql(
            f"SHOW CREATE TABLE {qualified_identifier(catalog, schema, view_name)}"
        ).collect()[0]["createtab_stmt"]

        execute_statement_sync(w, warehouse, view_stmt, backoff=response_backoff)

        return {
            "catalog": catalog,
            "schema": schema,
            "view_name": view_name,
            "status": "SUCCESS",
            "creation_time": time.time_ns(),
        }

    except StatementError as e:
        return {
            "catalog": catalog,
            "schema": schema,
            "view_name": view_name,
            "status": f"FAIL: {e}",
            "creation_time": time.time_ns(),
        }

    except Exception as e:
        return {
            "catalog": catalog,
            "schema": schema,
            "view_name": view_name,
            "status": f"FAIL: {e}",
            "creation_time": time.time_ns(),
        }


# pull all views from source ws
all_views = spark.sql("SELECT * FROM system.information_schema.views")

# create the WorkspaceClient pointed at the target WS
w_target = create_client(host=target_host, token=target_pat, profile=config.target_profile)

# initialize lists for status tracking
loaded_view_names = []
loaded_view_schemas = []
loaded_view_catalogs = []
loaded_view_status = []
loaded_view_times = []

if config.dry_run:
    # In dry-run mode, log what would happen without creating warehouses or executing SQL
    for cat in catalogs_to_copy:
        filtered_views = all_views.filter(
            (all_views.table_catalog == cat) & (all_views.table_schema != "information_schema")
        ).collect()

        schemas = [row["table_schema"] for row in filtered_views]
        view_names = [row["table_name"] for row in filtered_views]

        logger.info("[DRY RUN] Would create %d views in catalog %s", len(view_names), cat)
        for schema, view_name in zip(schemas, view_names, strict=False):
            logger.info("[DRY RUN] Would create view %s.%s.%s", cat, schema, view_name)
else:
    # create warehouse to run view creation statements, guaranteed cleanup
    logger.info("Creating warehouse in secondary workspace...")
    with managed_warehouse(w_target, size=warehouse_size) as wh_id:
        # load all views per catalog
        for cat in catalogs_to_copy:
            filtered_views = all_views.filter(
                (all_views.table_catalog == cat) & (all_views.table_schema != "information_schema")
            ).collect()

            # get schemas and view names
            schemas = [row["table_schema"] for row in filtered_views]
            view_names = [row["table_name"] for row in filtered_views]

            with ThreadPoolExecutor(max_workers=num_exec) as executor:
                threads = executor.map(
                    create_view,
                    repeat(w_target),
                    repeat(cat),
                    schemas,
                    view_names,
                    repeat(wh_id),
                )

                for thread in threads:
                    loaded_view_names.append(thread["view_name"])
                    loaded_view_schemas.append(thread["schema"])
                    loaded_view_catalogs.append(thread["catalog"])
                    loaded_view_status.append(thread["status"])
                    loaded_view_times.append(thread["creation_time"])
                    logger.info(
                        "Loaded view %s.%s.%s.",
                        thread["catalog"],
                        thread["schema"],
                        thread["view_name"],
                    )

        # create the table statuses as a df and write to a table in dr target
        status_df = pd.DataFrame(
            {
                "catalog": loaded_view_catalogs,
                "schema": loaded_view_schemas,
                "table": loaded_view_names,
                "status": loaded_view_status,
                "sync_time": loaded_view_times,
            }
        )

        # table will get a specific timestamp-based location per run
        ts = time.time_ns()
        (
            spark.createDataFrame(status_df)
            .write.mode("overwrite")
            .format("delta")
            .save(f"{landing_zone_url}/view_sync_status_{ts}")
        )
