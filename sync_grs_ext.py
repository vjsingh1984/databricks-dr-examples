# sync_grs_ext.py
#
# Baseline script to sync GRS-replicated tables from a primary metastore to a secondary metastore
#
# NOTE: This script must be run in the PRIMARY workspace. This simplifies and accelerates system table fetch and writes
# spark writes to the target bucket.
#
# This script will attempt to register all _external_ tables in the primary metastore into the secondary metastore. This
# assumes that all storage locations are identical between the two regions, i.e., georeplicated storage has been used.
# Storage URLs are not updated; they are just directly brought over to the secondary metastore.
#
# Please note that this script uses Serverless compute by default to avoid waiting for classic warehouse startup times.
#
# Params that must be specified below:
#   -landing_zone_url: the bucket, storage account, etc. where the status table will be written
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.
#   -warehouse_size: the size of the serverless warehouse to be created.
#
# To improve throughput, this script uses TheadPoolExecutors to parallelize submission of statements to the databricks
# warehouse. All table load statuses will be written to the delta table at {target_bucket}/sync_status_{time.time_ns()}.


import time
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

import pandas as pd

from dr_sync.cli import configure_runtime
from dr_sync.config import DRSyncConfig
from dr_sync.exceptions import StatementError
from dr_sync.log import setup_logging
from dr_sync.sql_utils import (
    drop_table_if_exists,
    execute_statement_sync,
    managed_warehouse,
    qualified_identifier,
    quote_string_literal,
)
from dr_sync.workspace import create_client

config = DRSyncConfig.load()
logger = (
    configure_runtime(
        config, "Sync GRS-replicated external tables to secondary Databricks workspace"
    )
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


# helper function to load tables from a specified location
def load_table(w, catalog, schema, table_name, location, warehouse):

    logger.info("Creating EXTERNAL table %s.%s.%s...", catalog, schema, table_name)

    try:
        target = qualified_identifier(catalog, schema, table_name)
        sqlstring = f"CREATE TABLE {target} USING delta LOCATION {quote_string_literal(location)}"
        execute_statement_sync(w, warehouse, sqlstring, backoff=response_backoff)

        return {
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "location": location,
            "status": "SUCCESS",
            "creation_time": time.time_ns(),
        }

    except StatementError as e:
        return {
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "location": location,
            "status": f"FAIL: {e}",
            "creation_time": time.time_ns(),
        }

    except Exception as e:
        return {
            "catalog": catalog,
            "schema": schema,
            "table_name": table_name,
            "location": location,
            "status": f"FAIL: {e}",
            "creation_time": time.time_ns(),
        }


# initialize lists for status tracking
loaded_table_names = []
loaded_table_schemas = []
loaded_table_catalogs = []
loaded_table_locations = []
loaded_table_status = []
loaded_table_times = []

# create the WorkspaceClient pointed at the target WS
w_target = create_client(host=target_host, token=target_pat, profile=config.target_profile)

if config.dry_run:
    # In dry-run mode, log what would happen without creating warehouses or executing SQL
    for cat in catalogs_to_copy:
        filtered_tables = spark.sql(
            """
            SELECT table_schema, table_name, storage_path
            FROM system.information_schema.tables
            WHERE table_catalog = :catalog
              AND table_schema != 'information_schema'
              AND table_type = 'EXTERNAL'
        """,
            args={"catalog": cat},
        ).collect()

        schemas = [row["table_schema"] for row in filtered_tables]
        table_names = [row["table_name"] for row in filtered_tables]
        table_locs = [row["storage_path"] for row in filtered_tables]

        logger.info(
            "[DRY RUN] Would process %d external tables in catalog %s",
            len(table_names),
            cat,
        )
        for schema, table_name, location in zip(schemas, table_names, table_locs, strict=False):
            logger.info(
                "[DRY RUN] Would drop and recreate external table %s.%s.%s at %s",
                cat,
                schema,
                table_name,
                location,
            )
else:
    # create warehouse to run table creation statements, guaranteed cleanup
    with managed_warehouse(w_target, size=warehouse_size) as wh_id:
        # loop through all catalogs to copy, then copy all tables excluding system tables.
        # we also skip views; these need to be created separately since they cannot be cloned.
        for cat in catalogs_to_copy:
            filtered_tables = spark.sql(
                """
                SELECT table_schema, table_name, storage_path
                FROM system.information_schema.tables
                WHERE table_catalog = :catalog
                  AND table_schema != 'information_schema'
                  AND table_type = 'EXTERNAL'
            """,
                args={"catalog": cat},
            ).collect()

            # get schemas, tables and types in list form
            schemas = [row["table_schema"] for row in filtered_tables]
            table_names = [row["table_name"] for row in filtered_tables]
            table_locs = [row["storage_path"] for row in filtered_tables]

            with ThreadPoolExecutor(max_workers=num_exec) as executor:
                threads = executor.map(
                    drop_table_if_exists,
                    repeat(w_target),
                    repeat(wh_id),
                    repeat(cat),
                    schemas,
                    table_names,
                )

                for thread in threads:
                    if thread["status"]:
                        logger.info(
                            "Dropped table %s.%s.%s.",
                            thread["catalog"],
                            thread["schema"],
                            thread["table_name"],
                        )
                    else:
                        logger.error(
                            "Error dropping table %s.%s.%s.",
                            thread["catalog"],
                            thread["schema"],
                            thread["table_name"],
                        )

            # use ThreadPool to copy tables in parallel
            with ThreadPoolExecutor(max_workers=num_exec) as executor:
                threads = executor.map(
                    load_table,
                    repeat(w_target),
                    repeat(cat),
                    schemas,
                    table_names,
                    table_locs,
                    repeat(wh_id),
                )

                # wait for threads to execute and build lists for status table
                for thread in threads:
                    loaded_table_names.append(thread["table_name"])
                    loaded_table_schemas.append(thread["schema"])
                    loaded_table_catalogs.append(thread["catalog"])
                    loaded_table_locations.append(thread["location"])
                    loaded_table_status.append(thread["status"])
                    loaded_table_times.append(thread["creation_time"])
                    logger.info(
                        "Loaded table %s.%s.%s.",
                        thread["catalog"],
                        thread["schema"],
                        thread["table_name"],
                    )

        # create the table statuses as a df and write to a table in dr target
        status_df = pd.DataFrame(
            {
                "catalog": loaded_table_catalogs,
                "schema": loaded_table_schemas,
                "table": loaded_table_names,
                "location": loaded_table_locations,
                "status": loaded_table_status,
                "create_time": loaded_table_times,
            }
        )

        # table will get a specific timestamp-based location per run
        (
            spark.createDataFrame(status_df)
            .write.mode("overwrite")
            .format("delta")
            .save(f"{landing_zone_url}/sync_status_{time.time_ns()}")
        )
