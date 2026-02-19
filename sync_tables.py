# sync_tables.py
#
# Baseline script to sync tables from a primary workspace to a secondary workspace.
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
# Params that must be specified below:
#   -landing_zone_url: the bucket, storage account, etc. where data will be written. This _must_ be in the secondary
#    region, not the primary region. It _must_ be accessible from both the primary and secondary workspace.
#   -source_host: the hostname of the primary workspace.
#   -source_pat: an access token for the primary workspace; must be an ADMIN user.
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -manifest_name: the name of the manifest file that will be generated to track table copies.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.
#   -warehouse_size: the size of the serverless warehouse to be created.
#
# To improve throughput, this script uses TheadPoolExecutors to parallelize submission of statements to the databricks
# warehouse. Table load statuses will be written to the delta table at {landing_zone_url}/sync_status_{time.time_ns()}.


import argparse
import logging
import os
import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
from dr_sync.sql_utils import execute_statement_sync, managed_warehouse, drop_table_if_exists
from dr_sync.exceptions import StatementError
from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging

logger = setup_logging()
config = DRSyncConfig.from_env() if os.environ.get("DR_SYNC_SOURCE_HOST") else DRSyncConfig.from_common_module()
target_host = config.target_host
target_pat = config.target_token
source_host = config.source_host
source_pat = config.source_token
catalogs_to_copy = config.catalogs_to_copy
num_exec = config.num_exec
landing_zone_url = config.landing_zone_url
warehouse_size = config.warehouse_size
response_backoff = config.response_backoff
manifest_name = config.manifest_name


# helper function to copy tables
def copy_table(w, catalog, schema, table_name, table_type, bucket, warehouse):
    try:
        sqlstring = f"CREATE OR REPLACE TABLE delta.`{bucket}/{catalog}_{schema}_{table_name}` DEEP CLONE {catalog}.{schema}.{table_name}"
        execute_statement_sync(w, warehouse, sqlstring, backoff=response_backoff)

        # return the table params in dict; used to build manifest
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": bucket}

    except StatementError as e:
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": f"COPY_ERROR: {e}",
                "location": "N/A"}

    except Exception as e:
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": f"COPY_ERROR: {e}",
                "location": "N/A"}


# helper function to load tables from a specified location
def load_table(w, catalog, schema, table_name, table_type, location, warehouse):
    if table_type == "MANAGED":
        logger.info("Creating MANAGED table %s.%s.%s...", catalog, schema, table_name)
        try:
            sqlstring = f"CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name} DEEP CLONE delta.`{location}`"
            execute_statement_sync(w, warehouse, sqlstring, backoff=response_backoff)

            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": "SUCCESS",
                    "creation_time": time.time_ns()}

        except Exception as e:
            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": f"FAIL: {e}",
                    "creation_time": time.time_ns()}

    elif table_type == "EXTERNAL":
        logger.info("Creating EXTERNAL table %s.%s.%s...", catalog, schema, table_name)

        try:
            # must drop table if it exists; CREATE_OR_REPLACE does not work when specifying external location
            sqlstring = f"CREATE TABLE {catalog}.{schema}.{table_name} USING delta LOCATION '{location}'"
            execute_statement_sync(w, warehouse, sqlstring, backoff=response_backoff)

            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": "SUCCESS",
                    "creation_time": time.time_ns()}

        except Exception as e:
            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": f"FAIL: {e}",
                    "creation_time": time.time_ns()}

    else:
        logger.warning("Skipping table %s.%s.%s; please check manifest file.", catalog, schema, table_name)
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": location,
                "status": "FAILURE",
                "creation_time": "N/A"}


# initialize lists
copied_table_names = []
copied_table_types = []
copied_table_schemas = []
copied_table_catalogs = []
copied_table_locations = []

# create the WorkspaceClient pointed at the source WS
w_source = WorkspaceClient(host=source_host, token=source_pat)

system_info = spark.sql("SELECT * FROM system.information_schema.tables")

if config.dry_run:
    # In dry-run mode, log what would happen without creating warehouses or executing SQL
    for cat in catalogs_to_copy:
        filtered_tables = system_info.filter(
            (system_info.table_catalog == cat) &
            (system_info.table_schema != "information_schema") &
            (system_info.table_type != "VIEW")).collect()

        schemas = [row['table_schema'] for row in filtered_tables]
        table_names = [row['table_name'] for row in filtered_tables]
        table_types = [row['table_type'] for row in filtered_tables]

        logger.info("[DRY RUN] Phase 1: Would copy %d tables from catalog %s to landing zone %s",
                    len(table_names), cat, landing_zone_url)
        for schema, table_name, table_type in zip(schemas, table_names, table_types):
            logger.info("[DRY RUN] Would deep clone %s table %s.%s.%s to %s/%s_%s_%s",
                        table_type, cat, schema, table_name, landing_zone_url, cat, schema, table_name)

    logger.info("[DRY RUN] Phase 2: Would create warehouse in secondary workspace and load tables from landing zone")
    for cat in catalogs_to_copy:
        filtered_tables = system_info.filter(
            (system_info.table_catalog == cat) &
            (system_info.table_schema != "information_schema") &
            (system_info.table_type != "VIEW")).collect()

        schemas = [row['table_schema'] for row in filtered_tables]
        table_names = [row['table_name'] for row in filtered_tables]
        table_types = [row['table_type'] for row in filtered_tables]

        for schema, table_name, table_type in zip(schemas, table_names, table_types):
            if table_type == "EXTERNAL":
                logger.info("[DRY RUN] Would drop and recreate external table %s.%s.%s", cat, schema, table_name)
            else:
                logger.info("[DRY RUN] Would deep clone %s table %s.%s.%s from landing zone", table_type, cat, schema, table_name)
else:
    # Phase 1: copy tables from source to landing zone
    logger.info("Creating warehouse in primary workspace...")
    with managed_warehouse(w_source, size=warehouse_size) as wh_source_id:
        # loop through all catalogs to copy, then copy all tables excluding system tables.
        # we also skip views; these need to be created separately since they cannot be cloned.
        for cat in catalogs_to_copy:
            filtered_tables = system_info.filter(
                (system_info.table_catalog == cat) &
                (system_info.table_schema != "information_schema") &
                (system_info.table_type != "VIEW")).collect()

            # get schemas, tables and types in list form
            schemas = [row['table_schema'] for row in filtered_tables]
            table_names = [row['table_name'] for row in filtered_tables]
            table_types = [row['table_type'] for row in filtered_tables]

            # use ThreadPool to copy tables in parallel
            with ThreadPoolExecutor(max_workers=num_exec) as executor:
                threads = executor.map(copy_table,
                                       repeat(w_source),
                                       repeat(cat),
                                       schemas,
                                       table_names,
                                       table_types,
                                       repeat(landing_zone_url),
                                       repeat(wh_source_id))

                # wait for threads to execute and build lists for manifest
                for thread in threads:
                    copied_table_names.append(thread["table_name"])
                    copied_table_types.append(thread["table_type"])
                    copied_table_schemas.append(thread["schema"])
                    copied_table_catalogs.append(thread["catalog"])
                    copied_table_locations.append(
                        "{}/{}_{}_{}".format(thread["location"], thread["catalog"], thread["schema"], thread["table_name"]))
                    logger.info("Copied table %s.%s.%s.", thread["catalog"], thread["schema"], thread["table_name"])

        # create the manifest as a df and write to a table in dr target
        # this contains catalog, schema, table and location
        manifest_df = pd.DataFrame({"catalog": copied_table_catalogs,
                                    "schema": copied_table_schemas,
                                    "table": copied_table_names,
                                    "location": copied_table_locations,
                                    "type": copied_table_types})

        # write the manifest to the target bucket in case it needs to be accessed later
        ts1 = time.time_ns()
        (spark.createDataFrame(manifest_df)
         .write.mode("overwrite")
         .format("delta")
         .save(f"{landing_zone_url}/{manifest_name}-{ts1}"))

    # Phase 2: load tables from landing zone to target
    # create the WorkspaceClient pointed at the target WS
    w_target = WorkspaceClient(host=target_host, token=target_pat)

    # initialize lists for status tracking
    loaded_table_names = []
    loaded_table_types = []
    loaded_table_schemas = []
    loaded_table_catalogs = []
    loaded_table_locations = []
    loaded_table_status = []
    loaded_table_times = []

    # create warehouse to run table creation statements, guaranteed cleanup
    logger.info("Creating warehouse in secondary workspace...")
    with managed_warehouse(w_target, size=warehouse_size) as wh_target_id:
        # drop external tables before loading due to CREATE TABLE restrictions
        external_df = manifest_df[manifest_df['type'] == 'EXTERNAL']
        with ThreadPoolExecutor(max_workers=num_exec) as executor:
            threads = executor.map(drop_table_if_exists,
                                   repeat(w_target),
                                   repeat(wh_target_id),
                                   list(external_df['catalog']),
                                   list(external_df['schema']),
                                   list(external_df['table']))

            for thread in threads:
                if thread["status"]:
                    logger.info("Dropped table %s.%s.%s.", thread["catalog"], thread["schema"], thread["table_name"])
                else:
                    logger.error("Error dropping table %s.%s.%s.", thread["catalog"], thread["schema"], thread["table_name"])

        # load all tables
        with ThreadPoolExecutor(max_workers=num_exec) as executor:
            threads = executor.map(load_table,
                                   repeat(w_target),
                                   list(manifest_df['catalog']),
                                   list(manifest_df['schema']),
                                   list(manifest_df['table']),
                                   list(manifest_df['type']),
                                   list(manifest_df['location']),
                                   repeat(wh_target_id))

            for thread in threads:
                loaded_table_names.append(thread["table_name"])
                loaded_table_types.append(thread["table_type"])
                loaded_table_schemas.append(thread["schema"])
                loaded_table_catalogs.append(thread["catalog"])
                loaded_table_locations.append(thread["location"])
                loaded_table_status.append(thread["status"])
                loaded_table_times.append(thread["creation_time"])
                logger.info("Loaded table %s.%s.%s.", thread["catalog"], thread["schema"], thread["table_name"])

        # create the table statuses as a df and write to a table in dr target
        status_df = pd.DataFrame({"catalog": loaded_table_catalogs,
                                  "schema": loaded_table_schemas,
                                  "table": loaded_table_names,
                                  "location": loaded_table_locations,
                                  "type": loaded_table_types,
                                  "status": loaded_table_status,
                                  "sync_time": loaded_table_times})

        # table will get a specific timestamp-based location per run
        ts2 = time.time_ns()
        (spark.createDataFrame(status_df)
         .write.mode("overwrite")
         .format("delta")
         .save(f"{landing_zone_url}/sync_status_{ts2}"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync tables from primary to secondary Databricks workspace via deep clone")
    parser.add_argument("--dry-run", action="store_true", help="Show planned operations without executing")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Set logging level")
    args = parser.parse_args()
    config.dry_run = args.dry_run
    logger = setup_logging(level=args.log_level)
