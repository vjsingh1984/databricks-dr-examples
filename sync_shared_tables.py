# sync_shared_tables.py
#
# This script will create and update a share in the secondary metastore (if it does not exist) and then use DEEP CLONE
# to replicate data to the secondary region. The target catalogs must already exist in the secondary metastore, i.e.,
# sync_catalogs_and_schemas.py should already have been run.
#
# Notes:
#   - ALL tables are created as managed tables when using this script. You can change this behavior by changing the
#     filter conditions on the system table (i.e., use table_type to filter filtered_tables).
#   - Your environment may need to be altered to allow SAS/S3 Presigned URL traffic. This is often the case if a
#     firewall is configured; be sure to check your rules if you receive errors when trying to clone.
#   - This script uses Serverless compute by default to avoid waiting for classic warehouse startup times.
#
# Params that must be specified below:
#   -source_host: the hostname of the primary workspace.
#   -source_pat: an access token for the primary workspace; must be an ADMIN user.
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.
#   -target_share_id: the sharing identifier of the secondary metastore.

import argparse
import logging
import os
import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import BadRequest
from databricks.sdk.service.catalog import Privilege, PermissionsChange
from databricks.sdk.service.sharing import (AuthenticationType, SharedDataObjectUpdate,
                                            SharedDataObjectUpdateAction, SharedDataObject,
                                            SharedDataObjectDataObjectType, SharedDataObjectStatus)
from dr_sync.sql_utils import execute_statement_sync, managed_warehouse
from dr_sync.exceptions import StatementError
from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging

config = DRSyncConfig.from_env() if os.environ.get("DR_SYNC_SOURCE_HOST") else DRSyncConfig.from_common_module()
logger = setup_logging()
target_host = config.target_host
target_pat = config.target_token
source_host = config.source_host
source_pat = config.source_token
catalogs_to_copy = config.catalogs_to_copy
num_exec = config.num_exec
landing_zone_url = config.landing_zone_url
warehouse_size = config.warehouse_size
response_backoff = config.response_backoff
metastore_id = config.metastore_id


# helper function to clone a table from one catalog to another
def clone_table(w, source_catalog, target_catalog, schema, table_name, warehouse):

    logger.info("Cloning table %s.%s.%s...", source_catalog, schema, table_name)
    try:
        sqlstring = (f"CREATE OR REPLACE TABLE {target_catalog}.{schema}.{table_name} "
                     f"DEEP CLONE {source_catalog}.{schema}.{table_name}")

        execute_statement_sync(w, warehouse, sqlstring, backoff=response_backoff)

        return {"catalog": target_catalog,
                "schema": schema,
                "table_name": table_name,
                "status": "SUCCESS",
                "creation_time": time.time_ns()}

    except StatementError as e:
        return {"catalog": target_catalog,
                "schema": schema,
                "table_name": table_name,
                "status": f"FAIL: {e}",
                "creation_time": time.time_ns()}

    except Exception as e:
        return {"catalog": target_catalog,
                "schema": schema,
                "table_name": table_name,
                "status": f"FAIL: {e}",
                "creation_time": time.time_ns()}


# other parameters
write_results = False  # set to true to write status df to disk

# create the WorkspaceClients for source and target workspaces
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# create the secondary metastore as a recipient
try:
    logger.info("Creating recipient with id %s...", metastore_id)
    recipient = w_source.recipients.create(name="dr_automation_recipient",
                                           authentication_type=AuthenticationType.DATABRICKS,
                                           data_recipient_global_metastore_id=metastore_id)
except BadRequest:

    try:
        recipient = [r for r in w_source.recipients.list() if r.data_recipient_global_metastore_id == metastore_id][0]
        logger.info("Recipient with id %s already exists. Skipping creation...", metastore_id)
    except IndexError:
        raise RuntimeError(f"Recipient with id {metastore_id} does not exist in source workspace. Please validate the id and create it manually.")

# get all tables in the primary metastore
system_info = spark.sql("SELECT * FROM system.information_schema.tables")

# get local metastore id
local_metastore_id = [r["current_metastore()"] for r in spark.sql("SELECT current_metastore()").collect()][0]

# get remote provider name; it may or may not be the same as local_metastore_id
try:
    remote_provider_name = [p.name for p in w_target.providers.list() if
                            p.data_provider_global_metastore_id == local_metastore_id][0]
except IndexError:
    raise RuntimeError("Provider could not be found in target workspace; please check that it was created.")

# initalize df lists
cloned_table_names = []
cloned_table_schemas = []
cloned_table_catalogs = []
cloned_table_status = []
cloned_table_times = []

if config.dry_run:
    # In dry-run mode, log what would happen without creating warehouses or executing SQL
    for cat in catalogs_to_copy:
        filtered_tables = system_info.filter(
            (system_info.table_catalog == cat) &
            (system_info.table_schema != "information_schema") &
            (system_info.table_type != "VIEW")).distinct().collect()

        unique_schemas = {row['table_schema'] for row in filtered_tables}
        all_tables = [row["table_name"] for row in filtered_tables]
        all_schemas = [row["table_schema"] for row in filtered_tables]

        logger.info("[DRY RUN] Would create/update share %s_share with %d schemas", cat, len(unique_schemas))
        for schema in unique_schemas:
            logger.info("[DRY RUN] Would add schema %s.%s to share", cat, schema)
        logger.info("[DRY RUN] Would create shared catalog %s_share in target workspace", cat)
        logger.info("[DRY RUN] Would clone %d tables from %s_share to %s", len(all_tables), cat, cat)
        for schema, table_name in zip(all_schemas, all_tables):
            logger.info("[DRY RUN] Would clone table %s_share.%s.%s to %s.%s.%s",
                        cat, schema, table_name, cat, schema, table_name)
else:
    # create warehouse in secondary to run table creation statements, guaranteed cleanup
    logger.info("Creating warehouse in secondary workspace...")
    with managed_warehouse(w_target, size=warehouse_size) as wh_id:
        # iterate through all catalogs to share
        for cat in catalogs_to_copy:
            filtered_tables = system_info.filter(
                (system_info.table_catalog == cat) &
                (system_info.table_schema != "information_schema") &
                (system_info.table_type != "VIEW")).distinct().collect()

            unique_schemas = {row['table_schema'] for row in filtered_tables}
            all_tables = [row["table_name"] for row in filtered_tables]
            all_schemas = [row["table_schema"] for row in filtered_tables]

            # create the share for the current catalog and update permissions
            logger.info("Creating share for catalog %s...", cat)
            try:
                share = w_source.shares.create(name=f"{cat}_share")
                share_name = share.name
            except BadRequest:
                logger.info("Share %s_share already exists. Skipping creation...", cat)
                share_name = f"{cat}_share"

            try:
                _ = w_source.shares.update_permissions(share_name,
                                                       changes=[PermissionsChange(add=[Privilege.SELECT],
                                                                                  principal=recipient.name)])
            except BadRequest:
                logger.error("Could not update permissions for share %s.", share_name)

            # build update object with all schemas in the current catalog
            updates = [
                SharedDataObjectUpdate(action=SharedDataObjectUpdateAction.ADD,
                                       data_object=SharedDataObject(name=f"{cat}.{schema}",
                                                                    data_object_type=SharedDataObjectDataObjectType.SCHEMA,
                                                                    status=SharedDataObjectStatus.ACTIVE))
                for schema in unique_schemas]

            # update the share
            try:
                _ = w_source.shares.update(share_name, updates=updates)
            except Exception as e:
                logger.error("Error updating share %s: %s", share_name, e)

            # create the shared catalog in the target workspace
            try:
                _ = w_target.catalogs.create(name=f"{cat}_share", provider_name=remote_provider_name, share_name=share_name)
            except BadRequest:
                logger.info("Shared catalog %s_share already exists. Skipping creation.", cat)

            with ThreadPoolExecutor(max_workers=num_exec) as executor:
                threads = executor.map(clone_table,
                                       repeat(w_target),
                                       repeat(f"{cat}_share"),
                                       repeat(cat),
                                       all_schemas,
                                       all_tables,
                                       repeat(wh_id))

                for thread in threads:
                    cloned_table_names.append(thread["table_name"])
                    cloned_table_schemas.append(thread["schema"])
                    cloned_table_catalogs.append(thread["catalog"])
                    cloned_table_status.append(thread["status"])
                    cloned_table_times.append(thread["creation_time"])

                    if thread["status"] == "SUCCESS":
                        logger.info("Loaded table %s.%s.%s.", thread["catalog"], thread["schema"], thread["table_name"])

        # create the table statuses as a df and write to a table in dr target
        status_df = pd.DataFrame({"catalog": cloned_table_catalogs,
                                  "schema": cloned_table_schemas,
                                  "table": cloned_table_names,
                                  "status": cloned_table_status,
                                  "sync_time": cloned_table_times})

        # table will get a specific timestamp-based location per run
        if write_results:
            ts2 = time.time_ns()
            (spark.createDataFrame(status_df)
             .write.mode("overwrite")
             .format("delta")
             .save(f"{landing_zone_url}/sync_status_{ts2}"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync tables via Delta Sharing to secondary Databricks workspace")
    parser.add_argument("--dry-run", action="store_true", help="Show planned operations without executing")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Set logging level")
    args = parser.parse_args()
    config.dry_run = args.dry_run
    logger = setup_logging(level=args.log_level)
