# sync_perms.py
#
# Baseline script to sync permissions of tables, volumes, schemas and catalogs between two workspaces.
#
# NOTE: This script must be run in the PRIMARY workspace.
#
# This script will attempt to sync the permissions of all catalog, schema and table securables between a source and
# target workspace. All securables should already exist in the target workspace, i.e., this script assumes a metadata
# sync has already been performed. It will also only update permissions on objects that exist in the primary; i.e., if
# an object exists in the secondary but not the primary, no updates will be applied.
#
# Params that must be specified below:
#   -source_host: the hostname of the primary workspace.
#   -source_pat: an access token for the primary workspace; must be an ADMIN user.
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.

import logging
import os
from itertools import repeat
from databricks.sdk.service import catalog
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import NotFound
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


# helper function to update object grants between source and target WS
def sync_grants(w_src, w_tgt, obj_name, obj_type):
    # get source and target grants
    source_grants = w_src.grants.get_effective(obj_type, obj_name)

    # if the object does not exist in the secondary workspace, we cannot fetch it
    try:
        target_grants = w_tgt.grants.get_effective(obj_type, obj_name)
    except NotFound:
        return {"name": obj_name, "status": "NotFound"}

    # get list of all distinct users with grants on the object
    user_list = {u.principal for u in source_grants.privilege_assignments}.union(
        {u.principal for u in target_grants.privilege_assignments})

    # create PermissionsChange object for each user where a change exists
    change_list = []
    for u in user_list:
        # get the source/target privileges; these may not exist in one or the other environment
        try:
            source_privs = [x.privilege for x in
                            [p.privileges for p in source_grants.privilege_assignments if p.principal == u][0]
                            if x.privilege is not None]
        except IndexError:
            source_privs = []

        try:
            target_privs = [x.privilege for x in
                            [p.privileges for p in target_grants.privilege_assignments if p.principal == u][0]
                            if x.privilege is not None]
        except IndexError:
            target_privs = []

        add_perms = list(set(source_privs) - set(target_privs))
        rem_perms = list(set(target_privs) - set(source_privs))

        # for the change list based on which types of changes exist
        if add_perms and rem_perms:
            change_list.append(catalog.PermissionsChange(
                add=add_perms,
                remove=rem_perms,
                principal=u))
        elif add_perms:
            change_list.append(catalog.PermissionsChange(
                add=add_perms,
                principal=u))
        elif rem_perms:
            change_list.append(catalog.PermissionsChange(
                remove=rem_perms,
                principal=u))

    # if any grants changed, update the object in target
    if change_list:
        w_tgt.grants.update(full_name=obj_name,
                            securable_type=obj_type,
                            changes=change_list)
        return {"name": obj_name, "status": "SUCCESS"}
    else:
        return {"name": obj_name, "status": None}


# create the WorkspaceClients for source and target workspaces
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# get all tables in the source ws
table_info = spark.sql("SELECT * FROM system.information_schema.tables")
volume_info = spark.sql("SELECT * FROM system.information_schema.volumes")

# iterate through catalogs
for cat in catalogs_to_copy:
    filtered_tables = table_info.filter(
        (table_info.table_catalog == cat) &
        (table_info.table_schema != "information_schema")).collect()

    filtered_volumes = volume_info.filter(volume_info.volume_catalog == cat).collect()

    # sync the catalog grants first
    res = sync_grants(w_source, w_target, cat, catalog.SecurableType.CATALOG)

    if res["status"] == "SUCCESS":
        logger.info("Synced grants for catalog %s.", cat)
    elif res["status"] == "NotFound":
        logger.error("Catalog %s does not exist in target workspace. Sync metadata and re-run.", cat)
    else:
        logger.info("No changes to sync for catalog %s.", cat)

    # get list of fully qualified schemas and tables
    schemas = {f"{cat}.{schema}" for schema in [row['table_schema'] for row in filtered_tables]}
    table_names = [f"{cat}.{schema}.{table}" for schema, table in
                   zip([row['table_schema'] for row in filtered_tables],
                       [row['table_name'] for row in filtered_tables])]
    volume_names = [f"{cat}.{schema}.{table}" for schema, table in
                    zip([row['volume_schema'] for row in filtered_volumes],
                        [row['volume_name'] for row in filtered_volumes])]

    # update schema grants in parallel
    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(sync_grants,
                               repeat(w_source),
                               repeat(w_target),
                               schemas,
                               repeat(catalog.SecurableType.SCHEMA))

        for thread in threads:
            name = thread["name"]
            if thread["status"] == "SUCCESS":
                logger.info("Synced grants for schema %s.", name)
            elif thread["status"] == "NotFound":
                logger.error("Schema %s does not exist in target workspace. Sync metadata and re-run.", name)
            else:
                logger.info("No changes to sync for schema %s.", name)

    # update table grants in parallel
    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(sync_grants,
                               repeat(w_source),
                               repeat(w_target),
                               table_names,
                               repeat(catalog.SecurableType.TABLE))

        for thread in threads:
            name = thread["name"]
            if thread["status"] == "SUCCESS":
                logger.info("Synced grants for table %s.", name)
            elif thread["status"] == "NotFound":
                logger.error("Table %s does not exist in target workspace. Sync metadata and re-run.", name)
            else:
                logger.info("No changes to sync for table %s.", name)

    # update volume grants in parallel
    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(sync_grants,
                               repeat(w_source),
                               repeat(w_target),
                               volume_names,
                               repeat(catalog.SecurableType.VOLUME))

        for thread in threads:
            name = thread["name"]
            if thread["status"] == "SUCCESS":
                logger.info("Synced grants for volume %s.", name)
            elif thread["status"] == "NotFound":
                logger.error("Volume %s does not exist in target workspace. Sync volumes and re-run.", name)
            else:
                logger.info("No changes to sync for volume %s.", name)
