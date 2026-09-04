# sync_ext_volumes.py
#
# Baseline script to sync GRS-replicated volumes from a primary metastore to a secondary metastore
#
# NOTE: This script must be run in the PRIMARY workspace.
#
# This script will attempt to register all _external_ volumes in the primary metastore into the secondary metastore.
# This assumes that all storage locations are identical between the two regions, i.e., georeplicated storage has been
# used. Storage URLs are not updated; they are just directly brought over to the secondary metastore. Files within the
# volumes will not be replicated, since the underlying storage will be georeplicated.
#
# Configuration is loaded from DR_SYNC_* environment variables or common.py. Use a
# TARGET unified-auth profile or workload identity; PATs are legacy-only. The identity
# needs only the catalog privileges used below, not blanket workspace administration.
# Set target workspace/profile, catalogs_to_copy, and num_exec.


from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

from databricks.sdk.errors.platform import ResourceAlreadyExists
from databricks.sdk.service import catalog

from dr_sync.cli import configure_runtime
from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging
from dr_sync.workspace import create_client

config = DRSyncConfig.load()
logger = (
    configure_runtime(config, "Sync external volumes between workspaces")
    if __name__ == "__main__"
    else setup_logging()
)
target_host = config.target_host
target_pat = config.target_token
source_host = config.source_host
source_pat = config.source_token
catalogs_to_copy = config.catalogs_to_copy
num_exec = config.num_exec


# helper function to create volumes and set appropriate owner
def create_volume(w, catalog_name, schema_name, volume_name, location, owner):
    logger.info("Creating volume %s in %s.%s...", volume_name, catalog_name, schema_name)

    # dry-run guard: log what would be created without executing
    if config.dry_run:
        logger.info(
            "[DRY RUN] Would create volume %s in %s.%s",
            volume_name,
            catalog_name,
            schema_name,
        )
        return {
            "volume": f"{catalog_name}.{schema_name}.{volume_name}",
            "status": "dry_run",
        }

    # try creating new volume
    try:
        volume = w.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            storage_location=location,
            volume_type=catalog.VolumeType.EXTERNAL,
        )

        _ = w.volumes.update(name=volume.full_name, owner=owner)
        return {"volume": volume.full_name, "status": "success"}

    # if volume already exists, just update the owner (in case it has changed)
    except ResourceAlreadyExists:
        _ = w.volumes.update(name=f"{catalog_name}.{schema_name}.{volume_name}", owner=owner)
        return {
            "volume": f"{catalog_name}.{schema_name}.{volume_name}",
            "status": "already_exists",
        }

    # for any other exception, return the error
    except Exception as e:
        return {
            "volume": f"{catalog_name}.{schema_name}.{volume_name}",
            "status": f"ERROR: {e}",
        }


# create the WorkspaceClient pointed at the target WS
w_target = create_client(host=target_host, token=target_pat, profile=config.target_profile)

# pull system tables from source ws
system_info = spark.sql("SELECT * FROM system.information_schema.volumes")

# loop through all catalogs to copy, then copy all volumes in these catalogs.
#
# note: we avoid listing volumes and doing a comparison since this would likely be slower than just looping through all
# volumes and dealing with the "already_exists" errors. We attempt to update owners in case the volume already exists
# but the owner has changed.
for cat in catalogs_to_copy:
    filtered_volumes = system_info.filter(
        (system_info.volume_catalog == cat)
        & (system_info.volume_schema != "information_schema")
        & (system_info.volume_type == "EXTERNAL")
    ).collect()

    # get schemas, tables and locations in list form
    schema_names = [row["volume_schema"] for row in filtered_volumes]
    volume_names = [row["volume_name"] for row in filtered_volumes]
    volume_locs = [row["storage_location"] for row in filtered_volumes]
    volume_owners = [row["volume_owner"] for row in filtered_volumes]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(
            create_volume,
            repeat(w_target),
            repeat(cat),
            schema_names,
            volume_names,
            volume_locs,
            volume_owners,
        )

        for thread in threads:
            if thread["status"] == "success":
                logger.info("Created volume %s.", thread["volume"])
            elif thread["status"] == "already_exists":
                logger.warning("Skipped volume %s because it already exists.", thread["volume"])
            else:
                logger.error(
                    "Could not create volume %s; error: %s",
                    thread["volume"],
                    thread["status"],
                )
