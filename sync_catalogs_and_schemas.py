# sync_catalogs_and_schemas.py
#
# This is a minimal script to sync catalogs and schemas between two workspaces.
# To run it, you must first fill out the appropriate mapping files:
#   - catalog_mapping.csv: contains the catalog name and the storage root URL for the catalog in the secondary
#     metastore. Storage root may be omitted if there is not a default storage location for this catalog.
#   - schema_mapping.csv: contains the schema name and the storage root URL for the schema in the secondary
#     metastore. Storage root may be omitted if there is not a default storage location for this schema.
#
# The script will compare the source and target metastores to first get any catalogs that exist in the source but
# not the target, and then will attempt to create those catalogs in the target. It will then do the same for the
# schemas in the source/target. Note that all comparisons here are done on the *name* of the objects; this is
# necessary since all other parameters will change when switching between metastores.
#
# Currently, we use PAT-based auth for the WorkspaceClient objects, so you must provide the host and token manually for
# each workspace. You can update this to use other auth methods if desired.

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk import WorkspaceClient
from dr_sync.csv_mapping import load_mapping
from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging

config = (
    DRSyncConfig.from_env()
    if os.environ.get("DR_SYNC_SOURCE_HOST")
    else DRSyncConfig.from_common_module()
)
logger = setup_logging()
target_host = config.target_host
target_pat = config.target_token
source_host = config.source_host
source_pat = config.source_token
catalogs_to_copy = config.catalogs_to_copy
catalog_mapping_file = config.catalog_mapping_file
schema_mapping_file = config.schema_mapping_file


# create WorkspaceClient objects
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# get source and target catalogs
source_catalogs = [x for x in w_source.catalogs.list() if x.name in catalogs_to_copy]
target_catalogs = [x for x in w_target.catalogs.list() if x.name in catalogs_to_copy]

# compare source and target catalogs
# we can only do this by name since the URL and IDs will change between workspaces
source_catalog_names = [x.name for x in source_catalogs]
target_catalog_names = [x.name for x in target_catalogs]
catalog_diff = list(set(source_catalog_names) - set(target_catalog_names))
catalogs_to_create = [x for x in source_catalogs if x.name in catalog_diff]
catalog_df = load_mapping(catalog_mapping_file)
catalog_lookup = catalog_df.set_index("source_catalog").to_dict("index")

if not catalogs_to_create:
    logger.info("All source catalogs exist in target metastore.")

for catalog in catalogs_to_create:
    # skip shared or external catalogs
    if catalog.connection_name or catalog.share_name:
        logger.warning(
            "External Catalogs and Shared Catalogs are not currently supported by this script. "
            "Skipping %s...",
            catalog.name,
        )
        continue

    # get parameters that map directly between catalogs
    catalog_name = catalog.name
    catalog_comment = catalog.comment
    catalog_options = catalog.options
    catalog_properties = catalog.properties

    logger.info("Creating catalog %s...", catalog_name)

    # get target storage root based off of catalog name
    row = catalog_lookup.get(catalog_name)
    if row is None:
        logger.error(
            "Could not create catalog %s. Please check mapping file.", catalog_name
        )
        continue
    storage_root = row["target_storage_root"]

    # create catalog in target metastore
    if config.dry_run:
        logger.info("[DRY RUN] Would create catalog %s", catalog_name)
        continue

    if storage_root:
        w_target.catalogs.create(
            name=catalog_name,
            comment=catalog_comment,
            options=catalog_options,
            properties=catalog_properties,
            storage_root=storage_root,
        )
    else:
        w_target.catalogs.create(
            name=catalog_name,
            comment=catalog_comment,
            options=catalog_options,
            properties=catalog_properties,
        )

    logger.info("Created catalog %s.", catalog_name)

schema_df = load_mapping(schema_mapping_file)
# Build a lookup keyed by (source_catalog, source_schema) for O(1) access
schema_lookup = {}
for _, srow in schema_df.iterrows():
    key = (srow["source_catalog"], srow["source_schema"])
    schema_lookup[key] = srow.to_dict()


def create_schema(catalog_name, schema_obj, storage_root):
    """Create a single schema in the target workspace. Returns a status dict."""
    schema_name = schema_obj.name
    schema_comment = schema_obj.comment
    schema_properties = schema_obj.properties

    try:
        if storage_root:
            w_target.schemas.create(
                name=schema_name,
                comment=schema_comment,
                properties=schema_properties,
                catalog_name=catalog_name,
                storage_root=storage_root,
            )
        else:
            w_target.schemas.create(
                name=schema_name,
                comment=schema_comment,
                properties=schema_properties,
                catalog_name=catalog_name,
            )
        logger.info("Created schema %s.%s.", catalog_name, schema_name)
    except Exception as e:
        logger.error("Error creating schema %s.%s: %s", catalog_name, schema_name, e)


# Collect all schema creation tasks across catalogs
schema_tasks = []
for cat in source_catalogs:
    source_schemas = [x for x in w_source.schemas.list(cat.name)]
    target_schemas = [x for x in w_target.schemas.list(cat.name)]
    source_schema_names = [x.name for x in source_schemas]
    target_schema_names = [x.name for x in target_schemas]
    schema_diff = list(set(source_schema_names) - set(target_schema_names))
    schemas_to_create = [x for x in source_schemas if x.name in schema_diff]

    for schema in schemas_to_create:
        row = schema_lookup.get((cat.name, schema.name))
        if row is None:
            logger.error(
                "Could not create schema %s.%s. Please check mapping file.",
                cat.name,
                schema.name,
            )
            continue

        storage_root = row["target_storage_root"]

        if config.dry_run:
            logger.info("[DRY RUN] Would create schema %s.%s", cat.name, schema.name)
            continue

        schema_tasks.append((cat.name, schema, storage_root))

# Execute schema creation in parallel
if schema_tasks:
    catalog_names = [t[0] for t in schema_tasks]
    schema_objs = [t[1] for t in schema_tasks]
    storage_roots = [t[2] for t in schema_tasks]
    with ThreadPoolExecutor(max_workers=config.num_exec) as executor:
        list(executor.map(create_schema, catalog_names, schema_objs, storage_roots))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync catalogs and schemas between workspaces"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned operations without executing",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )
    args = parser.parse_args()
    config.dry_run = args.dry_run
    logger = setup_logging(level=args.log_level)
