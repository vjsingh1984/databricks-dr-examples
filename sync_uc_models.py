import argparse
import os
from itertools import repeat
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import ResourceAlreadyExists
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
num_exec = config.num_exec


# helper function to create models and set appropriate owner
def create_model(w, catalog_name, schema_name, model_name, location, owner, comment):
    logger.info("Creating model %s in %s.%s...", model_name, catalog_name, schema_name)

    # dry-run guard: log what would be created without executing
    if config.dry_run:
        logger.info(
            "[DRY RUN] Would create model %s in %s.%s",
            model_name,
            catalog_name,
            schema_name,
        )
        return {
            "model": f"{catalog_name}.{schema_name}.{model_name}",
            "status": "dry_run",
        }

    # try creating new model
    try:
        model = w.registered_models.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=model_name,
            comment=comment,
            storage_location=location,
        )

        _ = w.registered_models.update(
            full_name=model.full_name, comment=comment, owner=owner
        )
        return {"model": model.full_name, "status": "success"}

    # if model already exists, just update the owner (in case it has changed)
    except ResourceAlreadyExists:
        _ = w.registered_models.update(
            full_name=f"{catalog_name}.{schema_name}.{model_name}", owner=owner
        )
        return {
            "model": f"{catalog_name}.{schema_name}.{model_name}",
            "status": "already_exists",
        }

    # for any other exception, return the error
    except Exception as e:
        return {
            "model": f"{catalog_name}.{schema_name}.{model_name}",
            "status": f"ERROR: {e}",
        }


# create the WorkspaceClient pointed at the target WS
w_source = WorkspaceClient(host=source_host, token=source_pat)

# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=target_host, token=target_pat)

# pull registered models from list
registered_models = w_source.registered_models.list()

# loop through all registered models to copy, then copy them.
#
# note: we avoid listing models and doing a comparison since this would likely be slower than just looping through all
# models and dealing with the "already_exists" errors. We attempt to update owners and comments
# in case the model already exists but the owner has changed.
for cat in catalogs_to_copy:
    filtered_models = [
        model
        for model in registered_models
        if model.catalog_name == cat and model.schema_name != "information_schema"
    ]

    # get schemas, tables and locations in list form
    schema_names = [model.schema_name for model in filtered_models]
    model_names = [model.name for model in filtered_models]
    model_locs = [model.storage_location for model in filtered_models]
    model_owners = [model.owner for model in filtered_models]
    model_comments = [model.comment for model in filtered_models]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(
            create_model,
            repeat(w_target),
            repeat(cat),
            schema_names,
            model_names,
            model_locs,
            model_owners,
            model_comments,
        )

        for thread in threads:
            if thread["status"] == "success":
                logger.info("Created model %s.", thread["model"])
            elif thread["status"] == "already_exists":
                logger.warning(
                    "Skipped model %s because it already exists.", thread["model"]
                )
            else:
                logger.error(
                    "Could not create model %s; error: %s",
                    thread["model"],
                    thread["status"],
                )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync Unity Catalog registered models between workspaces"
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
