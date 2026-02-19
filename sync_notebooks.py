"""Sync notebooks and workspace folders between workspaces.

This script exports notebooks from the source workspace and imports them to the target workspace,
preserving the folder structure.

Supported formats:
- SOURCE (Databricks source format with .scala, .python, .sql, .r extensions)
- JUPYTER (Jupyter notebooks with .ipynb extension)
- DBC (Databricks notebook archive)

Note: This does NOT sync:
- Git repositories (use Repos API for that)
- Workspace files other than notebooks (need to add separately)
"""

import argparse
import os
from databricks.sdk.service import workspace

from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging


def get_notebook_path(client, object_info):
    """Get the full path of a notebook object.

    Args:
        client: WorkspaceClient instance.
        object_info: Workspace object info.

    Returns:
        Full path as string.
    """
    status = client.workspace.get_status(object_info.path)
    return status.object_id


def sync_notebooks(config, source_client, target_client, logger):
    """Sync notebooks and folder structure from source to target workspace.

    Args:
        config: DRSyncConfig instance.
        source_client: Source WorkspaceClient.
        target_client: Target WorkspaceClient.
        logger: Logger instance.
    """
    # List all workspace objects (recursive)
    # Note: This is a simplified approach - production would use listing with recursion
    logger.info("Listing notebooks in source workspace...")

    # For each catalog in the list, we'll export/import notebooks
    # This is a simplified implementation that focuses on notebook files
    # In production, you would recursively list and filter by object_type

    # Example: List items in root directory
    root_items = source_client.workspace.list("/")

    notebook_count = 0
    folder_count = 0
    error_count = 0

    for item in root_items:
        # Only process notebooks and folders
        if item.object_type == workspace.ObjectType.NOTEBOOK:
            notebook_path = item.path

            # Skip notebooks in system paths
            if any(x in notebook_path for x in ["/Workspace/Shared/", "/Users/"]):
                logger.debug("Skipping shared/user notebook: %s", notebook_path)
                continue

            logger.info("Syncing notebook: %s", notebook_path)

            # Dry-run check
            if config.dry_run:
                logger.info("[DRY RUN] Would export/import notebook: %s", notebook_path)
                notebook_count += 1
                continue

            try:
                # Export notebook from source
                exported = source_client.workspace.export(
                    notebook_path, format=workspace.ExportFormat.SOURCE
                )

                # Create directory structure in target
                target_dir = "/".join(notebook_path.split("/")[:-1])
                if target_dir and target_dir != "/":
                    try:
                        target_client.workspace.mkdirs(target_dir)
                        logger.debug("Created directory: %s", target_dir)
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            logger.warning("Failed to create directory %s: %s", target_dir, e)

                # Import notebook to target
                target_client.workspace.import_(
                    path=notebook_path,
                    format=workspace.ImportFormat.SOURCE,
                    content=exported.content,
                    language=exported.language,
                )

                logger.info("Imported notebook: %s", notebook_path)
                notebook_count += 1

            except Exception as e:
                logger.error("Failed to sync notebook %s: %s", notebook_path, e)
                error_count += 1

        elif item.object_type == workspace.ObjectType.DIRECTORY:
            # Create folder in target
            dir_path = item.path

            # Skip system directories
            if any(x in dir_path for x in ["/Workspace/Shared/", "/Users/"]):
                logger.debug("Skipping shared/user directory: %s", dir_path)
                continue

            if dir_path == "/":
                continue

            logger.info("Syncing directory: %s", dir_path)

            if config.dry_run:
                logger.info("[DRY RUN] Would create directory: %s", dir_path)
                folder_count += 1
                continue

            try:
                target_client.workspace.mkdirs(dir_path)
                logger.info("Created directory: %s", dir_path)
                folder_count += 1

            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.debug("Directory already exists: %s", dir_path)
                    folder_count += 1
                else:
                    logger.warning("Failed to create directory %s: %s", dir_path, e)
                    error_count += 1

    logger.info(
        "Notebooks sync complete: %d notebooks, %d folders, %d errors",
        notebook_count,
        folder_count,
        error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync notebooks and workspace folders between workspaces"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show planned operations without executing"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )
    args = parser.parse_args()

    # Load config
    config = (
        DRSyncConfig.from_env()
        if os.environ.get("DR_SYNC_SOURCE_HOST")
        else DRSyncConfig.from_common_module()
    )
    config.validate()
    config.dry_run = args.dry_run

    # Setup logging
    logger = setup_logging(level=args.log_level)

    # Create clients
    from dr_sync.workspace import create_client

    source_client = create_client(host=config.source_host, token=config.source_token)
    target_client = create_client(host=config.target_host, token=config.target_token)

    # Sync notebooks
    sync_notebooks(
        config=config,
        source_client=source_client,
        target_client=target_client,
        logger=logger,
    )
