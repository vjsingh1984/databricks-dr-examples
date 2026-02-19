"""Sync instance pools between workspaces.

This script syncs instance pool definitions from the source workspace to the target workspace.
For AWS, it can remap instance types if needed (e.g., us-east-1 specific types to us-west-2 equivalents).
"""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import ResourceAlreadyExists

from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging


# AWS instance type mappings between regions (can be extended)
AWS_INSTANCE_TYPE_MAPPINGS = {
    # Example: us-east-1 to us-west-2 mappings
    # "us-east-1": "us-west-2",
}


def remap_instance_type_for_region(instance_type, source_region, target_region):
    """Remap AWS instance type for different region if needed.

    Args:
        instance_type: Original instance type (e.g., "i3.xlarge")
        source_region: Source region (e.g., "us-east-1")
        target_region: Target region (e.g., "us-west-2")

    Returns:
        Remapped instance type, or original if no mapping needed.
    """
    # If instance type uses family-based naming, try to remap
    if "." in instance_type:
        family = instance_type.split(".")[0]
        size = instance_type.split(".")[1] if len(instance_type.split(".")) > 1 else ""

        # Simple heuristic: if same family exists in target region, use it
        # For production use, you would have a comprehensive mapping table
        if source_region != target_region:
            # This is a simplified approach - production would use a full mapping table
            if instance_type.startswith("i3.") or instance_type.startswith("i3en."):
                # i3 instances are generally available across regions
                return f"{family}.{size}" if size else family

    return instance_type


def sync_instance_pools(config, source_client, target_client, logger):
    """Sync instance pools from source to target workspace.

    Args:
        config: DRSyncConfig instance.
        source_client: Source WorkspaceClient.
        target_client: Target WorkspaceClient.
        logger: Logger instance.
    """
    # List all instance pools from source
    source_pools = source_client.instance_pools.list()

    logger.info("Found %d instance pools in source workspace", len(source_pools))

    def sync_pool(pool):
        """Sync a single instance pool to target workspace."""
        logger.info("Syncing instance pool: %s", pool.instance_pool_name)

        # Dry-run check
        if config.dry_run:
            logger.info("[DRY RUN] Would create instance pool: %s", pool.instance_pool_name)
            return {"pool": pool.instance_pool_name, "status": "dry_run"}

        try:
            # Get full pool definition
            pool_details = source_client.instance_pools.get(pool.instance_pool_id)

            # Optionally remap instance types for cross-region
            # (no-op if regions match or mappings not defined)
            node_type_attributes = pool_details.node_type_attributes or {}
            current_instance_type = node_type_attributes.get("node_type_id", "")

            # Create pool in target
            try:
                target_pool = target_client.instance_pools.create(
                    instance_pool_name=pool.instance_pool_name,
                    node_type_id=current_instance_type,
                    min_size=pool.min_size,
                    max_size=pool.max_size,
                    idle_instance_autotermination_minutes=pool.idle_instance_autotermination_minutes,
                )

                logger.info(
                    "Created instance pool: %s (%s)",
                    pool.instance_pool_name,
                    target_pool.instance_pool_id,
                )
                return {
                    "pool": pool.instance_pool_name,
                    "status": "success",
                    "pool_id": target_pool.instance_pool_id,
                }

            except ResourceAlreadyExists:
                logger.warning("Instance pool already exists: %s", pool.instance_pool_name)
                return {"pool": pool.instance_pool_name, "status": "already_exists"}

        except Exception as e:
            logger.error("Failed to sync instance pool %s: %s", pool.instance_pool_name, e)
            return {"pool": pool.instance_pool_name, "status": f"error: {e}"}

    # Sync pools in parallel
    with ThreadPoolExecutor(max_workers=config.num_exec) as executor:
        results = executor.map(sync_pool, source_pools)

    # Summary
    success_count = sum(1 for r in results if r["status"] in ("success", "already_exists"))
    error_count = sum(1 for r in results if r["status"].startswith("error"))

    logger.info(
        "Instance pools sync complete: %d successful, %d failed",
        success_count,
        error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync instance pools between workspaces")
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

    # Sync instance pools
    sync_instance_pools(
        config=config,
        source_client=source_client,
        target_client=target_client,
        logger=logger,
    )
