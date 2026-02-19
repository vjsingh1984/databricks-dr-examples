"""Sync cluster policies between workspaces.

This script syncs cluster policy definitions from the source workspace to the target workspace.
"""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import ResourceAlreadyExists

from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging


def sync_cluster_policies(config, source_client, target_client, logger):
    """Sync cluster policies from source to target workspace.

    Args:
        config: DRSyncConfig instance.
        source_client: Source WorkspaceClient.
        target_client: Target WorkspaceClient.
        logger: Logger instance.
    """
    # List all policies from source
    source_policies = source_client.cluster_policies.list()

    logger.info("Found %d cluster policies in source workspace", len(source_policies))

    def sync_policy(policy):
        """Sync a single cluster policy to target workspace."""
        logger.info("Syncing cluster policy: %s", policy.name)

        # Dry-run check
        if config.dry_run:
            logger.info("[DRY RUN] Would create cluster policy: %s", policy.name)
            return {"policy": policy.name, "status": "dry_run"}

        try:
            # Get full policy definition
            policy_details = source_client.cluster_policies.get(policy.policy_id)

            # Create policy in target
            try:
                target_policy = target_client.cluster_policies.create(
                    name=policy.name,
                    policy=policy_details.policy,
                )
                logger.info("Created cluster policy: %s (%s)", policy.name, target_policy.policy_id)
                return {
                    "policy": policy.name,
                    "status": "success",
                    "policy_id": target_policy.policy_id,
                }

            except ResourceAlreadyExists:
                logger.warning("Cluster policy already exists: %s", policy.name)
                return {"policy": policy.name, "status": "already_exists"}

        except Exception as e:
            logger.error("Failed to sync cluster policy %s: %s", policy.name, e)
            return {"policy": policy.name, "status": f"error: {e}"}

    # Sync policies in parallel
    with ThreadPoolExecutor(max_workers=config.num_exec) as executor:
        results = executor.map(sync_policy, source_policies)

    # Summary
    success_count = sum(1 for r in results if r["status"] in ("success", "already_exists"))
    error_count = sum(1 for r in results if r["status"].startswith("error"))

    logger.info(
        "Cluster policies sync complete: %d successful, %d failed",
        success_count,
        error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync cluster policies between workspaces")
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

    # Sync cluster policies
    sync_cluster_policies(
        config=config,
        source_client=source_client,
        target_client=target_client,
        logger=logger,
    )
