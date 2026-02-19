"""Sync AWS instance profiles between workspaces.

This script registers AWS instance profiles in the target workspace.
The instance profile must already exist in the target AWS account.
"""

import argparse
import os

from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging


def sync_instance_profiles(config, source_client, target_client, logger):
    """Sync AWS instance profile registrations from source to target workspace.

    Note: This only registers the instance profile in the Databricks workspace.
    The IAM role/instance profile must already exist in the target AWS account.

    Args:
        config: DRSyncConfig instance.
        source_client: Source WorkspaceClient.
        target_client: Target WorkspaceClient.
        logger: Logger instance.
    """
    # List all instance profiles from source
    source_profiles = source_client.instance_profiles.list()

    logger.info("Found %d instance profiles in source workspace", len(source_profiles))

    success_count = 0
    error_count = 0

    for profile in source_profiles:
        logger.info("Syncing instance profile: %s", profile.instance_profile_arn)

        # Dry-run check
        if config.dry_run:
            logger.info("[DRY RUN] Would add instance profile: %s", profile.instance_profile_arn)
            success_count += 1
            continue

        try:
            # Add instance profile in target workspace
            target_client.instance_profiles.add(
                instance_profile_arn=profile.instance_profile_arn,
                skip_validation=True,
            )
            logger.info("Added instance profile: %s", profile.instance_profile_arn)
            success_count += 1

        except Exception as e:
            # Already exists is okay
            if "already exists" in str(e).lower():
                logger.warning("Instance profile already exists: %s", profile.instance_profile_arn)
                success_count += 1
            else:
                logger.error(
                    "Failed to add instance profile %s: %s", profile.instance_profile_arn, e
                )
                error_count += 1

    logger.info(
        "Instance profiles sync complete: %d successful, %d failed",
        success_count,
        error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync AWS instance profiles between workspaces")
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

    # Sync instance profiles
    sync_instance_profiles(
        config=config,
        source_client=source_client,
        target_client=target_client,
        logger=logger,
    )
