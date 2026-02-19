"""Sync secret scope metadata and ACLs between workspaces.

This script syncs secret scope metadata and ACLs from the source workspace to the target workspace.

IMPORTANT: This does NOT sync secret values themselves.
- Databricks-backed scopes: Secrets must be re-created manually in target
- AWS Secrets Manager-backed scopes: The scope is registered but secrets come from AWS

For AWS Secrets Manager scopes, the scope references the same AWS secret in both workspaces,
so no secret value sync is needed. The secret must exist in the target AWS account.
"""

import argparse
import os

from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging


def sync_secret_scopes(config, source_client, target_client, logger):
    """Sync secret scope metadata and ACLs from source to target workspace.

    Args:
        config: DRSyncConfig instance.
        source_client: Source WorkspaceClient.
        target_client: Target WorkspaceClient.
        logger: Logger instance.
    """
    # List all scopes from source
    source_scopes = source_client.scopes.list()

    logger.info("Found %d secret scopes in source workspace", len(source_scopes))

    success_count = 0
    error_count = 0

    for scope in source_scopes:
        scope_name = scope.name

        # Skip system scopes
        if scope_name in ["global_init", "databricks", "users"]:
            logger.info("Skipping system scope: %s", scope_name)
            continue

        logger.info("Syncing secret scope: %s", scope_name)

        # Dry-run check
        if config.dry_run:
            logger.info(
                "[DRY RUN] Would create/update secret scope: %s (backend: %s)",
                scope_name,
                scope.backend_type,
            )
            success_count += 1
            continue

        try:
            # Get ACLs for this scope
            acls = source_client.scopes.list_acls(scope_name)

            # Create or update scope in target
            try:
                # For AWS Secrets Manager backed scopes, the scope must reference the same AWS resource
                target_client.scopes.create(
                    scope=scope_name,
                    initial_manage_principal=acls[0].principal if acls else "users",
                    scope_backend_type=scope.backend_type,
                )
                logger.info(
                    "Created secret scope: %s (backend: %s)", scope_name, scope.backend_type
                )

            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info("Secret scope already exists: %s", scope_name)
                else:
                    raise

            # Sync ACLs
            for acl in acls:
                try:
                    target_client.scopes.patch_acls(
                        scope_name,
                        permission_changes=[
                            {
                                "principal_id": acl.principal,
                                "permissions": acl.permissions,
                            }
                        ],
                    )
                    logger.debug("Updated ACL for %s on scope %s", acl.principal, scope_name)
                except Exception as acl_error:
                    logger.warning(
                        "Failed to update ACL for %s on scope %s: %s",
                        acl.principal,
                        scope_name,
                        acl_error,
                    )

            success_count += 1

        except Exception as e:
            logger.error("Failed to sync secret scope %s: %s", scope_name, e)
            error_count += 1

    logger.info(
        "Secret scopes sync complete: %d successful, %d failed",
        success_count,
        error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync secret scope metadata and ACLs between workspaces"
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

    # Sync secret scopes
    sync_secret_scopes(
        config=config,
        source_client=source_client,
        target_client=target_client,
        logger=logger,
    )
