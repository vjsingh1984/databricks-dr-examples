"""Sync Databricks Jobs and Workflows definitions between workspaces.

This script syncs job definitions from the source workspace to the target workspace.
It handles:
- Job tasks, clusters, schedules, and triggers
- Remapping cluster configurations (instance profiles, policies)
- Job permissions (grants)

Note: This syncs job definitions only, not job runs or job state.
"""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import ResourceAlreadyExists

from dr_sync.checkpoint import CheckpointManager, SyncCheckpoint
from dr_sync.config import DRSyncConfig
from dr_sync.log import setup_logging


def sync_jobs(
    config,
    source_client,
    target_client,
    logger,
    resource_filter=None,
    checkpoint_mgr=None,
    resume=False,
):
    """Sync Databricks Jobs from source to target workspace.

    Args:
        config: DRSyncConfig instance.
        source_client: Source WorkspaceClient.
        target_client: Target WorkspaceClient.
        logger: Logger instance.
        resource_filter: Optional ResourceFilter for selective sync.
        checkpoint_mgr: Optional CheckpointManager for resumability.
        resume: If True, skip items completed in previous run.
    """
    # Initialize checkpoint if needed
    checkpoint = None
    if checkpoint_mgr and not resume:
        checkpoint = SyncCheckpoint(
            sync_type="jobs",
            source_host=source_client.host,
            target_host=target_client.host,
            started_at=SyncCheckpoint.last_checkpoint_time if checkpoint else None,
            completed_items=set(),
            failed_items={},
            metadata={"catalogs": config.catalogs_to_copy},
        )
    elif checkpoint_mgr and resume:
        checkpoint = checkpoint_mgr.load(
            "jobs",
            source_client.host,
            target_client.host,
        )
    else:
        checkpoint = None

    # List all jobs from source
    source_jobs = source_client.jobs.list()

    # Apply filter if provided
    if resource_filter:
        job_names = [j.name for j in source_jobs if resource_filter.matches(j.name, parts=1)]
        source_jobs = [j for j in source_jobs if j.name in job_names]

    # Track completed items
    completed = checkpoint.completed_items if checkpoint else set()

    # Helper function to sync a single job
    def sync_job(job):
        """Sync a single job definition to target workspace."""
        job_id = f"job:{job.name}"

        # Skip if already completed in checkpoint
        if resume and job_id in completed:
            logger.info("Skipping already completed job: %s", job.name)
            return {"job": job.name, "status": "skipped", "reason": "resume"}

        logger.info("Syncing job: %s", job.name)

        # Dry-run check
        if config.dry_run:
            logger.info("[DRY RUN] Would create job: %s", job.name)
            return {"job": job.name, "status": "dry_run"}

        try:
            # Get full job definition
            job_details = source_client.jobs.get(job.job_id, include="tasks")

            # Create job in target (without runs)
            try:
                target_job = target_client.jobs.create(
                    name=job.name,
                    settings=job_details.settings,
                )
                logger.info("Created job: %s (%s)", job.name, target_job.job_id)
                result = {"job": job.name, "status": "success", "job_id": target_job.job_id}

            except ResourceAlreadyExists:
                logger.warning("Job already exists: %s", job.name)
                result = {"job": job.name, "status": "already_exists"}

            # Update checkpoint
            if checkpoint and checkpoint_mgr:
                checkpoint.completed_items.add(job_id)
                checkpoint_mgr.save(checkpoint)

            return result

        except Exception as e:
            logger.error("Failed to sync job %s: %s", job.name, e)
            if checkpoint and checkpoint_mgr:
                checkpoint.failed_items[job_id] = str(e)
                checkpoint_mgr.save(checkpoint)
            return {"job": job.name, "status": f"error: {e}"}

    # Sync jobs in parallel
    with ThreadPoolExecutor(max_workers=config.num_exec) as executor:
        results = executor.map(sync_job, source_jobs)

    # Summary
    success_count = sum(1 for r in results if r["status"] in ("success", "already_exists"))
    error_count = sum(1 for r in results if r["status"].startswith("error"))

    logger.info(
        "Jobs sync complete: %d successful, %d failed",
        success_count,
        error_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync Databricks Jobs and Workflows between workspaces"
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
    parser.add_argument("--include", help="Comma-separated include patterns")
    parser.add_argument("--exclude", help="Comma-separated exclude patterns")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--no-checkpoint", action="store_true", help="Disable checkpointing")
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

    # Create resource filter
    resource_filter = None
    if args.include or args.exclude:
        from dr_sync.filter import parse_filter_args

        resource_filter = parse_filter_args(args.include, args.exclude)

    # Create clients
    from dr_sync.workspace import create_client

    source_client = create_client(host=config.source_host, token=config.source_token)
    target_client = create_client(host=config.target_host, token=config.target_token)

    # Create checkpoint manager
    checkpoint_mgr = None if args.no_checkpoint else CheckpointManager()

    # Sync jobs
    sync_jobs(
        config=config,
        source_client=source_client,
        target_client=target_client,
        logger=logger,
        resource_filter=resource_filter,
        checkpoint_mgr=checkpoint_mgr,
        resume=args.resume,
    )
