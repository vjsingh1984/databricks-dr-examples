"""CLI entry point for dr-sync tool."""

import argparse
import sys

from dr_sync.checkpoint import CheckpointManager
from dr_sync.config import DRSyncConfig
from dr_sync.filter import parse_filter_args
from dr_sync.log import setup_logging
from dr_sync.registry import get_registry
from dr_sync.workspace import create_client


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Databricks Disaster Recovery sync tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  dr-sync run --all                         # Run all sync modules in dependency order
  dr-sync run catalogs tables views         # Run specific modules
  dr-sync run --all --include "prod.*.*"    # Only sync prod catalogs
  dr-sync run --all --resume                # Resume from last checkpoint
  dr-sync list                              # List available sync modules
  dr-sync checkpoint list                   # List all checkpoints
  dr-sync checkpoint clear tables           # Clear checkpoint for tables module
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # 'run' command
    run_parser = subparsers.add_parser("run", help="Run sync modules")
    run_parser.add_argument(
        "modules",
        nargs="*",
        help="Sync modules to run (default: --all)",
    )
    run_parser.add_argument(
        "--all",
        action="store_true",
        help="Run all registered sync modules in dependency order",
    )
    run_parser.add_argument(
        "--include",
        help="Comma-separated include patterns (e.g., 'prod.*.*,*.staging.*')",
    )
    run_parser.add_argument(
        "--exclude",
        help="Comma-separated exclude patterns",
    )
    run_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint (skip completed items)",
    )
    run_parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpointing for this run",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned operations without executing",
    )
    run_parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    # 'list' command
    subparsers.add_parser("list", help="List available sync modules")

    # 'checkpoint' command
    checkpoint_parser = subparsers.add_parser("checkpoint", help="Manage checkpoints")
    checkpoint_subparsers = checkpoint_parser.add_subparsers(dest="checkpoint_cmd")

    checkpoint_subparsers.add_parser("list", help="List all checkpoints")

    clear_parser = checkpoint_subparsers.add_parser("clear", help="Clear a checkpoint")
    clear_parser.add_argument("module", help="Sync module name to clear checkpoint for")

    return parser


def cmd_list(args):
    """List available sync modules."""
    registry = get_registry()
    modules = registry.list_all()

    if not modules:
        print("No sync modules registered.")
        return

    print("Available sync modules:")
    print()
    for module in sorted(modules, key=lambda m: m.name):
        deps = f" (depends on: {', '.join(module.dependencies)})" if module.dependencies else ""
        print(f"  {module.name:20} {module.description}{deps}")
        print(f"                       Resources: {', '.join(module.resource_types)}")
        print()


def cmd_checkpoint_list(args):
    """List all checkpoints."""
    manager = CheckpointManager()
    checkpoints = manager.list_checkpoints()

    if not checkpoints:
        print("No checkpoints found.")
        return

    print("Checkpoints:")
    print()
    for cp in checkpoints:
        print(f"  File: {cp['file']}")
        print(f"  Type: {cp['sync_type']}")
        print(f"  Source: {cp['source_host']}")
        print(f"  Target: {cp['target_host']}")
        print(f"  Started: {cp['started_at']}")
        print(f"  Last update: {cp['last_checkpoint_time']}")
        print(f"  Completed: {cp['completed_count']}, Failed: {cp['failed_count']}")
        print()


def cmd_checkpoint_clear(args):
    """Clear a checkpoint."""
    config = (
        DRSyncConfig.from_env()
        if sys.environ.get("DR_SYNC_SOURCE_HOST")
        else DRSyncConfig.from_common_module()
    )

    manager = CheckpointManager()
    manager.delete(args.module, config.source_host or "", config.target_host or "")
    print(f"Cleared checkpoint for module: {args.module}")


def cmd_run(args):
    """Run sync modules."""
    # Setup logging
    logger = setup_logging(level=args.log_level)

    # Load config
    config = (
        DRSyncConfig.from_env()
        if sys.environ.get("DR_SYNC_SOURCE_HOST")
        else DRSyncConfig.from_common_module()
    )
    config.validate()

    # Override dry_run if specified
    if args.dry_run:
        config.dry_run = True

    # Create resource filter
    resource_filter = parse_filter_args(args.include, args.exclude)

    # Get sync modules to run
    registry = get_registry()

    if args.all:
        modules_to_run = [m.name for m in registry.list_all()]
    elif args.modules:
        modules_to_run = args.modules
    else:
        print("Error: Specify --all or list modules to run", file=sys.stderr)
        return 1

    # Get execution order (topological sort)
    try:
        execution_order = registry.get_execution_order(modules_to_run)
    except ValueError as e:
        logger.error("Dependency error: %s", e)
        return 1

    # Create clients
    source_client = (
        create_client(
            host=config.source_host,
            token=config.source_token,
        )
        if config.source_host
        else None
    )

    target_client = create_client(
        host=config.target_host,
        token=config.target_token,
    )

    # Run modules in order
    checkpoint_mgr = CheckpointManager() if not args.no_checkpoint else None

    for module in execution_order:
        logger.info("Running sync module: %s", module.name)

        try:
            module.function(
                config=config,
                source_client=source_client,
                target_client=target_client,
                logger=logger,
                resource_filter=resource_filter,
                checkpoint_mgr=checkpoint_mgr,
                resume=args.resume,
            )
            logger.info("Completed sync module: %s", module.name)
        except Exception as e:
            logger.error("Failed sync module %s: %s", module.name, e)
            return 1

    return 0


def main():
    """Main CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        return cmd_list(args)
    elif args.command == "checkpoint":
        if args.checkpoint_cmd == "list":
            return cmd_checkpoint_list(args)
        elif args.checkpoint_cmd == "clear":
            return cmd_checkpoint_clear(args)
        else:
            parser.print_help()
            return 1
    elif args.command == "run":
        return cmd_run(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
