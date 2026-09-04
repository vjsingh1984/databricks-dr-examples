"""Shared command-line controls for destructive DR examples."""

import argparse

from dr_sync.log import setup_logging


def configure_runtime(config, description, argv=None):
    """Parse safety/runtime flags before a script performs workspace operations."""
    parser = argparse.ArgumentParser(description=description)
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
    args = parser.parse_args(argv)
    config.dry_run = config.dry_run or args.dry_run
    return setup_logging(level=args.log_level)
