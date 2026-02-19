"""Logging setup for DR sync scripts."""

import logging
import sys


def setup_logging(level="INFO", log_file=None):
    """Configure logging with console and optional file handler.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional path to write logs to a file.

    Returns:
        The root logger configured for DR sync.
    """
    logger = logging.getLogger("dr_sync")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # avoid adding duplicate handlers on re-invocation
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # console handler (stdout for notebook compatibility)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # optional file handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
