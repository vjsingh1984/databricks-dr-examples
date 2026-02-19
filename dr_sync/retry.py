"""Retry decorator with exponential backoff and jitter for Databricks SDK calls."""

import random
import time
from functools import wraps
from typing import Callable, Type, Tuple

from databricks.sdk.errors import NotFound, BadRequest, DatabricksError
from databricks.sdk.errors import (
    InternalError,
    TooManyRequests,
    TemporarilyUnavailable,
)

from dr_sync.exceptions import SyncError


def retry_with_backoff(
    max_retries: int = 3,
    initial_backoff: float = 1.0,
    max_backoff: float = 32.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        DatabricksError,
        InternalError,
        TooManyRequests,
        TemporarilyUnavailable,
    ),
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (
        NotFound,
        BadRequest,
    ),
):
    """Decorator for retrying function calls with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (excluding initial call).
        initial_backoff: Initial backoff in seconds before first retry.
        max_backoff: Maximum backoff in seconds between retries.
        exponential_base: Base for exponential backoff calculation.
        jitter: If True, add random jitter to backoff to avoid thundering herd.
        retryable_exceptions: Exception types that should trigger retry.
        non_retryable_exceptions: Exception types that should NOT be retried.

    Returns:
        Decorated function that retries on retryable exceptions.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except non_retryable_exceptions:
                    # Non-retryable exceptions should fail immediately
                    raise
                except retryable_exceptions as e:
                    last_exception = e

                    if attempt >= max_retries:
                        # Max retries exhausted
                        break

                    # Calculate backoff with exponential increase
                    backoff = min(initial_backoff * (exponential_base**attempt), max_backoff)

                    # Add jitter to avoid synchronized retries
                    if jitter:
                        backoff = backoff * (0.5 + random.random())

                    # Log retry attempt if logger is available
                    import logging

                    logger = logging.getLogger("dr_sync")
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s. Retrying in %.2fs...",
                        attempt + 1,
                        max_retries + 1,
                        func.__name__,
                        e,
                        backoff,
                    )

                    time.sleep(backoff)

            # All retries exhausted
            raise SyncError(
                resource_type="function",
                resource_name=func.__name__,
                message=f"Failed after {max_retries} retries: {last_exception}",
            ) from last_exception

        return wrapper

    return decorator
