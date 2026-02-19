"""Thread pool utilities for parallel execution with error isolation."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


def parallel_map(func, items, max_workers=4):
    """Execute func on each item in parallel with per-item error isolation.

    Unlike executor.map(), uses as_completed() so one failure doesn't block others.

    Args:
        func: Callable that takes a single item and returns a result.
        items: Iterable of items to process.
        max_workers: Maximum concurrent workers.

    Returns:
        List of results (or exception objects for failed items).
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(func, item): item for item in items}
        for future in as_completed(future_to_item):
            try:
                results.append(future.result())
            except Exception as e:
                results.append(e)
    return results


class ProgressCounter:
    """Thread-safe counter for progress reporting."""

    def __init__(self, total, label="items"):
        self._count = 0
        self._total = total
        self._label = label
        self._lock = threading.Lock()

    def increment(self, item_name=""):
        """Increment counter and print progress."""
        with self._lock:
            self._count += 1
            msg = f"[{self._count}/{self._total}]"
            if item_name:
                msg += f" Processed {self._label}: {item_name}"
            print(msg)
            return self._count

    @property
    def count(self):
        with self._lock:
            return self._count
