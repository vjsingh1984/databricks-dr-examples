"""Tests for dr_sync.thread_utils module."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock


from dr_sync.thread_utils import parallel_map, ProgressCounter


class TestParallelMap:
    """Tests for parallel_map function."""

    def test_parallel_map_all_success(self):
        """Test parallel execution with all successes."""

        def square(x):
            return x * x

        results = parallel_map(square, [1, 2, 3, 4, 5], max_workers=2)

        assert sorted(results) == [1, 4, 9, 16, 25]

    def test_parallel_map_with_exceptions(self):
        """Test parallel execution with some exceptions."""

        def func(x):
            if x == 2:
                raise ValueError("Error on 2")
            return x * 2

        results = parallel_map(func, [1, 2, 3], max_workers=2)

        # Should contain both results and exceptions
        assert 2 in results  # 1 * 2
        assert 6 in results  # 3 * 2
        # Exception is also returned
        assert any(isinstance(r, ValueError) for r in results)


class TestProgressCounter:
    """Tests for ProgressCounter class."""

    def test_progress_counter_increment(self):
        """Test incrementing the counter."""
        counter = ProgressCounter(total=5, label="items")

        assert counter.count == 0

        counter.increment("item1")
        assert counter.count == 1

        counter.increment("item2")
        assert counter.count == 2

    def test_progress_counter_thread_safety(self):
        """Test thread-safe increment."""
        counter = ProgressCounter(total=100, label="items")

        def increment_many():
            for _ in range(10):
                counter.increment()

        threads = [MagicMock(side_effect=increment_many) for _ in range(10)]

        # Simulate parallel increments
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(lambda t: t(), threads))

        assert counter.count == 100
