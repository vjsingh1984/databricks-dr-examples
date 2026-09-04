import logging

from dr_sync.exceptions import MappingError, StatementError, SyncError
from dr_sync.log import setup_logging
from dr_sync.thread_utils import ProgressCounter, parallel_map


def test_parallel_map_isolates_failures():
    def work(value):
        if value == 2:
            raise ValueError("bad")
        return value * 2

    results = parallel_map(work, [1, 2, 3], max_workers=2)
    assert sorted(value for value in results if isinstance(value, int)) == [2, 6]
    assert any(isinstance(value, ValueError) for value in results)


def test_progress_counter_is_thread_safe_and_reports(capsys):
    counter = ProgressCounter(2, "catalog")
    assert counter.increment("one") == 1
    assert counter.increment() == 2
    assert counter.count == 2
    assert "Processed catalog: one" in capsys.readouterr().out


def test_logging_setup_is_idempotent(tmp_path):
    logger = logging.getLogger("dr_sync")
    logger.handlers.clear()
    first = setup_logging("DEBUG", tmp_path / "run.log")
    second = setup_logging("ERROR")
    assert first is second
    assert len(first.handlers) == 2


def test_exception_messages_are_bounded_and_useful():
    assert "No mapping found" in str(MappingError("map.csv", "name", "missing"))
    assert "SQL:" in str(StatementError("x" * 300, "failed"))
    assert "table 'one'" in str(SyncError("table", "one", "failed"))
