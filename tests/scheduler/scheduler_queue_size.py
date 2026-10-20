import subprocess
import sys

import pytest

from cloudquery.sdk.scheduler import Scheduler
from cloudquery.sdk.scheduler.scheduler import QUEUE_PER_WORKER


def test_default_queue_size_is_used_for_all_depths():
    concurrency = 10
    s = Scheduler(concurrency, max_depth=3)
    expected_size = concurrency * QUEUE_PER_WORKER
    for pool in s._pools:
        assert pool.maxsize == expected_size
        expected_size = expected_size // 2 if expected_size > 1 else 1
    s.shutdown()


@pytest.mark.parametrize(
    "scenario",
    [
        "drop_used_scheduler",
        "drop_used_scheduler_with_small_queue",
        "more_clients_than_top_level_queue",
        "grandchildren_with_small_queue",
    ],
)
def test_scenario_does_not_hang(scenario):
    result = subprocess.run(
        [sys.executable, "-m", "tests.scheduler.queue_size_scenarios", scenario],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
