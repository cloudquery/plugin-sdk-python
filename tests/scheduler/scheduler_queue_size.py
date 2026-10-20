import gc
import threading
from typing import Any, Generator

import pyarrow as pa

from cloudquery.sdk.scheduler import Scheduler, TableResolver
from cloudquery.sdk.scheduler.scheduler import QUEUE_PER_WORKER
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema.table import Table


class ParentResolver(TableResolver):
    def __init__(self) -> None:
        super().__init__(
            Table("parent_table", [Column("id", pa.int64())]),
            child_resolvers=[ChildResolver()],
        )

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for i in range(3):
            yield {"id": i}


class ChildResolver(TableResolver):
    def __init__(self) -> None:
        super().__init__(Table("child_table", [Column("id", pa.int64())]))

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        yield {"id": 1}


class QueueSizeTestClient:
    def id(self):
        return "test_client"


def test_default_queue_size_is_used_for_all_depths():
    concurrency = 10
    s = Scheduler(concurrency, max_depth=3)
    expected_size = concurrency * QUEUE_PER_WORKER
    for pool in s._pools:
        assert pool._work_queue.maxsize == expected_size
        expected_size = expected_size // 2 if expected_size > 1 else 1
    s.shutdown()


def test_dropping_used_scheduler_does_not_block():
    holder = {}
    for _ in range(10):
        holder["scheduler"] = Scheduler(10)
        list(holder["scheduler"].sync(QueueSizeTestClient(), [ParentResolver()]))

        def replace_scheduler():
            holder["scheduler"] = Scheduler(10)
            gc.collect()

        thread = threading.Thread(target=replace_scheduler, daemon=True)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()
