import gc
import sys
import threading
from typing import Any, Generator, List

import pyarrow as pa

from cloudquery.sdk.scheduler import Scheduler, TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema.table import Table


class ScenarioClient:
    def __init__(self, index: int = 0) -> None:
        self.index = index

    def id(self):
        return f"client_{self.index}"


class ScenarioResolver(TableResolver):
    def __init__(
        self,
        name: str,
        items: int,
        child_resolvers: List[TableResolver] = None,
        clients: int = 1,
    ) -> None:
        super().__init__(
            Table(name, [Column("id", pa.int64())]),
            child_resolvers=child_resolvers or [],
        )
        self.items = items
        self.clients = clients

    def multiplex(self, client):
        return [ScenarioClient(i) for i in range(self.clients)]

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for i in range(self.items):
            yield {"id": i}


def parent_with_child(clients: int = 1) -> ScenarioResolver:
    return ScenarioResolver(
        "parent", 3, [ScenarioResolver("child", 1)], clients=clients
    )


def sync_all(scheduler: Scheduler, resolver: TableResolver) -> None:
    list(scheduler.sync(ScenarioClient(), [resolver]))


def drop_used_scheduler(queue_size: int = 0) -> None:
    holder = {}
    for _ in range(10):
        holder["scheduler"] = Scheduler(10, queue_size=queue_size)
        sync_all(holder["scheduler"], parent_with_child())

        def replace_scheduler():
            holder["scheduler"] = Scheduler(10, queue_size=queue_size)
            gc.collect()

        thread = threading.Thread(target=replace_scheduler)
        thread.start()
        thread.join()


def drop_used_scheduler_with_small_queue() -> None:
    drop_used_scheduler(queue_size=1)


def more_clients_than_top_level_queue() -> None:
    sync_all(Scheduler(2), parent_with_child(clients=1000))


def grandchildren_with_small_queue() -> None:
    grandchild = ScenarioResolver("grandchild", 3)
    child = ScenarioResolver("child", 3, [grandchild])
    sync_all(Scheduler(4, queue_size=1), ScenarioResolver("parent", 10, [child]))


if __name__ == "__main__":
    globals()[sys.argv[1]]()
