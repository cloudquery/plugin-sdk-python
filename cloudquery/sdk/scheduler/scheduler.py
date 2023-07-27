from typing import List, Generator, Any
import queue
import time
import structlog
from enum import Enum
from cloudquery.sdk.schema import Table, Resource
from cloudquery.sdk.message import (
    SyncMessage,
    SyncInsertMessage,
    SyncMigrateTableMessage,
)
from concurrent import futures
from typing import Generator
from .table_resolver import TableResolver
import traceback

QUEUE_PER_WORKER = 100


class ThreadPoolExecutorWithQueueSizeLimit(futures.ThreadPoolExecutor):
    def __init__(self, maxsize, *args, **kwargs):
        super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
        self._work_queue = queue.Queue(maxsize=maxsize)


class TableResolverStarted:
    def __init__(self, count=1) -> None:
        self._count = count

    @property
    def count(self):
        return self._count


class TableResolverFinished:
    def __init__(self) -> None:
        pass


class Scheduler:
    def __init__(
        self, concurrency: int, queue_size: int = 0, max_depth: int = 3, logger=None
    ):
        self._queue = queue.Queue()
        self._max_depth = max_depth
        if logger is None:
            self._logger = structlog.get_logger()
        if concurrency <= 0:
            raise ValueError("concurrency must be greater than 0")
        if max_depth <= 0:
            raise ValueError("max_depth must be greater than 0")
        self._queue_size = (
            queue_size if queue_size > 0 else concurrency * QUEUE_PER_WORKER
        )
        self._pools: List[ThreadPoolExecutorWithQueueSizeLimit] = []
        current_depth_concurrency = concurrency
        current_depth_queue_size = queue_size
        for _ in range(max_depth + 1):
            self._pools.append(
                ThreadPoolExecutorWithQueueSizeLimit(
                    maxsize=current_depth_queue_size,
                    max_workers=current_depth_concurrency,
                )
            )
            current_depth_concurrency = (
                current_depth_concurrency // 2 if current_depth_concurrency > 1 else 1
            )
            current_depth_queue_size = (
                current_depth_queue_size // 2 if current_depth_queue_size > 1 else 1
            )

    def shutdown(self):
        for pool in self._pools:
            pool.shutdown()

    def resolve_resource(
        self, resolver: TableResolver, client, parent: Resource, item: Any
    ) -> Resource:
        resource = Resource(resolver.table, parent, item)
        resolver.pre_resource_resolve(client, resource)
        for column in resolver.table.columns:
            resolver.resolve_column(client, resource, column.name)
        resolver.post_resource_resolve(client, resource)
        return resource

    def resolve_table(
        self,
        resolver: TableResolver,
        depth: int,
        client,
        parent_item: Resource,
        res: queue.Queue,
    ):
        table_resolvers_started = 0
        try:
            if depth == 0:
                self._logger.info(
                    "table resolver started", table=resolver.table.name, depth=depth
                )
            else:
                self._logger.debug(
                    "table resolver started", table=resolver.table.name, depth=depth
                )
            total_resources = 0
            for item in resolver.resolve(client, parent_item):
                resource = self.resolve_resource(resolver, client, parent_item, item)
                res.put(SyncInsertMessage(resource.to_arrow_record()))
                for child_resolvers in resolver.child_resolvers:
                    self._pools[depth + 1].submit(
                        self.resolve_table,
                        child_resolvers,
                        depth + 1,
                        client,
                        resource,
                        res,
                    )
                    table_resolvers_started += 1
                total_resources += 1
            if depth == 0:
                self._logger.info(
                    "table resolver finished successfully",
                    table=resolver.table.name,
                    depth=depth,
                )
            else:
                self._logger.debug(
                    "table resolver finished successfully",
                    table=resolver.table.name,
                    depth=depth,
                )
        except Exception as e:
            self._logger.error(
                "table resolver finished with error",
                table=resolver.table.name,
                depth=depth,
                exception=e,
            )
        finally:
            res.put(TableResolverStarted(count=table_resolvers_started))
            res.put(TableResolverFinished())

    def _sync(
        self,
        client,
        resolvers: List[TableResolver],
        res: queue.Queue,
        deterministic_cq_id=False,
    ):
        total_table_resolvers = 0
        try:
            for resolver in resolvers:
                clients = resolver.multiplex(client)
                for client in clients:
                    self._pools[0].submit(
                        self.resolve_table, resolver, 0, client, None, res
                    )
                    total_table_resolvers += 1
        finally:
            res.put(TableResolverStarted(total_table_resolvers))

    def sync(
        self, client, resolvers: List[TableResolver], deterministic_cq_id=False
    ) -> Generator[SyncMessage, None, None]:
        res = queue.Queue()
        for resolver in resolvers:
            yield SyncMigrateTableMessage(schema=resolver.table.to_arrow_schema())
        thread = futures.ThreadPoolExecutor()
        thread.submit(self._sync, client, resolvers, res, deterministic_cq_id)
        total_table_resolvers = 0
        finished_table_resolvers = 0
        while True:
            message = res.get()
            if type(message) == TableResolverStarted:
                total_table_resolvers += message.count
                if total_table_resolvers == finished_table_resolvers:
                    break
                continue
            elif type(message) == TableResolverFinished:
                finished_table_resolvers += 1
                if total_table_resolvers == finished_table_resolvers:
                    break
                continue
            yield message
        thread.shutdown()
