import queue
import threading
from concurrent import futures
from typing import List, Generator, Any, Optional

import structlog

from cloudquery.sdk.message import (
    SyncMessage,
    SyncInsertMessage,
    SyncMigrateTableMessage,
    SyncErrorMessage,
)
from cloudquery.sdk.internal.checkpoint import Checkpoints, Scope
from cloudquery.sdk.schema import Resource
from cloudquery.sdk.stateclient.stateclient import StateClient
from .table_resolver import TableResolver, Client

QUEUE_PER_WORKER = 100


class ThreadPoolExecutorWithQueueSizeLimit(futures.ThreadPoolExecutor):
    def __init__(self, maxsize, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.maxsize = maxsize
        self._slots = threading.BoundedSemaphore(maxsize + self._max_workers)

    def submit(self, fn, /, *args, **kwargs):
        self._slots.acquire()
        try:
            future = super().submit(fn, *args, **kwargs)
        except BaseException:
            self._slots.release()
            raise
        future.add_done_callback(lambda _: self._slots.release())
        return future


class TableResolverStarted:
    def __init__(self) -> None:
        pass


class TableResolverFinished:
    def __init__(self) -> None:
        pass


class TableResolversSubmitted:
    def __init__(self) -> None:
        pass


class Scheduler:
    def __init__(
        self, concurrency: int, queue_size: int = 0, max_depth: int = 3, logger=None
    ):
        self._post_sync_hook = lambda: None
        self._queue = queue.Queue()
        # pass to StateClientBuilder.build so that state flushes are sent as checkpoints, when the CLI supports them
        self.checkpoints = Checkpoints()
        self._max_depth = max_depth
        if logger is None:
            self._logger = structlog.get_logger()
        else:
            self._logger = logger
        if concurrency <= 0:
            raise ValueError("concurrency must be greater than 0")
        if max_depth <= 0:
            raise ValueError("max_depth must be greater than 0")
        self._queue_size = (
            queue_size if queue_size > 0 else concurrency * QUEUE_PER_WORKER
        )
        self._pools: List[ThreadPoolExecutorWithQueueSizeLimit] = []
        current_depth_concurrency = concurrency
        current_depth_queue_size = self._queue_size
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
        self, resolver: TableResolver, client: Client, parent: Resource, item: Any
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
        client: Client,
        parent_item: Optional[Resource],
        res: queue.Queue,
        scope: Optional[Scope] = None,
    ):
        scope = scope or self.checkpoints.new_scope(None)
        try:
            with self.checkpoints.entered(scope):
                self._resolve_table(resolver, depth, client, parent_item, res, scope)
        finally:
            # after every resource of the resolver is on the queue, so its checkpoints follow them
            self.checkpoints.resolver_finished(scope)
            res.put(TableResolverFinished())

    def _resolve_table(
        self,
        resolver: TableResolver,
        depth: int,
        client: Client,
        parent_item: Optional[Resource],
        res: queue.Queue,
        scope: Scope,
    ):
        total_resources = 0
        try:
            if depth == 0:
                self._logger.info(
                    "table resolver started",
                    client_id=client.id(),
                    table=resolver.table.name,
                    depth=depth,
                )
            else:
                self._logger.debug(
                    "table resolver started",
                    client_id=client.id(),
                    table=resolver.table.name,
                    depth=depth,
                )
            for item in resolver.resolve(client, parent_item):
                try:
                    resource = self.resolve_resource(
                        resolver, client, parent_item, item
                    )
                except Exception as e:
                    self._logger.error(
                        "failed to resolve resource",
                        client_id=client.id(),
                        table=resolver.table.name,
                        depth=depth,
                        exc_info=e,
                    )
                    self._logger.debug(
                        "details about resource that failed to resolve",
                        client_id=client.id(),
                        table=resolver.table.name,
                        resource=item,
                    )
                    continue
                res.put(SyncInsertMessage(resource.to_arrow_record()))
                for child_resolvers in resolver.child_resolvers:
                    res.put(TableResolverStarted())
                    self._pools[depth + 1].submit(
                        self.resolve_table,
                        child_resolvers,
                        depth + 1,
                        client,
                        resource,
                        res,
                        self.checkpoints.new_scope(scope),
                    )
                total_resources += 1
            if depth == 0:
                self._logger.info(
                    "table resolver finished successfully",
                    client_id=client.id(),
                    table=resolver.table.name,
                    resources=total_resources,
                    depth=depth,
                )
            else:
                self._logger.debug(
                    "table resolver finished successfully",
                    client_id=client.id(),
                    table=resolver.table.name,
                    resources=total_resources,
                    depth=depth,
                )
        except Exception as e:
            self._logger.error(
                "table resolver finished with error",
                client_id=client.id(),
                table=resolver.table.name,
                resources=total_resources,
                depth=depth,
                exc_info=e,
            )
            res.put(SyncErrorMessage(resolver.table.name, str(e)))

    def _sync(
        self,
        client,
        resolvers: List[TableResolver],
        res: queue.Queue,
        deterministic_cq_id=False,
    ):
        try:
            for resolver in resolvers:
                clients = resolver.multiplex(client)
                for client in clients:
                    res.put(TableResolverStarted())
                    self._pools[0].submit(
                        self.resolve_table, resolver, 0, client, None, res
                    )
        finally:
            res.put(TableResolversSubmitted())

    def sync(
        self, client, resolvers: List[TableResolver], deterministic_cq_id=False
    ) -> Generator[SyncMessage, None, None]:
        res = queue.Queue()
        yield from self._send_migrate_table_messages(resolvers)

        self.checkpoints.start(res)
        try:
            thread = futures.ThreadPoolExecutor()
            thread.submit(self._sync, client, resolvers, res, deterministic_cq_id)
            submitted = False
            total_table_resolvers = 0
            finished_table_resolvers = 0
            while not (submitted and total_table_resolvers == finished_table_resolvers):
                message = res.get()
                if isinstance(message, TableResolversSubmitted):
                    submitted = True
                elif isinstance(message, TableResolverStarted):
                    total_table_resolvers += 1
                elif isinstance(message, TableResolverFinished):
                    finished_table_resolvers += 1
                else:
                    yield message

            self._post_sync_hook()
            # checkpoints the post-sync hook flushed, after every resource
            while not res.empty():
                yield res.get_nowait()
            thread.shutdown(wait=True)
        finally:
            self.checkpoints.stop()

    def _send_migrate_table_messages(
        self, resolvers: List[TableResolver]
    ) -> Generator[SyncMessage, None, None]:
        for resolver in resolvers:
            yield SyncMigrateTableMessage(table=resolver.table.to_arrow_schema())
            if resolver.child_resolvers:
                yield from self._send_migrate_table_messages(resolver.child_resolvers)

    def set_post_sync_hook(self, fn):
        """
        Use this to set a function that will be called after the sync is finished,
        a la `defer fn()` in Go (but for a single function, rather than a stack).

        This is necessary because plugins use this pattern on their sync method:

        ```
        return self._scheduler.sync(...)
        ```

        So if a plugin has a `state_client`, there's nowhere to call the flush method.
        """
        self._post_sync_hook = fn
