
from typing import List, Generator, Any
import queue
import time
from cloudquery.sdk.schema import Table, Resource
from cloudquery.sdk.message import SyncMessage, SyncInsertMessage, SyncMigrateMessage
from concurrent import futures
from typing import Generator
from .table_resolver import TableResolver

QUEUE_PER_WORKER = 100

class ThreadPoolExecutorWithQueueSizeLimit(futures.ThreadPoolExecutor):
    def __init__(self, maxsize, *args, **kwargs):
        super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
        self._work_queue = queue.Queue(maxsize=maxsize)

class Scheduler:
    def __init__(self, concurrency: int, queue_size: int = 0, max_depth : int = 3):
        self._queue = queue.Queue()
        self._max_depth = max_depth
        if concurrency <= 0:
           raise ValueError("concurrency must be greater than 0")
        if queue_size <= 0:
            raise ValueError("queue_size must be greater than 0")
        if max_depth <= 0:
            raise ValueError("max_depth must be greater than 0")
        self._queue_size = queue_size if queue_size > 0 else concurrency * QUEUE_PER_WORKER
        self._pools : List[ThreadPoolExecutorWithQueueSizeLimit] = []
        current_depth_concurrency = concurrency
        current_depth_queue_size = queue_size
        for _ in range(max_depth + 1):
          self._pools.append(ThreadPoolExecutorWithQueueSizeLimit(maxsize=current_depth_queue_size,max_workers=current_depth_concurrency))
          current_depth_concurrency = current_depth_concurrency // 2 if current_depth_concurrency > 1 else 1
          current_depth_queue_size = current_depth_queue_size // 2 if current_depth_queue_size > 1 else 1
    
    def resolve_resource(self, resolver: TableResolver, client, parent: Resource, item: Any) -> Resource:
      resource = Resource(resolver.table, None, item)
      resolver.pre_resource_resolve(client, resource)
      for column in resolver.table.columns:
        resolver.resolve_column(client, resource, column.name)
      resolver.post_resource_resolve(client, resource)
      return resource

    def resolve_table(self, resolver: TableResolver, client, parent_item: Any, res: queue.Queue):
      for item in resolver.resolve(client, parent_item):
        resource = self.resolve_resource(resolver, client, parent_item)
        res.put(SyncInsertMessage(resource))
      res.put(None)
    
    def _sync(self, client, resolvers: List[TableResolver], res: queue.Queue, deterministic_cq_id=False):
      internal_res = queue.Queue()
      for resolver in resolvers:
        clients = resolver.multiplex(client)
        for client in clients:
          self._pools[0].submit(self.resolve_table, resolver, client, None, internal_res)
      while True:
        message = internal_res.get()
        if message is None:
          break
        res.put(message)
      res.put(None)
       
    def sync(self, client, resolvers: List[TableResolver], deterministic_cq_id=False) -> Generator[SyncMessage]:
      res = queue.Queue()
      for resolver in resolvers:
        yield SyncMigrateMessage(record=resolver.table.to_arrow_schemas())
      thread = futures.ThreadPoolExecutor()
      thread.submit(self._sync, client, resolvers, res, deterministic_cq_id)
      while True:
        message = res.get()
        if message is None:
          break
        yield message
      thread.shutdown()
