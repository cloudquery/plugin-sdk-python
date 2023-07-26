
from typing import List, Generator, Any
import queue
import time
from enum import Enum
from cloudquery.sdk.schema import Table, Resource
from cloudquery.sdk.message import SyncMessage, SyncInsertMessage, SyncMigrateTableMessage
from concurrent import futures
from typing import Generator
from .table_resolver import TableResolver
import traceback

QUEUE_PER_WORKER = 100

class ThreadPoolExecutorWithQueueSizeLimit(futures.ThreadPoolExecutor):
    def __init__(self, maxsize, *args, **kwargs):
        super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
        self._work_queue = queue.Queue(maxsize=maxsize)


class WorkerStatus:
  def __init__(self, total_table_resolvers) -> None:
    self._total_table_resolvers = total_table_resolvers
  
  @property
  def total_table_resolvers(self):
   return self._total_table_resolvers


class TableResolverStatus:
  def __init__(self) -> None:
      pass


class Scheduler:
    def __init__(self, concurrency: int, queue_size: int = 0, max_depth : int = 3):
        self._queue = queue.Queue()
        self._max_depth = max_depth
        if concurrency <= 0:
           raise ValueError("concurrency must be greater than 0")
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
      try:
        for item in resolver.resolve(client, parent_item):
          resource = self.resolve_resource(resolver, client, parent_item, item)
          res.put(SyncInsertMessage(resource.to_arrow_record()))
      except Exception as e:
        traceback.print_exc()
        print("exception")
        print(e)
      finally:
        res.put(TableResolverStatus())
    
    def _sync(self, client, resolvers: List[TableResolver], res: queue.Queue, deterministic_cq_id=False):
      total_table_resolvers = 0
      for resolver in resolvers:
        clients = resolver.multiplex(client)
        for client in clients:
          self._pools[0].submit(self.resolve_table, resolver, client, None, res)
          total_table_resolvers += 1
      res.put(WorkerStatus(total_table_resolvers))
       
    def sync(self, client, resolvers: List[TableResolver], deterministic_cq_id=False) -> Generator[SyncMessage, None, None]:
      res = queue.Queue()
      for resolver in resolvers:
        yield SyncMigrateTableMessage(schema=resolver.table.to_arrow_schema())
      thread = futures.ThreadPoolExecutor()
      thread.submit(self._sync, client, resolvers, res, deterministic_cq_id)
      total_table_resolvers = 0
      finished_table_resovlers = 0
      while True:
        message = res.get()
        if type(message) == WorkerStatus:
          total_table_resolvers += message.total_table_resolvers
          if total_table_resolvers == finished_table_resovlers:
            break
          continue
        elif type(message) == TableResolverStatus:
          finished_table_resovlers += 1
          if total_table_resolvers == finished_table_resovlers:       
            break
          continue
        yield message
      thread.shutdown()
