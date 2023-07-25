
from typing import List, Generator, Any
import queue
import time
from cloudquery.sdk.schema import Table
from cloudquery.sdk.message import SyncMessage, SyncInsertMessage, SyncMigrateMessage
import concurrent.futures
from typing import Generator

# This is all WIP
class Task:
   def __init__(self, fetcher, parent_item):
     self._fetcher = fetcher
     self._parent_item = parent_item

class Item:
   def __init__(self, fetcher, parent_item):
     self._fetcher = fetcher
     self._parent_item = parent_item

class Fetcher:
    def __init__(self, relations: List[Any]):
        self._relations = relations
    
    def get(self, parent_item) -> Generator[SyncInsertMessage]:
        for i in range(10):
           yield SyncInsertMessage(None)
    
    def process_item(self, item: Any) -> Generator[SyncInsertMessage]:
       pass
    
    def transform_item(self, item: Any) -> Any:
       pass
        

def worker(fetcher: Fetcher, parent_item: Any):
  for arr in fetcher.get(parent_item):
     for res in arr:
        fetcher.get()
        
  # while True:
  #   item = q.get()
  #   if item is None:
  #       break
  #   do_work(item)
  #   q.task_done()

def worker_task(q, worker_id):
    for i in range(5):
        time.sleep(0.1)  # Simulate work
        task = (worker_id, i)
        print(f"Worker {worker_id} created task {task}")
        q.put(task)

def main_task(q: queue.Queue):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            try:
                task = q.get(timeout=1)  # Wait up to 1 second for a task.
            except queue.Empty:
                print("All tasks completed.")
                break  # Exit while loop if no more tasks.
            print(f"Main: Running task {task}")
            executor.submit(run_task, task)
            q.task_done()

def run_task(task):
    
    print(f"Running task {task}")


QUEUE_PER_WORKER = 100

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
        self._pools = []
        self._queues = []
        current_depth_concurrency = concurrency
        current_depth_queue_size = queue_size
        for i in range(max_depth + 1):
          self._queues.append(queue.Queue(maxsize=current_depth_queue_size))
          self._pools.append(concurrent.futures.ThreadPoolExecutor(max_workers=current_depth_concurrency))
          current_depth_concurrency = current_depth_concurrency // 2 if current_depth_concurrency > 1 else 1
          current_depth_queue_size = current_depth_queue_size // 2 if current_depth_queue_size > 1 else 1
    
    def worker(self, max_depth: int):
      while True:
        task = self._queues[max_depth].get()
        if task is None:
            break
        self._pools[max_depth].submit(*task)
    
    def table_resolver(self, table: Table, client, res: queue.Queue):
      for resource in table.resolve(client):
         pass
      #  task.resolve
    
    def sync(self, client, tables: List[Table], res: queue.Queue, deterministic_cq_id=False):
      for table in tables:
        res.put(SyncMigrateMessage(record=table.to_arrow_schemas()))
      for table in tables:
         clients = table.multiplex(client)
         for client in clients:
          self._queues[0].put((table.resolver, client, res))
      self._queues[0].put(None)