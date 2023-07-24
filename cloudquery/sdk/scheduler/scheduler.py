
from typing import List, Generator, Any
import queue
import time
from cloudquery.sdk.message import SyncMessage, SyncInsertMessage
import concurrent.futures
from typing import Generator

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

class Scheduler:
    def __init__(self):
        self._queue = queue.Queue()
        # self._pool = ThreadPool(processes=10)
    
    def sync(self, fetchers: List[Fetcher]) -> Generator[SyncMessage]:
      task_queue = queue.Queue()
      with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
          for worker_id in range(3):
              executor.submit(worker_task, task_queue, worker_id)
      main_task(task_queue)
        