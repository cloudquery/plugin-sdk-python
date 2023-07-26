
from typing import Any, List, Generator
import pyarrow as pa
from cloudquery.sdk.scheduler import Scheduler, TableResolver
from cloudquery.sdk.schema import Table, Column, Resource
from cloudquery.sdk.message import SyncMessage
from cloudquery.sdk.schema.table import Table

class SchedulerTestTable(Table):
    def __init__(self):
        super().__init__("test_table", [
            Column("test_column", pa.int64())
        ])

class SchedulerTestTableResolver(TableResolver):
    def __init__(self) -> None:
        super().__init__(SchedulerTestTable())
    
    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
      yield {"test_column": 1}

class TestClient:
    pass

def test_scheduler():
    client = TestClient()
    s = Scheduler(10)
    resources: List[SyncMessage] = []
    for resource in s.sync(client, [SchedulerTestTableResolver()]):
        resources.append(resource)
    assert len(resources) == 2
    print(resources[1].record.to_pydict())
    