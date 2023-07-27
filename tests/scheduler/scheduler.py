from typing import Any, List, Generator
import pyarrow as pa
import pytest
from cloudquery.sdk.scheduler import Scheduler, TableResolver
from cloudquery.sdk.schema import Table, Column, Resource
from cloudquery.sdk.message import SyncMessage
from cloudquery.sdk.schema.table import Table


class SchedulerTestTable(Table):
    def __init__(self):
        super().__init__("test_table", [Column("test_column", pa.int64())])


class SchedulerTestChildTable(Table):
    def __init__(self):
        super().__init__("test_child_table", [Column("test_child_column", pa.int64())])


class SchedulerTestTableResolver(TableResolver):
    def __init__(self) -> None:
        super().__init__(
            SchedulerTestTable(), child_resolvers=[SchedulerTestChildTableResolver()]
        )

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        yield {"test_column": 1}


class SchedulerTestChildTableResolver(TableResolver):
    def __init__(self) -> None:
        super().__init__(SchedulerTestChildTable())

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        yield {"test_child_column": 2}


class TestClient:
    pass


def test_scheduler():
    client = TestClient()
    s = Scheduler(10)
    table1 = Table("test_table", [Column("test_column", pa.int64())])
    expected_record1 = pa.record_batch([[1]], schema=table1.to_arrow_schema())
    table2 = Table("test_child_table", [Column("test_child_column", pa.int64())])
    expected_record2 = pa.record_batch([[2]], schema=table2.to_arrow_schema())
    resources: List[SyncMessage] = []
    for resource in s.sync(client, [SchedulerTestTableResolver()]):
        resources.append(resource)
    assert len(resources) == 3
    assert resources[1].record == expected_record1
    assert resources[2].record == expected_record2
    s.shutdown()
