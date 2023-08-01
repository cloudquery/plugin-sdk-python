from typing import Any, List, Generator

import pyarrow as pa

from cloudquery.sdk.message import SyncMessage
from cloudquery.sdk.scheduler import Scheduler, TableResolver
from cloudquery.sdk.schema import Column
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
    messages: List[SyncMessage] = []
    for message in s.sync(client, [SchedulerTestTableResolver()]):
        messages.append(message)
    assert len(messages) == 4
    assert Table.from_arrow_schema(messages[0].table).name == "test_table"
    assert Table.from_arrow_schema(messages[1].table).name == "test_child_table"
    assert messages[2].record == expected_record1
    assert messages[3].record == expected_record2
    s.shutdown()
