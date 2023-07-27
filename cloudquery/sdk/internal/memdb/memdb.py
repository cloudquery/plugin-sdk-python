from cloudquery.sdk import plugin
from cloudquery.sdk import message
from cloudquery.sdk import schema
from typing import List, Generator, Any, Dict
import pyarrow as pa

NAME = "memdb"
VERSION = "development"


class MemDB(plugin.Plugin):
    def __init__(self) -> None:
        super().__init__(NAME, VERSION)
        self._tables: List[schema.Table] = [
            schema.Table("test_table", [schema.Column("test_column", pa.int64())])
        ]
        self._memory_db: Dict[str, pa.record] = {
            "test_table": pa.record_batch([pa.array([1, 2, 3])], names=["test_column"])
        }

    def get_tables(self, options: plugin.TableOptions = None) -> List[plugin.Table]:
        return self._tables

    def sync(
        self, options: plugin.SyncOptions
    ) -> Generator[message.SyncMessage, None, None]:
        for table, record in self._memory_db.items():
            yield message.SyncInsertMessage(record)
