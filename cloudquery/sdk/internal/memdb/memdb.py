from cloudquery.sdk import plugin
from cloudquery.sdk import message
from cloudquery.sdk import schema
from typing import List, Generator, Dict
import pyarrow as pa

NAME = "memdb"
VERSION = "development"


class MemDB(plugin.Plugin):
    def __init__(self) -> None:
        super().__init__(NAME, VERSION)
        self._db: Dict[str, pa.RecordBatch] = {}
        self._tables: Dict[str, schema.Table] = {}

    def get_tables(self, options: plugin.TableOptions = None) -> List[plugin.Table]:
        tables = list(self._tables.values())
        return schema.filter_dfs(tables, options.tables, options.skip_tables)

    def sync(
        self, options: plugin.SyncOptions
    ) -> Generator[message.SyncMessage, None, None]:
        for table, record in self._db.items():
            yield message.SyncInsertMessage(record)

    def write(self, writer: Generator[message.WriteMessage, None, None]) -> None:
        for msg in writer:
            if isinstance(msg, message.WriteMigrateTableMessage):
                if msg.table.name not in self._db:
                    self._db[msg.table.name] = msg.table
                    self._tables[msg.table.name] = msg.table
            elif isinstance(msg, message.WriteInsertMessage):
                table = schema.Table.from_arrow_schema(msg.record.schema)
                self._db[table.name] = msg.record
            else:
                raise NotImplementedError(f"Unknown message type {type(msg)}")

    def close(self) -> None:
        self._db = {}
