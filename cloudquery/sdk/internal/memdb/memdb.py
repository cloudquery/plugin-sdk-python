import json

from cloudquery.sdk import plugin
from cloudquery.sdk import message
from cloudquery.sdk import schema
from typing import List, Generator, Dict
import pyarrow as pa
from cloudquery.sdk.types import JSONType
from dataclasses import dataclass, field

NAME = "memdb"
VERSION = "development"


@dataclass
class Spec:
    abc: str = field(default="abc")


class MemDB(plugin.Plugin):
    def __init__(self) -> None:
        super().__init__(
            NAME, VERSION, opts=plugin.plugin.Options(team="cloudquery", kind="source")
        )
        self._db: Dict[str, pa.RecordBatch] = {}
        self._tables: Dict[str, schema.Table] = {
            "table_1": schema.Table(
                name="table_1",
                columns=[
                    schema.Column(
                        name="name",
                        type=pa.string(),
                        primary_key=True,
                        not_null=True,
                        unique=True,
                    ),
                    schema.Column(
                        name="id",
                        type=pa.string(),
                        primary_key=True,
                        not_null=True,
                        unique=True,
                        incremental_key=True,
                    ),
                ],
                title="Table 1",
                description="Test Table 1",
                is_incremental=True,
                relations=[
                    schema.Table(
                        name="table_1_relation_1",
                        columns=[
                            schema.Column(
                                name="name",
                                type=pa.string(),
                                primary_key=True,
                                not_null=True,
                                unique=True,
                            ),
                            schema.Column(name="data", type=JSONType()),
                        ],
                        title="Table 1 Relation 1",
                        description="Test Table 1 Relation 1",
                    )
                ],
            ),
            "table_2": schema.Table(
                name="table_2",
                columns=[
                    schema.Column(
                        name="name",
                        type=pa.string(),
                        primary_key=True,
                        not_null=True,
                        unique=True,
                    ),
                    schema.Column(name="id", type=pa.string()),
                ],
                title="Table 2",
                description="Test Table 2",
            ),
        }

    def init(self, spec, no_connection: bool = False):
        if no_connection:
            return
        self._spec_json = json.loads(spec)
        self._spec = Spec(**self._spec_json)

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
