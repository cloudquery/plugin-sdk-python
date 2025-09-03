import json

from cloudquery.sdk import plugin
from cloudquery.sdk import message
from cloudquery.sdk import schema
from cloudquery.sdk.scheduler import Scheduler, TableResolver
from typing import List, Generator, Dict, Any
import pyarrow as pa
from cloudquery.sdk.schema.table import Table
from cloudquery.sdk.schema.arrow import METADATA_TABLE_NAME
from cloudquery.sdk.types import JSONType
from dataclasses import dataclass, field

NAME = "memdb"
VERSION = "development"


class Client:
    def __init__(self) -> None:
        pass

    def id(self):
        return "memdb"


class MemDBResolver(TableResolver):
    def __init__(
        self, table: Table, records: List, child_resolvers: list[TableResolver] = None
    ) -> None:
        super().__init__(table=table, child_resolvers=child_resolvers)
        self._records = records

    def resolve(self, client: None, parent_resource) -> Generator[Any, None, None]:
        for record in self._records:
            yield record


class Table1Relation1(Table):
    def __init__(self) -> None:
        super().__init__(
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

    @property
    def resolver(self):
        return MemDBResolver(
            self,
            records=[
                {"name": "a", "data": {"a": 1}},
                {"name": "b", "data": {"b": 2}},
                {"name": "c", "data": {"c": 3}},
            ],
        )


class Table1(Table):
    def __init__(self) -> None:
        super().__init__(
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
                    type=pa.int64(),
                    primary_key=True,
                    not_null=True,
                    unique=True,
                    incremental_key=True,
                ),
            ],
            title="Table 1",
            description="Test Table 1",
            is_incremental=True,
            relations=[Table1Relation1()],
        )

    @property
    def resolver(self):
        child_resolvers: list[TableResolver] = []
        for rel in self.relations:
            child_resolvers.append(rel.resolver)

        return MemDBResolver(
            self,
            records=[
                {"name": "a", "id": 1},
                {"name": "b", "id": 2},
                {"name": "c", "id": 3},
            ],
            child_resolvers=child_resolvers,
        )


class Table2(Table):
    def __init__(self) -> None:
        super().__init__(
            name="table_2",
            columns=[
                schema.Column(
                    name="name",
                    type=pa.string(),
                    primary_key=True,
                    not_null=True,
                    unique=True,
                ),
                schema.Column(name="id", type=pa.int64()),
            ],
            title="Table 2",
            description="Test Table 2",
        )

    @property
    def resolver(self):
        return MemDBResolver(
            self,
            records=[
                {"name": "a", "id": 1},
                {"name": "b", "id": 2},
                {"name": "c", "id": 3},
            ],
        )


@dataclass
class Spec:
    concurrency: int = field(default=1000)
    queue_size: int = field(default=1000)


class MemDB(plugin.Plugin):
    def __init__(self) -> None:
        super().__init__(
            NAME, VERSION, opts=plugin.plugin.Options(team="cloudquery", kind="source")
        )
        table1 = Table1()
        table2 = Table2()
        self._tables: Dict[str, schema.Table] = {
            table1.name: table1,
            table2.name: table2,
        }
        self._db: List[pa.RecordBatch] = []
        self._client = Client()

    def set_logger(self, logger) -> None:
        self._logger = logger

    def init(self, spec, no_connection: bool = False):
        if no_connection:
            return
        self._spec_json = json.loads(spec)
        self._spec = Spec(**self._spec_json)
        self._scheduler = Scheduler(
            concurrency=self._spec.concurrency,
            queue_size=self._spec.queue_size,
            logger=self._logger,
        )

    def get_tables(self, options: plugin.TableOptions = None) -> List[plugin.Table]:
        tables = list(self._tables.values())

        # set parent table relationships
        for table in tables:
            for relation in table.relations:
                relation.parent = table

        return schema.filter_dfs(tables, options.tables, options.skip_tables)

    def sync(
        self, options: plugin.SyncOptions
    ) -> Generator[message.SyncMessage, None, None]:
        resolvers: list[TableResolver] = []
        for table in self.get_tables(
            plugin.TableOptions(
                tables=options.tables,
                skip_tables=options.skip_tables,
                skip_dependent_tables=options.skip_dependent_tables,
            )
        ):
            resolvers.append(table.resolver)

        return self._scheduler.sync(
            self._client, resolvers, options.deterministic_cq_id
        )

    def write(self, writer: Generator[message.WriteMessage, None, None]) -> None:
        for msg in writer:
            if isinstance(msg, message.WriteMigrateTableMessage):
                pass
            elif isinstance(msg, message.WriteInsertMessage):
                self._db.append(msg.record)
            else:
                raise NotImplementedError(f"Unknown message type {type(msg)}")

    def read(self, table: Table) -> Generator[message.ReadMessage, None, None]:
        for record in self._db:
            recordMetadata = record.schema.metadata.get(METADATA_TABLE_NAME).decode(
                "utf-8"
            )
            if recordMetadata == table.name:
                yield message.ReadMessage(record)

    def close(self) -> None:
        self._db = {}
