from __future__ import annotations

import fnmatch
from typing import List

import pyarrow as pa

from cloudquery.sdk.schema import arrow
from .column import Column


class Client:
    pass


class Table:
    def __init__(
            self,
            name: str,
            columns: List[Column],
            title: str = "",
            description: str = "",
            parent: Table = None,
            relations: List[Table] = None,
            is_incremental: bool = False,
    ) -> None:
        self.name = name
        self.columns = columns
        self.title = title
        self.description = description
        if relations is None:
            relations = []
        self.parent = parent
        self.relations = relations
        self.is_incremental = is_incremental

    def multiplex(self, client) -> List[Table]:
        raise [client]

    def index_column(self, column_name: str) -> int:
        for i, column in enumerate(self.columns):
            if column.name == column_name:
                return i
        raise ValueError(f"Column {column_name} not found")

    @property
    def primary_keys(self):
        return [column.name for column in self.columns if column.primary_key]

    @property
    def incremental_keys(self):
        return [column.name for column in self.columns if column.incremental_key]

    @classmethod
    def from_arrow_schema(cls, schema: pa.Schema) -> Table:
        columns = []
        for field in schema:
            columns.append(Column.from_arrow_field(field))
        parent = None
        if arrow.METADATA_TABLE_DEPENDS_ON in schema.metadata:
            parent = Table(
                name=schema.metadata[arrow.METADATA_TABLE_DEPENDS_ON].decode("utf-8"),
                columns=[],
            )
        return cls(
            name=schema.metadata.get(arrow.METADATA_TABLE_NAME, b"").decode("utf-8"),
            title=schema.metadata.get(arrow.METADATA_TABLE_TITLE, b"").decode("utf-8"),
            columns=columns,
            description=schema.metadata.get(arrow.METADATA_TABLE_DESCRIPTION).decode(
                "utf-8"
            ),
            is_incremental=schema.metadata.get(arrow.METADATA_INCREMENTAL, arrow.METADATA_FALSE) == arrow.METADATA_TRUE,
            parent=parent,
        )

    def to_arrow_schema(self):
        fields = []
        md = {
            arrow.METADATA_TABLE_NAME: self.name,
            arrow.METADATA_TABLE_DESCRIPTION: self.description,
            arrow.METADATA_TABLE_TITLE: self.title,
            arrow.METADATA_TABLE_DEPENDS_ON: self.parent.name if self.parent else "",
            arrow.METADATA_INCREMENTAL: arrow.METADATA_TRUE if self.is_incremental else arrow.METADATA_FALSE,
        }
        for column in self.columns:
            fields.append(column.to_arrow_field())

        return pa.schema(fields, metadata=md)

    def __lt__(self, other):
        return self.name < other.name


def tables_to_arrow_schemas(tables: List[Table]):
    schemas = []
    for table in tables:
        schemas.append(table.to_arrow_schema())
    return schemas


def filter_dfs(
        tables: List[Table], include_tables: List[str], skip_tables: List[str]
) -> List[Table]:
    filtered: List[Table] = []
    for table in tables:
        matched = False
        for include_table in include_tables:
            if fnmatch.fnmatch(table.name, include_table):
                matched = True
        for skip_table in skip_tables:
            if fnmatch.fnmatch(table.name, skip_table):
                matched = False
        if matched:
            filtered.append(table)
    return filtered
