from __future__ import annotations

import copy
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

    def multiplex(self, client: Client) -> List[Client]:
        return [client]

    def index_column(self, column_name: str) -> int:
        for i, column in enumerate(self.columns):
            if column.name == column_name:
                return i
        raise ValueError(f"Column {column_name} not found")

    @property
    def resolver(self):
        raise NotImplementedError

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
            is_incremental=schema.metadata.get(
                arrow.METADATA_INCREMENTAL, arrow.METADATA_FALSE
            )
            == arrow.METADATA_TRUE,
            parent=parent,
        )

    def to_arrow_schema(self):
        fields = []
        md = {
            arrow.METADATA_TABLE_NAME: self.name,
            arrow.METADATA_TABLE_DESCRIPTION: self.description,
            arrow.METADATA_TABLE_TITLE: self.title,
            arrow.METADATA_TABLE_DEPENDS_ON: self.parent.name if self.parent else "",
            arrow.METADATA_INCREMENTAL: (
                arrow.METADATA_TRUE if self.is_incremental else arrow.METADATA_FALSE
            ),
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
    tables: List[Table],
    include_tables: List[str],
    skip_tables: List[str],
    skip_dependent_tables: bool = False,
) -> List[Table]:
    flattened_tables = flatten_tables(tables)
    for include_pattern in include_tables:
        matched = any(
            fnmatch.fnmatch(table.name, include_pattern) for table in flattened_tables
        )
        if not matched:
            raise ValueError(
                f"tables include a pattern {include_pattern} with no matches"
            )

    for exclude_pattern in skip_tables:
        matched = any(
            fnmatch.fnmatch(table.name, exclude_pattern) for table in flattened_tables
        )
        if not matched:
            raise ValueError(
                f"skip_tables include a pattern {exclude_pattern} with no matches"
            )

    def include_func(t):
        return any(
            fnmatch.fnmatch(t.name, include_pattern)
            for include_pattern in include_tables
        )

    def exclude_func(t):
        return any(
            fnmatch.fnmatch(t.name, exclude_pattern) for exclude_pattern in skip_tables
        )

    return filter_dfs_func(tables, include_func, exclude_func, skip_dependent_tables)


def filter_dfs_func(tt: List[Table], include, exclude, skip_dependent_tables: bool):
    filtered_tables = []
    for t in tt:
        filtered_table = copy.deepcopy(t)
        for r in filtered_table.relations:
            r.parent = filtered_table
        filtered_table = _filter_dfs_impl(
            filtered_table, False, include, exclude, skip_dependent_tables
        )
        if filtered_table is not None:
            filtered_tables.append(filtered_table)
    return filtered_tables


def _filter_dfs_impl(t, parent_matched, include, exclude, skip_dependent_tables):
    def filter_dfs_child(r, matched, include, exclude, skip_dependent_tables):
        filtered_child = _filter_dfs_impl(
            r, matched, include, exclude, skip_dependent_tables
        )
        if filtered_child is not None:
            return True, r
        return matched, None

    if exclude(t):
        return None

    matched = parent_matched and not skip_dependent_tables
    if include(t):
        matched = True

    filtered_relations = []
    for r in t.relations:
        matched, filtered_child = filter_dfs_child(
            r, matched, include, exclude, skip_dependent_tables
        )
        if filtered_child is not None:
            filtered_relations.append(filtered_child)

    t.relations = filtered_relations

    if matched:
        return t
    return None


def flatten_tables(tables: List[Table]) -> List[Table]:
    flattened: List[Table] = []
    for table in tables:
        flattened.append(table)
        flattened.extend(flatten_tables(table.relations))
    return flattened
