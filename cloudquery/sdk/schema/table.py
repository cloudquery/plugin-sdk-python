from __future__ import annotations

import copy
from enum import IntEnum
import fnmatch
from typing import List, Optional

import pyarrow as pa

from cloudquery.sdk.schema import arrow
from .column import Column


CQ_SYNC_TIME_COLUMN = "cq_sync_time"
CQ_SOURCE_NAME_COLUMN = "cq_source_name"


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


class TableColumnChangeType:
    ADD = 1
    REMOVE = 2
    REMOVE_UNIQUE_CONSTRAINT = 3


class TableColumnChange:
    def __init__(
        self,
        type: TableColumnChangeType,
        column_name: str,
        current: Optional[Column],
        previous: Optional[Column],
    ):
        self.type = type
        self.column_name = column_name
        self.current = current
        self.previous = previous


class TableColumnChangeType(IntEnum):
    UNKNOWN = 0
    ADD = 1
    UPDATE = 2
    REMOVE = 3
    REMOVE_UNIQUE_CONSTRAINT = 4
    MOVE_TO_CQ_ONLY = 5


def get_table_changes(new: Table, old: Table) -> List[TableColumnChange]:
    changes = []

    # Special case: Moving from individual PKs to singular PK on _cq_id
    new_pks = new.primary_keys
    if (
        len(new_pks) == 1
        and new_pks[0] == "CqIDColumn"
        and get_table_column(old, "CqIDColumn") is None
        and len(old.primary_keys) > 0
    ):
        changes.append(
            TableColumnChange(
                type=TableColumnChangeType.MOVE_TO_CQ_ONLY,
            )
        )

    for c in new.columns:
        other_column = get_table_column(old, c.name)
        # A column was added to the table definition
        if other_column is None:
            changes.append(
                TableColumnChange(
                    type=TableColumnChangeType.ADD,
                    column_name=c.name,
                    current=c,
                    previous=None,
                )
            )
            continue

        # Column type or options (e.g. PK, Not Null) changed in the new table definition
        if (
            c.type != other_column.type
            or c.not_null != other_column.not_null
            or c.primary_key != other_column.primary_key
        ):
            changes.append(
                TableColumnChange(
                    type=TableColumnChangeType.UPDATE,
                    column_name=c.name,
                    current=c,
                    previous=other_column,
                )
            )

        # Unique constraint was removed
        if not c.unique and other_column.unique:
            changes.append(
                TableColumnChange(
                    type=TableColumnChangeType.REMOVE_UNIQUE_CONSTRAINT,
                    column_name=c.name,
                    current=c,
                    previous=other_column,
                )
            )

    # A column was removed from the table definition
    for c in old.columns:
        if get_table_column(new, c.name) is None:
            changes.append(
                TableColumnChange(
                    type=TableColumnChangeType.REMOVE,
                    column_name=c.name,
                    current=None,
                    previous=c,
                )
            )

    return changes


def get_table_column(table: Table, column_name: str) -> Optional[Column]:
    for c in table.columns:
        if c.name == column_name:
            return c
    return None


def flatten_tables_recursive(original_tables: List[Table]) -> List[Table]:
    tables = []
    for table in original_tables:
        table_copy = Table(
            name=table.name,
            columns=table.columns,
            relations=table.relations,
            title=table.title,
            description=table.description,
            is_incremental=table.is_incremental,
            parent=table.parent,
        )
        tables.append(table_copy)
        tables.extend(flatten_tables_recursive(table.relations))
    return tables


def flatten_tables(original_tables: List[Table]) -> List[Table]:
    tables = flatten_tables_recursive(original_tables)
    seen = set()
    deduped = []
    for table in tables:
        if table.name not in seen:
            deduped.append(table)
            seen.add(table.name)
    return deduped
