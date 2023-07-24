from typing import List

import pyarrow as pa

from cloudquery.sdk.schema import arrow
from .column import Column


class Table:
    def __init__(self, name: str, columns: List[Column], title: str = "", description: str = "",
                 relations=None, is_incremental: bool = False) -> None:
        self.name = name
        self.columns = columns
        self.title = title
        self.description = description
        if relations is None:
            relations = []
        self.relations = relations
        self.is_incremental = is_incremental

    @property
    def primary_keys(self):
        return [column.name for column in self.columns if column.primary_key]

    @property
    def incremental_keys(self):
        return [column.name for column in self.columns if column.incremental_key]

    def to_arrow_schemas(self):
        fields = []
        md = {
            arrow.METADATA_TABLE_NAME: self.name,
            arrow.MATADATA_TABLE_DESCRIPTION: self.description,
            # arrow.METADATA_CONSTRAINT_NAME:
        }
        for column in self.columns:
            fields.append(column.to_arrow_field())

        return pa.schema(fields, metadata=md)

    def __lt__(self, other):
        return self.name < other.name


def tables_to_arrow_schemas(tables: List[Table]):
    schemas = []
    for table in tables:
        schemas.append(table.to_arrow_schemas())
    return schemas
