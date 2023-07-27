from __future__ import annotations

import pyarrow as pa

from cloudquery.sdk.schema import arrow


class Column:
    def __init__(
        self,
        name: str,
        type: pa.DataType,
        description: str = "",
        primary_key: bool = False,
        not_null: bool = False,
        incremental_key: bool = False,
        unique: bool = False,
    ) -> None:
        self.name = name
        self.type = type
        self.description = description
        self.primary_key = primary_key
        self.not_null = not_null
        self.incremental_key = incremental_key
        self.unique = unique

    def to_arrow_field(self):
        metadata = {
            arrow.METADATA_PRIMARY_KEY: arrow.METADATA_TRUE
            if self.primary_key
            else arrow.METADATA_FALSE,
            arrow.METADATA_UNIQUE: arrow.METADATA_TRUE
            if self.unique
            else arrow.METADATA_FALSE,
            arrow.METADATA_INCREMENTAL: arrow.METADATA_TRUE
            if self.incremental_key
            else arrow.METADATA_FALSE,
        }
        return pa.field(self.name, self.type, metadata=metadata)

    @staticmethod
    def from_arrow_field(field: pa.Field) -> Column:
        metadata = field.metadata
        primary_key = metadata.get(arrow.METADATA_PRIMARY_KEY) == arrow.METADATA_TRUE
        unique = metadata.get(arrow.METADATA_UNIQUE) == arrow.METADATA_TRUE
        incremental_key = (
            metadata.get(arrow.METADATA_INCREMENTAL) == arrow.METADATA_TRUE
        )
        return Column(
            field.name,
            field.type,
            primary_key=primary_key,
            not_null=not field.nullable,
            unique=unique,
            incremental_key=incremental_key,
        )
