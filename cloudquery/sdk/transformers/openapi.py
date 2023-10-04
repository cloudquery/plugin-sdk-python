from typing import Dict, List, Optional
import pyarrow as pa
from cloudquery.sdk.types import JSONType
from cloudquery.sdk.schema import Column


def oapi_type_to_arrow_type(field) -> pa.DataType:
    oapi_type = field.get("type")
    type_map = {
        "string": pa.string,
        "number": pa.int64,
        "integer": pa.int64,
        "boolean": pa.bool_,
        "array": JSONType,
        "object": JSONType,
    }
    _type = pa.string
    if oapi_type in type_map:
        _type = type_map[oapi_type]
    elif oapi_type is None and "$ref" in field:
        _type = JSONType
    return _type()


def get_column_by_name(columns: List[Column], name: str) -> Optional[Column]:
    for column in columns:
        if column.name == name:
            return column
    return None


def oapi_definition_to_columns(
    definition: Dict, override_columns: Optional[List] = None
) -> List[Column]:
    columns = []
    for key, value in definition["properties"].items():
        column_type = oapi_type_to_arrow_type(value)
        column = Column(
            name=key, type=column_type, description=value.get("description")
        )
        override_column = get_column_by_name(
            override_columns if override_columns is not None else [], key
        )
        if override_column is not None:
            column.type = override_column.type
            column.primary_key = override_column.primary_key
            column.unique = override_column.unique
        columns.append(column)
    return columns
