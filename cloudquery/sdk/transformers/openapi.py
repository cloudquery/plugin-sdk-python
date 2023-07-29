from typing import Dict, List
import pyarrow as pa
from cloudquery.sdk.types import JSONType
from cloudquery.sdk.schema import Column


def oapi_type_to_arrow_type(field) -> pa.DataType:
    oapi_type = field.get("type")
    if oapi_type == "string":
        return pa.string()
    elif oapi_type == "number":
        return pa.int64()
    elif oapi_type == "integer":
        return pa.int64()
    elif oapi_type == "boolean":
        return pa.bool_()
    elif oapi_type == "array":
        return JSONType()
    elif oapi_type == "object":
        return JSONType()
    elif oapi_type is None and "$ref" in field:
        return JSONType()
    else:
        return pa.string()


def oapi_definition_to_columns(definition: Dict) -> List[Column]:
    columns = []
    for key, value in definition["properties"].items():
        column_type = oapi_type_to_arrow_type(value)
        columns.append(
            Column(name=key, type=column_type, description=value.get("description"))
        )
    return columns
