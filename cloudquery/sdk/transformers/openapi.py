from typing import Dict, List
import pyarrow as pa
from cloudquery.sdk.schema import Column


def oapi_type_to_arrow_type(field) -> pa.DataType:
    oapi_type = field.get("type")
    if oapi_type == "string":
        return pa.string()
    elif oapi_type == "number":
        return pa.int64()
    elif oapi_type == "boolean":
        return pa.bool_()
    else:
        return pa.string()


def oapi_properties_to_columns(properties: Dict) -> List[Column]:
    columns = []
    for key, value in properties.items():
        columns.append(
            Column(name=key, type=value, description=value.get("description"))
        )
    return columns
