import pyarrow as pa
from cloudquery.sdk.transformers import oapi_definition_to_columns
from cloudquery.sdk.schema import Column
from cloudquery.sdk.types import JSONType

OAPI_SPEC = {
    "swagger": "2.0",
    "info": {
        "version": "2.0",
        "title": "Test API",
        "description": "Unit tests APIs",
    },
    "host": "cloudquery.io",
    "schemes": ["https"],
    "consumes": ["application/json"],
    "produces": ["application/json"],
    "paths": {},
    "definitions": {
        "TestDefinition": {
            "type": "object",
            "properties": {
                "string": {
                    "type": "string",
                },
                "number": {
                    "type": "number",
                },
                "integer": {
                    "type": "integer",
                },
                "boolean": {
                    "type": "boolean",
                },
                "object": {
                    "$ref": "#/definitions/SomeDefinition",
                },
                "array": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/SomeDefinition"},  
                },
            },
        },
    },
}


def test_oapi_properties_to_columns():
    expected_columns = [
        Column("string", pa.string() , description=None),
        Column("number", pa.int64() , description=None),
        Column("integer", pa.int64() , description=None),
        Column("boolean", pa.bool_() , description=None),
        Column("object", JSONType() , description=None),
        Column("array", JSONType() , description=None),
    ]
    columns = oapi_definition_to_columns(OAPI_SPEC["definitions"]["TestDefinition"])
    assert expected_columns == columns
