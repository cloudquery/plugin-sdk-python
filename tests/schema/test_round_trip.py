import pyarrow as pa
from cloudquery.plugin_v3 import arrow


def test_schema_round_trip():
    sc = pa.schema(
        fields=[pa.field("a", pa.int64())], metadata={"foo": "bar", "baz": "quux"}
    )
    b = arrow.schemas_to_bytes([sc])
    schemas = arrow.new_schemas_from_bytes(b)
    assert len(schemas) == 1
    assert schemas[0].equals(sc)


def test_record_round_trip():
    sc = pa.schema(
        fields=[pa.field("a", pa.int64())], metadata={"foo": "bar", "baz": "quux"}
    )
    rec = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=sc)
    b = arrow.record_to_bytes(rec)
    rec2 = arrow.new_record_from_bytes(b)
    assert rec.equals(rec2)
