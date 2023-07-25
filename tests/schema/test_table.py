import pyarrow as pa

from cloudquery.sdk.schema import Table, Column


def test_table():
    table = Table("test_table", [Column("test_column", pa.int32())])
    table.to_arrow_schema()
