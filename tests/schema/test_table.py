import pyarrow as pa

from cloudquery.sdk.schema import Table, Column


def test_table():
    table = Table(name="test_table",
                  columns=[Column("test_column", pa.int32())],
                  title="Test Table",
                  description="Test description",
                  parent=Table(name="parent_table", columns=[]),
                  relations=[],
                  is_incremental=True,
                  )
    sch = table.to_arrow_schema()
    got = Table.from_arrow_schema(sch)
    assert got.name == table.name
    assert got.title == table.title
    assert got.description == table.description
    assert got.is_incremental == table.is_incremental
    assert got.parent.name == table.parent.name
