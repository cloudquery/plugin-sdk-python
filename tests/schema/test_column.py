import pyarrow as pa
from cloudquery.sdk.schema import Column


def test_column():
    c = Column("test_column", pa.int32())
    f = c.to_arrow_field()
    c1 = Column.from_arrow_field(f)
    f1 = c1.to_arrow_field()
    assert f == f1
