from dataclasses import dataclass

import pyarrow as pa

from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Table, Resource, Column


@dataclass
class Item:
    test_column2: int = 1


def test_table_resolver_resolve_column():
    test_table = Table(
        name="test_table",
        columns=[
            Column(name="test_column", type=pa.int64()),
            Column(name="test_column2", type=pa.int64()),
        ],
    )
    resource_dict = Resource(table=test_table, parent=None, item={"test_column": 1})
    resource_obj = Resource(
        table=test_table, parent=None, item=Item(test_column2=2)
    )
    test_resolver = TableResolver(table=test_table, child_resolvers=[])
    test_resolver.resolve_column(
        client=None, resource=resource_dict, column_name="test_column"
    )
    test_resolver.resolve_column(
        client=None, resource=resource_obj, column_name="test_column2"
    )
    assert resource_dict.to_list_of_arr() == [[1], [None]]
    assert resource_obj.to_list_of_arr() == [[None], [2]]
