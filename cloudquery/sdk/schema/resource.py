from .table import Table
from typing import Any
import pyarrow as pa
from cloudquery.sdk.scalar import ScalarFactory


class Resource:
    def __init__(self, table: Table, parent, item: Any) -> None:
        self._table = table
        self._parent = parent
        self._item = item
        factory = ScalarFactory()
        self._data = [factory.new_scalar(i.type) for i in table.columns]

    @property
    def item(self):
        return self._item

    def set(self, column_name: str, value: Any):
        index_column = self._table.index_column(column_name)
        self._data[index_column].set(value)

    def to_list_of_arr(self):
        return [[self._data[i].value] for i, _ in enumerate(self._table.columns)]

    def to_arrow_record(self):
        return pa.record_batch(
            self.to_list_of_arr(), schema=self._table.to_arrow_schema()
        )
