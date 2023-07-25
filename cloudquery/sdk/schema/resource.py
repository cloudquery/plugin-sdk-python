
from .table import Table
from typing import Any
from cloudquery.sdk.scalar import ScalarFactory

class Resource:
    def __init__(self, table: Table, parent, item: Any) -> None:
      self._table = table
      self._parent = parent
      factory = ScalarFactory()
      self._data = [factory.new_scalar(i.type) for i in table.columns]
    
    def set(self, column_name: str, value: Any):
      index_column = self._table.index_column(column_name)
      self._data[index_column].set(value)
    