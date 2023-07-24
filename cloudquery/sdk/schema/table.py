
from .column import Column
from typing import List
import pyarrow as pa
from cloudquery.sdk.schema import arrow

class Table:
    def __init__(self, name: str, columns: List[Column], title: str = "", description: str = "", relations = None) -> None:
      self._name = name
      self._columns = columns
      self._title = title
      self._description = description
      self._relations = relations

    def to_arrow_schemas(self):
      fields = []
      md = {
        arrow.METADATA_TABLE_NAME: self._name,
        arrow.MATADATA_TABLE_DESCRIPTION: self._description,
        # arrow.METADATA_CONSTRAINT_NAME: 
      }
      for column in self._columns:
          fields.append(column.to_arrow_field())

      return pa.schema(fields, metadata=md)


def tables_to_arrow_schemas(tables: List[Table]):
    schemas = []
    for table in tables:
        schemas.append(table.to_arrow_schemas())
    return schemas