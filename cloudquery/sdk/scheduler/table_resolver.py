
from cloudquery.sdk.schema.table import Table
from cloudquery.sdk.schema import Resource
from typing import Any,Generator

class TableResolver:
    def __init__(self, table: Table) -> None:
        self._table = table
    
    @property
    def table(self) -> Table:
      return self._table

    def multiplex(self, client):
      return [client]
    
    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
      raise NotImplementedError()

    def pre_resource_resolve(self, client, resource):
      return
    
    def resolve_column(self, client, resource: Resource, column_name: str):
      if hasattr(resource.item, column_name):
         resource.set(column_name, resource.item[column_name])
    
    def post_resource_resolve(self, client, resource):
      return
