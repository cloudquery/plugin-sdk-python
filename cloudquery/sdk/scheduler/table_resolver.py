from cloudquery.sdk.schema.table import Table
from cloudquery.sdk.schema import Resource
from typing import Any, Generator, List, Optional


class Client:
    def id(self) -> str:
        raise NotImplementedError()


class TableResolver:
    def __init__(self, table: Table, child_resolvers: Optional[List] = None) -> None:
        if child_resolvers is None:
            child_resolvers = []
        self._table = table
        self._child_resolvers = child_resolvers

    @property
    def table(self) -> Table:
        return self._table

    @property
    def child_resolvers(self):
        return self._child_resolvers

    def multiplex(self, client: Client) -> List[Client]:
        return [client]

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        raise NotImplementedError()

    def pre_resource_resolve(self, client: Client, resource):
        return

    def resolve_column(self, client: Client, resource: Resource, column_name: str):
        if isinstance(resource.item, dict):
            if column_name in resource.item:
                resource.set(column_name, resource.item[column_name])
        else:
            if hasattr(resource.item, column_name):
                resource.set(column_name, getattr(resource.item, column_name))

    def post_resource_resolve(self, client: Client, resource):
        return
