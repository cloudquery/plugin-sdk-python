
from cloudquery.plugin_v3 import plugin_pb2, plugin_pb2_grpc
from typing import List
from cloudquery.sdk.schema import Table

class Plugin:
    def __init__(self, name: str, version: str) -> None:
        self._name = name
        self._version = version
    
    def init(self, spec: bytes) -> None:
        pass

    def name(self) -> str:
        return self._name

    def version(self) -> str:
        return self._version
    
    def get_tables(self, tables: List[str], skip_tables: List[str]) -> List(Table):
        raise NotImplementedError()
    
