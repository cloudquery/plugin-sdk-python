import queue
from dataclasses import dataclass
from typing import List

from cloudquery.sdk.schema import Table

MIGRATE_MODE_STRINGS = ["safe", "force"]


@dataclass
class TableOptions:
    tables: List[str] = None
    skip_tables: List[str] = None
    skip_dependent_tables: bool = False


@dataclass
class BackendOptions:
    connection: str = None
    table_name: str = None


@dataclass
class SyncOptions:
    deterministic_cq_id: bool = False
    skip_dependent_tables: bool = False
    skip_tables: List[str] = None
    tables: List[str] = None
    backend_options: BackendOptions = None


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

    def get_tables(self, options: TableOptions) -> List[Table]:
        raise NotImplementedError()

    def sync(self, options: SyncOptions, results: queue.Queue) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        pass
