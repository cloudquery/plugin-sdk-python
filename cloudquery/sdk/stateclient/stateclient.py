from __future__ import annotations

import grpc
from abc import ABC, abstractmethod
from typing import Generator, Optional, Tuple

import pyarrow as pa
from cloudquery.sdk import schema
from cloudquery.sdk.plugin.plugin import BackendOptions
from cloudquery.plugin_v3 import plugin_pb2, plugin_pb2_grpc, arrow
from functools import wraps

keyColumn = "key"
valueColumn = "value"


class StateClientBuilder:
    """
    Provides a `build` method that creates a `ConnectedStateClient` if you pass backend_options,
    or `NoOpStateClient` otherwise.

    Args:
        backend_options (BackendOptions): which has `connection` & `table_name` strings.

    Returns:
        `ConnectedStateClient` or `NoOpStateClient`

    """

    @staticmethod
    def build(*, backend_options: BackendOptions) -> StateClient:
        if not backend_options or not backend_options.table_name:
            return NoOpStateClient(backend_options=backend_options)

        return ConnectedStateClient(backend_options=backend_options)


class StateClient(ABC):
    """
    Abstract class that defines the interface for a state client.

    It implements all methods except those that require a connection,
    so it is a succinct overview of what a state client does.
    """

    def __init__(self, *, backend_options: BackendOptions):
        self.mem = {}
        self.changes = {}
        self.connection = getattr(backend_options, "connection", None)
        self.table = Table(getattr(backend_options, "table_name", None))

        self.migrate_state_table()
        self.read_all_state()

    def get_key(self, key: str) -> Optional[str]:
        return self.mem.get(key)

    def set_key(self, key: str, value: str) -> None:
        self.mem[key] = value
        self.changes[key] = True

    def flush(self):
        if not self.changes:
            return

        self.write_all_state(self._changed_keys())

    def _changed_keys(self):
        for key, _ in self.changes.items():
            yield (key, self.mem[key])

    @abstractmethod
    def migrate_state_table(self):
        pass

    @abstractmethod
    def read_all_state(self):
        pass

    @abstractmethod
    def write_all_state(self, changed_keys: Generator[Tuple[str, str], None, None]):
        pass


class NoOpStateClient(StateClient):
    """
    A state client implementation that does nothing. Used when there is no backend connection.
    """

    def get_key(self, key: str) -> Optional[str]:
        pass

    def set_key(self, key: str, value: str) -> None:
        pass

    def migrate_state_table(self):
        pass

    def read_all_state(self):
        pass

    def write_all_state(self, changed_keys: Generator[Tuple[str, str], None, None]):
        pass


def connected(func):
    """
    Decorator that provides a `backend_plugin` with a gRPC connection to the decorated function.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with grpc.insecure_channel(self.connection) as channel:
            backend_plugin = plugin_pb2_grpc.PluginStub(channel)
            return func(self, backend_plugin, *args, **kwargs)

    return wrapper


class ConnectedStateClient(StateClient):
    """
    A state client implementation that connects to a backend plugin via gRPC to read/write state.
    """

    @connected
    def migrate_state_table(self, backend_plugin: plugin_pb2_grpc.PluginStub):
        backend_plugin.Write(self._migrate_table_request())

    @connected
    def read_all_state(self, backend_plugin: plugin_pb2_grpc.PluginStub):
        response = backend_plugin.Read(
            plugin_pb2.Read.Request(table=self.table.bytes())
        )

        for record in read_response_to_records(response):
            self.mem[record[keyColumn]] = record[valueColumn]

    @connected
    def write_all_state(
        self,
        backend_plugin: plugin_pb2_grpc.PluginStub,
        changed_keys: Generator[Tuple[str, str], None, None],
    ):
        backend_plugin.Write(self._write_request(k, v) for k, v in changed_keys)

    def _write_request(self, key: str, value: str) -> plugin_pb2.Write.Request:
        record = pa.RecordBatch.from_arrays(
            [
                pa.array([key]),
                pa.array([value]),
            ],
            schema=self.table.arrow_schema(),
        )
        return plugin_pb2.Write.Request(
            insert=plugin_pb2.Write.MessageInsert(record=arrow.record_to_bytes(record))
        )

    def _migrate_table_request(self):
        yield plugin_pb2.Write.Request(
            migrate_table=plugin_pb2.Write.MessageMigrateTable(table=self.table.bytes())
        )


def read_response_to_records(response) -> Generator[dict[str, str], None, None]:
    for record in response:
        record_batch = arrow.new_record_from_bytes(record.record)
        for record in recordbatch_to_list_of_maps(record_batch):
            yield record


def recordbatch_to_list_of_maps(
    record_batch: pa.RecordBatch,
) -> Generator[dict[str, str], None, None]:
    table = pa.Table.from_batches([record_batch])
    for row in table.to_pandas().to_dict(orient="records"):
        yield row


class Table:
    """
    Represents a state table with two columns: key and value.
    Provides convenience methods for whatever the gRPC requests need.
    """

    def __init__(self, name):
        self.name = name
        self._arrow_schema = None
        self._bytes = None

        if self.name:
            self._arrow_schema = self._state_table_schema().to_arrow_schema()
            self._bytes = arrow.schema_to_bytes(self._arrow_schema)

    def arrow_schema(self):
        return self._arrow_schema

    def bytes(self):
        return self._bytes

    def _state_table_schema(self):
        return schema.Table(
            name=self.name,
            columns=[
                schema.Column(name=keyColumn, type=pa.string(), primary_key=True),
                schema.Column(name=valueColumn, type=pa.string()),
            ],
        )
