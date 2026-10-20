from __future__ import annotations

import grpc
import threading
from abc import ABC, abstractmethod
from typing import Dict, FrozenSet, Generator, List, Optional, Set, Tuple

import pyarrow as pa
from cloudquery.sdk import schema
from cloudquery.sdk.internal.checkpoint import Checkpoints, Scope
from cloudquery.sdk.message import SyncCheckpointMessage
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
        checkpoints (Checkpoints): optional, the `checkpoints` of the `Scheduler` that syncs.
            When the CLI supports checkpoints, `flush` then sends the state on the sync stream
            instead of writing it to the backend, and the CLI persists it once the destinations
            commit the resources synced before it. Changes made within a table resolver wait only
            for the resources that resolver produced before the flush, and their relations.
            Changes made outside table resolvers wait for nothing, so flush them after the
            resources they summarize are synced, e.g. in the scheduler's post-sync hook.

    Returns:
        `ConnectedStateClient` or `NoOpStateClient`

    """

    @staticmethod
    def build(
        *, backend_options: BackendOptions, checkpoints: Optional[Checkpoints] = None
    ) -> StateClient:
        if not backend_options or not backend_options.table_name:
            return NoOpStateClient(backend_options=backend_options)

        if not backend_options.supports_checkpoints:
            checkpoints = None
        return ConnectedStateClient(
            backend_options=backend_options, checkpoints=checkpoints
        )


class StateClient(ABC):
    """
    Abstract class that defines the interface for a state client.

    It implements all methods except those that require a connection,
    so it is a succinct overview of what a state client does.
    """

    def __init__(
        self,
        *,
        backend_options: BackendOptions,
        checkpoints: Optional[Checkpoints] = None,
    ):
        self.mem = {}
        # changed keys, each with the scopes of the table resolvers that changed it since the
        # last flush (None for changes made outside table resolvers)
        self.changes: Dict[str, Set[Optional[Scope]]] = {}
        self.connection = getattr(backend_options, "connection", None)
        self.table = Table(getattr(backend_options, "table_name", None))
        self.checkpoints = checkpoints
        # guards mem and changes, which table resolvers change from several threads
        self._lock = threading.RLock()
        # serializes flushes, so that a key's flushed values reach the state in the order they were set
        self._flush_lock = threading.Lock()

        self.migrate_state_table()
        self.read_all_state()

    def get_key(self, key: str) -> Optional[str]:
        with self._lock:
            return self.mem.get(key)

    def set_key(self, key: str, value: str) -> None:
        scope = None
        if self.checkpoints:
            scope = self.checkpoints.current_scope()
            self.checkpoints.check_scope(scope)
        with self._lock:
            self.mem[key] = value
            self.changes.setdefault(key, set()).add(scope)

    def flush(self):
        """
        Persists the keys changed since the last flush: writes them to the backend, or, with
        checkpoints, sends them on the sync stream after the resources they cover.
        If that fails, the keys stay changed, so a later flush retries them.
        """
        with self._flush_lock:
            with self._lock:
                if not self.changes:
                    return
                changes, self.changes = self.changes, {}
                values = {key: self.mem[key] for key in changes}
            try:
                if self.checkpoints is None:
                    self.write_all_state(iter(values.items()))
                else:
                    self._send_checkpoints(changes, values)
            except Exception:
                with self._lock:
                    for key, scopes in changes.items():
                        self.changes.setdefault(key, set()).update(scopes)
                raise

    def _send_checkpoints(
        self, changes: Dict[str, Set[Optional[Scope]]], values: Dict[str, str]
    ):
        # one checkpoint per set of scopes, so each waits only for the resources of the scopes that changed its keys
        groups: Dict[FrozenSet[Optional[Scope]], List[str]] = {}
        for key, scopes in changes.items():
            groups.setdefault(frozenset(scopes), []).append(key)
        self.checkpoints.schedule(
            [
                (
                    scopes,
                    keys,
                    SyncCheckpointMessage(
                        self.table.record([(key, values[key]) for key in keys])
                    ),
                )
                for scopes, keys in groups.items()
            ]
        )

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

    def flush(self):
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
        record = self.table.record([(key, value)])
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

    def record(self, rows: List[Tuple[str, str]]) -> pa.RecordBatch:
        """Returns the state rows as a record of this table, with the table name in its schema metadata."""
        return pa.RecordBatch.from_arrays(
            [
                pa.array([key for key, _ in rows], type=pa.string()),
                pa.array([value for _, value in rows], type=pa.string()),
            ],
            schema=self._arrow_schema,
        )

    def _state_table_schema(self):
        return schema.Table(
            name=self.name,
            columns=[
                schema.Column(name=keyColumn, type=pa.string(), primary_key=True),
                schema.Column(name=valueColumn, type=pa.string()),
            ],
        )
