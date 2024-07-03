import logging
import sys
import os
from abc import ABC, abstractmethod
from typing import Any, Generator

import structlog
import pyarrow as pa
from cloudquery.sdk import schema, message
from cloudquery.sdk.internal.servers.plugin_v3.plugin import PluginServicer
from cloudquery.sdk.plugin.plugin import BackendOptions, Plugin
from cloudquery.sdk.scheduler.table_resolver import TableResolver
from cloudquery.plugin_v3 import plugin_pb2, arrow

_IS_WINDOWS = sys.platform == "win32"

try:
    import colorama
except ImportError:
    colorama = None

if _IS_WINDOWS:  # pragma: no cover
    # On Windows, use colors by default only if Colorama is installed.
    _has_colors = colorama is not None
else:
    # On other OSes, use colors by default.
    _has_colors = True

keyColumn = "key"
valueColumn = "value"

def get_logger(args):
    log_level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level_map.get(args.log_level.lower(), logging.INFO),
    )

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.stdlib.filter_by_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%SZ", utc=True),
    ]
    if args.log_format == "text":
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=os.environ.get("NO_COLOR", "") == ""
                and (
                    os.environ.get("FORCE_COLOR", "") != ""
                    or (
                        _has_colors
                        and sys.stdout is not None
                        and hasattr(sys.stdout, "isatty")
                        and sys.stdout.isatty()
                    )
                )
            )
        )
    else:
        processors.append(structlog.processors.JSONRenderer())

    log = structlog.wrap_logger(logging.getLogger(), processors=processors)
    return log

class StateClient(ABC):
    @abstractmethod
    def get_key(self, key):
        pass

    @abstractmethod
    def set_key(self, key, value):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def send_migrate_table_message(self):
        pass

    @abstractmethod
    def get_resolver(self):
        pass

class StateClientBuilder:
    @staticmethod
    def build(plugin, backend_options: BackendOptions) -> StateClient:
        if not backend_options or not backend_options.table_name:
            return NoOpStateClient()
        
        return StateClientImpl(plugin, backend_options.table_name, backend_options)

class DefaultLoggerArgs:
    def __init__(self):
        self.log_level = "info"
        self.log_format = "text"

class StateClientImpl:
    def __init__(self, plugin, table_name, options):
        self.table_name = table_name
        self.options = options
        self.logger = get_logger(DefaultLoggerArgs())
        self.resolver = StateClientResolver(table_name)

        # TODO: it's still not reading initial state
        self.plugin: Plugin = plugin
        servicer = PluginServicer(self)
        result = servicer.Read(self, plugin_pb2.Read.Request(table=arrow.schema_to_bytes(table(table_name).to_arrow_schema())))
        breakpoint()

    def get_key(self, key):
        return self.resolver.get_key(key)

    def set_key(self, key, value):
        self.resolver.set_key(key, value)

    def get_resolver(self):
        return self.resolver
    
    def read(self, reader: Generator[message.ReadMessage, None, None]) -> None:
        for msg in reader:
            if isinstance(msg, message.ReadInsertMessage):
                for key, value in record_batch_to_map(msg.record).items():
                    self.set_key(key, value)
            else:
                raise NotImplementedError(f"Unknown message type {type(msg)}")


def record_batch_to_map(record_batch):
    records = []
    for i in range(record_batch.num_rows):
        record = {col: record_batch.column(col).to_pylist()[i] for col in range(record_batch.num_columns)}
        records.append(record)
    return records

class StateClientResolver(TableResolver):
    def __init__(self, table_name):
        super().__init__(table(table_name))
        self.mem = {}
        self.changes = {}

    def get_key(self, key):
        return self.mem.get(key)

    def set_key(self, key, value):
        self.mem[key] = value
        self.changes[key] = True

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for key, value in self.mem.items():
            if key in self.changes:
                yield {"key": key, "value": value}


class NoOpStateClient:
    def __init__(self):
        pass

    def get_key(self, key):
        pass

    def set_key(self, key, value):
        pass
    
    def get_resolver(self):
        return None

def table(name):
    return schema.Table(
        name=name,
        columns=[
            schema.Column(
                name=keyColumn,
                type=pa.string(),
                primary_key=True
            ),
            schema.Column(
                name=valueColumn,
                type=pa.string()
            )
        ]
    )