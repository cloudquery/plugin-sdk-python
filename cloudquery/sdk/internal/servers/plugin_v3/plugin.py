from typing import Generator

import pyarrow as pa
import structlog
from cloudquery.plugin_v3 import plugin_pb2, plugin_pb2_grpc, arrow

from cloudquery.sdk.message import (
    SyncInsertMessage,
    SyncMigrateTableMessage,
    WriteInsertMessage,
    WriteMigrateTableMessage,
    WriteMessage,
    WriteDeleteStale,
)
from cloudquery.sdk.plugin.plugin import Plugin, SyncOptions, TableOptions
from cloudquery.sdk.schema import tables_to_arrow_schemas, Table
from cloudquery.sdk.schema.table import flatten_tables


class PluginServicer(plugin_pb2_grpc.PluginServicer):
    def __init__(self, plugin: Plugin, logger=None):
        self._logger = logger if logger is not None else structlog.get_logger()
        self._plugin = plugin

    def GetName(self, request, context):
        return plugin_pb2.GetName.Response(name=self._plugin.name())

    def GetVersion(self, request, context):
        return plugin_pb2.GetVersion.Response(version=self._plugin.version())

    def GetSpecSchema(self, request, context):
        return plugin_pb2.GetSpecSchema.Response(json_schema=self._plugin.json_schema())

    def Init(self, request: plugin_pb2.Init.Request, context):
        self._plugin.init(
            sanitize_spec(request.spec), no_connection=request.no_connection
        )
        return plugin_pb2.Init.Response()

    def GetTables(self, request: plugin_pb2.GetTables.Request, context):
        tables = self._plugin.get_tables(
            TableOptions(
                tables=request.tables,
                skip_tables=request.skip_tables,
                skip_dependent_tables=request.skip_dependent_tables,
            )
        )
        flattened_tables = flatten_tables(tables)
        schema = tables_to_arrow_schemas(flattened_tables)
        tablesBytes = []
        for s in schema:
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, s)
            writer.close()
            buf = sink.getvalue().to_pybytes()
            tablesBytes.append(buf)

        return plugin_pb2.GetTables.Response(tables=tablesBytes)

    def Sync(self, request, context):
        options = SyncOptions(
            deterministic_cq_id=False,  # TODO
            skip_dependent_tables=request.skip_dependent_tables,
            skip_tables=request.skip_tables,
            tables=request.tables,
            backend_options=request.backend,
        )

        for msg in self._plugin.sync(options):
            if isinstance(msg, SyncInsertMessage):
                buf = arrow.record_to_bytes(msg.record)
                yield plugin_pb2.Sync.Response(
                    insert=plugin_pb2.Sync.MessageInsert(record=buf)
                )
            elif isinstance(msg, SyncMigrateTableMessage):
                buf = arrow.schema_to_bytes(msg.table)
                yield plugin_pb2.Sync.Response(
                    migrate_table=plugin_pb2.Sync.MessageMigrateTable(table=buf)
                )
            else:
                # unknown sync message type
                raise NotImplementedError()

    def Read(self, request, context):
        raise NotImplementedError()

    def Write(
        self, request_iterator: Generator[plugin_pb2.Write.Request, None, None], context
    ):
        def msg_iterator() -> Generator[WriteMessage, None, None]:
            for msg in request_iterator:
                field = msg.WhichOneof("message")
                if field == "migrate_table":
                    sc = arrow.new_schema_from_bytes(msg.migrate_table.table)
                    table = Table.from_arrow_schema(sc)
                    yield WriteMigrateTableMessage(table=table)
                elif field == "insert":
                    yield WriteInsertMessage(
                        record=arrow.new_record_from_bytes(msg.insert.record)
                    )
                elif field == "delete":
                    yield WriteDeleteStale(
                        table_name=msg.delete.table_name,
                        source_name=msg.delete.source_name,
                        sync_time=msg.delete.sync_time.ToDatetime(),
                    )
                elif field is None:
                    continue
                else:
                    raise NotImplementedError(f"unknown write message type {field}")

        self._plugin.write(msg_iterator())
        return plugin_pb2.Write.Response()

    def Close(self, request, context):
        self._plugin.close()
        return plugin_pb2.Close.Response()


def sanitize_spec(spec=None):
    return b"{}" if spec is None or spec == b"" or spec == b"null" else spec
