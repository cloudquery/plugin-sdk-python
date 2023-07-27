import pyarrow as pa
import structlog

from cloudquery.plugin_v3 import plugin_pb2, plugin_pb2_grpc
from cloudquery.sdk.message import SyncInsertMessage, SyncMigrateTableMessage
from cloudquery.sdk.plugin.plugin import Plugin, SyncOptions, TableOptions
from cloudquery.sdk.schema import tables_to_arrow_schemas


class PluginServicer(plugin_pb2_grpc.PluginServicer):
    def __init__(self, plugin: Plugin, logger=None):
        self._logger = logger if logger is not None else structlog.get_logger()
        self._plugin = plugin

    def GetName(self, request, context):
        return plugin_pb2.GetName.Response(name=self._plugin.name())

    def GetVersion(self, request, context):
        return plugin_pb2.GetVersion.Response(version=self._plugin.version())

    def Init(self, request: plugin_pb2.Init.Request, context):
        self._plugin.init(request.spec)
        return plugin_pb2.Init.Response()

    def GetTables(self, request: plugin_pb2.GetTables.Request, context):
        tables = self._plugin.get_tables(
            TableOptions(tables=request.tables, skip_tables=request.skip_tables)
        )
        schema = tables_to_arrow_schemas(tables)
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
            backend_options=None,
        )

        for msg in self._plugin.sync(options):
            if isinstance(msg, SyncInsertMessage):
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, msg.record.schema)
                writer.write_batch(msg.record)
                writer.close()
                buf = sink.getvalue().to_pybytes()
                yield plugin_pb2.Sync.Response(
                    insert=plugin_pb2.Sync.MessageInsert(record=buf)
                )
            elif isinstance(msg, SyncMigrateTableMessage):
                yield plugin_pb2.Sync.Response(
                    migrate_table=plugin_pb2.Sync.MessageMigrateTable(
                        table=msg.table.to_arrow_schema().serialize().to_pybytes()
                    )
                )
            else:
                # unknown sync message type
                raise NotImplementedError()

    def Read(self, request, context):
        raise NotImplementedError()

    def Write(self, request_iterator, context):
        raise NotImplementedError()

    def Close(self, request, context):
        return plugin_pb2.Close.Response()
