import pyarrow as pa

from cloudquery.plugin_v3 import plugin_pb2, plugin_pb2_grpc
from cloudquery.sdk.plugin.plugin import Plugin
from cloudquery.sdk.schema import tables_to_arrow_schemas


class PluginServicer(plugin_pb2_grpc.PluginServicer):
    def __init__(self, plugin: Plugin):
        self._plugin = plugin

    def GetName(self, request, context):
        return plugin_pb2.GetName.Response(name=self._plugin.name())

    def GetVersion(self, request, context):
        return plugin_pb2.GetVersion.Response(name=self._plugin.version())

    def Init(self, request: plugin_pb2.Init.Request, context):
        self._plugin.init(request.spec)
        return plugin_pb2.Init.Response()

    def GetTables(self, request: plugin_pb2.GetTables.Request, context):
        tables = self._plugin.get_tables(request.tables, request.skip_tables)
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
        plugin_pb2.Sync.Response()
        return plugin_pb2.Sync.Response()

    def Read(self, request, context):
        raise NotImplementedError()

    def Write(self, request_iterator, context):
        raise NotImplementedError()

    def Close(self, request, context):
        return plugin_pb2.Close.Response()
