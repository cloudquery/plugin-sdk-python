
import random
import grpc
import time
from concurrent import futures
from cloudquery.sdk import serve
from cloudquery.sdk import plugin
from cloudquery.plugin_v3 import plugin_pb2_grpc, plugin_pb2
from cloudquery.sdk.internal.memdb import MemDB


def test_plugin_serve():
    p = MemDB()
    cmd = serve.PluginCommand(p)
    port = random.randint(5000, 50000)
    pool = futures.ThreadPoolExecutor(max_workers=1)
    pool.submit(cmd.run, ["serve", "--address", f"[::]:{port}"])
    time.sleep(1)
    try:
      with grpc.insecure_channel(f'localhost:{port}') as channel:
          stub = plugin_pb2_grpc.PluginStub(channel)
          response = stub.GetName(plugin_pb2.GetName.Request())
          assert response.name == "memdb"
          
          response = stub.GetVersion(plugin_pb2.GetVersion.Request())
          assert response.version == "development"

          response = stub.Init(plugin_pb2.Init.Request(spec=b""))
          assert response is not None

          response = stub.GetTables(plugin_pb2.GetTables.Request())
          print(response.tables)
          assert response.tables is not None
    finally:
      cmd.stop()
      pool.shutdown()
