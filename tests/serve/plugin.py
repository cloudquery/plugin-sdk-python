import json
import os
import random
import grpc
import time
import pyarrow as pa
from concurrent import futures
from cloudquery.sdk.schema import Table, Column
from cloudquery.sdk import serve
from cloudquery.plugin_v3 import plugin_pb2_grpc, plugin_pb2, arrow
from cloudquery.sdk.internal.memdb import MemDB

test_table = Table(
    "test",
    [
        Column("id", pa.int64()),
        Column("name", pa.string()),
    ],
)


def test_plugin_serve():
    p = MemDB()
    cmd = serve.PluginCommand(p)
    port = random.randint(5000, 50000)
    pool = futures.ThreadPoolExecutor(max_workers=1)
    pool.submit(cmd.run, ["serve", "--address", f"[::]:{port}"])
    time.sleep(1)
    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = plugin_pb2_grpc.PluginStub(channel)
            response = stub.GetName(plugin_pb2.GetName.Request())
            assert response.name == "memdb"

            response = stub.GetVersion(plugin_pb2.GetVersion.Request())
            assert response.version == "development"

            response = stub.Init(plugin_pb2.Init.Request(spec=b""))
            assert response is not None

            def writer_iterator():
                buf = arrow.schema_to_bytes(test_table.to_arrow_schema())
                yield plugin_pb2.Write.Request(
                    migrate_table=plugin_pb2.Write.MessageMigrateTable(table=buf)
                )
                record = pa.RecordBatch.from_arrays(
                    [
                        pa.array([1, 2, 3]),
                        pa.array(["a", "b", "c"]),
                    ],
                    schema=test_table.to_arrow_schema(),
                )
                yield plugin_pb2.Write.Request(
                    insert=plugin_pb2.Write.MessageInsert(
                        record=arrow.record_to_bytes(record)
                    )
                )

            stub.Write(writer_iterator())

            response = stub.GetTables(plugin_pb2.GetTables.Request(tables=["*"]))
            schemas = arrow.new_schemas_from_bytes(response.tables)
            assert len(schemas) == 4

            response = stub.Sync(plugin_pb2.Sync.Request(tables=["*"]))
            total_records = 0
            for msg in response:
                if msg.insert is not None:
                    rec = arrow.new_record_from_bytes(msg.insert.record)
                    total_records += 1
            assert total_records == 1
    finally:
        cmd.stop()
        pool.shutdown()


def test_plugin_package():
    p = MemDB()
    cmd = serve.PluginCommand(p)
    cmd.run(["package", "-m", "test", "v1.0.0", "."])
    assert os.path.isfile("dist/tables.json")
    assert os.path.isfile("dist/package.json")
    assert os.path.isfile("dist/docs/overview.md")
    assert os.path.isfile("dist/plugin-memdb-v1.0.0-linux-amd64.tar")
    assert os.path.isfile("dist/plugin-memdb-v1.0.0-linux-arm64.tar")

    with open("dist/tables.json", "r") as f:
        tables = json.loads(f.read())
        assert tables == [
            {
                "name": "table_1",
                "title": "Table 1",
                "description": "Test Table 1",
                "is_incremental": True,
                "parent": "",
                "relations": ["table_1_relation_1"],
                "columns": [
                    {
                        "name": "name",
                        "type": "string",
                        "description": "",
                        "incremental_key": False,
                        "primary_key": True,
                        "not_null": True,
                        "unique": True,
                    },
                    {
                        "name": "id",
                        "type": "string",
                        "description": "",
                        "incremental_key": True,
                        "primary_key": True,
                        "not_null": True,
                        "unique": True,
                    },
                ],
            },
            {
                "name": "table_1_relation_1",
                "title": "Table 1 Relation 1",
                "description": "Test Table 1 Relation 1",
                "is_incremental": False,
                "parent": "table_1",
                "relations": [],
                "columns": [
                    {
                        "name": "name",
                        "type": "string",
                        "description": "",
                        "incremental_key": False,
                        "primary_key": True,
                        "not_null": True,
                        "unique": True,
                    },
                    {
                        "name": "data",
                        "type": "extension<json<JSONType>>",
                        "description": "",
                        "incremental_key": False,
                        "primary_key": False,
                        "not_null": False,
                        "unique": False,
                    },
                ],
            },
            {
                "name": "table_2",
                "title": "Table 2",
                "description": "Test Table 2",
                "is_incremental": False,
                "parent": "",
                "relations": [],
                "columns": [
                    {
                        "name": "name",
                        "type": "string",
                        "description": "",
                        "incremental_key": False,
                        "primary_key": True,
                        "not_null": True,
                        "unique": True,
                    },
                    {
                        "name": "id",
                        "type": "string",
                        "description": "",
                        "incremental_key": False,
                        "primary_key": False,
                        "not_null": False,
                        "unique": False,
                    },
                ],
            },
        ]
    with open("dist/package.json", "r") as f:
        package = json.loads(f.read())
        assert package["schema_version"] == 1
        assert package["name"] == "memdb"
        assert package["version"] == "v1.0.0"
        assert package["team"] == "cloudquery"
        assert package["kind"] == "source"
        assert package["message"] == "test"
        assert package["protocols"] == [3]
        assert len(package["supported_targets"]) == 2
        assert package["package_type"] == "docker"
        assert package["supported_targets"][0]["os"] == "linux"
        assert package["supported_targets"][0]["arch"] == "amd64"
        assert (
            package["supported_targets"][0]["path"]
            == "plugin-memdb-v1.0.0-linux-amd64.tar"
        )
        assert (
            package["supported_targets"][0]["docker_image_tag"]
            == "registry.cloudquery.io/cloudquery/source-memdb:v1.0.0-linux-amd64"
        )
        assert package["supported_targets"][1]["os"] == "linux"
        assert package["supported_targets"][1]["arch"] == "arm64"
        assert (
            package["supported_targets"][1]["path"]
            == "plugin-memdb-v1.0.0-linux-arm64.tar"
        )
        assert (
            package["supported_targets"][1]["docker_image_tag"]
            == "registry.cloudquery.io/cloudquery/source-memdb:v1.0.0-linux-arm64"
        )
