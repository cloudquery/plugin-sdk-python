import json
import os
import random
from uuid import UUID
import grpc
import time
import pyarrow as pa
from concurrent import futures
from cloudquery.sdk.schema import Table, Column
from cloudquery.sdk import serve
from cloudquery.plugin_v3 import plugin_pb2_grpc, plugin_pb2, arrow
from cloudquery.sdk.internal.memdb import MemDB
from cloudquery.sdk.types.json import JSONType
from cloudquery.sdk.types.uuid import UUIDType

test_table = Table(
    "test_table",
    [
        Column("id", pa.int64()),
        Column("name", pa.string()),
        Column("json", JSONType()),
        Column("uuid", UUIDType()),
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
                        pa.array([None, b"{}", b'{"a":null}']),
                        pa.array(
                            [
                                None,
                                UUID("550e8400-e29b-41d4-a716-446655440000").bytes,
                                UUID("123e4567-e89b-12d3-a456-426614174000").bytes,
                            ],
                            type=pa.binary(16),
                        ),
                    ],
                    schema=test_table.to_arrow_schema(),
                )
                yield plugin_pb2.Write.Request(
                    insert=plugin_pb2.Write.MessageInsert(
                        record=arrow.record_to_bytes(record)
                    )
                )

            stub.Write(writer_iterator())

            response = stub.GetTables(
                plugin_pb2.GetTables.Request(tables=["*"], skip_tables=[])
            )
            schemas = arrow.new_schemas_from_bytes(response.tables)
            assert len(schemas) == 3

            response = stub.Sync(plugin_pb2.Sync.Request(tables=["*"], skip_tables=[]))
            total_migrate_tables = 0
            total_records = 0
            total_errors = 0
            for msg in response:
                message_type = msg.WhichOneof("message")
                if message_type == "insert":
                    rec = arrow.new_record_from_bytes(msg.insert.record)
                    assert rec.num_rows > 0
                    total_records += 1
                elif message_type == "migrate_table":
                    total_migrate_tables += 1
                elif message_type == "error":
                    total_errors += 1
                else:
                    raise NotImplementedError(f"Unknown message type {type(msg)}")
            assert total_migrate_tables == 3
            assert total_records == 15
            assert total_errors == 0
    finally:
        cmd.stop()
        pool.shutdown()


def test_plugin_read():
    p = MemDB()
    sample_record_1 = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3]),
            pa.array(["a", "b", "c"]),
            pa.array([None, b"{}", b'{"a":null}']),
            pa.array(
                [
                    None,
                    UUID("550e8400-e29b-41d4-a716-446655440000").bytes,
                    UUID("123e4567-e89b-12d3-a456-426614174000").bytes,
                ],
                type=pa.binary(16),
            ),
        ],
        schema=test_table.to_arrow_schema(),
    )
    sample_record_2 = pa.RecordBatch.from_arrays(
        [
            pa.array([2, 3, 4]),
            pa.array(["b", "c", "d"]),
            pa.array([b'""', b'{"a":true}', b'{"b":1}']),
            pa.array(
                [
                    UUID("9bba4c2a-1a37-4fbe-b489-6b40303a8a25").bytes,
                    None,
                    UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6").bytes,
                ],
                type=pa.binary(16),
            ),
        ],
        schema=test_table.to_arrow_schema(),
    )
    p._db = [sample_record_1, sample_record_2]

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

            request = plugin_pb2.Read.Request(
                table=arrow.schema_to_bytes(test_table.to_arrow_schema())
            )
            reader_iterator = stub.Read(request)

            output_records = []
            for msg in reader_iterator:
                output_records.append(arrow.new_record_from_bytes(msg.record))

            assert len(output_records) == 2
            assert output_records[0].equals(sample_record_1)
            assert output_records[1].equals(sample_record_2)
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
                        "type": "int64",
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
                        "type": "json",
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
                        "type": "int64",
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
            == "docker.cloudquery.io/cloudquery/source-memdb:v1.0.0-linux-amd64"
        )
        assert package["supported_targets"][1]["os"] == "linux"
        assert package["supported_targets"][1]["arch"] == "arm64"
        assert (
            package["supported_targets"][1]["path"]
            == "plugin-memdb-v1.0.0-linux-arm64.tar"
        )
        assert (
            package["supported_targets"][1]["docker_image_tag"]
            == "docker.cloudquery.io/cloudquery/source-memdb:v1.0.0-linux-arm64"
        )
