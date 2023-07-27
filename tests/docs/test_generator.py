import glob
import os
import unittest
from tempfile import TemporaryDirectory

import pyarrow as pa

from cloudquery.sdk.docs.generator import Generator
from cloudquery.sdk.schema import Table, Column

dirname = os.path.dirname(__file__)
SNAPSHOT_DIRECTORY = os.path.join(dirname, "snapshots")


def read_snapshot(name):
    with open(os.path.join(SNAPSHOT_DIRECTORY, name)) as f:
        return f.read()


def update_snapshot(name, content):
    with open(os.path.join(SNAPSHOT_DIRECTORY, name), "w") as f:
        f.write(content)


class T(unittest.TestCase):
    def test_docs_generator_markdown(self):
        tables = [
            Table(
                name="test_table",
                title="Test Table",
                columns=[
                    Column("string", pa.string(), primary_key=True),
                    Column("int32", pa.int32()),
                ],
            ),
            Table(
                name="test_table_composite_pk",
                title="Composite PKs",
                is_incremental=True,
                columns=[
                    Column("pk1", pa.string(), primary_key=True, incremental_key=True),
                    Column("pk2", pa.string(), primary_key=True, incremental_key=True),
                    Column("int32", pa.int32()),
                ],
            ),
            Table(
                name="test_table_relations",
                title="Table Relations",
                is_incremental=True,
                columns=[
                    Column("pk1", pa.string(), primary_key=True),
                ],
                relations=[
                    Table(
                        name="test_table_child",
                        title="Child Table",
                        columns=[
                            Column("pk1", pa.string(), primary_key=True),
                            Column("fk1", pa.string()),
                        ],
                        relations=[
                            Table(
                                name="test_table_grandchild",
                                title="Grandchild Table",
                                columns=[Column("pk1", pa.string(), primary_key=True)],
                            )
                        ],
                    )
                ],
            ),
        ]

        # set parent relations
        tables[2].relations[0].parent = tables[2]
        tables[2].relations[0].relations[0].parent = tables[2].relations[0]

        gen = Generator("test_plugin", tables)
        with TemporaryDirectory() as d:
            gen.generate(d, format="markdown")

            files = glob.glob(os.path.join(d, "*.md"))
            file_names = [os.path.basename(f) for f in files]
            assert sorted(file_names) == sorted(
                [
                    "README.md",
                    "test_table_composite_pk.md",
                    "test_table.md",
                    "test_table_relations.md",
                    "test_table_child.md",
                    "test_table_grandchild.md",
                ]
            )

            updated_snapshots = False
            for file_name in file_names:
                with self.subTest(msg=file_name):
                    with open(os.path.join(d, file_name)) as f:
                        content = f.read()
                        try:
                            snapshot = read_snapshot(file_name)
                            self.assertEqual(snapshot, content)
                        except FileNotFoundError:
                            update_snapshot(file_name, content)
                            updated_snapshots = True
            assert not updated_snapshots, "Updated snapshots"
