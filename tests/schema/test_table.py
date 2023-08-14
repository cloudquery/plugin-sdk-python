import pyarrow as pa
import pytest

from cloudquery.sdk.schema import Table, Column, filter_dfs
from cloudquery.sdk.schema.table import flatten_tables


def test_table():
    table = Table("test_table", [Column("test_column", pa.int32())])
    table.to_arrow_schema()


def test_filter_dfs_warns_no_matches():
    with pytest.raises(ValueError):
        tables = [Table("test1", []), Table("test2", [])]
        filter_dfs(tables, include_tables=["test3"], skip_tables=[])

    with pytest.raises(ValueError):
        tables = [Table("test1", []), Table("test2", [])]
        filter_dfs(tables, include_tables=["*"], skip_tables=["test3"])


def test_filter_dfs():
    table_grandchild = Table("test_grandchild", [Column("test_column", pa.int32())])
    table_child = Table(
        "test_child",
        [Column("test_column", pa.int32())],
        relations=[
            table_grandchild,
        ],
    )
    table_top1 = Table(
        "test_top1",
        [Column("test_column", pa.int32())],
        relations=[
            table_child,
        ],
    )
    table_top2 = Table("test_top2", [Column("test_column", pa.int32())])

    tables = [table_top1, table_top2]

    cases = [
        {
            "include_tables": ["*"],
            "skip_tables": [],
            "skip_dependent_tables": False,
            "expect_top": ["test_top1", "test_top2"],
            "expect_flattened": [
                "test_top1",
                "test_top2",
                "test_child",
                "test_grandchild",
            ],
        },
        {
            "include_tables": ["*"],
            "skip_tables": ["test_top1"],
            "skip_dependent_tables": False,
            "expect_top": ["test_top2"],
            "expect_flattened": ["test_top2"],
        },
        {
            "include_tables": ["test_top1"],
            "skip_tables": ["test_top2"],
            "skip_dependent_tables": True,
            "expect_top": ["test_top1"],
            "expect_flattened": ["test_top1"],
        },
        {
            "include_tables": ["test_child"],
            "skip_tables": [],
            "skip_dependent_tables": True,
            "expect_top": ["test_top1"],
            "expect_flattened": ["test_top1", "test_child"],
        },
    ]
    for case in cases:
        got = filter_dfs(
            tables=tables,
            include_tables=case["include_tables"],
            skip_tables=case["skip_tables"],
            skip_dependent_tables=case["skip_dependent_tables"],
        )
        assert sorted([t.name for t in got]) == sorted(case["expect_top"]), case

        got_flattened = flatten_tables(got)
        want_flattened = sorted(case["expect_flattened"])
        got_flattened = sorted([t.name for t in got_flattened])
        assert got_flattened == want_flattened, case
