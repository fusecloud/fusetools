"""Tests for db_tools module."""

import pandas as pd


def test_import_db_tools() -> None:
    """db_tools should be importable without any optional deps installed."""
    from fusetools import db_tools

    assert hasattr(db_tools, "Oracle")
    assert hasattr(db_tools, "Postgres")
    assert hasattr(db_tools, "Generic")


def test_dump_sql(tmp_path: object) -> None:
    """_dump_sql should write SQL string to file."""
    import os
    import tempfile

    from fusetools.db_tools import _dump_sql

    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "test.sql")
        _dump_sql("SELECT 1", filepath)
        with open(filepath) as f:
            assert f.read() == "SELECT 1"


def test_make_groupby() -> None:
    """make_groupby should generate a GROUP BY clause."""
    from fusetools.db_tools import Generic

    sql = "SELECT col1 as dim1, col2 as dim2, --SPLIT sum(col3) as metric1"
    result = Generic.make_groupby(sql, "--SPLIT")
    assert "GROUP BY" in result
    assert "dim1" in result
    assert "dim2" in result


def test_make_db_cols() -> None:
    """make_db_cols should normalize column names for database standards."""
    from fusetools.db_tools import Generic

    df = pd.DataFrame({"Test #Col": [1], "Another%Val": [2], "Normal": [3]})
    result = Generic.make_db_cols(df)
    assert "test_numcol" in result.columns
    assert "anotherpctval" in result.columns
    assert "normal" in result.columns


def test_make_db_schema() -> None:
    """make_db_schema should create a mapping of pandas types to SQL types."""
    from fusetools.db_tools import Generic

    df = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
            "str_col": ["a", "b", "c"],
        }
    )
    schema = Generic.make_db_schema(df)
    assert "col" in schema.columns
    assert "dtype_final" in schema.columns
    assert len(schema) == 3
