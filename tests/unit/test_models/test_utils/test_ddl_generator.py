import polars as pl
import pytest
from models.utils.ddl_utils.ddl_generator import PolarsAthenaDdlGenerator
from models.utils.ddl_utils.exceptions import DdlGenerationException


def test_polars_athena_data_type_map(caplog):
    """
    Test that data type mapping from polars to Athena works as expected.
    """
    # Arrange
    schema = {
        "col1": pl.Int64,
        "col2": pl.Float64,
        "col3": pl.Utf8,
        "col4": pl.Datetime("ns"),
        "col5": pl.Boolean,
        "col6": pl.Datetime("us"),
    }
    expected = {"col1": "bigint", "col2": "double", "col3": "string", "col4": "bigint", "col5": "boolean", "col6": "timestamp"}

    # Act
    ddl_generator = PolarsAthenaDdlGenerator()
    actual = ddl_generator.data_type_map(schema)

    # Assert
    assert actual == expected
    assert "Athena does not support nanosecond precision for timestamp in column col4." in caplog.text


def test_polars_athena_data_type_map_decimal(caplog):
    """
    Test handling of decimal data type.
    """
    # Arrange
    schema = {"col1": pl.Decimal(7, 11)}

    expected = {"col1": "decimal(11,7)"}
    ddl_generator = PolarsAthenaDdlGenerator()

    actual = ddl_generator.data_type_map(schema)

    assert actual == expected


def test_polars_athena_data_type_map_uint64(caplog):
    """
    Test handling of unsupported UInt64 data type.
    """
    schema = {"col1": pl.UInt64}
    ddl_generator = PolarsAthenaDdlGenerator()

    with pytest.raises(DdlGenerationException):
        ddl_generator.data_type_map(schema)

    assert "Athena does not support UInt64 for column col1. Cannot use bigint without loosing accuracy." in caplog.text


def test_polars_athena_unsupported_data_type(caplog):
    """
    Test handling of unsupported polars data type.
    """
    schema = {"col1": pl.List}
    ddl_generator = PolarsAthenaDdlGenerator()

    with pytest.raises(DdlGenerationException):
        ddl_generator.data_type_map(schema)

    assert "Unsupported data type" in caplog.text


def test_polars_athena_data_type_map_decimal_error(caplog):
    """
    Test handling of unsupported decimal data type.
    """
    # Arrange
    schema = {"col1": pl.Decimal(4, 39)}
    ddl_generator = PolarsAthenaDdlGenerator()

    with pytest.raises(DdlGenerationException):
        ddl_generator.data_type_map(schema)

    assert "Column col1 error. Maximum precision for decimal is 38." in caplog.text


def test_create_table():
    """
    Test that ddl generation for table creation works as expected.
    """
    schema = {
        "col1": pl.Int64,
        "col2": pl.Float64,
        "col3": pl.Utf8,
        "col4": pl.Datetime("ns"),
        "col5": pl.Boolean,
        "col6": pl.Decimal(7, 11),
    }
    table_name = "test_table"
    database_name = "test_database"
    partition = ("dt", "string")
    location = "s3://test_bucket/test_location"
    file_format = "parquet"

    expected = """
CREATE EXTERNAL TABLE if not exists test_database.test_table (
    col1 bigint,
    col2 double,
    col3 string,
    col4 bigint,
    col5 boolean,
    col6 decimal(11,7)
)
PARTITIONED BY (dt string)
STORED AS parquet
LOCATION 's3://test_bucket/test_location';
"""

    ddl_generator = PolarsAthenaDdlGenerator()
    new_schema = ddl_generator.data_type_map(schema)
    actual = ddl_generator.create_table(new_schema, database_name, table_name, location, partition, file_format)

    assert actual == expected


def test_create_table_no_partition():
    """
    Test that ddl generation for table creation works as expected in the case of no partitioning.
    """
    schema = {
        "col1": pl.Int64,
        "col2": pl.Float64,
        "col3": pl.Utf8,
        "col4": pl.Datetime("ns"),
        "col5": pl.Boolean,
        "col6": pl.Decimal(7, 11),
    }
    table_name = "test_table"
    database_name = "test_database"
    partition = None
    location = "s3://test_bucket/test_location"
    file_format = "parquet"

    expected = """
CREATE EXTERNAL TABLE if not exists test_database.test_table (
    col1 bigint,
    col2 double,
    col3 string,
    col4 bigint,
    col5 boolean,
    col6 decimal(11,7)
)
STORED AS parquet
LOCATION 's3://test_bucket/test_location';
"""

    ddl_generator = PolarsAthenaDdlGenerator()
    new_schema = ddl_generator.data_type_map(schema)
    actual = ddl_generator.create_table(new_schema, database_name, table_name, location, partition, file_format)

    assert actual == expected
