from unittest.mock import MagicMock, patch

import pendulum
import pytest
from airflow.models import Pool

from dags.operators.db_operator import PgOperator, DbOperator
from dags.operators.exceptions import OperatorException

context = {
    "data_interval_start": pendulum.datetime(2001, 5, 21, 12, 0, 0),
    "data_interval_end": pendulum.datetime(2001, 5, 21, 12, 5, 0),
}


@patch("sqlalchemy.create_engine")
@patch("airflow.models.Pool.get_pools")
def test_pg_operator_execute(
        mock_get_pools: MagicMock, mock_creator: MagicMock, mock_operational_db_connection
):
    """Test that the operator calls the correct methods"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_creator.return_value.execute.return_value.fetchall.return_value = ["some_data", "some_more_data"]

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
    )

    operator.execute(context=context)

    mock_creator.assert_called_once_with(url="http://some_username:super_secret_password@some.host.com:8080/myschema")
    mock_creator.assert_called_once()


@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
@patch("sqlalchemy.create_engine")
def test_pg_to_s3_operator_execute_error_on_pgsql(mock_creator: MagicMock, mock_get_pools: MagicMock, mock_mssql: MagicMock,
                                                  mock_operational_db_connection):
    """Test that the operator raises an exception if there is an error on the sql side"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    mock_engine_execute = MagicMock(side_effect=Exception("test error"))
    mock_mssql.return_value = MagicMock(execute_sql_query=mock_engine_execute)

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
    )

    with pytest.raises(Exception) as e:
        operator.execute(context=context)
    assert "Error while trying to read data from Database" in str(e)

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")


@patch("airflow.models.Pool.get_pools")
def test_build_query_with_chunk_pg(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        column_pk="id",
        chunk_size_kb=1000,
    )

    expected = (
        "SELECT Montero.dbo.People.* "
        "FROM Montero.dbo.People "
        "ORDER BY Montero.dbo.People.id LIMIT {chunk_size} OFFSET {offset}"
    )

    actual = operator.build_query()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_pg(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
    )

    expected = (
        "SELECT Montero.dbo.People.* "
        "FROM Montero.dbo.People "
    )

    actual = operator.build_query()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_get_init_query_pg_with_chunk(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_pool")]

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name="test_db_name",
        schema_name="test_schema_name",
        table_name="test_table_name",
        column_pk="id",
        chunk_size_kb=1000,
    )

    expected = (
        "SELECT test_db_name.test_schema_name.test_table_name.* FROM test_db_name.test_schema_name.test_table_name ORDER BY test_db_name.test_schema_name.test_table_name.id LIMIT {chunk_size} OFFSET {offset}"
    )

    actual = operator.build_query()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_no_db_no_schema_pg(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table_name",
    )

    expected = (
        "SELECT test_table_name.* "
        "FROM test_table_name "
    )

    actual = operator.build_query()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_no_db_pg(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name=None,
        schema_name="test_schema_name",
        table_name="test_table_name",
    )

    expected = (
        "SELECT test_schema_name.test_table_name.* "
        "FROM test_schema_name.test_table_name "
    )

    actual = operator.build_query()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_db_to_s3_operator_no_table(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    with pytest.raises(ValueError):
        DbOperator(
            task_id="test_task",
            db_conn_id="test_sql_conn",
            db_name="test_db_name",
            schema_name="test_schema_name",
            table_name="",
        )


@patch("airflow.models.Pool.get_pools")
def test_db_to_s3_operator_no_schema(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    with pytest.raises(ValueError):
        DbOperator(
            task_id="test_task",
            db_conn_id="test_sql_conn",
            db_name="test_db_name",
            schema_name=None,
            table_name="test_table",
        )




@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
def test_get_table_size_pg(mock_get_pools: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection):
    """Test that the table size is correctly calculated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_list = [[8192]]
    mock_engine_execute = MagicMock(return_value=mock_list)
    mock_mssql.return_value = MagicMock(execute_sql_query=mock_engine_execute)

    operator = PgOperator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        db_name="test_db_name",
        schema_name="test_schema_name",
        table_name="test_table_name",
        column_pk='"id"',
        chunk_size_kb=1,
    )

    expected = 8

    actual = operator.get_table_size()

    assert expected == actual










