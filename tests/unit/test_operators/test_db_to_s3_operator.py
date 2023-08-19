from unittest.mock import MagicMock, patch

import pendulum
import polars as pl
import psycopg2
import pytest
from airflow import DAG
from airflow.models import Pool
from models.utils.datetime_constraints import DateTimeBounds, DateTimeColumnConstraint
from models.utils.operational_events import PIPELINELAYER, PIPELINESTATUS
from operators.db_to_s3_operator import DbToS3Operator, PgToS3Operator, SqlServerToS3Operator
from operators.exceptions import OperatorException

context = {
    "data_interval_start": pendulum.datetime(2001, 5, 21, 12, 0, 0),
    "data_interval_end": pendulum.datetime(2001, 5, 21, 12, 5, 0),
}


@pytest.fixture
def datetime_constraints():
    bounds = DateTimeBounds(start=context["data_interval_start"], end=context["data_interval_end"])

    c1 = DateTimeColumnConstraint.create(
        table_prefix="Montero.dbo.",
        table_name="People",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=None,
    )

    c2 = DateTimeColumnConstraint.create(
        table_prefix="Montero.dbo.",
        table_name="OtherPeople",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=["PeopleId", "OtherPeopleId"],
    )
    return [c1, c2]


@pytest.fixture
def datetime_constraints_same_table():
    bounds = DateTimeBounds(start=context["data_interval_start"], end=context["data_interval_end"])

    c1 = DateTimeColumnConstraint.create(
        table_prefix="Montero.dbo.",
        table_name="OtherPeople",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=["PeopleId", "OtherPeopleId"],
    )

    c2 = DateTimeColumnConstraint.create(
        table_prefix="Montero.dbo.",
        table_name="OtherPeople",
        column_name="BirthDate2",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=["PeopleId", "OtherPeopleId"],
    )
    return [c1, c2]


@pytest.fixture
def datetime_constraints_no_prefix():
    bounds = DateTimeBounds(start=context["data_interval_start"], end=context["data_interval_end"])

    c1 = DateTimeColumnConstraint.create(
        table_prefix="",
        table_name="People",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=None,
    )

    c2 = DateTimeColumnConstraint.create(
        table_prefix="",
        table_name="OtherPeople",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=["PeopleId", "OtherPeopleId"],
    )
    return [c1, c2]


@pytest.fixture
def datetime_constraints_schema_prefix():
    bounds = DateTimeBounds(start=context["data_interval_start"], end=context["data_interval_end"])

    c1 = DateTimeColumnConstraint.create(
        table_prefix="dbo.",
        table_name="People",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=None,
    )

    c2 = DateTimeColumnConstraint.create(
        table_prefix="dbo.",
        table_name="OtherPeople",
        column_name="BirthDate",
        format="YYYY-MM-DD",
        datetime_bounds=bounds,
        target_timezone="UTC",
        join_on_columns=["PeopleId", "OtherPeopleId"],
    )
    return [c1, c2]


@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_sql_server_to_s3_operator_execute(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection
):
    """Test that the operator calls the correct methods"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_df = pl.DataFrame({"Name": ["Alice"], "Age": [25], "City": ["New York"]})
    mock_read_sql_query_with_polars = MagicMock(return_value=mock_df)
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    mock_save_df_to_s3_parquet_format = MagicMock()
    mock_aws_s3.return_value = MagicMock(save_df_to_s3_parquet_format=mock_save_df_to_s3_parquet_format)
    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    with patch.object(SqlServerToS3Operator, "save_operational_event") as mock_method:
        operator.execute(context=context)

    mock_method.assert_called_once_with(PIPELINESTATUS.success, "dt=20010521", "20010521_120000.parquet")

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")
    mock_read_sql_query_with_polars.assert_called_once()
    mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
    mock_save_df_to_s3_parquet_format.assert_called_once_with(
        df=mock_df, partition="dt=20010521", filename="20010521_120000.parquet"
    )


@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_sql_server_to_s3_operator_execute_with_chunk(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection
):
    """Test that the operator calls the correct methods"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_list = [("AllRecruitmentStatisticsEx", "dbo", "18 KB", "8 KB")]
    mock_read_sql_query_with_execute = MagicMock(return_value=mock_list)
    mock_mssql.return_value = MagicMock(execute_sql_query=mock_read_sql_query_with_execute)
    mock_df_1 = pl.DataFrame({"rows_count": [1]})
    mock_df_2 = pl.DataFrame({"Name": ["Alice"], "Age": [25], "City": ["New York"]})
    mock_df_3 = pl.DataFrame({"Name": [], "Age": [], "City": []})
    mock_read_sql_query_with_polars = MagicMock(side_effect=[mock_df_1, mock_df_2, mock_df_3])
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    mock_save_df_to_s3_parquet_format = MagicMock()
    mock_aws_s3.return_value = MagicMock(save_df_to_s3_parquet_format=mock_save_df_to_s3_parquet_format)
    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk="Name",
        chunk_size_kb=1,
    )

    with patch.object(SqlServerToS3Operator, "save_operational_event") as mock_method:
        operator.execute(context=context)

    mock_method.assert_called_with(PIPELINESTATUS.success, "dt=20010521", "20010521_120000_0.parquet")

    mock_mssql.assert_called_with(conn_id="test_sql_conn")
    assert mock_read_sql_query_with_polars.call_count == 3
    mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
    mock_save_df_to_s3_parquet_format.assert_called_once_with(
        df=mock_df_2, partition="dt=20010521", filename="20010521_120000_0.parquet"
    )


@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_pg_to_s3_operator_execute(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection
):
    """Test that the operator calls the correct methods"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_df = pl.DataFrame({"Name": ["Alice"], "Age": [25], "City": ["New York"]})
    mock_read_sql_query_with_polars = MagicMock(return_value=mock_df)
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    mock_save_df_to_s3_parquet_format = MagicMock()
    mock_aws_s3.return_value = MagicMock(save_df_to_s3_parquet_format=mock_save_df_to_s3_parquet_format)
    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    with patch.object(PgToS3Operator, "save_operational_event") as mock_method:
        operator.execute(context=context)

    mock_method.assert_called_once_with(PIPELINESTATUS.success, "dt=20010521", "20010521_120000.parquet")

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")
    mock_read_sql_query_with_polars.assert_called_once()
    mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
    mock_save_df_to_s3_parquet_format.assert_called_once_with(
        df=mock_df, partition="dt=20010521", filename="20010521_120000.parquet"
    )


@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
def test_sql_server_to_s3_operator_execute_error_on_mssql(
    mock_get_pools: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection
):
    """Test that the operator raises an exception if there is an error on the sql side"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_read_sql_query_with_polars = MagicMock(side_effect=Exception("test error"))
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    with patch.object(SqlServerToS3Operator, "save_operational_event") as mock_method:
        try:
            operator.execute(context=context)
        except Exception as err:
            assert isinstance(err, OperatorException)

    mock_method.assert_called_once_with(PIPELINESTATUS.failure, "dt=20010521", "20010521_120000.parquet")

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")
    mock_read_sql_query_with_polars.assert_called_once()


@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
def test_pg_to_s3_operator_execute_error_on_pgsql(mock_get_pools: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection):
    """Test that the operator raises an exception if there is an error on the sql side"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_read_sql_query_with_polars = MagicMock(side_effect=Exception("test error"))
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    with patch.object(PgToS3Operator, "save_operational_event") as mock_method:
        try:
            operator.execute(context=context)
        except Exception as err:
            assert isinstance(err, OperatorException)

    mock_method.assert_called_once_with(PIPELINESTATUS.failure, "dt=20010521", "20010521_120000.parquet")

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")
    mock_read_sql_query_with_polars.assert_called_once()


@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_sql_server_to_s3_operator_execute_error_on_s3(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection
):
    """Test that the operator raises an exception if there is an error on the s3 side"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_df = pl.DataFrame({"Name": ["Alice"], "Age": [25], "City": ["New York"]})
    mock_read_sql_query_with_polars = MagicMock(return_value=mock_df)
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    mock_save_df_to_s3_parquet_format = MagicMock(side_effect=Exception("test error"))
    mock_aws_s3.return_value = MagicMock(save_df_to_s3_parquet_format=mock_save_df_to_s3_parquet_format)
    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    with patch.object(SqlServerToS3Operator, "save_operational_event") as mock_method:
        try:
            operator.execute(context=context)
        except Exception as err:
            assert isinstance(err, OperatorException)

    mock_method.assert_called_once_with(PIPELINESTATUS.failure, "dt=20010521", "20010521_120000.parquet")

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")
    mock_read_sql_query_with_polars.assert_called_once()
    mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
    mock_save_df_to_s3_parquet_format.assert_called_once_with(
        df=mock_df, partition="dt=20010521", filename="20010521_120000.parquet"
    )


@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_pg_to_s3_operator_execute_error_on_s3(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection
):
    """Test that the operator raises an exception if there is an error on the s3 side"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_df = pl.DataFrame({"Name": ["Alice"], "Age": [25], "City": ["New York"]})
    mock_read_sql_query_with_polars = MagicMock(return_value=mock_df)
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    mock_save_df_to_s3_parquet_format = MagicMock(side_effect=Exception("test error"))
    mock_aws_s3.return_value = MagicMock(save_df_to_s3_parquet_format=mock_save_df_to_s3_parquet_format)
    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    with patch.object(PgToS3Operator, "save_operational_event") as mock_method:
        try:
            operator.execute(context=context)
        except Exception as err:
            assert isinstance(err, OperatorException)

    mock_method.assert_called_once_with(PIPELINESTATUS.failure, "dt=20010521", "20010521_120000.parquet")

    mock_mssql.assert_called_once_with(conn_id="test_sql_conn")
    mock_read_sql_query_with_polars.assert_called_once()
    mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
    mock_save_df_to_s3_parquet_format.assert_called_once_with(
        df=mock_df, partition="dt=20010521", filename="20010521_120000.parquet"
    )


@pytest.mark.parametrize("some_status", [(PIPELINESTATUS.success,), (PIPELINESTATUS.failure,)])
@patch("models.data_model.OpEvents")
@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_save_operational_event(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_opevents: MagicMock, some_status: PIPELINESTATUS
):
    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    mock_aws_s3.return_value = MagicMock(bucket_name="SOME_BUCKET", key="SOME_KEY")
    mock_mssql.return_value = MagicMock()

    mock_save_operational_event = MagicMock()
    mock_opevents.return_value = MagicMock(save_operational_event=mock_save_operational_event)

    operator = SqlServerToS3Operator(
        task_id="SOME_TASK_ID",
        dag=DAG(dag_id="DAG_ID", start_date=pendulum.now()),
        db_conn_id="SQL_CONN_ID",
        s3_conn_id="S3_CONN_ID",
        db_name="SOME_DB",
        schema_name="SOME_SCHEMA",
        table_name="SOME_TABLE",
        s3_bucket="S3_BUCKET",
        s3_key="S3_KEY",
        date_column="DATE_COLUMN",
        timestamp_column="TS_COLUMN",
        updatedat_column="UPD_COLUMN",
        date_format="DATE_FORMAT",
        timestamp_format="TS_FORMAT",
        updatedat_format="UPD_FORMAT",
        target_timezone="SOMETZ",
    )

    mock_opevent = MagicMock()
    with patch("models.utils.operational_events.OperationalEvent", return_value=mock_opevent) as mock_opevent_ctor:
        operator.save_operational_event(some_status, "PARTITION", "FNAME")

    mock_save_operational_event.assert_called_once_with(mock_opevent)

    mock_opevent_ctor.assert_called_once_with(
        dag_id="DAG_ID",
        task_id="SOME_TASK_ID",
        source_database="SOME_DB",
        source_schema="SOME_SCHEMA",
        source_table="SOME_TABLE",
        target_s3_bucket="SOME_BUCKET",
        target_key="SOME_KEY",
        target_partition="PARTITION",
        target_filename="FNAME",
        pipelinelayer=PIPELINELAYER.landing,
        pipelinestatus=some_status,
        task_configuration={},
    )


@pytest.mark.parametrize("some_status", [(PIPELINESTATUS.success,), (PIPELINESTATUS.failure,)])
@patch("models.data_model.OpEvents")
@patch("models.data_model.Database")
@patch("models.data_model.AwsS3")
@patch("airflow.models.Pool.get_pools")
def test_bad_save_operational_event(
    mock_get_pools: MagicMock, mock_aws_s3: MagicMock, mock_mssql: MagicMock, mock_opevents: MagicMock, some_status: PIPELINESTATUS
):
    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    mock_aws_s3.return_value = MagicMock(bucket_name="SOME_BUCKET", key="SOME_KEY")
    mock_mssql.return_value = MagicMock()

    def simulated_failure(*args, **kwargs):
        raise psycopg2.OperationalError()

    mock_save_operational_event = MagicMock(side_effect=simulated_failure)
    mock_opevents.return_value = MagicMock(save_operational_event=mock_save_operational_event, dsn="blahblah")

    operator = SqlServerToS3Operator(
        task_id="SOME_TASK_ID",
        dag=DAG(dag_id="DAG_ID", start_date=pendulum.now()),
        db_conn_id="SQL_CONN_ID",
        s3_conn_id="S3_CONN_ID",
        db_name="SOME_DB",
        schema_name="SOME_SCHEMA",
        table_name="SOME_TABLE",
        s3_bucket="S3_BUCKET",
        s3_key="S3_KEY",
        date_column="DATE_COLUMN",
        timestamp_column="TS_COLUMN",
        updatedat_column="UPD_COLUMN",
        date_format="DATE_FORMAT",
        timestamp_format="TS_FORMAT",
        updatedat_format="UPD_FORMAT",
        target_timezone="SOMETZ",
    )

    mock_opevent = MagicMock()
    with patch("models.utils.operational_events.OperationalEvent", return_value=mock_opevent) as mock_opevent_ctor:
        with pytest.raises(OperatorException) as ex:
            operator.save_operational_event(some_status, "PARTITION", "FNAME")

    mock_save_operational_event.assert_called_once_with(mock_opevent)

    mock_opevent_ctor.assert_called_once_with(
        dag_id="DAG_ID",
        task_id="SOME_TASK_ID",
        source_database="SOME_DB",
        source_schema="SOME_SCHEMA",
        source_table="SOME_TABLE",
        target_s3_bucket="SOME_BUCKET",
        target_key="SOME_KEY",
        target_partition="PARTITION",
        target_filename="FNAME",
        pipelinelayer=PIPELINELAYER.landing,
        pipelinestatus=some_status,
        task_configuration={},
    )

    assert str(ex.value) == "I cannot insert operational event to the db with dsn blahblah"


@patch("airflow.models.Pool.get_pools")
def test_build_query(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT Montero.dbo.People.* "
        "FROM Montero.dbo.People WITH (NOLOCK) "
        "JOIN Montero.dbo.OtherPeople WITH (NOLOCK) ON Montero.dbo.People.PeopleId = Montero.dbo.OtherPeople.OtherPeopleId WHERE "
        "(Montero.dbo.People.BirthDate >= '2001-05-21' AND Montero.dbo.People.BirthDate < '2001-05-21') "
        "OR (Montero.dbo.OtherPeople.BirthDate >= '2001-05-21' AND Montero.dbo.OtherPeople.BirthDate < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_with_chunk_sql(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk="id",
        chunk_size_kb=1000,
    )

    expected = (
        "SELECT Montero.dbo.People.* "
        "FROM Montero.dbo.People WITH (NOLOCK) "
        "JOIN Montero.dbo.OtherPeople WITH (NOLOCK) ON Montero.dbo.People.PeopleId = Montero.dbo.OtherPeople.OtherPeopleId WHERE "
        "(Montero.dbo.People.BirthDate >= '2001-05-21' AND Montero.dbo.People.BirthDate < '2001-05-21') "
        "OR (Montero.dbo.OtherPeople.BirthDate >= '2001-05-21' AND Montero.dbo.OtherPeople.BirthDate < '2001-05-21')"
        " ORDER BY Montero.dbo.People.id OFFSET {offset} "
        "ROWS FETCH NEXT {chunk_size} ROWS ONLY"
    )

    actual = operator.build_query(datetime_constraints)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_with_chunk_pg(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk="id",
        chunk_size_kb=1000,
    )

    expected = (
        "SELECT Montero.dbo.People.* "
        "FROM Montero.dbo.People "
        "JOIN Montero.dbo.OtherPeople ON Montero.dbo.People.PeopleId = Montero.dbo.OtherPeople.OtherPeopleId WHERE "
        "(Montero.dbo.People.BirthDate >= '2001-05-21' AND Montero.dbo.People.BirthDate < '2001-05-21') "
        "OR (Montero.dbo.OtherPeople.BirthDate >= '2001-05-21' AND Montero.dbo.OtherPeople.BirthDate < '2001-05-21')"
        " ORDER BY Montero.dbo.People.id LIMIT {chunk_size} OFFSET {offset}"
    )

    actual = operator.build_query(datetime_constraints)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_same_table_constraints(
    mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints_same_table
):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT Montero.dbo.People.* FROM Montero.dbo.People WITH (NOLOCK) JOIN Montero.dbo.OtherPeople WITH (NOLOCK) "
        "ON Montero.dbo.People.PeopleId = Montero.dbo.OtherPeople.OtherPeopleId WHERE "
        "(Montero.dbo.OtherPeople.BirthDate >= '2001-05-21' AND Montero.dbo.OtherPeople.BirthDate < '2001-05-21') OR "
        "(Montero.dbo.OtherPeople.BirthDate2 >= '2001-05-21' AND Montero.dbo.OtherPeople.BirthDate2 < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints_same_table)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_get_init_query_sql_server_with_chunk(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk="id",
        chunk_size_kb=1000,
    )

    expected = (
        "SELECT Montero.dbo.People.* FROM Montero.dbo.People WITH (NOLOCK) ORDER BY Montero.dbo.People.id OFFSET {offset} "
        "ROWS FETCH NEXT {chunk_size} ROWS ONLY"
    )

    actual = operator.build_query(datetime_constraints=[None, None, None])

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_pg(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT Montero.dbo.People.* "
        "FROM Montero.dbo.People "
        "JOIN Montero.dbo.OtherPeople ON Montero.dbo.People.PeopleId = Montero.dbo.OtherPeople.OtherPeopleId WHERE "
        "(Montero.dbo.People.BirthDate >= '2001-05-21' AND Montero.dbo.People.BirthDate < '2001-05-21') "
        "OR (Montero.dbo.OtherPeople.BirthDate >= '2001-05-21' AND Montero.dbo.OtherPeople.BirthDate < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_get_init_query_pg_with_chunk(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk="id",
        chunk_size_kb=1000,
    )

    expected = (
        "SELECT Montero.dbo.People.* FROM Montero.dbo.People ORDER BY Montero.dbo.People.id LIMIT {chunk_size} OFFSET {offset}"
    )

    actual = operator.build_query(datetime_constraints=[None, None, None])

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_no_db_no_schema_sql_server(
    mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints_no_prefix
):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT People.* "
        "FROM People WITH (NOLOCK) "
        "JOIN OtherPeople WITH (NOLOCK) ON People.PeopleId = OtherPeople.OtherPeopleId WHERE "
        "(People.BirthDate >= '2001-05-21' AND People.BirthDate < '2001-05-21') "
        "OR (OtherPeople.BirthDate >= '2001-05-21' AND OtherPeople.BirthDate < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints_no_prefix)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_no_db_no_schema_pg(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints_no_prefix):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name=None,
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT People.* "
        "FROM People "
        "JOIN OtherPeople ON People.PeopleId = OtherPeople.OtherPeopleId WHERE "
        "(People.BirthDate >= '2001-05-21' AND People.BirthDate < '2001-05-21') "
        "OR (OtherPeople.BirthDate >= '2001-05-21' AND OtherPeople.BirthDate < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints_no_prefix)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_no_db_sql_server(
    mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints_schema_prefix
):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT dbo.People.* "
        "FROM dbo.People WITH (NOLOCK) "
        "JOIN dbo.OtherPeople WITH (NOLOCK) ON dbo.People.PeopleId = dbo.OtherPeople.OtherPeopleId WHERE "
        "(dbo.People.BirthDate >= '2001-05-21' AND dbo.People.BirthDate < '2001-05-21') "
        "OR (dbo.OtherPeople.BirthDate >= '2001-05-21' AND dbo.OtherPeople.BirthDate < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints_schema_prefix)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_build_query_no_db_pg(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints_schema_prefix):
    """Test that the query is correctly generated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name=None,
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = (
        "SELECT dbo.People.* "
        "FROM dbo.People "
        "JOIN dbo.OtherPeople ON dbo.People.PeopleId = dbo.OtherPeople.OtherPeopleId WHERE "
        "(dbo.People.BirthDate >= '2001-05-21' AND dbo.People.BirthDate < '2001-05-21') "
        "OR (dbo.OtherPeople.BirthDate >= '2001-05-21' AND dbo.OtherPeople.BirthDate < '2001-05-21');"
    )

    actual = operator.build_query(datetime_constraints_schema_prefix)

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
def test_db_to_s3_operator_no_table(mock_get_pools: MagicMock, mock_operational_db_connection, datetime_constraints):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    with pytest.raises(ValueError):
        DbToS3Operator(
            task_id="test_task",
            db_conn_id="test_sql_conn",
            s3_conn_id="test_s3_conn",
            db_name="Montero",
            schema_name="dbo",
            table_name="",
            s3_bucket="test_bucket",
            s3_key="test_key",
        )


@patch("airflow.models.Pool.get_pools")
def test_db_to_s3_operator_no_schema(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    with pytest.raises(ValueError):
        DbToS3Operator(
            task_id="test_task",
            db_conn_id="test_sql_conn",
            s3_conn_id="test_s3_conn",
            db_name="Montero",
            schema_name=None,
            table_name="test_table",
            s3_bucket="test_bucket",
            s3_key="test_key",
        )


@patch("airflow.models.Pool.get_pools")
def test_get_filter_names(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    t = DbToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
        date_join_on=["id1", "id2"],
        date_column="t1.id1",
        timestamp_join_on=["id1", "id2"],
        timestamp_column="t2.id1",
        updatedat_join_on=["id1", "id2"],
        updatedat_column="t3.id1",
    )

    actual = t._get_filter_names()

    expected = {
        "date_column_table_name": "t1",
        "date_column_name": "id1",
        "timestamp_column_table_name": "t2",
        "timestamp_column_name": "id1",
        "updatedat_column_table_name": "t3",
        "updatedat_column_name": "id1",
    }

    assert actual == expected


@patch("airflow.models.Pool.get_pools")
def test_get_filter_names_error_date(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    t = DbToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
        date_join_on=["id1", "id2"],
        date_column="id1",
    )

    with pytest.raises(OperatorException):
        t._get_filter_names()


@patch("airflow.models.Pool.get_pools")
def test_get_filter_names_error_timestamp(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    t = DbToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
        timestamp_join_on=["id1", "id2"],
        timestamp_column="id1",
    )

    with pytest.raises(OperatorException):
        t._get_filter_names()


@patch("airflow.models.Pool.get_pools")
def test_get_filter_names_error_updatedat(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test that the appropriate exception is raised"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]

    t = DbToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="test_table",
        s3_bucket="test_bucket",
        s3_key="test_key",
        updatedat_join_on=["id1", "id2"],
        updatedat_column="id1",
    )

    with pytest.raises(OperatorException):
        t._get_filter_names()


@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
def test_get_table_size_pg(mock_get_pools: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection):
    """Test that the table size is correctly calculated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_df = pl.DataFrame({"pg_total_relation_size": [8192]})
    mock_read_sql_query_with_polars = MagicMock(return_value=mock_df)
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    operator = PgToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk='"id"',
        chunk_size_kb=1,
    )

    expected = 8

    actual = operator.get_table_size()

    assert expected == actual


@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
def test_get_table_size_sql_server(mock_get_pools: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection):
    """Test that the table size is correctly calculated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_list = [("AllRecruitmentStatisticsEx", "dbo", "18 KB", "8 KB")]
    mock_read_sql_query_with_execute = MagicMock(return_value=mock_list)
    mock_mssql.return_value = MagicMock(execute_sql_query=mock_read_sql_query_with_execute)

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk='"id"',
        chunk_size_kb=1,
    )

    expected = 8

    actual = operator.get_table_size()

    assert expected == actual


@patch("models.data_model.Database")
@patch("airflow.models.Pool.get_pools")
def test_get_table_rows(mock_get_pools: MagicMock, mock_mssql: MagicMock, mock_operational_db_connection):
    """Test that the table size is correctly calculated"""

    mock_get_pools.return_value = [Pool(pool="test_sql_conn")]
    mock_df = pl.DataFrame({"rows_count": [10]})
    mock_read_sql_query_with_polars = MagicMock(return_value=mock_df)
    mock_mssql.return_value = MagicMock(read_sql_query_with_polars=mock_read_sql_query_with_polars)

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk='"id"',
        chunk_size_kb=1,
    )

    expected = 10

    actual = operator.get_table_rows_count()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
@patch("operators.db_to_s3_operator.SqlServerToS3Operator.get_table_rows_count")
@patch("operators.db_to_s3_operator.SqlServerToS3Operator.get_table_size")
def test_get_chunk_size_kb_rows_with_chunk(
    mock_table_size: MagicMock, mock_row_count: MagicMock, mock_get_pools: MagicMock, mock_operational_db_connection
):
    """Test that the table size is correctly calculated"""
    mock_row_count.return_value = 10
    mock_table_size.return_value = 18

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
        column_pk='"id"',
        chunk_size_kb=4,
    )

    expected = 2

    actual = operator.get_number_of_rows_per_chunk()

    assert expected == actual


@patch("airflow.models.Pool.get_pools")
@patch("operators.db_to_s3_operator.SqlServerToS3Operator.get_table_rows_count")
@patch("operators.db_to_s3_operator.SqlServerToS3Operator.get_table_size")
def test_get_chunk_size_kb_rows_without_chunk(
    mock_table_size: MagicMock, mock_row_count: MagicMock, mock_get_pools: MagicMock, mock_operational_db_connection
):
    """Test that the table size is correctly calculated"""
    mock_row_count.return_value = 10
    mock_table_size.return_value = 18

    operator = SqlServerToS3Operator(
        task_id="test_task",
        db_conn_id="test_sql_conn",
        s3_conn_id="test_s3_conn",
        db_name="Montero",
        schema_name="dbo",
        table_name="People",
        s3_bucket="test_bucket",
        s3_key="test_key",
    )

    expected = None

    actual = operator.get_number_of_rows_per_chunk()

    assert expected == actual
