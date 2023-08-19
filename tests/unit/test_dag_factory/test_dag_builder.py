from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG
from airflow.models import Pool
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from callbacks.slack_alerts import slack_alert_failure
from dagfactory.dagbuilder import DagBuilder
from dagfactory.exceptions import DagFactoryConfigException
from operators.db_to_s3_operator import PgToS3Operator
from pendulum import DateTime
from pendulum.tz.timezone import Timezone


@pytest.fixture()
def default_config_fixture():
    return {
        "test_dict_key": {
            "test_key": "test_value",
        },
        "test_list_key": ["test_value_1", "test_value_2"],
        "test_nested_dict_key": {
            "test_dict_key": {
                "test_nested_value": "test_value",
            },
        },
    }


def test_get_dag_params(tmpdir, default_config_fixture):
    """Test that dag params are returned correctly"""

    template_path = str(tmpdir)

    config = {
        "default_args": {
            "start_date": "2010-01-01",
            "end_date": "2020-01-01",
            "retry_delay_sec": 200,
            "sla_secs": 12,
        },
        "template_searchpath": template_path,
        "dagrun_timeout_sec": 10,
    }

    expected_dag_params = {
        "dag_id": "test_dag",
        "test_dict_key": {"test_key": "test_value"},
        "test_list_key": ["test_value_1", "test_value_2"],
        "test_nested_dict_key": {"test_dict_key": {"test_nested_value": "test_value"}},
        "default_args": {
            "start_date": DateTime(2010, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
            "end_date": DateTime(2020, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
            "retry_delay": timedelta(seconds=200),
            "sla": timedelta(seconds=12),
            "on_failure_callback": slack_alert_failure,
        },
        "template_searchpath": template_path,
        "dagrun_timeout": timedelta(seconds=10),
    }

    dag = DagBuilder(dag_name="test_dag", dag_config=config, default_config=default_config_fixture)
    actual_dag_params = dag.get_dag_params()

    assert actual_dag_params == expected_dag_params


def test_get_dag_params_no_callback(tmpdir, default_config_fixture):
    """Test that dag params are returned correctly"""

    template_path = str(tmpdir)

    config = {
        "default_args": {
            "start_date": "2010-01-01",
            "end_date": "2020-01-01",
            "retry_delay_sec": 200,
            "sla_secs": 12,
        },
        "template_searchpath": template_path,
        "dagrun_timeout_sec": 10,
    }

    expected_dag_params = {
        "dag_id": "integration_test_dag",
        "test_dict_key": {"test_key": "test_value"},
        "test_list_key": ["test_value_1", "test_value_2"],
        "test_nested_dict_key": {"test_dict_key": {"test_nested_value": "test_value"}},
        "default_args": {
            "start_date": DateTime(2010, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
            "end_date": DateTime(2020, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC")),
            "retry_delay": timedelta(seconds=200),
            "sla": timedelta(seconds=12),
        },
        "template_searchpath": template_path,
        "dagrun_timeout": timedelta(seconds=10),
    }

    dag = DagBuilder(dag_name="integration_test_dag", dag_config=config, default_config=default_config_fixture)
    actual_dag_params = dag.get_dag_params()

    assert actual_dag_params == expected_dag_params


def test_get_dag_params_with_invalid_template_searchpath(default_config_fixture):
    """Test that dag params are returned correctly"""

    config = {
        "default_args": {
            "start_date": "2010-01-01",
            "end_date": "2020-01-01",
            "retry_delay_sec": 200,
            "sla_secs": 12,
        },
        "template_searchpath": "not/a/valid/path",
    }

    with pytest.raises(DagFactoryConfigException) as e:
        dag = DagBuilder(dag_name="test_dag", dag_config=config, default_config=default_config_fixture)
        dag.get_dag_params()
    assert "Invalid template search path" in str(e.value)


def test_get_dag_params_exception_missing_start_date(caplog, default_config_fixture):
    """Test that exception is raised when start_date is missing from config"""

    config = {
        "default_args": {
            "end_date": "2020-01-01",
            "retry_delay_sec": 200,
            "sla_secs": 12,
        },
        "dagrun_timeout_sec": 10,
    }

    with pytest.raises(DagFactoryConfigException) as e:
        dag = DagBuilder(dag_name="test_dag", dag_config=config, default_config=default_config_fixture)
        dag.get_dag_params()

    assert "test_dag DAG config missing start_date" in caplog.text
    assert "ERROR" in caplog.text
    assert "DAG config missing start_date" in str(e.value)


def test_get_dag_params_exception_merge_configs():
    """Test that exception is raised when merge_configs fails"""

    config = {
        "default_args": {
            "owner": "airflow",
            "retries": 1,
            "retry_delay_sec": 300,
        },
    }

    try:
        dag = DagBuilder(dag_name="test_dag", dag_config=[], default_config=config)
        dag.get_dag_params()
    except DagFactoryConfigException as err:
        assert "Failed to merge config with default config" in str(err)


def test_make_task_valid():
    """Test that valid task is created"""

    operator = "airflow.operators.bash.BashOperator"
    task_params = {
        "task_id": "test_1",
        "bash_command": "echo_1",
        "retries": 2,
    }

    actual = DagBuilder.make_task(operator, task_params)

    assert actual.task_id == "test_1"
    assert actual.retries == 2
    assert actual.bash_command == "echo_1"
    assert isinstance(actual, BashOperator)


@patch("airflow.models.Pool.get_pools")
@patch("airflow.models.Variable.get")
def test_make_task_valid_with_s3_bucket_variable(
    mock_airflow_variable: MagicMock, mock_get_pools: MagicMock, mock_operational_db_connection, caplog
):
    """Test that valid task is created"""
    mock_get_pools.return_value = [Pool(pool="test_db_conn_id")]

    operator = "operators.db_to_s3_operator.PgToS3Operator"
    task_params = {
        "task_id": "test_db_conn_id_to_s3",
        "db_conn_id": "test_db_conn_id",
        "s3_conn_id": "test_s3_conn_id",
        "s3_bucket_variable": "s3_bucket",
        "s3_key": "test_key",
        "db_name": "test_db_name",
        "schema_name": "test_schema_name",
        "table_name": "test_table_name",
    }
    mock_airflow_variable.return_value = "test_bucket_airflow_variable"

    actual = DagBuilder.make_task(operator, task_params)

    assert actual.task_id == "test_db_conn_id_to_s3"
    assert actual.db.conn_id == "test_db_conn_id"
    assert actual.aws_s3.conn_id == "test_s3_conn_id"
    assert actual.aws_s3.bucket_name == "test_bucket_airflow_variable"
    assert actual.aws_s3.key == "test_key"
    assert actual.db_name == "test_db_name"
    assert isinstance(actual, PgToS3Operator)
    assert "test_bucket_airflow_variable" in caplog.text


@patch("airflow.models.Variable.get")
def test_make_task_valid_with_body_bearer_dict_variable(mock_airflow_variable: MagicMock, caplog):
    """Test that valid task is created"""

    operator = "operators.birdie_operator.BirdieToS3Operator"
    task_params = {
        "task_id": "test_db_conn_id_to_s3",
        "oauth_url": "test_oauth_url",
        "api_url": "test_api_url",
        "body_bearer_dict_variable": "bearer_dict",
        "s3_conn_id": "s3_bucket",
        "s3_bucket": "s3_bucket",
        "s3_key": "airflows/ncproject_fintransacts",
    }
    mock_airflow_variable.return_value = '{"Test":"test_body_bearer_dict_variable"}'
    actual = DagBuilder.make_task(operator, task_params)

    assert actual.task_id == "test_db_conn_id_to_s3"
    assert actual.body_bearer_dict == {"Test": "test_body_bearer_dict_variable"}


def test_make_task_python_operator():
    """Test that valid python operator is created."""

    operator = "airflow.operators.python.PythonOperator"
    task_params = {
        "task_id": "test_1",
        "python_callable": "os.chdir",
        "retries": 2,
    }

    actual = DagBuilder.make_task(operator, task_params)

    assert callable(actual.python_callable)
    assert isinstance(actual, PythonOperator)


def test_make_task_python_operator_bad(caplog):
    """Test appopriate exception is raised when invalid python operator attempted to be created."""

    operator = "airflow.operators.python.PythonOperator"
    # python_callable argument is missing from task_params
    task_params = {
        "task_id": "test_1",
        "retries": 2,
    }

    with pytest.raises(DagFactoryConfigException):
        DagBuilder.make_task(operator, task_params)

    assert "PythonOperator requires python_callable" in caplog.text


def test_make_http_operator():
    """Test that a valid http operator is created."""

    operator = "airflow.providers.http.operators.http.SimpleHttpOperator"
    task_params = {
        "task_id": "test_1",
        "endpoint": "test_endpoint",
        "method": "GET",
        "headers": {"test_header": "test_value"},
        "response_filter": "operators.talkdesk.get_token_response_filter",
        "response_check": "operators.talkdesk.get_job_status_response_check",
    }

    actual = DagBuilder.make_task(operator, task_params)

    assert isinstance(actual, SimpleHttpOperator)
    assert callable(actual.response_filter)
    assert callable(actual.response_check)


@patch("airflow.models.Variable.get")
def test_make_task_athena_operator(mock_airflow_variable):
    """Test that valid python operator is created."""

    operator = "airflow.providers.amazon.aws.operators.athena.AthenaOperator"
    task_params = {
        "task_id": "test_1",
        "query": "models.athena_queries.newcross_newxtimebat.attach_partition_bronze",
        "retries": 2,
    }

    mock_airflow_variable.return_value = "test_bucket_airflow_variable"

    actual = DagBuilder.make_task(operator, task_params)

    assert (
        task_params["query"]
        == "ALTER TABLE newcross_bronze.newxtimebat ADD IF NOT EXISTS PARTITION (dt = {{data_interval_start.strftime('%Y%m%d')}})"
    )
    assert "database" in task_params
    assert "output_location" in task_params
    assert isinstance(actual, AthenaOperator)


@patch("airflow.models.Variable.get")
def test_make_task_athena_operator_all_parameters(mock_airflow_variable):
    """Test that valid python operator is created."""

    operator = "airflow.providers.amazon.aws.operators.athena.AthenaOperator"
    task_params = {
        "task_id": "test_1",
        "query": "models.athena_queries.newcross_newxtimebat.attach_partition_bronze",
        "retries": 2,
        "database": "test_database",
        "output_location": "s3://test-bucket/test-location",
    }

    mock_airflow_variable.return_value = "test_bucket_airflow_variable"

    actual = DagBuilder.make_task(operator, task_params)

    assert (
        task_params["query"]
        == "ALTER TABLE newcross_bronze.newxtimebat ADD IF NOT EXISTS PARTITION (dt = {{data_interval_start.strftime('%Y%m%d')}})"
    )
    assert task_params["database"] == "test_database"
    assert task_params["output_location"] == "s3://test-bucket/test-location"
    assert isinstance(actual, AthenaOperator)


def test_make_task_athena_operator_bad(caplog):
    """Test appopriate exception is raised when invalid python operator attempted to be created."""

    operator = "airflow.providers.amazon.aws.operators.athena.AthenaOperator"
    # python_callable argument is missing from task_params
    task_params = {"task_id": "test_1", "retries": 2, "s3_bucket": "data-team-workspace"}

    with pytest.raises(DagFactoryConfigException):
        DagBuilder.make_task(operator, task_params)

    assert "AthenaOperator requires query" in caplog.text


def test_make_task_bad_operator(caplog):
    """Test that bad operator raises exception"""

    operator = "not_real"
    task_params = {"task_id": "test_task", "bash_command": "echo 1"}
    with pytest.raises(Exception) as e:
        DagBuilder.make_task(operator, task_params)

    assert "ERROR" in caplog.text
    assert "Failed to import operator not_real" in caplog.text
    assert "Failed to import operator" in str(e)


def test_make_task_missing_required_param(caplog):
    """Test that missing required param raises exception"""

    operator = "airflow.operators.bash.BashOperator"
    task_params = {"task_id": "test_task"}
    with pytest.raises(Exception) as e:
        DagBuilder.make_task(operator, task_params)

    assert "ERROR" in caplog.text
    assert "Failed to create operator airflow.operators.bash.BashOperator" in caplog.text
    assert "Failed to create operator" in str(e)


def test_set_dependencies():
    """Test that dependencies are set correctly"""

    dag = DAG(dag_id="my_dag", start_date=datetime.now())
    tasks_config = {
        "task_1": {
            "task_id": "test_1",
            "bash_command": "echo_1",
            "retries": 2,
        },
        "task_2": {
            "task_id": "test_2",
            "bash_command": "echo_2",
            "retries": 2,
            "dependencies": ["task_1"],
        },
    }
    operators_dict = {
        "task_1": BashOperator(task_id="test_1", bash_command="echo_1", retries=2, dag=dag),
        "task_2": BashOperator(task_id="test_2", bash_command="echo_2", retries=2, dag=dag),
    }
    DagBuilder.set_dependencies(tasks_config, operators_dict)
    assert operators_dict["task_1"].downstream_list == [operators_dict["task_2"]]
    assert operators_dict["task_2"].upstream_list == [operators_dict["task_1"]]
    assert operators_dict["task_1"].downstream_task_ids == {"test_2"}
    assert operators_dict["task_2"].upstream_task_ids == {"test_1"}


def test_set_dependencies_task_groups():
    """Test that dependencies are set correctly when there are no previous tasks"""

    dag = DAG(dag_id="my_dag", start_date=datetime.now())
    tasks_config = {
        "task_group_1": {
            "name": "this is a task group",
            "dependencies": ["task_1"],
        },
        "task_1": {
            "task_id": "test_1",
            "bash_command": "echo_1",
            "retries": 2,
        },
        "task_2": {
            "task_id": "test_2",
            "bash_command": "echo_2",
            "retries": 2,
        },
    }
    operators_dict = {
        "task_1": BashOperator(task_id="test_1", bash_command="echo_1", retries=2, dag=dag),
        "task_2": BashOperator(task_id="test_2", bash_command="echo_2", retries=2, dag=dag),
        "task_group_1": TaskGroup("task_group_name_1", dag=dag),
    }
    DagBuilder.set_dependencies(tasks_config, operators_dict)
    assert operators_dict["task_group_1"].upstream_list == [operators_dict["task_1"]]


def test_set_dependencies_without_previous_tasks():
    """Test that dependencies are set correctly when there are no previous tasks"""

    dag = DAG(dag_id="my_dag", start_date=datetime.now())
    tasks_config = {
        "task_1": {
            "task_id": "test_1",
            "bash_command": "echo_1",
            "retries": 2,
        },
        "task_2": {
            "task_id": "test_2",
            "bash_command": "echo_2",
            "retries": 2,
        },
    }
    operators_dict = {
        "task_1": BashOperator(task_id="test_1", bash_command="echo_1", retries=2, dag=dag),
        "task_2": BashOperator(task_id="test_2", bash_command="echo_2", retries=2, dag=dag),
    }
    DagBuilder.set_dependencies(tasks_config, operators_dict)
    assert operators_dict["task_1"].downstream_list == []
    assert operators_dict["task_2"].upstream_list == []


@patch("airflow.models.Pool.get_pools")
def test_build(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test build method of DagBuilder"""

    mock_get_pools.return_value = [Pool(pool="bookings-read-replica")]

    config = {
        "default_args": {"owner": "Airflow", "start_date": datetime(2022, 5, 3), "retries": 1, "retry_delay_sec": 300},
        "schedule": "@once",
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout_sec": 120,
        "default_view": "grid",
        "orientation": "LR",
        "description": "Dag representing flow from Montero.People to S3 bucket.",
        "tasks": {
            "montero_people_to_s3_vol1": {
                "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                "db_conn_id": "bookings-read-replica",
                "s3_conn_id": "s3-data-team-workspace",
                "s3_bucket": "data-team-workspace",
                "s3_key": "airflows/montero_people.parquet",
                "db_name": "Montero",
                "schema_name": "dbo",
                "table_name": "People",
            },
            "montero_people_to_s3_vol2": {
                "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                "db_conn_id": "bookings-read-replica",
                "s3_conn_id": "s3-data-team-workspace",
                "s3_bucket": "data-team-workspace",
                "s3_key": "airflows/montero_people.parquet",
                "db_name": "Montero",
                "schema_name": "dbo",
                "table_name": "People",
                "dependencies": ["montero_people_to_s3_vol1"],
            },
        },
    }

    dag = DagBuilder(dag_name="test_dag", dag_config=config, default_config=config)
    actual = dag.build()
    assert actual["dag_id"] == "test_dag"
    assert isinstance(actual["dag"], DAG)
    assert len(actual["dag"].tasks) == 2
    assert actual["dag"].task_dict["montero_people_to_s3_vol1"].downstream_task_ids == {"montero_people_to_s3_vol2"}


@patch("airflow.models.Pool.get_pools")
def test_build_exception_doc_md_absolute_path(mock_get_pools: MagicMock, default_config_fixture):
    """Test that build method raises exception when dag is not valid."""
    mock_get_pools.return_value = [Pool(pool="bookings-read-replica")]
    config = {
        "default_args": {
            "start_date": "2010-01-01",
            "end_date": "2020-01-01",
            "retry_delay_sec": 200,
            "sla_secs": 12,
        },
        "doc_md_file_path": "/this/is/an/absolute/path",
        "dagrun_timeout_sec": 10,
    }

    with pytest.raises(DagFactoryConfigException) as e:
        dag = DagBuilder(dag_name="test_dag", dag_config=config, default_config=default_config_fixture)
        dag.build()
    assert "Failed to read doc_md_file_path" in str(e.value)


@patch("airflow.models.Pool.get_pools")
def test_build_exception_doc_md_not_absolute_path(mock_get_pools: MagicMock, default_config_fixture):
    """Test that build method raises exception when dag is not valid."""
    mock_get_pools.return_value = [Pool(pool="bookings-read-replica")]
    config = {
        "default_args": {
            "start_date": "2010-01-01",
            "end_date": "2020-01-01",
            "retry_delay_sec": 200,
            "sla_secs": 12,
        },
        "doc_md_file_path": "this/is/not/absolute/path",
        "dagrun_timeout_sec": 10,
    }

    with pytest.raises(DagFactoryConfigException) as e:
        dag = DagBuilder(dag_name="test_dag", dag_config=config, default_config=default_config_fixture)
        dag.build()
    assert "Doc_md file path must be absolute path" in str(e.value)


def test_build_exception_dag(caplog):
    """Test that build method raises exception when dag is not valid."""

    config = {
        "default_args": {"owner": "Airflow", "start_date": datetime(2022, 5, 3), "retries": 1, "retry_delay_sec": 300},
        "schedule": "@once",
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout_sec": 120,
        "default_view": "grid",
        "orientation": "LR",
        "description": "Dag representing flow from Montero.People to S3 bucket.",
        "tasks": {
            "montero_people_to_s3": {
                "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                "db_conn_id": "bookings-read-replica",
                "s3_conn_id": "s3-data-team-workspace",
                "s3_bucket": "data-team-workspace",
                "s3_key": "airflows/montero_people.parquet",
                "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
            }
        },
    }
    with pytest.raises(Exception) as e:
        dag = DagBuilder(dag_name=[], dag_config=config, default_config=config)
        dag.build()

    assert "ERROR" in caplog.text
    assert "The key has to be a string and is <class 'list'>" in caplog.text
    assert "Failed to create DAG" in str(e)
