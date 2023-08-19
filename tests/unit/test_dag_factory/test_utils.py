import os
from datetime import datetime, timedelta
from typing import Callable

import pytest
from dagfactory.exceptions import DagFactoryException
from dagfactory.utils import check_dict_key, check_template_searchpath, get_datetime, get_time_delta, merge_configs, set_callable
from pendulum.tz.timezone import Timezone

# Constants
SEARCHPATH_NOT_ABSOLUTE_ERROR_MESSAGE = "Template searchpath isn't absolute. Ignoring it."
SEARCHPATH_DOES_NOT_EXIST_ERROR_MESSAGE = "Template searchpath doesn't exist. Ignoring it."
INVALID_PATH = "/invalid/path"
NON_ABSOLUTE_PATH = "non/absolute/path"


def test_merge_configs_exists_only_in_default():
    """Test merge_configs when key exists only in default config."""

    config = {
        "default": {"max_active_runs": 1},
        "montero_people_dag": {
            "default_args": {"owner": "Airflow", "start_date": datetime(2022, 5, 3), "retries": 1, "retry_delay_sec": 300},
            "schedule": "@once",
            "concurrency": 1,
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
                    "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                },
                "montero_people_to_s3_vol2": {
                    "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                    "db_conn_id": "bookings-read-replica",
                    "s3_conn_id": "s3-data-team-workspace",
                    "s3_bucket": "data-team-workspace",
                    "s3_key": "airflows/montero_people.parquet",
                    "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                    "dependencies": ["montero_people_to_s3_vol1"],
                },
            },
        },
    }
    expected_config = {
        "default_args": {
            "owner": "Airflow",
            "start_date": datetime(2022, 5, 3, 0, 0),
            "retries": 1,
            "retry_delay_sec": 300,
        },
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
                "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
            },
            "montero_people_to_s3_vol2": {
                "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                "db_conn_id": "bookings-read-replica",
                "s3_conn_id": "s3-data-team-workspace",
                "s3_bucket": "data-team-workspace",
                "s3_key": "airflows/montero_people.parquet",
                "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                "dependencies": ["montero_people_to_s3_vol1"],
            },
        },
    }
    actual_config = merge_configs(config=config["montero_people_dag"], default_config=config["default"])
    assert actual_config == expected_config


def test_merge_configs_exist_arg_in_dag_conf_and_default():
    """Test merge_configs when key exists in dag config and default config."""

    config = {
        "default": {"max_active_runs": 2},
        "montero_people_dag": {
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
                    "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                },
                "montero_people_to_s3_vol2": {
                    "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                    "db_conn_id": "bookings-read-replica",
                    "s3_conn_id": "s3-data-team-workspace",
                    "s3_bucket": "data-team-workspace",
                    "s3_key": "airflows/montero_people.parquet",
                    "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                    "dependencies": ["montero_people_to_s3_vol1"],
                },
            },
        },
    }
    expected_config = {
        "default_args": {
            "owner": "Airflow",
            "start_date": datetime(2022, 5, 3, 0, 0),
            "retries": 1,
            "retry_delay_sec": 300,
        },
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
                "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
            },
            "montero_people_to_s3_vol2": {
                "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                "db_conn_id": "bookings-read-replica",
                "s3_conn_id": "s3-data-team-workspace",
                "s3_bucket": "data-team-workspace",
                "s3_key": "airflows/montero_people.parquet",
                "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                "dependencies": ["montero_people_to_s3_vol1"],
            },
        },
    }
    actual_config = merge_configs(config=config["montero_people_dag"], default_config=config["default"])
    assert actual_config == expected_config


def test_check_dict_key():
    """Test that check_dict_key raises an error if the key is not in the dictionary."""

    config = {
        "default": {"max_active_runs": 1},
        "montero_people_dag": {
            "default_args": {"owner": "Ilias Nikas", "start_date": datetime(2022, 5, 3), "retries": 1, "retry_delay_sec": 300},
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
                    "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                },
                "montero_people_to_s3_vol2": {
                    "operator": "operators.db_to_s3_operator.SqlServerToS3Operator",
                    "db_conn_id": "bookings-read-replica",
                    "s3_conn_id": "s3-data-team-workspace",
                    "s3_bucket": "data-team-workspace",
                    "s3_key": "airflows/montero_people.parquet",
                    "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                    "dependencies": ["montero_people_to_s3_vol1"],
                },
            },
        },
    }
    actual_answer_true = check_dict_key(item_dict=config["montero_people_dag"], key="default_args")
    actual_answer_false = check_dict_key(item_dict=config["montero_people_dag"], key="error")

    assert actual_answer_true is True
    assert actual_answer_false is False


def test_get_datetime():
    """Test get_datetime function."""

    actual_time_1 = get_datetime("2022-05-03")
    actual_time_2 = get_datetime("2022-05-03 00:00:00")
    actual_time_3 = get_datetime(datetime(2022, 5, 3))
    actual_time_4 = get_datetime("0 hours, 10 days, 4 minutes, 10 second")  # it catches only the first parameter

    try:
        get_datetime("2022-05-03", timezone="error")
    except Exception as err:
        assert "Failed to create timezone" in str(err)

    assert actual_time_1 == datetime(2022, 5, 3, 0, 0, tzinfo=Timezone("UTC"))
    assert actual_time_2 == datetime(2022, 5, 3, 0, 0, tzinfo=Timezone("UTC"))
    assert actual_time_3 == datetime(2022, 5, 3, 0, 0, tzinfo=Timezone("UTC"))
    assert actual_time_4 == datetime(
        datetime.today().year, datetime.today().month, datetime.today().day, 0, 0, tzinfo=Timezone("UTC")
    )


def test_get_time_delta():
    """Test get_time_delta function."""

    actual_time_delta_1 = get_time_delta("2 hours, 4 minutes, 10 second, 10 days")
    assert actual_time_delta_1 == timedelta(hours=2)

    with pytest.raises(DagFactoryException) as e:
        get_time_delta(" hours,  days,  minutes, second")
        assert "Failed to create timedelta" in str(e.value)


def test_set_callable():
    """Test set_callable function."""
    dag_params = {
        "default_args": {
            "owner": "Airflow",
            "start_date": datetime(2022, 5, 3),
            "retries": 1,
            "retry_delay_sec": 300,
            "on_success_callback": "operators.db_to_s3_operator.SqlServerToS3Operator",
        },
    }
    set_callable(dag_params["default_args"], "on_success_callback")

    assert isinstance(dag_params["default_args"]["on_success_callback"], Callable)


def test_set_callable_exceptions():
    """Test set_callable function exceptions."""

    dag_params = {
        "default_args": {
            "owner": "Airflow",
            "start_date": datetime(2022, 5, 3),
            "retries": 1,
            "retry_delay_sec": 300,
            "on_success_callback": "error.path",
        },
    }
    try:
        set_callable(dag_params["default_args"], "on_success_callback")
    except Exception as err:
        assert "Failed to import callback" in str(err)

    try:
        set_callable(dag_params["default_args"], "retry_delay_sec")
    except Exception as err:
        assert "Invalid callback name." in str(err)


def test_check_template_searchpath_str_true(tmpdir):
    """Test check_template_searchpath function for a string input path that exist."""

    answer = check_template_searchpath(str(tmpdir))

    assert answer is True


def test_check_template_searchpath_list_true(tmpdir):
    """Test check_template_searchpath function for a string input path that exist."""
    dir_list = [str(tmpdir) + "/dir" + str(i) for i in range(1, 3)]
    for path in dir_list:
        os.mkdir(path)

    answer = check_template_searchpath(dir_list)

    assert answer is True


def test_check_template_searchpath_list_false_path_not_absolute(tmpdir, caplog):
    """Test check_template_searchpath function for a string input path that exist."""
    dir_list = [str(tmpdir) + "/dir" + str(i) for i in range(1, 3)]
    for path in dir_list:
        os.mkdir(path)
    dir_list = [dir_path.replace("/", "", 1) for dir_path in dir_list]
    answer = check_template_searchpath(dir_list)

    assert SEARCHPATH_NOT_ABSOLUTE_ERROR_MESSAGE in caplog.text
    assert answer is False


def test_check_template_searchpath_list_false_no_dir(caplog):
    """Test check_template_searchpath function for a string input path that exist."""

    dir_list = [INVALID_PATH, NON_ABSOLUTE_PATH]
    answer = check_template_searchpath(dir_list)

    assert SEARCHPATH_DOES_NOT_EXIST_ERROR_MESSAGE in caplog.text
    assert answer is False


@pytest.mark.parametrize(
    "searchpath, expected_message",
    [(NON_ABSOLUTE_PATH, SEARCHPATH_NOT_ABSOLUTE_ERROR_MESSAGE), (INVALID_PATH, SEARCHPATH_DOES_NOT_EXIST_ERROR_MESSAGE)],
)
def test_check_template_searchpath_str_false(searchpath, expected_message, caplog):
    """Test check_template_searchpath function for a string input path that does not exist."""

    answer = check_template_searchpath(searchpath)
    assert expected_message in caplog.text
    assert answer is False
