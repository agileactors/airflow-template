import allure
import polars as pl
from models.utils.operational_events import PIPELINELAYER, PIPELINESTATUS, OperationalEvent
from utils.operational_events import assert_operational_event_contents


@allure.tag("Integration test")
@allure.feature("Successful ingestion")
@allure.story("Ingestion happens successfully for Postgres Server")
def test_ingestion_happens_successfully_pg_server(setup_and_teardown_fixture, s3_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_ingestion_test_pg_dag"

    expected_dataframe = pl.DataFrame(
        {
            "id": [1, 2],
            "last_transaction_date": [
                "1968-10-23",
                "1968-10-24",
            ],
            "description": ["buy 1", "buy 2"],
        },
        schema={"id": pl.Int32, "last_transaction_date": str, "description": str},
    )

    expected_operational_event_data = {
        "dag_id": dag_under_test_id,
        "pipelinestatus": PIPELINESTATUS.success,
        "task_id": "successful_ingestion_test_pg_dag_task",
        "pipelinelayer": PIPELINELAYER.landing,
        "source_database": "postgres",
        "source_schema": "ncproject",
        "source_table": "fintransacts",
        "target_s3_bucket": "datateambucket",
        "target_key": "airflows/ncproject_fintransacts",
        "task_configuration": {},
    }

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    s3_fixture.assert_number_of_files_in_s3_bucket(1)

    s3_file_absolute_path = s3_fixture.get_data_paths()[0]

    s3_fixture.assert_parquet_file_contents(s3_file_absolute_path, expected_dataframe)

    with allure.step("Retrieve the operational events from the database"):
        actual_operational_events = (
            setup_and_teardown_fixture.query(OperationalEvent).filter(OperationalEvent.dag_id == dag_under_test_id).all()
        )

    with allure.step("Assert that 1 operational event was written to database"):
        assert len(actual_operational_events) == 1

    expected_operational_event_data["target_filename"] = s3_fixture.get_file_name(0)
    expected_operational_event_data["target_partition"] = s3_fixture.partition_name
    actual_operational_event_data = actual_operational_events[0].__dict__

    assert_operational_event_contents(actual_operational_event_data, expected_operational_event_data)


@allure.tag("Integration test")
@allure.feature("Successful ingestion")
@allure.story("Ingestion happens successfully for Postgres Server")
def test_ingestion_happens_successfully_pg_server_with_chunk(setup_and_teardown_fixture, s3_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_ingestion_test_pg_with_chunk_dag"

    expected_dataframe_1 = pl.DataFrame(
        {
            "id": [1],
            "last_transaction_date": ["1968-10-23"],
            "description": ["buy 1"],
        },
        schema={"id": pl.Int32, "last_transaction_date": str, "description": str},
    )
    expected_dataframe_2 = pl.DataFrame(
        {
            "id": [2],
            "last_transaction_date": ["1968-10-24"],
            "description": ["buy 2"],
        },
        schema={"id": pl.Int32, "last_transaction_date": str, "description": str},
    )

    expected_operational_event_data = {
        "dag_id": dag_under_test_id,
        "pipelinestatus": PIPELINESTATUS.success,
        "task_id": "successful_ingestion_test_pg_with_chunk_dag_task",
        "pipelinelayer": PIPELINELAYER.landing,
        "source_database": "postgres",
        "source_schema": "ncproject",
        "source_table": "fintransacts",
        "target_s3_bucket": "datateambucket",
        "target_key": "airflows/ncproject_fintransacts",
        "task_configuration": {},
    }

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    s3_fixture.assert_number_of_files_in_s3_bucket(2)

    s3_file_absolute_path_1 = s3_fixture.get_data_paths()[0]
    s3_file_absolute_path_2 = s3_fixture.get_data_paths()[1]

    s3_fixture.assert_parquet_file_contents(s3_file_absolute_path_1, expected_dataframe_1)
    s3_fixture.assert_parquet_file_contents(s3_file_absolute_path_2, expected_dataframe_2)

    with allure.step("Retrieve the operational events from the database"):
        actual_operational_events = (
            setup_and_teardown_fixture.query(OperationalEvent).filter(OperationalEvent.dag_id == dag_under_test_id).all()
        )

    with allure.step("Assert that 2 operational event was written to database"):
        assert len(actual_operational_events) == 2

    expected_operational_event_data["target_filename"] = s3_fixture.get_file_name(0)
    expected_operational_event_data["target_partition"] = s3_fixture.partition_name
    actual_operational_event_data = actual_operational_events[0].__dict__

    assert_operational_event_contents(actual_operational_event_data, expected_operational_event_data)


@allure.tag("Integration test")
@allure.feature("Successful ingestion")
@allure.story("Ingestion happens successfully for Postgres Server")
def test_ingestion_happens_successfully_pg_server_with_chunk_2(setup_and_teardown_fixture, s3_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_ingestion_test_pg_with_chunk_2_dag"

    expected_dataframe = pl.DataFrame(
        {
            "id": [1, 2],
            "last_transaction_date": [
                "1968-10-23",
                "1968-10-24",
            ],
            "description": ["buy 1", "buy 2"],
        },
        schema={"id": pl.Int32, "last_transaction_date": str, "description": str},
    )

    expected_operational_event_data = {
        "dag_id": dag_under_test_id,
        "pipelinestatus": PIPELINESTATUS.success,
        "task_id": "successful_ingestion_test_pg_with_chunk_2_dag_task",
        "pipelinelayer": PIPELINELAYER.landing,
        "source_database": "postgres",
        "source_schema": "ncproject",
        "source_table": "fintransacts",
        "target_s3_bucket": "datateambucket",
        "target_key": "airflows/ncproject_fintransacts",
        "task_configuration": {},
    }

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    s3_fixture.assert_number_of_files_in_s3_bucket(1)

    s3_file_absolute_path = s3_fixture.get_data_paths()[0]

    s3_fixture.assert_parquet_file_contents(s3_file_absolute_path, expected_dataframe)

    with allure.step("Retrieve the operational events from the database"):
        actual_operational_events = (
            setup_and_teardown_fixture.query(OperationalEvent).filter(OperationalEvent.dag_id == dag_under_test_id).all()
        )

    with allure.step("Assert that 2 operational event was written to database"):
        assert len(actual_operational_events) == 1

    expected_operational_event_data["target_filename"] = s3_fixture.get_file_name(0)
    expected_operational_event_data["target_partition"] = s3_fixture.partition_name
    actual_operational_event_data = actual_operational_events[0].__dict__

    assert_operational_event_contents(actual_operational_event_data, expected_operational_event_data)


@allure.tag("Integration test")
@allure.feature("Unsuccessful ingestion")
@allure.story("Ingestion fails for Postgres Server")
def test_ingestion_happens_unsuccessfully_pg_server(setup_and_teardown_fixture, s3_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "unsuccessful_ingestion_test_pg_dag"

    expected_operational_event = {
        "dag_id": dag_under_test_id,
        "pipelinelayer": PIPELINELAYER.landing,
        "pipelinestatus": PIPELINESTATUS.failure,
        "source_database": "postgres",
        "source_schema": "bad_ncproject",
        "source_table": "fintransacts",
        "target_key": "airflows/ncproject_fintransacts",
        "target_s3_bucket": "datateambucket",
        "task_configuration": {},
        "task_id": "unsuccessful_ingestion_test_pg_dag_task",
    }

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    with allure.step("Retrieve the operational events from the database"):
        actual_operational_events = (
            setup_and_teardown_fixture.query(OperationalEvent).filter(OperationalEvent.dag_id == dag_under_test_id).all()
        )

    with allure.step("Assert that 2 operational events are written to database when the dag fails"):
        assert len(actual_operational_events) == 2

    for operation_event in actual_operational_events:
        operational_event_data = operation_event.__dict__

        assert_operational_event_contents(operational_event_data, expected_operational_event)

    s3_fixture.assert_s3_bucket_is_empty()
