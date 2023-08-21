def test_ingestion_happens_successfully_pg_server(setup_and_teardown_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_ingestion_test_pg_dag"

    expected_list = [(1, "1968-10-23", "buy 1"), (2, "1968-10-24", "buy 2")]

    airflow_api_utils_fixture.run_dag(dag_under_test_id)
    logs = airflow_api_utils_fixture.get_dag_run_logs(dag_id=dag_under_test_id, task_id="successful_ingestion_test_pg_dag_task")

    assert f"Data read successfully : {expected_list}" in logs[0]


def test_ingestion_happens_successfully_pg_server_with_chunk(setup_and_teardown_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_ingestion_test_pg_with_chunk_dag"

    expected_list_1 = [(1, "1968-10-23", "buy 1")]
    expected_list_2 = [(2, "1968-10-24", "buy 2")]

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    logs = airflow_api_utils_fixture.get_dag_run_logs(
        dag_id=dag_under_test_id, task_id="successful_ingestion_test_pg_with_chunk_dag_task"
    )

    assert f"Data read successfully : {expected_list_1}" in logs[0]
    assert f"Data read successfully : {expected_list_2}" in logs[0]


def test_ingestion_happens_successfully_pg_server_with_chunk_2(setup_and_teardown_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_ingestion_test_pg_with_chunk_2_dag"

    expected_list = [(1, "1968-10-23", "buy 1"), (2, "1968-10-24", "buy 2")]

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    logs = airflow_api_utils_fixture.get_dag_run_logs(
        dag_id=dag_under_test_id, task_id="successful_ingestion_test_pg_with_chunk_2_dag_task"
    )

    assert f"Data read successfully : {expected_list}" in logs[0]
