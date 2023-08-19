def test_restart_failed_dags(airflow_api_utils_fixture):
    """
    Test that all failed DAGs are restarted.
    """

    # run bad dag
    failed_dag_under_test_id = "failed_test_api_dag"
    airflow_api_utils_fixture.run_dag(failed_dag_under_test_id)

    # run good dag
    succeeded_dag_under_test_id = "succeeded_test_dag"
    airflow_api_utils_fixture.run_dag(succeeded_dag_under_test_id)

    # get lists with dag runs
    bad_dag_run_collection = airflow_api_utils_fixture.list_dag_runs(failed_dag_under_test_id)
    good_dag_run_collection = airflow_api_utils_fixture.list_dag_runs(succeeded_dag_under_test_id)

    # get first bad dag run id
    first_bad_run_id = bad_dag_run_collection.dag_runs[0].dag_run_id

    assert len(bad_dag_run_collection.dag_runs) == 1
    assert len(good_dag_run_collection.dag_runs) == 1

    # run restarting dag
    airflow_api_utils_fixture.run_dag("restart_failed_dags_test_dag")

    # get lists with dag runs
    bad_dag_run_collection = airflow_api_utils_fixture.list_dag_runs(failed_dag_under_test_id)
    good_dag_run_collection = airflow_api_utils_fixture.list_dag_runs(succeeded_dag_under_test_id)

    # get second bad dag run id
    second_bad_run_id = bad_dag_run_collection.dag_runs[1].dag_run_id

    assert len(bad_dag_run_collection.dag_runs) == 2
    assert bad_dag_run_collection.dag_runs[0].dag_run_id == first_bad_run_id
    assert second_bad_run_id != first_bad_run_id
    assert len(good_dag_run_collection.dag_runs) == 1
