def test_for_import_errors(airflow_api_utils_fixture):
    import_errors_response = airflow_api_utils_fixture.get_import_errors()

    assert import_errors_response.total_entries == 0
