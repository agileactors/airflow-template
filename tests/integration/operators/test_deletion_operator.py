import allure


@allure.tag("Integration test")
@allure.feature("Successful file deletion")
@allure.story("File deletion happens successfully for custom S3 Deletion Operator")
def test_file_deletion_happens_successfully_s3(s3_custom_partitioning_fixture, airflow_api_utils_fixture):
    dag_under_test_id = "successful_deletion_operator_test_dag"

    s3_custom_partitioning_fixture.create_object("hello1.txt", "Hello World 1!")
    s3_custom_partitioning_fixture.create_object("hello2.txt", "Hello World 2!")

    s3_custom_partitioning_fixture.assert_number_of_files_in_s3_bucket(2)

    airflow_api_utils_fixture.run_dag(dag_under_test_id)

    s3_custom_partitioning_fixture.assert_s3_bucket_is_empty()
