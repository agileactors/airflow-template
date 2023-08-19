import pathlib
import shutil
from unittest.mock import MagicMock, patch

import pendulum
from operators.transfer_delete_logs import (
    delete_local_dag_processor_manager_logs,
    delete_local_scheduler_logs,
    transfer_dag_processor_manager_logs_to_s3,
    transfer_scheduler_logs_to_s3,
)


@patch("operators.transfer_delete_logs.conf")
@patch("models.data_model.AwsS3.transfer_local_file_to_s3")
def test_transfer_scheduler_logs_to_s3(
    mock_transfer_local_file_to_s3: MagicMock, mock_airflow_conf: MagicMock, tmp_path: pathlib.Path, caplog
):
    """
    Test that transfer_scheduler_logs_to_s3 transfers the wanted files to S3
    by calling transfer_local_file_to_s3 for each file.
    """
    # Mock the function that transfers local files to S3
    mock_transfer_local_file_to_s3.return_value = MagicMock()

    # Mock the get function of the config object
    def conf_side_effect(section, key):
        if section == "scheduler" and key == "child_process_log_directory":
            return str(tmp_path)
        elif section == "logging" and key == "dag_processor_log_target":
            return "file"

    mock_airflow_conf.get.side_effect = conf_side_effect

    data_interval_start = pendulum.DateTime(2022, 2, 1)

    # Create folder for scheduler logs
    scheduler_inner_log_dir = tmp_path.joinpath(data_interval_start.strftime("%Y-%m-%d"))
    scheduler_inner_log_dir.mkdir()

    test_log_1 = scheduler_inner_log_dir.joinpath("test_log_1.log")
    test_log_2 = scheduler_inner_log_dir.joinpath("test_log_2.log")

    # Create 2 test log files
    test_log_1.touch(mode=644)
    test_log_2.touch(mode=644)

    conn_id = "test_conn_id"
    bucket_name = "test_bucket_name"
    key = "test_key"

    transfer_scheduler_logs_to_s3(conn_id, bucket_name, key, data_interval_start)
    assert "Dag processor logs transferred to S3" in caplog.text
    assert mock_transfer_local_file_to_s3.call_count == 2

    # Delete temporary directory
    shutil.rmtree(tmp_path)


@patch("operators.transfer_delete_logs.conf")
@patch("models.data_model.AwsS3.transfer_local_file_to_s3")
def test_transfer_dag_processor_manager_logs_to_s3(
    mock_transfer_local_file_to_s3: MagicMock, mock_airflow_conf: MagicMock, tmp_path: pathlib.Path
):
    """
    Test that transfer_dag_processor_manager_logs_to_s3 copies the dag processor manager log
    file locally and uploads the new file to S3 by calling transfer_local_file_to_s3.
    """
    # Mock the function that transfers local files to S3
    mock_transfer_local_file_to_s3.return_value = MagicMock()
    # Mock the get function of the config object
    dag_processor_manager_log_filepath = tmp_path.joinpath("dag_processor_manager.log")
    mock_airflow_conf.get.return_value = str(dag_processor_manager_log_filepath)

    data_interval_start = pendulum.DateTime(2022, 2, 1)

    # Create dag_processor_manager.log file
    dag_processor_manager_log_filepath.touch()

    conn_id = "test_conn_id"
    bucket_name = "test_bucket_name"
    key = "test_key"

    # Call function
    transfer_dag_processor_manager_logs_to_s3(conn_id, bucket_name, key, data_interval_start)

    # Make sure the tmp file was created
    tmp_filepath = tmp_path.joinpath("tmp.log")
    assert tmp_filepath.exists()

    # Make sure the function was called once for the tmp file with the correct arguments
    mock_transfer_local_file_to_s3.assert_called_once_with(
        local_file_path=str(tmp_filepath), partition=data_interval_start.strftime("%Y-%m-%d"), filename="dag_processor_manager.log"
    )

    # Delete temporary directory
    shutil.rmtree(tmp_path)


@patch("operators.transfer_delete_logs.conf")
@patch("models.data_model.AwsS3.transfer_local_file_to_s3")
def test_transfer_dag_processor_manager_logs_to_s3_no_logs(
    mock_transfer_local_file_to_s3: MagicMock, mock_airflow_conf: MagicMock, tmp_path: pathlib.Path
):
    """
    Test that transfer_dag_processor_manager_logs_to_s3 doesn't fail when there are no
    dag processor manager logs.
    """
    # Mock the function that transfers local files to S3
    mock_transfer_local_file_to_s3.return_value = MagicMock()
    # Mock the get function of the config object - this filepath is invalid because the file doesn't exist
    dag_processor_manager_log_filepath = tmp_path.joinpath("dag_processor_manager.log")
    mock_airflow_conf.get.return_value = str(dag_processor_manager_log_filepath)

    data_interval_start = pendulum.DateTime(2022, 2, 1)

    conn_id = "test_conn_id"
    bucket_name = "test_bucket_name"
    key = "test_key"

    # Call function
    transfer_dag_processor_manager_logs_to_s3(conn_id, bucket_name, key, data_interval_start)

    mock_transfer_local_file_to_s3.assert_not_called()

    # Delete temporary directory
    shutil.rmtree(tmp_path)


@patch("operators.transfer_delete_logs.conf")
def test_delete_local_scheduler_logs(mock_airflow_conf: MagicMock, tmp_path: pathlib.Path, caplog):
    """
    Test that scheduler log files are deleted for specified day.
    """

    data_interval_start = pendulum.DateTime(2022, 2, 1)

    # Mock the get function of the config object - this filepath is invalid because the file doesn't exist
    scheduler_inner_log_dir = tmp_path.joinpath(data_interval_start.strftime("%Y-%m-%d"))
    mock_airflow_conf.get.return_value = str(tmp_path)

    # Create folder for scheduler logs
    scheduler_inner_log_dir.mkdir()

    # Create a test log file
    test_log_filepath = scheduler_inner_log_dir.joinpath("test_log.log")
    test_log_filepath.touch(mode=644)

    # Call function
    delete_local_scheduler_logs(data_interval_start)

    # Make sure log folder for that day was deleted
    assert not scheduler_inner_log_dir.exists()

    # Call function again and make sure it doesn't fail
    delete_local_scheduler_logs(data_interval_start)
    assert "No scheduler logs" in caplog.text

    # Delete temporary directory
    shutil.rmtree(tmp_path)


@patch("operators.transfer_delete_logs.conf")
def test_delete_local_dag_processor_manager_logs(mock_airflow_conf: MagicMock, tmp_path: pathlib.Path):
    """
    Test that temp dag processor manager log file is deleted and main file is truncated
    to only contain lines that aren't in the tmp file (can only be on the end of the file).
    """
    dag_processor_manager_log_filepath = tmp_path.joinpath("dag_processor_manager.log")
    mock_airflow_conf.get.return_value = str(dag_processor_manager_log_filepath)

    main_log_filepath = tmp_path.joinpath("dag_processor_manager.log")
    tmp_log_filepath = tmp_path.joinpath("tmp.log")

    # Create log files
    text_main = "test line 1\ntest line 2\ntest line 3"
    text_tmp = "test line 1\n test line 2"
    main_log_filepath.write_text(text_main)
    tmp_log_filepath.write_text(text_tmp)

    # Call function
    delete_local_dag_processor_manager_logs()

    # Make sure that tmp file is deleted
    assert not tmp_log_filepath.exists()

    # Make sure main file is truncated
    assert main_log_filepath.read_text() == "test line 3"

    # Delete temporary directory
    shutil.rmtree(tmp_path)


@patch("operators.transfer_delete_logs.conf")
def test_delete_local_dag_processor_manager_logs_empty(mock_airflow_conf: MagicMock, tmp_path: pathlib.Path, caplog):
    """
    Test that appropriate message is logged when there are no dag processor manager logs or
    tmp dag processor manager logs.
    """
    dag_processor_manager_log_filepath = tmp_path.joinpath("dag_processor_manager.log")
    mock_airflow_conf.get.return_value = str(dag_processor_manager_log_filepath)

    # Call function
    delete_local_dag_processor_manager_logs()

    assert "No processor manager logs" in caplog.text

    # Delete temporary directory
    shutil.rmtree(tmp_path)
