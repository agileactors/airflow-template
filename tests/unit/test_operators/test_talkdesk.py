"""
Contains tests for functions used in the talkdesk DAG.
"""

from json import dumps
from unittest.mock import MagicMock, patch

import pendulum
import polars as pl
import pytest
import requests
from operators.talkdesk import (
    delete_report_talkdesk_response_check,
    delete_report_talkdesk_response_filter,
    get_job_status_response_check,
    get_token_response_filter,
    send_job_request_response_check,
    send_job_request_response_filter,
    talkdesk_report_transfer,
)


def test_get_token_response_filter():
    """
    Test that the function returns the secret token from the response.
    """
    # Create test response
    res = requests.Response()

    # Set response content
    utf_string = '{"secret_token": "iamasecret", "other_part_of_response": "test"}'
    bytes_string = utf_string.encode("utf-8")
    res._content = bytes_string

    # Call function
    token = get_token_response_filter(res)

    assert token == "iamasecret"


def test_send_job_request_response_check_true():
    """
    Test that the function correctly checks the existense of a job id.
    """
    # Create test response
    res = requests.Response()

    # Set response content
    body_json = {"job": {"id": "jobid"}, "other_part_of_response": "test"}

    bytes_string = dumps(body_json).encode("utf-8")
    res._content = bytes_string

    # Call function
    check = send_job_request_response_check(res)

    assert check


def test_send_job_request_response_check_false():
    """
    Test that the function correctly checks the existense of a job id.
    """
    # Create test response
    res = requests.Response()

    # Set response content
    body_json = {"job": {"notid": "jobid"}, "other_part_of_response": "test"}

    bytes_string = dumps(body_json).encode("utf-8")
    res._content = bytes_string

    # Call function
    check = send_job_request_response_check(res)

    assert not check


def test_send_job_request_response_filter():
    """
    Test that the function returns the job id from the response.
    """
    # Create test response
    res = requests.Response()

    # Set response content
    body_json = {"job": {"id": "jobid"}, "other_part_of_response": "test"}

    bytes_string = dumps(body_json).encode("utf-8")
    res._content = bytes_string

    # Call function
    job_id = send_job_request_response_filter(res)

    assert job_id == "jobid"


def test_get_job_status_response_check_true():
    """
    Test that the function correctly checks if a job status is completed.
    """
    # Create test response
    res = requests.Response()

    # Set response content
    body_json = {"job": {"id": "jobid", "status": "done"}, "other_part_of_response": "test"}

    bytes_string = dumps(body_json).encode("utf-8")
    res._content = bytes_string

    # Call function
    check = get_job_status_response_check(res)

    assert check


def test_get_job_status_response_check_false():
    """
    Test that the function correctly checks if a job status is completed.
    """
    # Create test response
    res = requests.Response()

    # Set response content
    body_json = {"job": {"id": "jobid", "status": "notdone"}, "other_part_of_response": "test"}

    bytes_string = dumps(body_json).encode("utf-8")
    res._content = bytes_string

    # Call function
    check = get_job_status_response_check(res)

    assert not check


def test_delete_report_talkdesk_response_check_true():
    """
    Test that the delete operation succesfully completed.
    """
    res = requests.Response()
    res.status_code = 204

    check = delete_report_talkdesk_response_check(res)

    assert check


def test_delete_report_talkdesk_response_check_false():
    """
    Test that the delete operation succesfully completed.
    """
    res = requests.Response()
    res.status_code = 404

    check = delete_report_talkdesk_response_check(res)

    assert not check


def test_delete_report_talkdesk_response_filter():
    """
    Test that function returns status code of response.
    """
    res = requests.Response()
    res.status_code = 204

    status_code = delete_report_talkdesk_response_filter(res)

    assert status_code == 204


@patch("requests.get")
@patch("airflow.hooks.base.BaseHook.get_connection")
@patch("models.data_model.AwsS3.save_df_to_s3_parquet_format")
def test_talkdesk_report_transfer(
    mock_save_df_to_s3_parquet_format: MagicMock,
    mock_get_connection: MagicMock,
    mock_get_request: MagicMock,
):
    """
    Test that function downloads report and transfers it to s3.
    """

    # Mock save_df_to_s3_parquet_format function
    mock_save_df_to_s3_parquet_format.return_value = MagicMock()

    # Mock airflow connection
    mock_get_connection.return_value = MagicMock()
    mock_get_connection.return_value.host = "test_host"

    # Mock get request
    mock_get_request_response = MagicMock()
    mock_get_request_response.text = "name,email,address\nilias,ilias.nikas@newcrosshealthcare.com,testaddress\n"
    mock_get_request_response.status_code = 200
    mock_get_request.return_value = mock_get_request_response

    # Mock context argumnent
    ti = MagicMock()
    xcom_pull = MagicMock()
    xcom_pull.return_value = "text_xcom"
    ti.xcom_pull = xcom_pull
    data_interval_start = pendulum.DateTime(2020, 1, 1, 0, 0, 0)

    # Create expected dataframe to be transferred
    data = {"name": ["ilias"], "email": ["ilias.nikas@newcrosshealthcare.com"], "address": ["testaddress"]}
    expected_df = pl.DataFrame(data)

    # Call the function
    talkdesk_report_transfer(ti=ti, data_interval_start=data_interval_start)

    # -- Assertions begin here

    # Save dataframe
    mock_save_df_to_s3_parquet_format.call_count == 1
    assert mock_save_df_to_s3_parquet_format.call_args[1]["df"].frame_equal(expected_df)
    assert mock_save_df_to_s3_parquet_format.call_args[1]["partition"] == "dt=20200101"
    assert mock_save_df_to_s3_parquet_format.call_args[1]["filename"] == "20200101_000000.parquet"

    # Get airflow connection
    assert mock_get_connection.call_count == 1
    assert mock_get_connection.call_args[0][0] == "talkdesk_eu"

    # Get request
    expected_headers = {
        "accept": "text/csv",
        "Content-type": "application/json",
        "Authorization": "Bearer text_xcom",
    }
    assert mock_get_request.call_count == 1
    assert mock_get_request.call_args[0][0] == "test_host/data/reports/calls/files/text_xcom"
    assert mock_get_request.call_args[1]["headers"] == expected_headers


@patch("requests.get")
@patch("airflow.hooks.base.BaseHook.get_connection")
@patch("models.data_model.AwsS3.save_df_to_s3_parquet_format")
@patch("airflow.models.Variable.get")
def test_talkdesk_report_transfer_bad(
    mock_airflow_variable: MagicMock,
    mock_save_df_to_s3_parquet_format: MagicMock,
    mock_get_connection: MagicMock,
    mock_get_request: MagicMock,
    caplog,
):
    """
    Test that function raises exception when response is not 200.
    """

    # Mock airflow variable
    mock_airflow_variable.return_value = "test_bucket_airflow_variable"

    # Mock save_df_to_s3_parquet_format function
    mock_save_df_to_s3_parquet_format.return_value = MagicMock()

    # Mock airflow connection
    mock_get_connection.return_value = MagicMock()
    mock_get_connection.return_value.host = "test_host"

    # Mock get request
    mock_get_request_response = MagicMock()
    mock_get_request_response.text = "name,email,address\nilias,ilias.nikas@newcrosshealthcare.com,testaddress\n"
    mock_get_request_response.status_code = 404
    mock_get_request.return_value = mock_get_request_response

    # Mock context argumnent
    ti = MagicMock()
    xcom_pull = MagicMock()
    xcom_pull.return_value = "text_xcom"
    ti.xcom_pull = xcom_pull
    data_interval_start = pendulum.DateTime(2020, 1, 1, 0, 0, 0)

    # Call the function
    with pytest.raises(Exception):
        talkdesk_report_transfer(ti=ti, data_interval_start=data_interval_start)

    assert "Request failed with status code 404" in caplog.text
