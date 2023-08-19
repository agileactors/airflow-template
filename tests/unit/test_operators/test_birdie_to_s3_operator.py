from unittest.mock import MagicMock, patch

import pendulum
import pytest
from models.data_model import HttpClientHandler
from operators.birdie_operator import BirdieToS3Operator

context = {
    "data_interval_start": pendulum.datetime(2001, 5, 21, 12, 0, 0),
    "data_interval_end": pendulum.datetime(2001, 5, 21, 12, 5, 0),
}


@patch("models.data_model.HttpClientHandler")
@patch("models.data_model.AwsS3")
def test_birdie_to_s3_operator_execute(
    mock_aws_s3: MagicMock,
    mock_api: MagicMock,
):
    """Test that the operator calls the correct methods"""
    mock_api.get_data.return_value = MagicMock()

    mock_save_json_to_s3 = MagicMock()
    mock_aws_s3.return_value = MagicMock(save_json_to_s3=mock_save_json_to_s3)
    operator = BirdieToS3Operator(
        task_id="test_task",
        oauth_url="test_oauth_url",
        api_url="test_api_url",
        s3_conn_id="test_s3_conn",
        s3_bucket="test_bucket",
        s3_key="test_key",
        body_bearer_dict={"test": "test"},
        params_dict={"test": "test"},
    )

    operator.execute(context=context)

    mock_api.assert_called_once()
    mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
    mock_save_json_to_s3.assert_called_once_with(
        partition="dt=20010521", filename="20010521_120000.json", json_data=mock_api().get_data()
    )


@patch.object(HttpClientHandler, "get_data")
@patch("models.data_model.AwsS3")
def test_birdie_to_s3_operator_execute_error_api_get(
    mock_aws_s3: MagicMock,
    mock_api: MagicMock,
    caplog,
):
    """Test that the operator calls the correct methods"""

    mock_api.side_effect = Exception("test error")

    mock_save_json_to_s3 = MagicMock()
    mock_aws_s3.return_value = MagicMock(save_json_to_s3=mock_save_json_to_s3)
    operator = BirdieToS3Operator(
        task_id="test_task",
        oauth_url="test_oauth_url",
        api_url="test_api_url",
        s3_conn_id="test_s3_conn",
        s3_bucket="test_bucket",
        s3_key="test_key",
        body_bearer_dict={"test": "test"},
        params_dict={"test": "test"},
    )

    with pytest.raises(Exception) as e:
        operator.execute(context=context)

        mock_api.assert_called_once_with(
            oauth_url="test_oauth_url",
            api_url="test_api_url",
            body_bearer_dict={"test": "test"},
            params_dict={"test": "test"},
        )

        mock_aws_s3.assert_not_called()
        mock_save_json_to_s3.assert_not_called()

    assert "Error getting data from api: test error" in caplog.text
    assert "Error getting data from api: test error" in str(e.value)


@patch("operators.birdie_operator.BirdieApi")
@patch("models.data_model.AwsS3")
def test_birdie_to_s3_operator_execute_error_aws(
    mock_aws_s3: MagicMock,
    mock_api: MagicMock,
    caplog,
):
    """Test that the operator calls the correct methods"""
    mock_api.get_data = MagicMock()

    mock_save_json_to_s3 = MagicMock()
    mock_save_json_to_s3.side_effect = Exception("test error")
    mock_aws_s3.return_value = MagicMock(save_json_to_s3=mock_save_json_to_s3)

    operator = BirdieToS3Operator(
        task_id="test_task",
        oauth_url="test_oauth_url",
        api_url="test_api_url",
        s3_conn_id="test_s3_conn",
        s3_bucket="test_bucket",
        s3_key="test_key",
        body_bearer_dict={"test": "test"},
        params_dict={"test": "test"},
    )

    with pytest.raises(Exception) as e:
        operator.execute(context=context)

        mock_api.assert_called_once_with(
            oauth_url="test_oauth_url",
            api_url="test_api_url",
            body_bearer_dict={"test": "test"},
            params_dict={"test": "test"},
        )
        mock_aws_s3.assert_called_once_with(conn_id="test_s3_conn", bucket_name="test_bucket", key="test_key")
        mock_save_json_to_s3.assert_called_once_with(
            partition="dt=20010521", filename="20010521_120000.json", json_data=mock_api().get_data()
        )

    assert "Error while trying to write data to S3: test error" in caplog.text
    assert "Error while trying to write data to S3: test error" in str(e.value)
