from unittest.mock import MagicMock, patch

import pendulum
import pytest
from models.api_definitions.zoho_recruit_bulk_read import ZohoRecruitBulkReadApi
from operators.exceptions import ApiException


# Create fixture that is available to all test cases.
@pytest.fixture(scope="module", autouse=True)
def zoho_api():
    """Fixture to create ZohoRecruitBulkReadApi object."""
    oauth_url = "1https://accounts.zoho.eu/oauth/v2/token"
    api_url = "1https://recruit.zoho.eu/recruit/bulk/v2/read"

    body_bearer_dict = {
        "client_id": "my_client",
        "grant_type": "refresh_token",
        "client_secret": "secret_client",
        "refresh_token": "my_refresh_token",
    }

    return ZohoRecruitBulkReadApi(oauth_url=oauth_url, api_url=api_url, body_bearer_dict=body_bearer_dict)


@patch("requests.post")
def test_required_oauth(mock_post: MagicMock, zoho_api: ZohoRecruitBulkReadApi):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "access_token": "test_token",
        "api_domain": "https://www.zohoapis.eu",
        "token_type": "Bearer",
        "expires_in": 3600,
    }

    mock_post.return_value = mock_response

    bearer_token = zoho_api.required_oauth()

    assert mock_post.call_args[0][0] == zoho_api.oauth_url
    assert mock_post.call_args[1] == {"data": zoho_api.body_bearer_dict}
    assert bearer_token == "test_token"


def test_required_oauth_error(caplog, zoho_api: ZohoRecruitBulkReadApi):
    with pytest.raises(ApiException):
        zoho_api.required_oauth()
    assert "Error getting oauth" in caplog.text


@patch("requests.post")
def test_create_job(mock_post, zoho_api: ZohoRecruitBulkReadApi):
    # Set necessary parameters for the request
    start = pendulum.datetime(2023, 7, 29, 12, 30, 0, tz="utc")
    end = pendulum.datetime(2023, 7, 30, 12, 30, 0, tz="utc")
    module_name = "Interviews"
    token = "bearer_token"

    expected_headers = {"Authorization": "Zoho-oauthtoken bearer_token"}

    expected_payload = {
        "query": {
            "module": "Interviews",
            "criteria": {
                "group_operator": "or",
                "group": [
                    {
                        "api_name": "Created_Time",
                        "value": ["2023-07-29T12:30:00+00:00", "2023-07-30T12:30:00+00:00"],
                        "comparator": "between",
                    },
                    {
                        "api_name": "Modified_Time",
                        "value": ["2023-07-29T12:30:00+00:00", "2023-07-30T12:30:00+00:00"],
                        "comparator": "between",
                    },
                ],
            },
        }
    }

    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"status": "success", "details": {"id": "test_id"}}]}
    mock_post.return_value = mock_response

    job_id = zoho_api.create_job(start=start, end=end, module_name=module_name, token=token)

    assert job_id == "test_id"
    assert mock_post.call_args[0][0] == zoho_api.api_url
    assert mock_post.call_args[1]["headers"] == expected_headers
    assert mock_post.call_args[1]["json"] == expected_payload


def test_create_job_error(caplog, zoho_api: ZohoRecruitBulkReadApi):
    start = pendulum.datetime(2023, 7, 29, 12, 30, 0, tz="utc")
    end = pendulum.datetime(2023, 7, 30, 12, 30, 0, tz="utc")
    module_name = "Candidates"
    token = "bearer_token"
    with pytest.raises(ApiException):
        zoho_api.create_job(start=start, end=end, module_name=module_name, token=token)
    assert "Error creating job" in caplog.text


@patch("requests.post")
def test_create_job_error_bad_response(mock_post: MagicMock, caplog, zoho_api: ZohoRecruitBulkReadApi):
    start = pendulum.datetime(2023, 7, 29, 12, 30, 0, tz="utc")
    end = pendulum.datetime(2023, 7, 30, 12, 30, 0, tz="utc")
    module_name = "Candidates"
    token = "bearer_token"

    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"status": "error", "details": {"id": "test_id"}}]}
    mock_post.return_value = mock_response

    with pytest.raises(ApiException):
        zoho_api.create_job(start=start, end=end, module_name=module_name, token=token)
    assert "Error creating job. Response status: error" in caplog.text


@patch("requests.get")
def test_wait_for_job(mock_get: MagicMock, zoho_api: ZohoRecruitBulkReadApi):
    job_id = "test_job"
    token = "test_token"
    poke_interval = 5
    max_pokes = 10

    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"id": "test_id", "state": "COMPLETED"}]}
    mock_get.return_value = mock_response

    is_done = zoho_api.wait_for_job(job_id=job_id, token=token, poke_interval=poke_interval, max_pokes=max_pokes)

    assert is_done == "OK"


def test_wait_for_job_bad_request(caplog, zoho_api: ZohoRecruitBulkReadApi):
    job_id = "test_job"
    token = "test_token"
    poke_interval = 5
    max_pokes = 10

    with pytest.raises(ApiException):
        zoho_api.wait_for_job(job_id=job_id, token=token, poke_interval=poke_interval, max_pokes=max_pokes)

    assert "Error getting job status" in caplog.text


@patch("requests.get")
def test_wait_for_job_error(mock_get: MagicMock, caplog, zoho_api: ZohoRecruitBulkReadApi):
    job_id = "test_job"
    token = "test_token"
    poke_interval = 1
    max_pokes = 2

    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"id": "test_id", "state": "QUEUED"}]}
    mock_get.return_value = mock_response

    with pytest.raises(ApiException):
        zoho_api.wait_for_job(job_id=job_id, token=token, poke_interval=poke_interval, max_pokes=max_pokes)

    assert mock_get.call_count == 2
    assert "Job status is in state QUEUED. Retrying in 1 seconds." in caplog.text
    assert "Job did not complete after 2 pokes." in caplog.text


@patch("requests.get")
def test_required_get_data(mock_get: MagicMock, zoho_api: ZohoRecruitBulkReadApi):
    job_id = "test_job"
    token = "test_token"

    mock_response = MagicMock()
    mock_response.content = b"\x68\x65\x6c\x6c\x6f"
    mock_get.return_value = mock_response

    res = zoho_api.required_get_data(job_id=job_id, token=token)

    assert res == {"data": b"\x68\x65\x6c\x6c\x6f"}


def test_required_get_data_error(caplog, zoho_api: ZohoRecruitBulkReadApi):
    job_id = "test_job"
    token = "test_token"

    with pytest.raises(ApiException):
        zoho_api.required_get_data(job_id=job_id, token=token)

    assert "Error getting data" in caplog.text
