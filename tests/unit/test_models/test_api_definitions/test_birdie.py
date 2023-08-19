from unittest.mock import MagicMock, patch

import pytest
from models.api_definitions.birdie import BirdieApi
from models.data_model import HttpClientHandler
from operators.exceptions import ApiException


@patch("requests.post")
def test_get_bearer_token(mock_post: MagicMock):
    """Test get_bearer_token."""
    oauth_url = "http://test_oauth_url"
    api_url = "http://test_api_url"

    api = HttpClientHandler(BirdieApi(oauth_url=oauth_url, api_url=api_url, body_bearer_dict={}))

    mock_response = MagicMock()
    mock_response.json.return_value = {"success": True, "access_token": "test_token"}

    mock_post.return_value = mock_response

    bearer_token = api.oauth()

    assert bearer_token == "test_token"


def test_get_bearer_token_error(caplog):
    """Test get_bearer_token."""
    oauth_url = "2http://test_oauth_url"
    api_url = "2http://test_api_url"

    api = HttpClientHandler(BirdieApi(oauth_url=oauth_url, api_url=api_url, body_bearer_dict={}))

    with pytest.raises(ApiException):
        api.oauth()


@patch("requests.post")
@patch("requests.get")
def test_get_data(mock_get: MagicMock, mock_post: MagicMock):
    """Test get_data."""
    oauth_url = "http://test_oauth_url"
    api_url = "http://test_api_url"

    api = HttpClientHandler(BirdieApi(oauth_url=oauth_url, api_url=api_url, body_bearer_dict={}))

    mock_response_post = MagicMock()
    mock_response_post.json.return_value = {"success": True, "access_token": "test_token"}
    mock_response_get = MagicMock()
    mock_response_get.json.return_value = {"success": True, "data": "test_data"}

    mock_post.return_value = mock_response_post
    mock_get.return_value = mock_response_get

    data = api.get_data()

    assert data == {"success": True, "data": "test_data"}


@patch("requests.post")
def test_get_data_error(mock_post: MagicMock, caplog):
    """Test get_data."""
    oauth_url = "2http://test_oauth_url"
    api_url = "2http://test_api_url"

    api = HttpClientHandler(BirdieApi(oauth_url=oauth_url, api_url=api_url, body_bearer_dict={}))

    mock_response_post = MagicMock()
    mock_response_post.json.return_value = {"success": True, "access_token": "test_token"}

    mock_post.return_value = mock_response_post
    # requests.get = MagicMock(side_effect=Exception("test error"))

    with pytest.raises(ApiException):
        api.get_data()
