from unittest.mock import MagicMock, patch

import pytest
from airflow.hooks.base import BaseHook


@pytest.fixture()
def globals_fixture():
    """This fixture is used in every test that needs the globals() object. This is used to avoid test contamination"""
    return {}


@pytest.fixture()
def mock_operational_db_connection():
    with patch.object(BaseHook, "get_connections") as mock_connections:
        mock_connections.return_value = [
            MagicMock(get_uri=lambda: "http://some_username:super_secret_password@some.host.com:8080/myschema")
        ]
        yield mock_connections
