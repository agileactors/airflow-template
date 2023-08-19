import pytest
from utils.airflow_api_utils import AirflowApiUtils


@pytest.fixture(scope="module")
def airflow_api_utils_fixture():
    return AirflowApiUtils()
