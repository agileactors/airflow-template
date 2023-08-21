
import pytest
from utils.airflow_api_utils import AirflowApiUtils



@pytest.fixture(scope="module")
def airflow_api_utils_fixture():
    return AirflowApiUtils()


@pytest.fixture()
def setup_and_teardown_fixture(airflow_api_utils_fixture):
    airflow_api_utils_fixture.delete_all_dag_runs()
