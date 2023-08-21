import tempfile
from unittest.mock import MagicMock, patch

import pytest
import yaml
from airflow import DAG
from airflow.models import Pool
from dagfactory.dagfactory import DagFactory, load_yaml_dags
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException


def test_validate_config_filepath():
    """Test _validate_config_filepath method"""

    with pytest.raises(DagFactoryConfigException) as e:
        DagFactory._validate_config_filepath("relative/path/to/config.yaml")
    assert str(e.value) == 'DAG Factory "config_filepath" must be absolute path'


def test_load_config():
    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = {"name": "John", "age": 30, "is_student": True}
        yaml.dump(mock_data, f)
        actual_data = DagFactory._load_config(f.name)
        assert actual_data == mock_data


def test_load_config_exception():
    """Test _load_config method"""

    with pytest.raises(DagFactoryConfigException):
        DagFactory._load_config("relative/path/to/config.yaml")


def test_get_dag_configs():
    """
    Test get_dag_configs method
    """
    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = {"name": "John", "age": 30, "is_student": True}
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        actual_data = dag_factory.get_dag_configs()
        assert actual_data == mock_data


def test_get_default_config():
    """
    Test get_default_config method
    """
    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = {"default": "test", "name": "John"}
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        actual_data = dag_factory.get_default_config()
        assert actual_data == mock_data["default"]


@patch("airflow.models.Pool.get_pools")
def test_build_dags(mock_get_pools: MagicMock, mock_operational_db_connection):
    """Test build_dags method"""

    mock_get_pools.return_value = [Pool(pool="test_pool")]
    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = _create_mock_dict_with_dag_conf(dag_name="test_dag")
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        actual_data = dag_factory.build_dags()

        assert isinstance(actual_data["test_dag"], DAG)


def test_build_dags_exception():
    """
    Test build_dags method
    """

    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = {
            "test_dag": {
                "default_args": {"owner": "airflow", "start_date": "2022-05-03", "retries": 1, "retry_delay_sec": 300},
                "schedule": "@once",
                "concurrency": 1,
                "max_active_runs": 1,
                "dagrun_timeout_sec": 120,
                "default_view": "grid",
                "orientation": "LR",
                "description": "Dag representing flow from test_schema.test_table",
                "tasks": {
                    "montero_people_to_s3": {
                        "operator": "ERROR",
                        "db_conn_id": "test_db_conn_id",
                        "sql_query": "SELECT * FROM Montero.dbo.People WITH (NOLOCK)",
                    }
                },
            }
        }
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        with pytest.raises(DagFactoryException):
            dag_factory.build_dags()


def test_register_dags(globals_fixture):
    """Test register_dags method"""

    dag_dict = {
        "dag1": {
            "name": "dag1",
            "nodes": [
                {"name": "task1", "dependencies": []},
                {"name": "task2", "dependencies": ["task1"]},
                {"name": "task3", "dependencies": ["task2"]},
            ],
        }
    }

    DagFactory.register_dags(dags=dag_dict, globals=globals_fixture)

    assert "dag1" in globals_fixture
    assert globals_fixture["dag1"] == dag_dict["dag1"]


@patch("airflow.models.Pool.get_pools")
def test_generate_dags(mock_get_pools: MagicMock, globals_fixture, mock_operational_db_connection):
    """Test generate_dags method"""

    mock_get_pools.return_value = [Pool(pool="test_pool")]

    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = _create_mock_dict_with_dag_conf(dag_name="test_dag")
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        dag_factory.generate_dags(globals=globals_fixture)

        assert "test_dag" in globals_fixture


def test_generate_dags_exception(globals_fixture):
    """Test generate_dags method exception"""

    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = _create_mock_dict_with_dag_conf()
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        with pytest.raises(DagFactoryException) as e:
            dag_factory.generate_dags(globals=globals_fixture)

        assert "Failed to generate dag. Verify config is correct." in str(e.value)


@patch("airflow.models.Pool.get_pools")
def test_clean_dags(mock_get_pools: MagicMock, globals_fixture, mock_operational_db_connection):
    """Test clean_dags method"""

    mock_get_pools.return_value = [Pool(pool="test_pool")]

    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = _create_mock_dict_with_dag_conf(dag_name="TEST")
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        dag_factory.generate_dags(globals=globals_fixture)

    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = _create_mock_dict_with_dag_conf(dag_name="test_dag")
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        dag_factory.generate_dags(globals=globals_fixture)

        dag_factory.clean_dags(globals=globals_fixture)

        assert "TEST" not in globals_fixture
        assert "test_dag" in globals_fixture


@patch("airflow.models.Pool.get_pools")
def test_clean_dags_for_non_factory_generated_dag(mock_get_pools: MagicMock, globals_fixture, mock_operational_db_connection):
    """Test clean_dags method when there is a DAG in globals without the is_dagfactory_auto_generated attribute"""

    mock_get_pools.return_value = [Pool(pool="test_pool")]
    dag_not_generated_by_factory = DAG("TEST")
    globals_fixture["NOT_GENERATED_BY_FACTORY"] = dag_not_generated_by_factory

    # Create temporary file and write mock YAML data to it
    with tempfile.NamedTemporaryFile(mode="w") as f:
        mock_data = _create_mock_dict_with_dag_conf(dag_name="test_dag")
        yaml.dump(mock_data, f)
        dag_factory = DagFactory(f.name)
        dag_factory.generate_dags(globals=globals_fixture)
        dag_factory.clean_dags(globals=globals_fixture)

    assert "NOT_GENERATED_BY_FACTORY" in globals_fixture
    assert "test_dag" in globals_fixture


@patch("airflow.models.Pool.get_pools")
def test_load_yaml_dags(mock_get_pools: MagicMock, globals_fixture, tmpdir, mock_operational_db_connection):
    mock_get_pools.return_value = [Pool(pool="test_pool")]

    """Test load_yaml_dags method"""
    with tempfile.NamedTemporaryFile(mode="w", dir=tmpdir, suffix=".yaml") as f:
        mock_data = _create_mock_dict_with_dag_conf(dag_name="test_dag")
        yaml.dump(mock_data, f)

        load_yaml_dags(
            globals_dict=globals_fixture,
            dags_folder=tmpdir,
        )

        assert "test_dag" in globals_fixture


def _create_mock_dict_with_dag_conf(dag_name: str | None = None) -> dict:
    """
    Create mock dictionary with dag configuration
    :param dag_name: name of dag
    :return: dictionary with dag configuration
    """
    return {
        dag_name: {
            "default_args": {"owner": "Data Squad", "start_date": "2022-05-03", "retries": 1, "retry_delay_sec": 300},
            "schedule": "@once",
            "concurrency": 1,
            "max_active_runs": 1,
            "dagrun_timeout_sec": 120,
            "default_view": "grid",
            "orientation": "LR",
            "description": "Dag representing flow from test_schema.test_table",
            "tasks": {
                "montero_people": {
                    "operator": "operators.db_operator.PgOperator",
                    "db_conn_id": "test_db_conn_id",
                    "db_name": "test_db_name",
                    "schema_name": "test_schema",
                    "table_name": "test_table",
                }
            },
        }
    }
