from unittest.mock import MagicMock, patch

import airflow
import pendulum
import pytest

from dags.operators.restart_failed_dags import restart_failed_dags

context = {
    "data_interval_start": pendulum.datetime(2001, 5, 21, 12, 0, 0),
}


@patch.object(airflow.models.DagRun, "find")
@patch("airflow.api.common.trigger_dag.trigger_dag")
def test_restart_failed_dags_success(mock_trigger_dag, mock_dag_run, caplog):
    # Mock the return value of DagRun.find()
    mock_dag_run.return_value = [MagicMock(dag_id="dag1", run_id="run1", state="failed")]

    # Call the function to be tested
    restart_failed_dags(**context)

    # Assertions
    assert mock_trigger_dag.call_count == 1
    assert "Successfully restarted dag_run:" in caplog.text


@patch.object(airflow.models.DagRun, "find")
@patch("airflow.api.common.trigger_dag.trigger_dag")
def test_restart_failed_dags_failed(mock_trigger_dag, mock_dag_run, caplog):
    # Mock the return value of DagRun.find()

    mock_dag_run.side_effect = Exception("test error")

    with pytest.raises(Exception):
        # Call the function to be tested
        restart_failed_dags(**context)

    # Assertions
    assert mock_trigger_dag.call_count == 0
    assert "Failed to find DAGs. Error: test error" in caplog.text


@patch.object(airflow.models.DagRun, "find")
@patch.object(airflow.api.common.trigger_dag, "trigger_dag")
def test_restart_failed_dags_failed_trigger(mock_trigger_dag, mock_dag_run, caplog):
    # Mock the return value of DagRun.find()
    mock_dag_run.return_value = [MagicMock(dag_id="dag1", run_id="run1", state="failed")]
    mock_trigger_dag.side_effect = Exception("test error")

    # Call the function to be tested
    restart_failed_dags(**context)

    # Assertions
    assert mock_trigger_dag.call_count == 1
    assert "Failed to restart dag_run:" in caplog.text
