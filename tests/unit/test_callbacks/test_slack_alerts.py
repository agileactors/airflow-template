"""
Test callback functions related to slack alerts.
"""
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest
from callbacks.slack_alerts import slack_alert_failure


@pytest.fixture
def context():
    """
    Mock context object.
    """

    @dataclass
    class MockDag:
        dag_id: str = "test_dag_id"

    @dataclass
    class MockTask:
        task_id: str = "test_task_id"

    return {
        "dag": MockDag(),
        "run_id": "test_run_id",
        "task": MockTask(),
        "task_instance_key_str": "test_task_instance_key_str",
    }


@patch("callbacks.slack_alerts.SlackWebhookHook")
def test_slack_alert_failure(mock_hook: MagicMock, context, caplog):
    mock_hook_response = MagicMock()
    mock_hook.return_value = mock_hook_response
    mock_hook_response.send_text = MagicMock()

    expected_message = (
        "**" * 45 + "\n\n"
        ":warning: DAG RUN FAILED :warning:\n\n"
        "ID: `test_dag_id`\n"
        "Run ID: `test_run_id`\n"
        "Task ID: `test_task_id`\n"
        "Task Instance Key: `test_task_instance_key_str`\n\n"
    )
    expected_message += "**" * 45 + "\n\n"

    slack_alert_failure(context)

    mock_hook_response.send_text.assert_called_once_with(expected_message)
    assert "Posted slack alert" in caplog.text
