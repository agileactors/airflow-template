from unittest.mock import MagicMock, patch

import pytest
from operators.delete_s3key_operator import DeleteS3KeyOperator
from operators.exceptions import OperatorException

TEST_STRING = "{{key1}} {{key2}}"


def test_templated_prefix_to_prefix():
    context = {"key1": "Hello", "key2": "World"}
    prefix_import = "tests.unit.test_operators.test_delete_s3key_operator.TEST_STRING"
    op = DeleteS3KeyOperator(prefix=prefix_import, task_id="XXX")
    rendered_string = op._templated_prefix_to_prefix(context)

    assert rendered_string == "Hello World"


def test_templated_prefix_to_prefix_no_prefix():
    context = {"key1": "Hello", "key2": "World"}

    op = DeleteS3KeyOperator(prefix=None, task_id="XXX")

    with pytest.raises(OperatorException) as ex:
        op._templated_prefix_to_prefix(context)

    assert str(ex.value) == "Empty prefix for DeleteS3KeyOperator"


@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
def test_execute_DeleteS3KeyOperator(mock_hook: MagicMock, caplog):
    op = DeleteS3KeyOperator(s3_conn_id="MY_CONN", s3_bucket="MY_S3_BUCKET", prefix="MY_PREFIX", task_id="XXX")

    op._templated_prefix_to_prefix = MagicMock(name="_templated_prefix_to_prefix", return_value="BLAHBLAH")

    context = MagicMock()

    s3_hook_mock = MagicMock()
    s3_hook_mock.list_keys = MagicMock(return_value=["MICKEY", "MOUSE"])
    s3_hook_mock.delete_objects = MagicMock()

    mock_hook.return_value = s3_hook_mock

    op.execute(context)

    mock_hook.assert_called_once_with(aws_conn_id="MY_CONN")

    s3_hook_mock.list_keys.assert_called_once_with("MY_S3_BUCKET", "BLAHBLAH")
    s3_hook_mock.delete_objects.assert_called_once_with("MY_S3_BUCKET", ["MICKEY", "MOUSE"])

    assert "Listing MY_S3_BUCKET for BLAHBLAH" in caplog.text
    assert "Going to delete ['MICKEY', 'MOUSE']" in caplog.text
