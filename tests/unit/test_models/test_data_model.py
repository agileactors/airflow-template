from unittest.mock import MagicMock, patch

from dags.models.data_model import Database


@patch("sqlalchemy.create_engine")
@patch("models.data_model.BaseHook.get_connections")
def test_execute_sql_query(mock_mssql_conn: MagicMock, mock_creator: MagicMock):
    """Test execute_sql_query."""
    mssql = Database()
    mock_connections = [MagicMock(get_uri=MagicMock(return_value="some_uri"))]
    mock_mssql_conn.return_value = mock_connections

    mock_creator.return_value.execute.return_value.fetchall.return_value = ["some_data", "some_more_data"]

    data_list = mssql.execute_sql_query(sql_query="SELECT * FROM SOME_TABLE")

    assert data_list == ["some_data", "some_more_data"]
