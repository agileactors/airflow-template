from unittest.mock import MagicMock, patch

import polars as pl
from _pytest.logging import LogCaptureFixture
from models.data_model import AwsS3, Database, OpEvents, _fix_uri_for_mssql


@patch("models.data_model.pl")
@patch("models.data_model.BaseHook.get_connections")
def test_read_sql_query_with_polars(mock_mssql_conn: MagicMock, mock_pl: MagicMock):
    mssql = Database()
    mock_mssql_conn[0].get_uri = "some_uri"

    mock_pl.read_database.return_value = pl.DataFrame()

    df = mssql.read_sql_query_with_polars(sql_query="SELECT * FROM SOME_TABLE")

    assert mock_pl.read_database.call_count == 1
    assert isinstance(df, pl.DataFrame)


@patch("models.data_model.S3Hook")
def test_save_df_to_s3_parquet_format(mock_s3_hook: MagicMock, tmpdir, caplog):
    """Test save_df_to_s3_parquet_format."""
    awss3 = AwsS3(
        conn_id="SOME_CONN_ID",
        bucket_name="SOME_BUCKET",
        key="SOME_KEY",
    )

    mock_load = MagicMock()
    mock_s3_hook.return_value = mock_load

    awss3.save_df_to_s3_parquet_format(df=pl.DataFrame(), partition="partition", filename=tmpdir)

    mock_s3_hook.assert_called_once()
    mock_load.load_file_obj.assert_called_once()
    assert "Stored" in caplog.text


@patch("models.data_model.S3Hook")
def test_save_json_to_s3(mock_s3_hook: MagicMock, tmpdir, caplog):
    """Test save_json_to_s3."""
    awss3 = AwsS3(
        conn_id="SOME_CONN_ID",
        bucket_name="SOME_BUCKET",
        key="SOME_KEY",
    )
    mock_load = MagicMock()
    mock_s3_hook.return_value = mock_load

    awss3.save_json_to_s3(json_data={"key": "value"}, partition="partition", filename=tmpdir)

    mock_s3_hook.assert_called_once()
    mock_load.load_file_obj.assert_called_once()
    assert "Stored" in caplog.text


@patch("sqlalchemy.create_engine")
@patch("sqlalchemy.orm.sessionmaker")
def test_save_operational_event(mock_sessionmaker: MagicMock, mock_creator: MagicMock, mock_operational_db_connection):
    opevents = OpEvents()

    mock_engine = MagicMock()
    mock_creator.return_value = mock_engine

    mock_session = MagicMock()
    mock_session.add = MagicMock()
    mock_session.commit = MagicMock()
    mock_sessionmaker.return_value = MagicMock(return_value=mock_session)

    operational_event = MagicMock()

    opevents.save_operational_event(operational_event)

    mock_creator.assert_called_once_with(url="http://some_username:super_secret_password@some.host.com:8080/myschema")
    mock_sessionmaker.assert_called_once_with(bind=mock_engine)
    mock_session.add.assert_called_once_with(operational_event)
    mock_session.commit.assert_called_once_with()


@patch("models.data_model.S3Hook")
def test_transfer_local_file_to_s3(mock_s3_hook: MagicMock):
    awss3 = AwsS3(conn_id="SOME_CONN_ID", bucket_name="SOME_BUCKET", key="SOME_KEY")

    awss3.transfer_local_file_to_s3(local_file_path="some_path", partition="partition", filename="filename")
    mock_s3_hook.return_value.load_file.assert_called_once_with("some_path", "s3://SOME_BUCKET/SOME_KEY/partition/filename")


@patch("models.data_model.S3Hook")
def test_transfer_local_file_to_s3_double_file(mock_s3_hook: MagicMock, caplog: LogCaptureFixture):
    awss3 = AwsS3(conn_id="SOME_CONN_ID", bucket_name="SOME_BUCKET", key="SOME_KEY")

    def double_load(*args, **kwargs):
        raise ValueError("XXXXX")

    moc_load_file = MagicMock(side_effect=double_load)
    mock_load = MagicMock(load_file=moc_load_file)
    mock_s3_hook.return_value = mock_load

    awss3.transfer_local_file_to_s3(local_file_path="some_path", partition="partition", filename="filename")
    mock_load.load_file.assert_called_once_with("some_path", "s3://SOME_BUCKET/SOME_KEY/partition/filename")
    assert "XXXXX" in caplog.text


@patch("sqlalchemy.create_engine")
@patch("models.data_model.BaseHook.get_connections")
def test_execute_sql_query(mock_mssql_conn: MagicMock, mock_creator: MagicMock):
    """Test execute_sql_query."""
    mssql = Database()
    mock_connections = [MagicMock(get_uri=MagicMock(return_value="some_uri"))]
    mock_mssql_conn.return_value = mock_connections

    mock_creator.return_value.execute.return_value.fetchall.return_value = ["some_data", "some_more_data"]

    data_list = mssql.execute_sql_query(sql_query="SELECT * FROM SOME_TABLE", db_name="myschema")

    assert data_list == ["some_data", "some_more_data"]


def test_fix_uri_for_mssql():
    # Initial connection URI with the database parameters
    uri = "mssql://some_username:super_secret_password@some_host:8080/?database=Montero"

    # Expected connection URI after fixing, including additional parameters
    expected_uri = (
        "mssql+pyodbc://some_username:super_secret_password@some_host:8080/myschema?TrustServerCertificate"
        "=yes&driver=ODBC+Driver+18+for+SQL+Server"
    )

    # Calling the function to be tested and getting the actual modified URI
    actual_uri = _fix_uri_for_mssql(connection_uri=uri, db_name="myschema")

    # Asserting that the actual modified URI matches the expected URI
    assert actual_uri == expected_uri

    # Another scenario: Initial connection URI with additional parameters directly included
    uri = (
        "mssql://some_username:super_secret_password@some_host:8080/?database=Montero?TrustServerCertificate=yes"
        "&driver=ODBC+Driver+18+for+SQL+Server"
    )

    # Expected connection URI after fixing, with additional parameters and proper formatting
    expected_uri = (
        "mssql+pyodbc://some_username:super_secret_password@some_host:8080/myschema?TrustServerCertificate"
        "=yes&driver=ODBC+Driver+18+for+SQL+Server"
    )

    # Calling the function with the new URI and getting the actual modified URI
    actual_uri = _fix_uri_for_mssql(connection_uri=uri, db_name="myschema")

    # Asserting that the actual modified URI matches the expected URI for this scenario
    assert actual_uri == expected_uri
