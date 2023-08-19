from unittest.mock import MagicMock, patch

import pendulum
import polars as pl
import pytest
from operators.zoho_recruit_bulk_read_operator import ZohoRecruitBulkReadToS3


# Create zoho operator fixture that is available to all test cases.
@pytest.fixture(scope="module", autouse=True)
def zoho_operator():
    """Fixture to create ZohoRecruitBulkReadToS3 object."""

    task_id = "zoho_recruit_bulk_read"
    module_name = "Candidates"
    s3_conn_id = "s3_conn_id"
    s3_bucket = "s3_data_lake"
    s3_key = "airflows/zoho_recruit/candidates"
    oauth_url = "1https://accounts.zoho.eu/oauth/v2/token"
    api_url = "1https://recruit.zoho.eu/recruit/bulk/v2/read"
    body_bearer_dict = {
        "client_id": "my_client",
        "grant_type": "refresh_token",
        "client_secret": "secret_client",
        "refresh_token": "my_refresh_token",
    }

    zoho_operator = ZohoRecruitBulkReadToS3(
        task_id=task_id,
        module_name=module_name,
        oauth_url=oauth_url,
        api_url=api_url,
        s3_conn_id=s3_conn_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        body_bearer_dict=body_bearer_dict,
    )

    return zoho_operator


# Create context fixture
@pytest.fixture(scope="module", autouse=True)
def context():
    context = {
        "data_interval_start": pendulum.datetime(2023, 7, 29, 12, 30, 0, tz="utc"),
        "data_interval_end": pendulum.datetime(2023, 7, 30, 12, 30, 0, tz="utc"),
    }

    return context


@patch("models.api_definitions.zoho_recruit_bulk_read.ZohoRecruitBulkReadApi.required_oauth")
@patch("models.api_definitions.zoho_recruit_bulk_read.ZohoRecruitBulkReadApi.create_job")
@patch("models.api_definitions.zoho_recruit_bulk_read.ZohoRecruitBulkReadApi.wait_for_job")
@patch("models.api_definitions.zoho_recruit_bulk_read.ZohoRecruitBulkReadApi.required_get_data")
def test_required_api(
    get_data_mock: MagicMock,
    wait_job_mock: MagicMock,
    create_job_mock: MagicMock,
    oauth_mock: MagicMock,
    zoho_operator: ZohoRecruitBulkReadToS3,
    context: dict,
):
    # Mock return value as zipped csv file in bytes format (header and 1 row of data)
    get_data_mock.return_value = {
        "data": b"PK\x03\x04\x14\x00\x08\x08\x08\x00ji\xfeV\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x00\x00\x0052087000005402065.csv\x8dXio#\xc7\x11\xfdn\xc0\xff\x81\x18\xe4C\x82\xb4\x8c\x19\xde\xd4\xa7H\x14wE\xafx\x84\xe4Z\xb0\x83\xa01\x9ci\x92m\r\xa7\xe99(\xcbA\xfe{^U\xcfE\xaevc\x02^\xf5\xf4Q]\xc7\xabW\xd5\x9e\x86b\xec\xc7\xa1\x0e\xfdL\xc9\xe9\x83\xf8\xa0\x934\x93s\xff\xa8\xc4\x93_\x8e>\xe4QdG\x93\xa3\xaf#\xb1<\x98\x18\xb3\xfe\xefbf\xb6:R\xe2YmS\x9d)q\x97\xa6&\xd0\x10\x15\xca\x8d\xbfO\xc5:K\x94\xca\xc4Xgo\x18c^\xfc\xa2OrlB%\xc6&\x8f\xb3\xe4ML~?\xa9D\xab8PR\xc7\xf2g\xe5'\xa9\x18\xe7I\xa2\xe2LN\x8e\xa7\xc8\xbc\xa9\xa4\x9a\xf8\xd1l\xe5Fg\xb8q\xfd\xa2\xa1\xd3\x1a\xc2\x1f\xf5\xfe\xa0\xa0\xe9?s?\xd2;\x1d\xf8\x996\xb1|TQ\xc8\xb2\x03Rf\xedG>\xee*\xc5\x94\x9f\xfe\xf1\xe4\xeb},\xd7&O\x02\x92\xf9vb'\xdc\x85\xa1&)~$\xa7\xf1\xce\x88q\xa2\xd8\xa6\xfb7\x18\x1c\xe2\x12;.\xa77\x1a\x9e\xf9|\n\xf9c\x11\x0b\x88\xcf3V\xa3\xb81 3\x83\x83\x1f\xef\x95\\\x91\x17\xd8\xb5wA\xa6\xcf\xf0\x8c=\xcfS3x\xb7\x14\xb8y\xd5Y\x06\xdb\x1b>\xf5\xe37\xe8\x8a\x8fH.\x13\xb3\xc3\xde\xb4\x11\xbd\xc5k\x8c\xed\x85-\x1c(\xb98er\x91gb\x9a\xcaG\x93\xc9j/M<\x99\xe0E\x854\xfa\x1c\xfff}g?\xef\xb2\xcc\x0f\x0eGr\xd42Q)\xfe6.\xa1(\xe6t+\x02\x9b\xc8\xa5\x0f\x8b\xa61\xac\xa8V\x9e\xfc7\x83\x1ba\xa6\x8e\xf7\xe2'\r\\\x98D\xae\x03\x93(\xf1\xf1a\xb9B\xf4\xe3\x94\x83`\xf7_\xcc\xcd\x08\x19\x173\x0f\xa4\xedZ\x05&\x0e\x112i\xf1\xb7H\xf4^\xc7\x97Z\xed\x01Hh{h\x1897\xd2\xec\xe4\xdd\xe9\x14\x15\xa8H\xa1O\x92\xc1V\xb9>\xe8\x1d\xf9\xe3\x94\xe5\xd0k\xa5v\x8a\xc2\xa4\xd2j\n\x10\xcf\x8c\xbc\x8bCxG\xefLr\xac7\x03o\x99\xc4\xe2\xb3I^\xaa\xd9I\xbc\x87\n\xec\xb4\x19 \x0f\xdbkQ\x89:k\x93\xa7\x9cA\xd5\xec\xc3\xfd\xba\x1a\x03oP\xbd\xbe\xfcQ\xf9Qv\x90\x0f*\x00N-\x9e\xcb%\xbe\xf4Q\xa7p\xea[5y\xff\xf3\xe2A.\r\xcc|\x03\xaa\x02u\x02X\x04y\x8e\xec\xbf\x87\xc9\x07\xa4\xdbY\x87\xd2\x1b}\xb1m\xa6\xc30R\xac[*>\xc5\xe65\x96w\xa9x\x84\x13S\x89{\x0f\xacJ\x80p\xcbW\xbaZU\xe9*\x96~\x9a\x9eL\x92\xe1\x06sTY\xa2\x03\x89\x84\xd3\xd0\x8b\x83\xb6\xda<\xcb\x07\x13\xe4\xe4\x93\xb4\x8c6\xf0\xe0\xcb\r\xd2\xcc\x8eh\xcf\xc5\x19$2\xe4p\xac\xe8v\xda\xc4\x82\x96>lX\x1fH\rf\x8f\xc9\x18\xde;\xa8\xe0\xe5\xbd\xbb\x9e\xe0\x1e\x88\xf2SH\xf8@\xe0\xab\xceI\\\x90o\x8f:M)9\x17\xf1N\x87F~^=\xb1\x80B\xc5M\x1e\xc3\x92\xb5\xfe\x039\x9d\x02\xe3\xe0\x83K\xd4T\xa6\\L2\xc0/\xe7\x90H\xeax\"\x85\xd4\x0e9\x18\x879[V\n@rZ\x8e\xd9$\xbe\x8e\t1\x8c\xfcbu\x86\x80a\x8a\x10H\xc1\x88\xe8\xe3\xab\x07\xc7&\xc1\xc4Y'y\xe5\xe9w0T\xac00 \x1e\x842\x8d\xc5}\xe2\xc7\xc1A\xdc\x1b\xf3\xc2*$~h\xfd9\xf7\xed-\xcc\xddL*t_\xc0\xbb\xc8e\x10\xe3\x13\r!kR\x9a\xbb\xf7S\x9d\xe2\xbe\x0c\x19\x9a\xbe\xbf*\x96\x91\x1f\xd8$!\x02\x915\xf7\x8b\xe7\x83\x9f\xa5HU9\xcf\x8f[b\xbd\"%\x9et\xacd\xbbd\xee\xe8\xad\xac\t!U\x0b\x16\xb22(\x07\xf3<I/\xe4\xcd\x14\xf0\xfb^\x88V\xfe\x87\x92\\\x8a1\xec-8\x02z16\xd7Y\x1e\x92\x92\x84\x1d\xaa3\xb8\x8d\xf2\x0e`8\x13 \xc1\x91\xf7\t\n\x04\xc8\x06;\xa6\t\r\x02|\xff\xa1\x08S\x16\xb4\x8f~\xda\xc0\x1d\x7fB\xe1Sb\xf6l\x16\xc9\x91~\xcdLW\xac\x82\xad\xd9A\xc9\xcf\x9f\xc4F\x81z\xa8\x1e\x80\xa6a8\x82\x13\xa2Fg\x8ck\xb6\xfc\x01\x92P\x83\xa9\x8e,\x12pO\x08\x89\x11YhKX\xcaDS\xe6F\x05\x01\xcc5\x86c\x95d\xb6t\xaa\xd2\xff\\\xad\xf9\xc8b''g\xce\xab\xb4>1M\xd3\x1c^\xe1\xe4+*9m\xb4\\C\x1bV(\x025\xb1\x15\x84U\x15\xe0&\xb66D9\xf5Y\xd2\x81\x04pHp\x05\x0c\x15\xc57\xb2wW\xea7\x97\xd3rX\xf1\xeb\x93\xda\xfbQ\xc1dt\x84c\xc9\xca4hE|Tq\xa8\x92b\x1e\x90\x00\xc3s\xe9\xb6\xc6,\xe1oK\x0f\x12\xac\xcf\x9a0\xcf\x08\xdbv\x8c\xe5\x98|\xbb\xd3\xc9\xd1\xc6\xed.\x8a\xcc+\xd4,#\xf7\xacaD\xa3\x7f\xc0v\x0e\xfe\x12D\xe9o#\xb6\xad@\xab\r@]&T\x92\xf2\x91\"\x85X\x1fY\xf4\x18\xe2\x83R\x17L9\x9f\x8d\xe5R\xc7\x9eXN\xe7\xb0#V\xaf8\xb9Rg\xf84\xb4\xd9n\xcd\xc1*\xeb\x0f\x11+\xb5G\x18P\xe8BI\xd3\xdc\xcfU\xeb\xe8+&%$\xacz\xcc\x04 ^\x10\xbc\xd7d\xd3\xfa\xc8\x05\x99V\xa1\x85b\x8d\x9b\xf8\x96\xa6\xc0'uV\x112z\x8d\xd6\x0c\x8d\x0c\xf6\xc1\xa1\xc7#H\x17\x08^\xe6[d\x84,\xb8\x8b\x8eQ\x13Q\xfc\x95U'\x94Z\xb5\x00\x9a\xcdAU\xb7\t\x1bN\xf6\xf2E3\x98^\x99\xd9\xd4[Z\xcbDA+\xdc\xfe]\x86\x87\xa3o\xa7\xb8OH|\x8a\x8d\tJo\x81\r6\xd4\x7fR\xb1\x10\xd4\xa0B\xc25\xbb1\x02|\x14\xe0\xc6\\\x03i\x1bC\xa0B\xd7E\x9dV\x04|\x82\xe7\xb8\xe2-\x7f\xfahQL\x03\xce\xb9\"\xac\xf8,\xae\xa7!8 \x88L\n\x14U9\x81\xd9\x99\xa2az\xd0'\xfe\x1cG\xe8\xad\x01\x03\x1a7z\"2\xea\x92#\xb8|RR5\x12\xaai0g\x82\xf7\xd5L\xf1\xdec\x1c\xaf\xe2\x19\xf9\x01mk\x99yp\x95\xf5?\x07\xed.\xfc5O3[\xc3\xe7J\x11\xd5\x95\xb9@}\\cu\xa5~\xcb\xe1sj_P\x01\x1b\xd2\x8a\xee\x98\xfd\xff\xecS\x7f\xfc^cFj2\xf2\xe8D`2\xa6\xf2\xe2\xe8Z%g\xcd\x9d h-\xa0\xc7Nb2\xbc\"\xe8\xe0Oy\x84\x16\x9bu\xfd\x98\x98\xfc\x94r\x1f\x826\x13H,\x8b=O\xa1\xe7\x0e\xd0\x13\xe1]\x14\xd1_\xe8\x83\xc7J9\xc9\x9d~\xf9\\\xd8\x827\xef&\xb6\xe3or\xc4\xb5\x9b\xf8>\xe9In\xfaib\x06\xc0\x1c\xa8Gz4\x11\xe5\xf6\xaf*\xc8\x1axnF\xd8\xde%\xcaF\xf6J\xf2\x06M\x03y\xe2\x02\x14\xcdy\xe2\xb5\x1dXN\xda\x17\xdc;\x0e\x9d\xc6\x19yM\xbdB+\x9c\x81\xec\xaa\xc4W\xad5\x89\xac\xcb\x8e'\x98\xa6\xb9\x12S\xf2b;\x81\x88\xdc\x04\xe4\xe0\xe1X\xb3\x11\r\xc7~n{B\xec?\x97\xfd!\x83\xe7\xb7\\\xa3\xc2\xb0\xc7V:}!\x8e@\xc1!\xed\x04\xf9u\x82\xedh\x8d\xf8\x9b\x9b\x9a\xcbX\xd9)\x10\n\x8f\xae\x1cC\x81,\x93\x8c\xe4\x17\xcf\x89\xf2\xec\x83J\x83Ds+\xc6\xbc8I\x12C\x058M\xe9-b{}~\xf2\xa5W\x82\xd3\"\x98%8\xd6\xcafQ\x89\x0fRE\xc7i\xbe\x03\x7fi\xf2\x8c\xe67hY\xd1\x89\xe7\xb1\xe3h}\xff\xc5\xfc\x95\x85\\\x9f\xd0Lh\\M\xcb\xbe]\x10\xeb\\ge\xdf@\x9e\xa7v\x83\xfa\x89\xc6\x9b\xa8D\x94}J\xae\xc9\x85T\xdd\xc2\xc4\xb7\xb5\x86q\xb6L\xf4\xd9\xc7{\xe2\xfaY\xc1\xdf\x1a\xb6\x973\xf40&\x92C\x0b\tR\xc0\xab\xdb\x82\xbf~\xadUxC\xcb\xf7\"&3*\x0b\xa7Hee\x9b\xf1\x0e\xee\x00\xecjW\xfd\x88+M\xe0\xe5\xb9z\r\x12\x83\x16\xa4\xeeE\xbf\xb2\xf3\x1dA+\x85\xd5\xb8T\xa0\x9e\xa7\x97O9\xbb\x88\xb7\xc6O\xc2\xa2=O\xc8v\x8e\xfd\xcc\xcf\x82\x03\xec\xaf\xcc\xb2\x85\xa4~\xa5\xe4\xd4\x86\xc8\xa0(\x9b1\xd5\xc9i\xbc+|\xce\x9e2\x11,\xf5\x99\xd6\xad{\x05\xdf\xc0=FmX\xf3A\x19\xd0\x1d\xa9<\xf8g%\xb7J\xc52(}\x88\xa6\x91\xdb\x11\xbb\xa1ha\xa8)\x00&\xbeq\xaaV\xb7^E\xdb\x9a\xd0\xeb\xa1r\x08\xd9~@\x85\xfb\xd6\x8dTs\xa3\xac\xf8\xbf$\x8d$\x03\\\xd7\x07 trd\xc2\xcd\xd0;\x9dUUk\xf9M^\xb5\x16\xe4\xef6\xbd\x11UFX\x16\x7f\t\xc0\x06\x8a]^\x0f7\x80\xf7\xf7\xdf\xf5\xda\xeep\xe0\xd2\xaf\xdb\x1dv\xbd\xeeP\xfc\xb2\x1a{}\xb7\xd7\x16\xd3,\x8fs\xb1\xd8\xc3\xff\x1ao\x1f\xe1\xf0D\xab\x9ap\xec\x8e\xd0D\xfe\xc9\xfccO\xe0\xff\x01F\t!\xdc\xc1\xb0\xd7\x19x\xc3a\xd7\xc5\x97\xe3\r;\xadE\x14n\xf3\xe4\xad\xb5Bg\xee\x08ge^#\x85/\xeaH\xf0\xf9\x8c\x1a\xd5\xc2\x03;\xa2\xd74\xbe\xef\xfb\xbd\x96;\xdf`\x04.\x86\xae\xadOp]h\x8e\x8e\xa8\x7f\xb5\xe6\xde\x00\nw\x07\x973\x9d\xd1\xb0-\xdan\xbbs\xe3\xf6o\xbc\xee\xc6\xf3n\xbb\xfd\xdb\xde\xe8\xef\xaew\xeb\xba\xc5\xca\xe0\xa6=\xdcx\xbd\xdbv\xe7\xb6\xd7)Vf\tx\xe7~)\xbc\x1f\xdc\xf2\xf7\x8d\xdd\xd5Jo\xe3un;\xfd\xdb\xf6\xb0Xij\xe8\x8ez\xddv\xbf-\x9cE\x8c\xf7\xa9j5*H\x0b\x94\x08\xbbvh\x96\xd4W\xffu\x9a\x07\x8a\xecq\x84\xdb\xbc\xc1\xed\xf4\xdc\x8eG\xee\xc6\xab\xf7f\x1a\xb7b\x93\xb5\x92\xb2\xfc\xb3\xe3\xec\xe34,\x08\",d{\xc2\x01\xb3\xd7R\x9d2aZ5\x138\xa2Jz\x07\xa2\x97\xc5\xeb\xec\xab\xd3\xffG\xc6\x97\x83,\xc9\xa1\xc9h\xd4\xbeq\xe1\xceQ\xa1\xda\"\xca_\xfdm\xfe\x02\x84\x15h\xf4\xda\xa2\xf9sf\xad\xbf\x8e\x06\xadW\x9f:\xb1\xd6\xd0k\x11\x93d\x7fsDw\x08L\x8c\xae,\x13\x9d/L\xfd\xb3_\x96\xc8\x8ah\xf7n\\\xaf\xe9\xfaN\xb7\xdb\xf3zb\xfd\xdc\xd0ln\xa0\xdd:?\xd1\xff\xf7i\xd97\xb1S.\xfc\x8cT\xe5\x7fh\x93\x00Z\x87\x9d\xf6\xa8\xdf\xed\x8d\x10\xbe\x12\xb1\xb8a\xae\xf7h\x81}Q{\xa9\x98A*.\xdeL\x8b^\x99\xca\xa9\x10\xe8\xb9\xe2q\xe5v\xddQ\xdf\xebw\xc4\xe6\xd3\xc8\x1b\xc0\x9fw|\xd1$F\x81\xa5\xd0\xe3ytD\xb7g\xaf'\xa4\xb3.\xe2\xca\xdaR\x89vg\xe3\x0en\xbb\xeem{T\x81\xba\xf9k\xbb\xbdN\xbbk\x8d\xba$\x12\xb73d\xd1\xe5\x7f\x97?\xb2\xfbK\xe2\xb9\n\xd6\xb7\xc2!\xc47R\xb2!\x19,4\xecu\x85\xf3\xaf\xff8\x8e\x0e\x1d\xe7\xd6q\x1a\xab\xa3\xc1\xd0\x1b\xb9\x0e\x84;{j4x\xdds\x9c\xff\x8a\xf7\xb7\x0f)\xd6\x7f~{\x7f\xd0\x1f\r\xae\xb6\xb7\xb7\xd8\xff\xef2\xdd\x9b^\xe8\x01\x05\xee` \x06\xc1\xc8\x0b\xbc\xfe\xe0f\xe7v\x82\x9b\xeev4\xba\xc1\x84\xba\xd9u:a\x7f\xb8\xc3\xf6A\xff\xc2{\xbd\xae\xe7uG\xf0WU\x82\x90\xf6\x81\xd2gr\x94\xf5}q\x1b\x87\x0f\t\xc4\xee\xa0:j\xdb\xae\xe9\x17\xaa\x0f:^\xdb\x1d\xb1\xea\xd4F\xdb\"\xf4\xde\xb6vg\xd8\xb3\x06\x89\xea\x9a\n\xad\xc5\xad\xa5\xa9\xe5\x8fU*h\xed:\xa8\xb5\xae\x17\x7f\xec\xbf\xdf\x7f\xf7?PK\x07\x08\t\n\xcbH\xe6\n\x00\x00\x84\x19\x00\x00PK\x01\x02\x14\x00\x14\x00\x08\x08\x08\x00ji\xfeV\t\n\xcbH\xe6\n\x00\x00\x84\x19\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0052087000005402065.csvPK\x05\x06\x00\x00\x00\x00\x01\x00\x01\x00C\x00\x00\x00)\x0b\x00\x00\x00\x00"  # noqa
    }

    df = zoho_operator.required_api(context)

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 252)
    assert df["Id"][0] == 52087000004484148


@patch("models.data_model.AwsS3.save_df_to_s3_parquet_format")
def test_required_s3(aws_s3_mock: MagicMock, zoho_operator: ZohoRecruitBulkReadToS3, context: dict):
    df = pl.DataFrame()

    zoho_operator.required_s3(df, context)

    aws_s3_mock.assert_called_once_with(df=df, partition="dt=20230729", filename="20230729_123000.parquet")
