"""
Holds classes that represent data sources or destinations.

These classes help to abstract away the details of the data sources
and destinations, and they contain useful methods to interact with them.

"""

from typing import List

import sqlalchemy
from airflow.hooks.base import BaseHook


class Database:
    """
    Interact with DB Server.

    Will be updated to add functionality.
    """

    def __init__(self, conn_id=None):
        self.conn_id = conn_id

    # read data from DB Server

    def execute_sql_query(self, sql_query: str) -> List:
        """
        Make Connection , Execute sql query and return results
        :param sql_query:
        :return: List of results
        """
        connection = BaseHook.get_connections(self.conn_id)[0].get_uri()

        # create engine and execute query
        engine = sqlalchemy.create_engine(url=connection)
        results = engine.execute(sql_query)
        engine.dispose()
        return results.fetchall()
