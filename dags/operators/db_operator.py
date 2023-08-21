"""
Holds operators whose main function is to move data from a database to S3.
"""

import logging

from airflow.models import BaseOperator, Pool
from models import data_model
from operators.exceptions import OperatorException


class DbOperator(BaseOperator):
    """
    Custom operator to read data from DB and log the result.

    To avoid DB congestion, the operator will be assigned to a pool named
    after the sql server connection id. If the pool does not exist (even though
    is should), the default pool will be used.

    !! DB pool names must have the same name as their corresponding connection id !!

    Args:
        - db_conn_id: Airflow Connection ID for SQL Server
        - sql_query: SQL query to run on SQL Server
        - db_name: Name of the database in SQL Server
        - schema_name: Name of the schema in SQL Server
        - table_name: Name of the table in SQL Server
        - column_pk: Name of the column in table_name that contains the primary key. This will be used to filter
        the tables in chunks.
        - chunk_size_kb: Number of rows to read from the database at a time. If None, all rows will be read at once.
    """

    def __init__(
        self,
        db_conn_id,
        db_name,
        schema_name,
        table_name,
        column_pk=None,
        chunk_size_kb=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.db = data_model.Database(conn_id=db_conn_id)
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        # If pool with db_conn_id exists, use it. Else, use default pool.
        self.pool = db_conn_id if db_conn_id in [str(pool) for pool in Pool.get_pools()] else Pool.DEFAULT_POOL_NAME
        self.column_pk = column_pk
        self.chunk_size_kb = chunk_size_kb
        self.table_prefix = self._get_table_prefix()

    def _get_table_prefix(self):
        """
        Return expression that fully describes table location.
        e.g. db_name.schema_name.table_name
        """
        if not self.table_name:
            logging.error("No table name provided. Cant generate query")
            raise ValueError("No table name is provided.")
        table_prefix = ""

        if self.db_name and self.schema_name:
            # Both database name and schema name are provided
            table_prefix = f"{self.db_name}.{self.schema_name}."
        elif self.schema_name:
            # Only schema name is provided (assumes a default database is used)
            logging.info("Using default database.")
            table_prefix = f"{self.schema_name}."
        elif self.db_name and self.table_name:
            # This should never happen. Database name is provided but schema name is not.
            logging.error("Queries in the form: database_name.table_name are invalid. Specify schema_name.")
            raise ValueError("No schema name is provided.")

        if table_prefix == "":
            logging.info("Using default database and schema.")

        return table_prefix

    def build_query(self) -> str:
        """
        Creates query that will be run in the database.

        Adds select statemnt, from statement, possible join statements and possible
        where statement. The latter happens when datetime_constraints are provided.
        If a constraint has to be retrieved from a different table join statement(s)
        are also added.
        """

        table_full_name = self.table_prefix + self.table_name

        query = f"SELECT {table_full_name}.* FROM {table_full_name} "

        return query

    def get_table_size(self):
        """
        Get the size of the table in kilobytes
        """
        pass

    def get_table_rows_count(self):
        """
        Get the number of rows in the table
        """
        query = f"SELECT COUNT(*) as rows_count FROM {self.table_prefix}{self.table_name}"
        logging.info(f"Query to be run for finding table rows: {query}")
        df = self.db.execute_sql_query(sql_query=query)
        return df[0][0]

    def get_number_of_rows_per_chunk(self):
        """
        Get the chunk size in rows
        """
        if self.chunk_size_kb:
            table_kb = self.get_table_size()
            table_rows = self.get_table_rows_count()
            chunk_size_number_of_rows = int(self.chunk_size_kb) / (table_kb / table_rows)
            return max(int(chunk_size_number_of_rows), 1)
        else:
            return None

    def execute(self, context):
        # build query
        query = self.build_query()

        # initialize offset and file number and chunk rows
        chunk_size_rows = self.get_number_of_rows_per_chunk()

        # initialize offset and file number
        offset = 0
        file_number = 0

        while True:
            if self.chunk_size_kb is not None:
                # Add offset and chunk size to query
                filtered_query_with_var = query.format(
                    offset=offset,
                    chunk_size=chunk_size_rows,
                )

                #  Increase offset and file number
                offset += chunk_size_rows
                file_number += 1
            else:
                filtered_query_with_var = query
            logging.info("Query to be run: %s", filtered_query_with_var)
            try:
                # Get data with polars
                df = self.db.execute_sql_query(sql_query=filtered_query_with_var)
                logging.info(f"Data read successfully : {df}")

            except Exception as err:
                logging.error("Error while trying to read data from Database: %s", str(err))
                raise OperatorException("Error while trying to read data from Database") from err

            # If no data was read, break
            if len(df) == 0:
                # No more data to read
                break

            if self.chunk_size_kb is None:
                # If chunk size is None, we only want to read one chunk
                break


class PgOperator(DbOperator):
    """
    Operator that reads data from Postgres and writes it to S3.
    The operator will read data from the query that came from _get_init_query()

    """

    def build_query(self) -> str:
        """
        Creates an initial query that will eventually be run in SQL Server
        after being augmented with a WHERE clause if necessary.
        """
        query = super().build_query()
        # If chunk size is None, we want to read all data
        if self.chunk_size_kb is not None:
            query = query.replace(";", " ")
            # If chunk size is not None, we want to read data in chunks
            query = query + (
                f"ORDER BY {self.table_prefix}{self.table_name}.{self.column_pk} LIMIT {{chunk_size}} " f"OFFSET {{offset}}"
            )
        return query

    def get_table_size(self):
        """
        Get the size of the table in kilobytes
        """
        query = f"SELECT pg_total_relation_size('{self.table_prefix}{self.table_name}')"
        logging.info(f"Query to be run for finding table size: {query}")
        table_kb_size = self.db.execute_sql_query(sql_query=query)
        return table_kb_size[0][0] / 1024
