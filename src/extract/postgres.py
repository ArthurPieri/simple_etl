# pylint: disable=import-error, no-name-in-module
from abc import abstractmethod

from psycopg2 import connect  # type: ignore

from pandas import DataFrame

from airflow.hooks.base_hook import BaseHook  # type: ignore

from .interface.extract import ExtractInterface


class FromPostgres(ExtractInterface):
    """
    Extract data from Postgres and return a dataframe
    """

    def extract(
        self,
        delta_date_columns: list,
        batch_size: int = 10000,
        **kwargs,
    ):
        """
        Extract data from postgres and return a DataFrame
        Kwargs arguments:
        - schema
        - table
        """
        select_query = self._get_select_query(
            schema=kwargs["schema"],  # type: ignore
            table=kwargs["table"],
            delta_date_columns=delta_date_columns,
        )

        self.log.info("Executing query: %s", select_query)

        with self.conn.cursor(name="get_delta_cursor") as cursor:
            cursor.itersize = batch_size
            cursor.execute(select_query)

            # get rows and transform to dataframe
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            self.log.info("Extracted %s rows from Postgres", len(rows))

            df = DataFrame(rows, columns=columns)

        return df

    @abstractmethod
    def get_last_load_date(
        self, delta_date_columns: list = [], **kwargs
    ):  # pylint: disable=dangerous-default-value
        """
        Get the last load date from the table
        This is an abstract method, that should be implemented by the load class
        """

    def _get_connection(self, **kwargs):
        """
        Get connection to Postgres
        Kwargs arguments:
        - conn_name: name of the connection in Airflow
        - host
        - port
        - user
        - password
        - database
        """
        if kwargs["conn_name"]:
            conn = BaseHook.get_connection(kwargs["conn_name"])
            host = conn.host
            port = conn.port
            user = conn.login
            password = conn.password
            database = conn.database
        else:
            host = kwargs["host"]
            port = kwargs["port"]
            user = kwargs["user"]
            password = kwargs["password"]
            database = kwargs["database"]

        self.conn = connect(  # pylint: disable=attribute-defined-outside-init
            host=host, port=port, user=user, password=password, database=database
        )

    def _get_select_query(  # pylint: disable=dangerous-default-value
        self,
        schema: str,
        table: str,
        delta_date_columns: list = [],
    ):
        sql_query = f"SELECT * FROM {schema}.{table}"

        if delta_date_columns:
            sql_query = self.__generate_where_clause(
                select_statment=sql_query, delta_date_columns=delta_date_columns
            )

        return sql_query

    def __generate_where_clause(self, select_statment: str, delta_date_columns: list):
        last_date = self.get_last_load_date(delta_date_columns)

        if not last_date:
            return select_statment

        where_clause = " where "

        for column in delta_date_columns:
            where_clause += f"{column} >= '{last_date}' or "

        where_clause = where_clause[:-4]
        select_statment += where_clause

        return select_statment
