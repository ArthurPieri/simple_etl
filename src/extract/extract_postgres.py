# pylint: disable=import-error, no-name-in-module, too-few-public-methods
from psycopg2 import connect

from .interface.extract import ExtractInterface


class FromPostgres(ExtractInterface):
    """
    Extract data from Postgres and return a dictionary
    """

    def extract(  # pylint: disable=dangerous-default-value
        self,
        delta_date_columns: list = [],
        batch_size: int = 10000,
        **kwargs,
    ) -> list:
        """
        Extract data from postgres and return a dictionary
        Kwargs arguments:
        - schema
        - table
        """
        select_query = self._get_select_query(
            schema=kwargs["schema"],
            table=kwargs["table"],
            delta_date_columns=delta_date_columns,
        )

        self.log.info("Executing query: %s", select_query)

        with self.conn.cursor(name="get_delta_cursor") as cursor:
            cursor.itersize = batch_size
            cursor.execute(select_query)

            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            self.log.info("Extracted %s rows from Postgres", len(rows))

            data = self._transform_to_dict(rows, columns)

        return data

    def _get_connection(self, **kwargs) -> None:
        """
        Get connection to Postgres
        Kwargs arguments:
        - host
        - port
        - user
        - password
        - database
        """
        self.conn = connect(  # pylint: disable=attribute-defined-outside-init
            host=kwargs["host"],
            port=kwargs["port"],
            user=kwargs["user"],
            password=kwargs["password"],
            database=kwargs["database"],
        )

    def _get_select_query(
        self,
        schema: str,
        table: str,
        delta_date_columns: list,
    ) -> str:
        sql_query = f"SELECT * FROM {schema}.{table}"

        if delta_date_columns:
            sql_query = self.__generate_where_clause(
                select_statment=sql_query, delta_date_columns=delta_date_columns
            )

        return sql_query

    def __generate_where_clause(
        self, select_statment: str, delta_date_columns: list, last_date=None
    ) -> str:
        if not last_date:
            return select_statment

        where_clause = " where "

        for column in delta_date_columns:
            where_clause += f"{column} >= '{last_date}' or "

        where_clause = where_clause[:-4]
        select_statment += where_clause

        return select_statment

    def _transform_to_dict(self, rows: list, columns: list) -> list:
        data = []

        for row in rows:
            data.append(dict(zip(columns, row)))

        return data