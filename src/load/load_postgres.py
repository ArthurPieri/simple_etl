"""'
This class is used to load data into a postgres database.
It recieves a dataframe, a schema name, a table name, and a connection string.
Makes all the treatment necessary and then loads the data into the specified table.
"""

from psycopg2 import connect

from .interface.load import LoadInterface


class ToPostgres(LoadInterface):
    """
    Makes all the necessary treatments to load data into postgres.
    """

    def load(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        merge_ids: list,
        columns_to_drop: list = [],
        columns_to_rename: dict = {},
        **kwargs,
    ) -> None:
        """
        Load data into postgres
        **Kwargs parameters:
        - Database
        - Schema
        - Table
        """
        data = self._add_loaddate(data=data)

        columns, data = self._treat_columns(
            data=data,
            columns_to_drop=columns_to_drop,
            columns_to_rename=columns_to_rename,
        )

        data_columns_types = self._get_python_types(columns, data)
        table_columns = self._get_postgres_columns()

        if not table_columns:
            self.log.info(
                "Table %s.%s.%s does not exist. Creating it...",
                kwargs["database"],
                kwargs["schema"],
                kwargs["table"],
            )
            self._create_empty_table(columns_types=data_columns_types, **kwargs)

        if table_columns:
            diff = {
                k: v for k, v in data_columns_types.items() if k not in table_columns
            }
            if diff:
                self.log.info(
                    "Table %s.%s.%s does not have all the columns. Adding %s...",
                    kwargs["database"],
                    kwargs["schema"],
                    kwargs["table"],
                    str(diff),
                )
                self._add_columns_to_table(columns_types=diff, **kwargs)

        self._load_data(
            columns_types=data_columns_types, data=data, merge_ids=merge_ids, **kwargs
        )

    def _add_columns_to_table(self, columns_types: dict, **kwargs):
        ...

    def _create_empty_table(self, columns_types: dict, **kwargs):
        ...

    # pylint: disable=duplicate-code
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

    def _get_max_dates_from_table(self, delta_date_columns, **kwargs):
        ...

    def _get_postgres_columns(self, **kwargs) -> list:
        columns = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{kwargs["schema"]}'
                AND table_name = '{kwargs["table"]}'
                """
            )
            columns = cursor.fetchall()
        return columns

    def _load_data(
        self, columns_and_types: dict, data: list[dict], merge_ids: list, **kwargs
    ) -> None:
        ...
