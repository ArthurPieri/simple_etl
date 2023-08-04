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
    ) -> None:
        data = self._treat_columns(data, columns_to_drop, columns_to_rename)

        data = self._add_loaddate(data)

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
