# pylint: disable=duplicate-code

from abc import ABC, abstractmethod
from logging import getLogger, shutdown
from datetime import datetime

from pytz import timezone


class LoadInterface(ABC):
    """
    This class is used to load data to lake.
    It recieves a dataframe, a schema name, and a table name.
    Makes all the treatment necessary to load the data.
    """

    # Class is organized in this order:
    # 1. Abstract methods that should be implemented or overridable methods
    # 2. Non-abstract methods
    @abstractmethod
    def __init__(self, **kwargs):
        self.__start_log()

        self._get_connection(conn_name=kwargs["conn_name"], **kwargs)

    @abstractmethod
    def load(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        merge_ids: list,
        columns_to_drop: list = [],
        columns_to_rename: dict = {},
    ):
        """
        Treat the DataFrame column names and types and load data to Lake
        """

    @abstractmethod
    def _get_connection(self, **kwargs):
        """
        Get connection from Airflow or from .env file
        Parameters:
        - **Kwargs parameters are used to get connection
        - kwargs['conn_name']: name of the connection
        """

    @abstractmethod
    def _get_max_dates_from_lake(self, delta_date_columns, **kwargs):
        """This method should return the max date from lake table"""

    def get_last_load_date(
        self, delta_date_columns: list = [], **kwargs
    ):  # pylint: disable=dangerous-default-value
        """
        Get the last date from lake table.
        delta_date_columns: list of columns that will be used to filter last loaddate
        """
        self.log.info("Getting last date from table %s", kwargs["table"])

        if not delta_date_columns:
            self.log.error("delta_date_columns cannot be empty")
            raise ValueError("delta_date_columns cannot be empty")

        last_date = self._get_max_dates_from_lake(delta_date_columns, **kwargs)

        return last_date

    def _add_loaddate(self, data: list[dict]) -> list:
        """
        Add a loaddate column to the data.
        """
        for row in data:
            row["loaddate"] = datetime.now(timezone("UTC"))

        return data

    @abstractmethod
    def _get_columns_and_types(self, data: dict):
        """Get names and types for columns"""

    def _treat_columns(
        self, data: list[dict], columns_to_drop: list, columns_to_rename
    ) -> list:
        if columns_to_drop:
            data = self.__drop_columns(data, columns_to_drop)

        if columns_to_rename:
            data = self.__rename_columns(data, columns_to_rename)

        data = self.__treat_column_names(data)

        return data

    def __treat_column_names(self, data: list[dict]) -> list[dict]:
        """Remove $oid, $date, ., $, space and diactrics from column names"""
        columns = self.__get_columns(data)
        for col in columns:
            if ".$oid" in col:
                ...
        return data

    def __get_columns(self, data: list[dict]) -> list:
        """
        Get columns from data.
        """
        columns = []
        for row in data:
            for column in row:
                if column not in columns:
                    columns.append(column)

        return columns

    def __drop_columns(self, data: list[dict], columns_to_drop: list) -> list[dict]:
        """
        Drop columns from data.
        """
        for row in data:
            for column in columns_to_drop:
                if column in row:
                    del row[column]

        return data

    def __rename_columns(self, data: list[dict], columns_to_rename: dict) -> list[dict]:
        """
        Rename columns from data.
        """
        for row in data:
            for column in columns_to_rename:
                if column in row:
                    row[columns_to_rename[column]] = row[column]
                    del row[column]
        return data

    def __start_log(self):  # pylint: disable=unused-private-member
        """
        Start logging for class
        """
        self.log = getLogger(__name__)  # pylint: disable=attribute-defined-outside-init
        self.log.info("-----------------------------------------")
        self.log.info("Initializing %s class", self.__class__.__name__)
        self.log.info("-----------------------------------------")

    def __del__(self):
        self.conn.close()  # pylint: disable=no-member # type: ignore
        self.log.info("Connection %s closed", self.__class__.__name__)
        shutdown()
