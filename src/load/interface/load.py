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
        **kwargs,
    ) -> None:
        """
        Treat the DataFrame column names and types and load data to Lake
        """

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

        last_date = self._get_max_dates_from_table(delta_date_columns, **kwargs)

        return last_date

    @abstractmethod
    def _add_columns_to_table(self, columns_types: dict, **kwargs):
        """
        Add columns to table
        """

    @abstractmethod
    def _create_empty_table(self, columns_types: dict, **kwargs):
        """
        Create empty table on lake
        """

    @abstractmethod
    def _get_connection(self, **kwargs):
        """
        Parameters:
        - **Kwargs parameters are used to get connection
        """

    @abstractmethod
    def _get_max_dates_from_table(self, delta_date_columns, **kwargs):
        """This method should return the max date from lake table"""

    @abstractmethod
    def _load_data(
        self, columns_and_types: dict, data: list[dict], merge_ids: list, **kwargs
    ):
        """Load data to target"""

    def _add_loaddate(self, data: list[dict]) -> list:
        """
        Add a loaddate column to the data.
        """
        loaddate = datetime.now(timezone("UTC"))
        for row in data:
            row["loaddate"] = loaddate

        return data

    def _get_python_types(self, columns: set, data: list[dict]):
        """Get types for columns"""
        cols_and_types = {}
        for col in columns:
            types = set()
            for dictionary in data:
                if col in dictionary.keys():
                    types.add(type(dictionary[col]))
            cols_and_types[col] = types
        return cols_and_types

    def _treat_columns(
        self, data: list[dict], columns_to_drop: list, columns_to_rename
    ) -> [set, list]:
        if columns_to_drop:
            data = self.__drop_columns(data, columns_to_drop)

        if columns_to_rename:
            data = self.__rename_columns(data, columns_to_rename)

        columns, data = self.__treat_column_names(data)

        return columns, data

    def __drop_columns(self, data: list[dict], columns_to_drop: list) -> list[dict]:
        """
        Drop columns from data.
        """
        for row in data:
            for column in columns_to_drop:
                if column in row:
                    del row[column]

        return data

    def __get_columns(self, data: list[dict]) -> set:
        """
        Get columns from data.
        """
        columns = set()
        for dictionary in data:
            for key in dictionary.keys():
                columns.add(key)

        return columns

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

    def __treat_column_names(self, data: list[dict]) -> [set, list[dict]]:
        """Remove $oid, $date, ., $, space and diactrics from column names"""
        columns = self.__get_columns(data)
        for col in columns:
            if ".$oid" in col:
                ...
        return columns, data

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
