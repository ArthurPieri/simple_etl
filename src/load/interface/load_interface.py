# pylint: disable=duplicate-code, import-error

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
        """
        In the child class you should define what parameters to be used to get connection
        """
        self.__start_log()

        self._get_connection(**kwargs)

    @abstractmethod
    def load(
        self,
        data: list[dict],
        merge_ids: list,
        **kwargs,
    ) -> None:
        """
        Load data to destination
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

        if last_date:
            last_date = last_date[0]
            self.log.info("Last date from table %s is %s", kwargs["table"], last_date)

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
            types = []
            for dictionary in data:
                if col in dictionary.keys():
                    types.append(type(dictionary[col]))
            # check if <class 'NoneType'> is in types
            if type(None) in types:
                types.remove(type(None))
            types = list(set(types))
            cols_and_types[col] = types
        return cols_and_types

    def _get_columns(self, data: list[dict]) -> set:
        """
        Get columns from data.
        """
        columns = set()
        for dictionary in data:
            for key in dictionary.keys():
                columns.add(key)

        return columns

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
