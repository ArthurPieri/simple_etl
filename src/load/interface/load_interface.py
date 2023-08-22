# pylint: disable=duplicate-code

from abc import ABC, abstractmethod
from logging import getLogger, shutdown
from datetime import datetime

from pytz import timezone

import unidecode


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
    def load(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        merge_ids: list,
        columns_to_drop: list = [],
        columns_to_rename: dict = {},
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
            if "NoneType" in types:
                types.remove("NoneType")
            types = list(set(types))
            cols_and_types[col] = types
        return cols_and_types

    def _treat_columns(
        self, data: list[dict], columns_to_drop: list, columns_to_rename
    ) -> [set, list]:
        if columns_to_drop:
            data = self._drop_columns(data, columns_to_drop)

        if columns_to_rename:
            data = self._rename_columns(data, columns_to_rename)

        columns, data = self._treat_column_names(data)

        return columns, data

    def _drop_columns(self, data: list[dict], columns_to_drop: list) -> list[dict]:
        """
        Drop columns from data.
        """
        for row in data:
            for column in columns_to_drop:
                if column in row:
                    del row[column]

        return data

    def _get_columns(self, data: list[dict]) -> set:
        """
        Get columns from data.
        """
        columns = set()
        for dictionary in data:
            for key in dictionary.keys():
                columns.add(key)

        return columns

    def _rename_columns(self, data: list[dict], columns_to_rename: dict) -> list[dict]:
        """
        Rename columns from data.
        Parameters:
        - data: list of dictionaries
        - columns_to_rename: dictionary with old column name as key and new column name as value
        """
        for row in data:
            for old_col, new_col in columns_to_rename.items():
                if old_col in row:
                    row[new_col] = row[old_col]
                    row.pop(old_col)
        return data

    def _treat_column_names(self, data: list[dict]) -> [set, list[dict]]:
        """Remove $oid, $date, ., $, space and diactrics from column names"""
        columns = self._get_columns(data)
        columns_to_rename = {}

        for col in columns:
            if ".$oid" in col:
                self.log.info("Remove .$oid from column: %s", col)
                if "id" not in col.replace(".$oid", ""):
                    columns_to_rename[col] = col.replace(".$oid", "_id")
                    col = col.replace(".$oid", "_id")
                else:
                    columns_to_rename[col] = col.replace(".$oid", "")
                    col = col.replace(".$oid", "")
            if ".$date" in col:
                self.log.info("Remove .$date from column: %s", col)
                if "date" not in col.replace(".$date", ""):
                    columns_to_rename[col] = col.replace(".$date", "_date")
                    col = col.replace(".$date", "_date")
                else:
                    columns_to_rename[col] = col.replace(".$date", "")
                    col = col.replace(".$date", "")
            if "." in col:
                self.log.info("Replace . to _ in column: %s", col)
                columns_to_rename[col] = col.replace(".", "_")
                col = col.replace(".", "_")
            if "$" in col:
                self.log.info("Remove $ from column: %s", col)
                columns_to_rename[col] = col.replace("$", "")
                col = col.replace("$", "")
            if " " in col:
                self.log.info("Replace space to _ in column: %s", col)
                columns_to_rename[col] = col.replace(" ", "_")
                col = col.replace(" ", "_")
            if not col.isascii():
                self.log.info("Remove diactrics from column: %s", col)
                columns_to_rename[col] = unidecode.unidecode(col).replace(" ", "_")
                col = unidecode.unidecode(col).replace(" ", "_")

        self.log.info("Columns to rename: %s", columns_to_rename)

        data = self._rename_columns(data, columns_to_rename)

        columns = self._get_columns(data)

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
