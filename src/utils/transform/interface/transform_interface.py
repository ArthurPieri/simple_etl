from abc import ABC, abstractmethod
from logging import getLogger, shutdown

import unidecode  # pylint: disable=import-error


class TransformInterface(ABC):
    """
    This class is used to transform data to be sent.
    It recieves a list of dicts and makes all
    the treatments necessary to adapt the data to the destination
    It returns a list of dicts.
    """

    def __init__(self) -> object:
        self.__start_log()

    @abstractmethod
    def transform(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        columns_to_drop: list = [],
        columns_to_rename: dict = {},
        **kwargs
    ) -> [dict]:
        """
        Transform data from source and return a list of dicts
        """

    def _flatten_data(self, data: list[dict]) -> list[dict]:
        """
        Flatten data to be sent to postgres.
        """
        # needs testing
        for row in data:
            for key, value in row.items():
                if isinstance(value, dict):
                    for key2, value2 in value.items():
                        row[key + "_" + key2] = value2
                    row.pop(key)
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

    def __start_log(self) -> None:
        """
        Start logging for class
        """
        self.log = getLogger(__name__)
        self.log.info("-----------------------------------------")
        self.log.info("Initializing %s class", self.__class__.__name__)
        self.log.info("-----------------------------------------")

    def __del__(self) -> None:
        self.log.info("Connection %s closed", self.__class__.__name__)
        shutdown()
