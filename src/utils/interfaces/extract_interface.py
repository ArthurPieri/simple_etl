# pylint: disable=duplicate-code, line-too-long

from abc import ABC, abstractmethod
from .my_log import LoggingEtl


class ExtractInterface(ABC, LoggingEtl):
    """'
    This class is used to extract data from the source.
    It receives a schema name, a table name, and a list of delta_date_columns.
    Makes all the treatments necessary to extract the data and return a Dict.
    """

    def __init__(self, **kwargs) -> object:
        super().__init__()

        self._get_connection(**kwargs)

    @abstractmethod
    def extract(
        self,
        delta_date_columns: list,
        batch_size: int = 10000,
        last_date=None,
        **kwargs,
    ) -> list[dict]:
        """
        Extract data from source and return a list of dicts
        """

    @abstractmethod
    def _get_connection(self, **kwargs) -> object:
        """
        Get connection from .env file or other source
        Parameters:
        - **Kwargs parameters are used to get connection
        """

    @abstractmethod
    def __del__(self) -> None:
        """
        Close connection to source and shutdown logging
        """
