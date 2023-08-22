# pylint: disable=duplicate-code

from abc import ABC, abstractmethod
from logging import getLogger, shutdown


class ExtractInterface(ABC):
    """'
    This class is used to extract data from the source.
    It receives a schema name, a table name, and a list of delta_date_columns.
    Makes all the treatments necessary to extract the data and return a Dict.
    """

    def __init__(self, **kwargs) -> object:
        self.__start_log()

        self._get_connection(**kwargs)

    @abstractmethod
    def extract(
        self,
        delta_date_columns: list,
        batch_size: int = 10000,
        last_date=None,
        **kwargs
    ) -> dict:
        """
        Extract data from source and return a dict
        """

    @abstractmethod
    def _get_connection(self, **kwargs) -> object:
        """
        Get connection from .env file or other source
        Parameters:
        - **Kwargs parameters are used to get connection
        - kwargs['conn_name']: name of the connection
        """

    def __start_log(self) -> None:
        """
        Start logging for class
        """
        self.log = getLogger(__name__)  # pylint: disable=attribute-defined-outside-init
        self.log.info("-----------------------------------------")
        self.log.info("Initializing %s class", self.__class__.__name__)
        self.log.info("-----------------------------------------")

    def __del__(self) -> None:
        self.conn.close()  # pylint: disable=no-member # type: ignore
        self.log.info("Connection %s closed", self.__class__.__name__)
        shutdown()
