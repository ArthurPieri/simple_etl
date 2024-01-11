# pylint: disable=duplicate-code, line-too-long

from abc import ABC, abstractmethod
import logging


class ExtractInterface(ABC):
    """'
    This class is used to extract data from the source.
    It receives a schema name, a table name, and a list of delta_date_columns.
    Makes all the treatments necessary to extract the data and return a Dict.
    """

    def __init__(self, **kwargs) -> object:
        self.__start_log(**kwargs)

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

    def __start_log(self, **kwargs) -> None:
        """
        Start logging for class
        """
        if kwargs["filename"]:
            logging.basicConfig(
                filename=f"{kwargs['filename']}.log",
                encoding="utf-8",
                level=logging.DEBUG,
                format="[DATA] %(filename)s Line:%(lineno)d %(asctime)s [%(levelname)s] - %(message)s",
            )
        else:
            logging.basicConfig(
                filename=f"{self.__class__.__name__}.log",
                encoding="utf-8",
                level=logging.DEBUG,
                format="[DATA] %(filename)s Line:%(lineno)d %(asctime)s [%(levelname)s] - %(message)s",
            )
        self.log = logging.LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {"class": self.__class__.__name__},
        )
