from abc import ABC, abstractmethod
from logging import getLogger, shutdown


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
    def transform(self, **kwargs) -> [dict]:
        """
        Transform data from source and return a list of dicts
        """

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
