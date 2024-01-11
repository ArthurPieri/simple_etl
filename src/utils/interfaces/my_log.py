# pylint: disable=too-few-public-methods, unused-private-member
import logging


class LoggingEtl:
    """
    Centralizing logging into a class.
    """

    def __init__(self):
        """
        Start logging for class.
        """
        logging.basicConfig(
            filename=f"{self.__class__.__name__}.log",
            encoding="utf-8",
            level=logging.DEBUG,
            format="[DATA][%(levelname)s]%(filename)s:%(lineno)d %(asctime)s - %(message)s",
        )
        self.log = logging.LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {"class": self.__class__.__name__},
        )
