# pylint: disable=import-error, no-name-in-module, too-few-public-methods
from logging import shutdown

from pymongo import MongoClient

# from bson import decode_all
# from bson.json_util import dumps

from .interface.extract_interface import ExtractInterface


class FromMongodb(ExtractInterface):
    """
    Extract data from Mongodb and return a list of dictionaries
    """

    def extract(  # pylint: disable=dangerous-default-value
        self,
        delta_date_columns: list = [],
        batch_size: int = 10000,
        last_date: str = None,
        **kwargs,
    ) -> list[dict]:
        """
        Extract data from postgres and return a list of dictionaries
        Kwargs arguments:
        - collection
        """

    def _get_connection(self, **kwargs) -> None:
        """
        Get connection to Mongodb
        Kwargs arguments:
        - host
        - port
        - user
        - password
        - database
        """

        self.client = MongoClient(
            f"""mongodb://{kwargs["user"]}:{kwargs["password"]}
            @{kwargs["host"]}:{kwargs["port"]}/""",
        )

        # Select a database
        self.db = self.client[kwargs["database"]]

    def __del__(self) -> None:
        """
        Close connection to Mongodb
        """
        self.client.close()
        shutdown()
