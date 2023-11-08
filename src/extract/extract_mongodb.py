# pylint: disable=import-error, no-name-in-module, too-few-public-methods, attribute-defined-outside-init, protected-access, unused-import
import json
from logging import shutdown
from datetime import datetime

from pymongo import MongoClient

from bson import decode_all
from bson.json_util import dumps

from .interface.extract_interface import ExtractInterface


class FromMongodb(ExtractInterface):
    """
    Extract data from Mongodb and return a list of dictionaries
    # Kwargs arguments
    ## Required
    - database
    - collection
    - aggregation_clause: dict
    - delta_date_columns: list
    - filter: list

    ## Optional
    - last_date: datetime
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
        cursor = self._get_cursor(
            batch_size=batch_size,
            delta_date_columns=delta_date_columns,
            last_date=last_date,
            **kwargs,
        )
        data = []
        for batch in cursor:
            try:
                data = json.loads(dumps(decode_all(batch)))
            except Exception as exc:
                self.log.error("Error extracting data: %s", exc)
                raise RuntimeError(exc) from exc

        return data

    def _get_connection(self, **kwargs) -> None:
        """
        Get connection to Mongodb
        Kwargs arguments:
        - auth : bool
        - host
        - port
        - user
        - password
        - database
        """

        if not kwargs["auth"]:
            self.client = MongoClient(
                f"""mongodb://{kwargs["host"]}:{kwargs["port"]}/""",
            )
        else:
            self.client = MongoClient(
                f"""mongodb://{kwargs["user"]}:{kwargs["password"]}
                @{kwargs["host"]}:{kwargs["port"]}/""",
            )

        # Select a database
        self.db = self.client[kwargs["database"]]

    def _get_cursor(
        self, batch_size: int, delta_date_columns: list, last_date: datetime, **kwargs
    ):
        """
        Get cursor to Mongodb
        Kwargs arguments:
        - filter
        - aggregation_clause: dict
        """
        condition: dict = {}
        agg_clause = kwargs["aggregation_clause"]

        if last_date is not None or kwargs["filter"] is not None:
            condition = {"$or": []}

        if last_date is not None:
            _list: list = []
            for col in delta_date_columns:
                _list.append({col: {"$gt": last_date}})

            condition["$or"].extend(_list)

        if kwargs["filter"] is not None:
            condition["$or"].extend(kwargs["filter"])

        if not agg_clause:
            cursor = self.db[kwargs["collection"]].find_raw_batches(
                condition, batch_size=batch_size
            )
        elif not condition:
            self.log.info("condition is none")
            cursor = self.db[kwargs["collection"]].aggregate_raw_batches([agg_clause])
        else:
            cursor = self.db[kwargs["collection"]].aggregate_raw_batches(
                [{"$match": condition}, agg_clause]
            )

        return cursor

    def __del__(self) -> None:
        """
        Close connection to Mongodb
        """
        self.client.close()
        shutdown()
