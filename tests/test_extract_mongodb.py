# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import
from unittest.mock import MagicMock
from datetime import datetime

import pytest


from ..src.extract.extract_mongodb import FromMongodb  # pylint: disable=import-error


@pytest.fixture(scope="module")
def obj():
    yield FromMongodb(
        host="localhost",
        port=27017,
        database="mydatabase",
        auth=False,
    )


class TestFromMongodb:
    """
    Testing extract mongodb class
    """

    def test_extract(self, obj):
        data = obj.extract(
            aggregation_clause=None,
            batch_size=10000,
            collection="users",
            delta_date_columns=None,
            filter=None,
            last_date=None,
        )
        assert data
        assert len(data) == 4
        assert isinstance(data, list)

    def test_connection_and_log(self, obj):
        assert obj
        assert obj.client
        assert obj.db.name == "mydatabase"
        assert obj.log

    def test_get_cursor(self, obj):
        cursor = obj._get_cursor(
            batch_size=10000,
            delta_date_columns=None,
            last_date=None,
            filter=None,
            aggregation_clause=None,
            collection="users",
        )
        assert cursor
        assert cursor.alive

    def test_get_cursor_with_agg_clause(self, obj):
        cursor = obj._get_cursor(
            batch_size=10000,
            delta_date_columns=None,
            last_date=None,
            filter=None,
            aggregation_clause={
                "$project": {
                    "name": 1,
                    "email": 1,
                    "created_at": 1,
                }
            },
            collection="users",
        )
        assert cursor
        assert cursor.alive

    def test_get_cursor_with_last_date(self, obj):
        cursor = obj._get_cursor(
            batch_size=10000,
            delta_date_columns=["created_at"],
            last_date=datetime(2021, 1, 1),
            filter=None,
            aggregation_clause=None,
            collection="users",
        )
        assert cursor
        assert cursor.alive

    def test_get_cursor_with_filter(self, obj):
        cursor = obj._get_cursor(
            batch_size=10000,
            delta_date_columns=None,
            last_date=None,
            filter=[{"name": "John"}],
            aggregation_clause=None,
            collection="users",
        )
        assert cursor
        assert cursor.alive

    def test_get_cursor_with_filter_and_agg_clause(self, obj):
        cursor = obj._get_cursor(
            batch_size=10000,
            delta_date_columns=None,
            last_date=None,
            filter=[{"name": "John"}],
            aggregation_clause={
                "$project": {
                    "name": 1,
                    "email": 1,
                    "created_at": 1,
                }
            },
            collection="users",
        )
        assert cursor
        assert cursor.alive

    def test_auth_connection(self, obj):
        obj._get_connection(
            auth=True,
            host="localhost",
            port=27017,
            database="mydatabase",
            user="myuser",
            password="mypassword",
        )
        assert obj.client

    @pytest.fixture(scope="module")
    def mock_cursor(self):
        batch1 = b"{'invalid': 'json"
        yield batch1

    def test_extract_method_raises_exception(self, mocker, mock_cursor):
        mocker.patch("simple_etl.src.extract.extract_mongodb.MongoClient")

        instance = FromMongodb(
            host="localhost",
            port=27017,
            database="mydatabase",
            auth=False,
        )
        instance.log = MagicMock()
        instance.log.error = MagicMock()
        instance._get_cursor = MagicMock(return_value=mock_cursor)

        with pytest.raises(RuntimeError):
            instance.extract(
                aggregation_clause=None,
                batch_size=10000,
                collection="users",
                delta_date_columns=None,
                filter=None,
                last_date=None,
            )

        assert instance.log.error.call_count == 1
