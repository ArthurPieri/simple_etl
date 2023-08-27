# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import
import pytest
from ..src.extract.extract_mongodb import FromMongodb  # pylint: disable=import-error


@pytest.fixture(scope="module")
def obj():
    yield FromMongodb(
        host="localhost",
        port=27017,
        database="test_users",
        user="mongoadmin",
        password="mongoadmin",
    )


class TestFromMongodb:
    """
    Testing extract mongodb class
    """

    def test_connection_and_log(self, obj):
        assert obj
        assert obj.client
        assert obj.log

    def test_get_cursor(self, obj):
        cursor = obj._get_cursor(
            batch_size=10000,
            delta_date_columns=None,
            last_date=None,
            filter=None,
            aggregation_clause=None,
            collection="usuarios",
        )
        assert cursor
