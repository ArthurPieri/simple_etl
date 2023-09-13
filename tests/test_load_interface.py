# pylint: disable= missing-function-docstring, redefined-outer-name, protected-access, duplicate-code
"""
Tests for load_postgres.py and load_interface.py
"""

from datetime import datetime
import pytest  # pylint: disable=import-error
from .postgres.fixture_postgres import (  # pylint: disable=unused-import
    fixture_extracted_data,
    fixture_load_data,
)

from ..src.load.load_postgres import ToPostgres


@pytest.fixture(scope="module")
def obj():
    yield ToPostgres(
        host="localhost",
        port="5432",
        database="postgres_test",
        user="postgres",
        password="postgres",
    )


class TestLoadInterface:  # pylint: disable=too-few-public-methods
    """
    Makes all the necessary treatments to load data into postgres.
    """

    def test_add_loaddate(self, fixture_extracted_data, obj):
        data = obj._add_loaddate(data=fixture_extracted_data)
        assert data[0]["loaddate"]
        assert data[1]["loaddate"]

    def test_get_python_types(self, fixture_extracted_data, obj):
        data = fixture_extracted_data
        data[0]["loaddate"] = datetime.now()
        data[1]["loaddate"] = datetime.now()

        cols_and_types = obj._get_python_types(
            columns=["id", "first_name", "last_name", "email", "loaddate"],
            data=data,
        )
        assert cols_and_types["id"][0].__name__ == "int"
        assert cols_and_types["first_name"][0].__name__ == "str"
        assert cols_and_types["last_name"][0].__name__ == "str"
        assert cols_and_types["email"][0].__name__ == "str"
        assert cols_and_types["loaddate"][0].__name__ == "datetime"

    def test_get_columns(self, obj, fixture_extracted_data):
        cols = obj._get_columns(data=fixture_extracted_data)
        assert cols == {"id", "last_name", "email", "first_name"}

    def test_get_last_load_date(self, obj):
        last_date = obj.get_last_load_date(
            delta_date_columns=["loaddate"],
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        assert last_date

    def test_get_last_load_date_without_delta_date_columns(self, obj):
        try:
            with pytest.raises(ValueError):
                obj.get_last_load_date(
                    delta_date_columns=[],
                    database="postgres_test",
                    schema="public",
                    table="employees_test_load",
                )
        except Exception as exc:
            assert exc
            assert isinstance(exc, ValueError)
            assert str(exc) == "delta_date_columns cannot be empty"

    def test_get_python_types_with_none_type(self, obj):
        data = [
            {"id": 1, "first_name": "John", "last_name": "Doe", "email": None},
            {"id": 2, "first_name": "Jane", "last_name": "Doe", "email": None},
        ]
        cols_and_types = obj._get_python_types(
            columns=["id", "first_name", "last_name", "email"],
            data=data,
        )
        assert cols_and_types["email"][0] == type(None)
