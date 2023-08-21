# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import
from datetime import datetime
import pytest
from .postgres.fixture_postgres import (
    fixture_extracted_data,
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


class TestToPostgres:
    """
    Makes all the necessary treatments to load data into postgres.
    """

    # def test_load(self, fixture_extracted_data) -> None:
    #     ...

    def test_connection_and_log(self, obj):
        assert obj.conn
        assert obj.log

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
        assert cols_and_types["id"] == {int}
        assert cols_and_types["first_name"] == {str}
        assert cols_and_types["last_name"] == {str}
        assert cols_and_types["email"] == {str}
        assert cols_and_types["loaddate"] == {datetime}

    def test_treat_columns(self, obj, fixture_extracted_data):
        data = fixture_extracted_data
        data[0]["customer.$oid"] = "5f8d4f8f9d6d7d0001c2d3a0"
        data[1]["customer.$oid"] = "5f8d4f8f9d6d7d0001c2d3a1"
        columns, data = obj._treat_columns(
            data=data,
            columns_to_drop=["loaddate"],
            columns_to_rename={"first_name": "first_name_new"},
        )
        assert columns == {"id", "last_name", "email", "first_name_new", "customer_id"}
        assert data[0]["first_name_new"] == "John"
        assert "loaddate" not in data[0].keys()
        assert "first_name" not in data[0].keys()
        assert data[0]["customer_id"] == "5f8d4f8f9d6d7d0001c2d3a0"
        assert data[1]["customer_id"] == "5f8d4f8f9d6d7d0001c2d3a1"

    def test_drop_columns(self, obj, fixture_extracted_data):
        data = obj._drop_columns(
            data=fixture_extracted_data, columns_to_drop=["customer_id"]
        )
        assert data[0]["first_name"] == "John"
        assert "customer_id" not in data[0].keys()
        assert "customer" not in data[0].keys()

    def test_rename_columns(self, obj, fixture_extracted_data):
        data = obj._rename_columns(
            data=fixture_extracted_data,
            columns_to_rename={"first_name_new": "first_name"},
        )
        assert data[0]["first_name"] == "John"
        assert "first_name_new" not in data[0].keys()

    def test_get_columns(self, obj, fixture_extracted_data):
        cols = obj._get_columns(data=fixture_extracted_data)
        assert cols == {"id", "last_name", "email", "first_name"}

    def test_treat_column_names(self, obj, fixture_extracted_data):
        data = fixture_extracted_data
        data[0]["create.$date"] = "2020-10-19T00:00:00.000Z"
        data[1]["create.$date"] = "2020-10-19T00:00:00.000Z"
        columns, data = obj._treat_column_names(data=data)
        assert columns == {"id", "last_name", "email", "first_name", "create_date"}

    # def test_add_columns_to_table(self, obj):
    #     ...

    # def test_create_empty_table(self, obj):
    #     ...

    # def test_get_max_dates_from_table(self, obj):
    #     ...

    # def test_get_postgres_columns(self, obj):
    #     ...

    # def test_load_data(self, obj):
    #     ...
