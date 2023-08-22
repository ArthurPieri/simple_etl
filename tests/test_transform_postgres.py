# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import
import pytest
from .postgres.fixture_postgres import (
    fixture_extracted_data,
    fixture_load_data,
)

from ..src.transform.transform_to_postgres import TrasnformPostgres


@pytest.fixture(scope="module")
def obj():
    yield TrasnformPostgres()


class TestTransformPostgres:
    """
    Test transformations necessary to load data into postgres.
    """

    def test_transform(self, obj, fixture_extracted_data):
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

    def test_treat_column_names(self, obj, fixture_extracted_data):
        data = fixture_extracted_data
        data[0]["create.$date"] = "2020-10-19T00:00:00.000Z"
        data[1]["create.$date"] = "2020-10-19T00:00:00.000Z"
        columns, data = obj._treat_column_names(data=data)
        assert columns == {"id", "last_name", "email", "first_name", "create_date"}
