# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import, duplicate-code
import pytest  # pylint: disable=import-error
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

    def test_drop_columns(self, obj, fixture_extracted_data):
        data = obj._drop_columns(
            data=fixture_extracted_data, columns_to_drop=["customer_id"]
        )
        assert data[0]["first_name"] == "John"
        assert "customer_id" not in data[0].keys()
        assert "customer" not in data[0].keys()
        # assert if del row[column] is working

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
        # test every if in treat_column_names

    def test_get_python_types(self):
        ...
