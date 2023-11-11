# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import, duplicate-code
import pytest  # pylint: disable=import-error
from .postgres.fixture_postgres import (
    fixture_extracted_data,
    fixture_load_data,
)

from ..src.transform.transform_to_postgres import TransformPostgres


@pytest.fixture(scope="module")
def obj():
    yield TransformPostgres()


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

    def test_treat_column_names(self, obj):
        data = [
            {
                "id.$oid": "5f8d0c5c0b3a7f7e9d5f0e9d",
                "last_name": "Doe",
                "email": "",
                "first_name": "John",
                "create.$date": "2020-10-19T00:00:00.000Z",
                "user.$oid": "teste",
                "purchase.$date": "2020-10-19T00:00:00.000Z",
                "teste.dot": "teste",
                "remove$": "teste",
                "name with spaces": "teste",
                "something_with_ç_diactrics": "teste",
            }
        ]
        columns, data = obj._treat_column_names(data=data)
        assert columns == {
            "id",
            "last_name",
            "email",
            "first_name",
            "create_date",
            "user_id",
            "purchase_date",
            "teste_dot",
            "remove",
            "name_with_spaces",
            "something_with_c_diactrics",
        }

    def test_get_python_types(self, obj):
        columns = {"id", "last_name", "email", "first_name", "create_date"}
        data = [
            {
                "id": "5f8d0c5c0b3a7f7e9d5f0e9d",
                "last_name": "Doe",
                "email": "",
                "first_name": "John",
                "create_date": "2020-10-19T00:00:00.000Z",
            },
        ]
        cols_and_types = obj._get_python_types(columns=columns, data=data)
        assert cols_and_types == {
            "id": [str],
            "last_name": [str],
            "email": [str],
            "first_name": [str],
            "create_date": [str],
        }
