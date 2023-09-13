# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import
import pytest
from ..src.extract.extract_postgres import FromPostgres  # pylint: disable=import-error
from .postgres.fixture_postgres import (
    fixture_extracted_data,
    fixture_rows_columns,
)


@pytest.fixture(scope="module")
def obj():
    yield FromPostgres(
        host="localhost",
        port="5432",
        database="postgres_test",
        user="postgres",
        password="postgres",
    )


class TestExtractPostgres:  # pylint: disable=too-few-public-methods
    """
    Testing extract postgres class
    """

    def test_extract_from_postgres(  # pylint: disable=redefined-outer-name
        self,
        obj,
        fixture_extracted_data,
    ):
        """
        Test extract method
        """
        data = obj.extract(schema="public", table="employees")
        assert data
        assert data == fixture_extracted_data

    def test_connection_and_log(self, obj):
        assert obj
        assert obj.conn
        assert obj.log

    def test_get_select_query(self, obj):
        # Test get select query
        sql = obj._get_select_query(
            schema="public",
            table="employees",
            delta_date_columns=[],
        )
        assert sql == "SELECT * FROM public.employees"

    def test_transform_to_dict(self, obj, fixture_rows_columns, fixture_extracted_data):
        data = obj._transform_to_dict(
            fixture_rows_columns["rows"], fixture_rows_columns["columns"]
        )
        assert data == fixture_extracted_data

    def test_generate_where_clause(self, obj):
        sql = obj._get_select_query(
            schema="public",
            table="employees",
            delta_date_columns=["last_update"],
            last_date="2021-01-01",
        )
        assert sql == "SELECT * FROM public.employees where last_update >= '2021-01-01'"

    def test_generate_where_clause_without_date(self, obj):
        sql = obj._get_select_query(
            schema="public",
            table="employees",
            delta_date_columns=["last_update"],
            last_date=None,
        )
        assert sql == "SELECT * FROM public.employees"
