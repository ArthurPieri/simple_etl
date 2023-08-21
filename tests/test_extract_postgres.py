# pylint: disable=missing-module-docstring, missing-function-docstring, protected-access
from ..src.extract.extract_postgres import FromPostgres  # pylint: disable=import-error
from .postgres.fixture_postgres import (  # pylint: disable=unused-import
    fixture_extracted_data,
    fixture_postgres_connection,
    fixture_rows_columns,
)


class TestExtractPostgres:  # pylint: disable=too-few-public-methods
    """
    Testing extract postgres class
    """

    def test_extract_from_postgres(  # pylint: disable=redefined-outer-name
        self,
        fixture_postgres_connection,
        fixture_extracted_data,
        fixture_rows_columns,
    ):
        """
        Test extract method
        """
        # Test connection
        obj = FromPostgres(
            host=fixture_postgres_connection["host"],
            port=fixture_postgres_connection["port"],
            database=fixture_postgres_connection["database"],
            user=fixture_postgres_connection["user"],
            password=fixture_postgres_connection["password"],
        )
        assert obj

        # Test log
        assert obj.log

        # Test extract
        data = obj.extract(schema="public", table="employees")
        assert data
        assert data == fixture_extracted_data

        # Test get select query
        sql = obj._get_select_query(
            schema="public",
            table="employees",
            delta_date_columns=[],
        )
        assert sql == "SELECT * FROM public.employees"

        # Test _transform_to_dict
        data = obj._transform_to_dict(
            fixture_rows_columns["rows"], fixture_rows_columns["columns"]
        )
        assert data == fixture_extracted_data

        del obj.conn
        # Test _get_connection
        obj._get_connection(
            host=fixture_postgres_connection["host"],
            port=fixture_postgres_connection["port"],
            database=fixture_postgres_connection["database"],
            user=fixture_postgres_connection["user"],
            password=fixture_postgres_connection["password"],
        )
        assert obj.conn

        # Test __generate_where_clause
        sql = obj._get_select_query(
            schema="public",
            table="employees",
            delta_date_columns=["last_update"],
            last_date="2021-01-01",
        )
        assert sql == "SELECT * FROM public.employees where last_update >= '2021-01-01'"

        obj.conn.close()

        assert obj.conn.closed
