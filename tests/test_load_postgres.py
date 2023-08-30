# pylint: disable= missing-function-docstring, redefined-outer-name, protected-access, duplicate-code
"""
Tests for load_postgres.py and load_interface.py
"""

from datetime import datetime
from pytz import timezone  # pylint: disable=import-error
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


class TestToPostgres:  # pylint: disable=too-many-public-methods
    """
    Makes all the necessary treatments to load data into postgres.
    """

    def test_load_postgres(self, fixture_load_data, obj) -> None:
        success = obj.load(
            data=fixture_load_data,
            merge_ids=["id"],
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        assert success

    def test_connection_and_log(self, obj):
        assert obj.conn
        assert obj.log

    def test_get_postgres_columns(self, obj):
        columns = obj._get_postgres_columns(schema="public", table="employees")
        assert columns == ["id", "first_name", "last_name", "email"]

    def test_create_empty_table(self, obj):
        cursor = obj.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.employees_load")
        cursor.close()

        success = obj._create_empty_table(
            columns_types={
                "id": int,
                "first_name": str,
                "last_name": str,
                "email": str,
            },
            database="postgres_test",
            schema="public",
            table="employees_load",
        )
        assert success

    def test_get_max_dates_from_table(self, obj):
        cursor = obj.conn.cursor()
        cursor.execute("DELETE FROM public.employees_test_load WHERE id = 5;")
        cursor.execute("DELETE FROM public.employees_test_load WHERE id = 6;")
        cursor.close()
        max_dates = obj._get_max_dates_from_table(
            delta_date_columns=["loaddate"],
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        assert max_dates
        assert max_dates == datetime(2023, 8, 22, 0, 0, tzinfo=timezone("UTC"))

    def test_add_columns_to_table(self, obj):
        cursor = obj.conn.cursor()
        cursor.execute(
            "ALTER TABLE public.employees_test_load DROP COLUMN IF EXISTS update_date"
        )
        cursor.close()

        success = obj._add_columns_to_table(
            columns_types={"update_date": datetime},
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        assert success

    def test_get_add_columns_sql(self, obj):
        sql = obj._get_add_columns_sql(
            columns_types={"update_date": datetime},
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        assert sql

    def test_get_create_table_sql(self, obj):
        sql = obj._get_create_table_sql(
            columns_types={
                "id": int,
                "first_name": str,
                "last_name": str,
                "email": str,
            },
            database="postgres_test",
            schema="public",
            table="employees_test_load",
            is_temp=False,
            primary_key="id",
        )
        assert sql

    def test_get_insert_sql(self, obj):
        sql = obj._get_insert_sql(
            columns_and_types={
                "id": int,
                "first_name": str,
                "last_name": str,
                "email": str,
            },
            database="postgres_test",
            schema="public",
            table="employees_test_load",
            merge_ids=["id"],
        )
        assert sql

    def test_load_data(self, obj, fixture_load_data):
        cursor = obj.conn.cursor()
        cursor.execute("DELETE FROM public.employees_test_load WHERE id = 5;")
        cursor.execute("DELETE FROM public.employees_test_load WHERE id = 6;")
        cursor.close()

        success = obj._load_data(
            columns_and_types={
                "id": int,
                "first_name": str,
                "last_name": str,
                "email": str,
            },
            data=fixture_load_data,
            merge_ids=[],
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        assert success

    def test_create_table(self):
        # mock the return of _get_postgres_columns
        # create_empty_table
        # add_columns_to_table
        pass

    def test_extract_add_columns_to_table(self):
        # mock the return of _get_postgres_columns
        # add_columns_to_table
        pass

    def test_add_columns_to_table_error(self):
        # mock the return of _get_postgres_columns
        # add_columns_to_table
        pass

    def test_create_empty_table_error(self):
        pass

    def test_get_create_table_sql_temp(self):
        pass

    def test_get_create_table_sql_error(self):
        pass

    def test_get_max_dates_from_table_with_none(self):
        pass

    def test_get_max_dates_from_table_no_dates_found(self):
        pass

    def test_get_max_dates_from_table_error(self):
        pass

    def test_get_max_date_error(self):
        pass

    def test_get_postgres_types(self):
        # test all types
        pass

    def test_load_data_error(self):
        pass
