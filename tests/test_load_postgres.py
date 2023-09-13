# pylint: disable= missing-function-docstring, redefined-outer-name, protected-access, duplicate-code
"""
Tests for load_postgres.py and load_interface.py
"""
from io import StringIO
import logging

from datetime import datetime
import pytest  # pylint: disable=import-error
from .postgres.fixture_postgres import (  # pylint: disable=unused-import
    fixture_extracted_data,
    fixture_load_data,
    fixture_new_column_data,
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

    def test_create_table(self, obj, fixture_load_data):
        cursor = obj.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.test_create")
        log_stream = self.setup_log_stream(obj)

        success = obj.load(
            data=fixture_load_data,
            merge_ids=["id"],
            database="postgres_test",
            schema="public",
            table="test_create",
        )
        log_contents = log_stream.getvalue()
        assert (
            "Table postgres_test.public.test_create does not exist. Creating it..."
            in log_contents
        )
        assert success
        cursor.execute("DROP TABLE IF EXISTS public.test_create")
        cursor.close()

    def test_extract_add_columns_to_table(self, obj, fixture_new_column_data):
        cursor = obj.conn.cursor()
        cursor.execute(
            "ALTER TABLE public.employees_test_load DROP COLUMN IF EXISTS new_column"
        )
        log_stream = self.setup_log_stream(obj)

        success = obj.load(
            data=fixture_new_column_data,
            merge_ids=["id"],
            database="postgres_test",
            schema="public",
            table="employees_test_load",
        )
        log_contents = log_stream.getvalue()
        assert success
        assert "Adding columns to table public.employees_test_load" in log_contents
        cursor.execute(
            "ALTER TABLE public.employees_test_load DROP COLUMN IF EXISTS new_column"
        )
        cursor.close()

    def test_add_columns_to_table_error(self, obj):
        cursor = obj.conn.cursor()
        cursor.execute(
            "ALTER TABLE public.employees_test_load DROP COLUMN IF EXISTS new_column"
        )
        cursor.close()

        try:
            obj._add_columns_to_table(
                columns_types={"new_column": str},
                database="postgres_test",
                schema="public",
                table="not_table",
            )
        except Exception as exc:
            assert exc

    def test_create_empty_table_error(self, obj):
        try:
            obj._create_empty_table(
                columns_types={"new_column": str},
                database="postgres_test",
                schema="not_schema",
                table="not_table",
            )
        except Exception as exc:
            assert exc

    def test_get_create_table_sql_temp(self, obj):
        sql = obj._get_create_table_sql(
            columns_types={
                "id": int,
                "first_name": str,
                "last_name": str,
                "email": str,
            },
            database="postgres_test",
            schema="public",
            table="test_temp_create",
            is_temp=True,
            primary_key="id",
        )
        assert sql

    def test_get_max_dates_from_table_with_none(self):
        pass

    def test_get_max_dates_from_table_no_dates_found(self):
        pass

    def test_get_max_dates_from_table_error(self):
        # nao estou conseguindo forcar um erro aqui
        pass

    def test_get_max_date_error(self):
        pass

    def test_get_postgres_types(self):
        # test all types
        pass

    def test_load_data_error(self):
        # nao estou conseguindo forcar um erro aqui
        pass

    def setup_log_stream(
        self,
        obj,
        log_level=logging.INFO,
        log_format="%(name)s - %(levelname)s - %(message)s",
    ):
        """
        Set up a StringIO stream for logging and return the logger and the stream.
        """
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        log_formatter = logging.Formatter(log_format)
        log_handler.setFormatter(log_formatter)

        logger = logging.getLogger(obj.log.name)
        logger.setLevel(log_level)
        logger.addHandler(log_handler)

        return log_stream
