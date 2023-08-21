# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods
# from ..src.load.load_postgres import ToPostgres  # pylint: disable=import-error

from .postgres.fixture_postgres import (  # pylint: disable=unused-import
    fixture_postgres_connection,
)


class TestToPostgres:
    """
    Makes all the necessary treatments to load data into postgres.
    """

    def test_load(
        self,
    ) -> None:
        ...

        # test load

        # test_add_columns_to_table

        # test_create_empty_table

        # test_get_connection

        # test_get_max_dates_from_table

        # test_get_postgres_columns

        # test_load_data
