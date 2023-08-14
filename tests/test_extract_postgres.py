from ..src.extract.extract_postgres import FromPostgres  # pylint: disable=import-error
from .postgres.fixture_postgres import (  # pylint: disable=unused-import
    fixture_extracted_data,
    fixture_postgres_connection,
)


class TestExtractPostgres:  # pylint: disable=too-few-public-methods
    """
    Testing extract postgres class
    """

    def test_extract_from_postgres(  # pylint: disable=redefined-outer-name
        self,
        fixture_postgres_connection,
        fixture_extracted_data,
    ):
        """
        Test extract method
        """
        try:
            obj = FromPostgres(
                host=fixture_postgres_connection["host"],
                port=fixture_postgres_connection["port"],
                database=fixture_postgres_connection["database"],
                user=fixture_postgres_connection["user"],
                password=fixture_postgres_connection["password"],
            )
            assert obj
        except Exception as exc:
            print(exc)
            assert not obj

        data = obj.extract(schema="public", table="employees")
        assert data

        assert data == fixture_extracted_data

        obj.conn.close()

        assert obj.conn.closed
