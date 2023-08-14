import pytest

from ..src.extract.postgres import FromPostgres  # pylint: disable=import-error


class TestExtractPostgres:
    """
    Testing extract postgres class
    """

    @pytest.fixture
    def fixture_extracted_data(self):
        """
        When running docker-compose up, the postgres container will be created
        and the data will be inserted into the table.
        The data is as it follows and will be used to test if it is extracting
        the data correctly
        """
        return [
            {
                "id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com",
            },
            {
                "id": 2,
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "jane.doe@example.com",
            },
        ]

    def test_extract_from_postgres(self, fixture_extracted_data):
        """
        Test extract method
        """
        try:
            obj = FromPostgres(
                host="localhost",
                port="5432",
                database="postgres_test",
                user="postgres",
                password="postgres",
            )
            # test if the connection is working
            assert obj
        except Exception as exc:
            print(exc)
            assert not obj

        data = obj.extract(schema="public", table="employees")
        # test if extract is returning an array
        assert data
        # test if the extracted array is correctely populated
        assert data == fixture_extracted_data
        # make sure the connection is closed
        obj.conn.close()
        # test if the connection is closed
        assert obj.conn.closed
