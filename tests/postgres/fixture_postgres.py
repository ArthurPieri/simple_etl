import pytest


@pytest.fixture
def fixture_extracted_data():
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


@pytest.fixture
def fixture_postgres_connection():
    """
    Postgres connection data
    """
    return {
        "host": "localhost",
        "port": "5432",
        "database": "postgres_test",
        "user": "postgres",
        "password": "postgres",
    }
