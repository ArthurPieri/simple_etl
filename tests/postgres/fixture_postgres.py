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
def fixture_load_data():
    """
    Data to be loaded
    """
    return [
        {
            "id": 5,
            "first_name": "Elizabeth",
            "last_name": "Smith",
            "email": "lizzy@smith.com",
        },
        {
            "id": 6,
            "first_name": "Joseph",
            "last_name": "Smith",
            "email": "jose@smith.com",
        },
    ]


@pytest.fixture
def fixture_new_column_data():
    """
    Data to be loaded
    """
    return [
        {
            "id": 9,
            "first_name": "joselito",
            "last_name": "Smith",
            "email": "joselito@jose.com",
            "new_column": "new_value",
        },
    ]


@pytest.fixture
def fixture_rows_columns():
    """
    Rows and columns to test
    """
    return {
        "rows": [
            (1, "John", "Doe", "john.doe@example.com"),
            (2, "Jane", "Doe", "jane.doe@example.com"),
        ],
        "columns": ["id", "first_name", "last_name", "email"],
    }
