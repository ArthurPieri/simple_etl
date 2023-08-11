import unittest
from ..src.extract.postgres import FromPostgres  # pylint: disable=import-error

# import pytest


class TestExtractPostgres(unittest.TestCase):
    """
    Testing extract postgres class
    """

    def test_extract(self):
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
            self.assertTrue(obj)
        except:
            self.assertFalse(obj)

        data = obj.extract(schema="public", table="employees")
        self.assertTrue(data)
        original_data = [
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
        self.assertCountEqual(data, original_data)
        obj.conn.close()


if __name__ == "__main__":
    unittest.main()
