import unittest
import psycopg2


class TestExtractPostgres(unittest.TestCase):
    """
    Testing extract postgres class
    """

    def test_db_connection(self):
        """
        Testing database connection
        """
        try:
            conn = psycopg2.connect(
                host="localhost",
                port="5432",
                database="postgres_test",
                user="postgres",
                password="postgres",
            )
            self.assertTrue(conn)
        except:
            self.assertFalse(conn)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
