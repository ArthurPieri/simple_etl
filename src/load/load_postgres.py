# pylint: disable=import-error, no-name-in-module
"""'
This class is used to load data into a postgres database.
It recieves a dataframe, a schema name, a table name, and a connection string.
Makes all the treatment necessary and then loads the data into the specified table.
"""
import re

from psycopg2 import connect

from pytz import timezone

from .interface.load_interface import LoadInterface


class ToPostgres(LoadInterface):
    """
    Load data into postgres.
    """

    def load(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        merge_ids: list,
        **kwargs,
    ) -> None:
        """
        Load data into postgres
        **Kwargs parameters:
        - Database
        - Schema
        - Table
        """
        data = self._add_loaddate(data=data)

        columns = self._get_columns(data=data)

        data_columns_types = self._get_python_types(columns, data)
        table_columns = self._get_postgres_columns(**kwargs)

        if not table_columns:
            self.log.info(
                "Table %s.%s.%s does not exist. Creating it...",
                kwargs["database"],
                kwargs["schema"],
                kwargs["table"],
            )
            self._create_empty_table(columns_types=data_columns_types, **kwargs)

        if table_columns:
            diff = {
                k: v for k, v in data_columns_types.items() if k not in table_columns
            }
            if diff:
                self.log.info(
                    "Table %s.%s.%s does not have all the columns. Adding %s...",
                    kwargs["database"],
                    kwargs["schema"],
                    kwargs["table"],
                    str(diff),
                )
                self._add_columns_to_table(columns_types=diff, **kwargs)

        self._load_data(
            columns_and_types=data_columns_types,
            data=data,
            merge_ids=merge_ids,
            **kwargs,
        )
        return True

    def _add_columns_to_table(self, columns_types: dict, **kwargs) -> bool:
        cursor = self.conn.cursor()
        self.log.info(
            "Adding columns to table %s.%s", kwargs["schema"], kwargs["table"]
        )
        alter_table_sql = self._get_add_columns_sql(
            columns_types=columns_types, **kwargs
        )
        try:
            cursor.execute(alter_table_sql)
            self.conn.commit()
            return True
        except Exception as error:
            self.log.error(
                "Error adding columns to table %s.%s", kwargs["schema"], kwargs["table"]
            )
            self.log.error(error)
            self.conn.rollback()
            raise error
        finally:
            cursor.close()

    def _get_add_columns_sql(self, columns_types: dict, **kwargs) -> str:
        table_name = f'{kwargs["schema"]}.{kwargs["table"]}'
        sql = f"ALTER TABLE {table_name} ADD COLUMN "
        columns_types = self._get_postgres_types(columns_types)

        for col, col_type in columns_types.items():
            sql += f"{col} {col_type}, "

        sql = sql[:-2]
        self.log.info("SQL statement: %s", sql)

        return sql

    def _create_empty_table(self, columns_types: dict, **kwargs) -> bool:
        cursor = self.conn.cursor()
        self.log.info("Creating table %s.%s", kwargs["schema"], kwargs["table"])
        create_table_sql = self._get_create_table_sql(
            columns_types=columns_types, is_temp=False, primary_key="id", **kwargs
        )
        try:
            cursor.execute(create_table_sql)
            self.conn.commit()
            return True
        except Exception as error:  # pylint: disable=broad-except
            self.log.error(
                "Error creating table %s.%s", kwargs["schema"], kwargs["table"]
            )
            self.log.error(error)
            self.conn.rollback()
            raise error
        finally:
            cursor.close()

    def _get_create_table_sql(
        self, columns_types: dict, is_temp: bool, primary_key: str, **kwargs
    ) -> str:
        table_name = f'{kwargs["schema"]}.{kwargs["table"]}'

        columns_types = self._get_postgres_types(columns_types)

        if is_temp:
            table_name += "_temp"

        sql = f"CREATE TABLE {table_name} ("

        for col, col_type in columns_types.items():
            if col == primary_key:
                sql += f"{col} {col_type} PRIMARY KEY,"
            else:
                sql += f"{col} {col_type},"

        sql = sql[:-1] + ")"
        match = re.sub(r"\(\s?\)", "", sql)

        if match != sql:
            self.log.error("Empty parentheses in SQL statement")
            raise ValueError("Empty parentheses in SQL statement")
        self.log.info("SQL statement: %s", sql)

        return sql

    # pylint: disable=duplicate-code
    def _get_connection(self, **kwargs) -> None:
        """
        Get connection to Postgres
        Kwargs arguments:
        - host
        - port
        - user
        - password
        - database
        """
        self.conn = connect(  # pylint: disable=attribute-defined-outside-init
            host=kwargs["host"],
            port=kwargs["port"],
            user=kwargs["user"],
            password=kwargs["password"],
            database=kwargs["database"],
        )

    def _get_max_dates_from_table(self, delta_date_columns: list, **kwargs):
        cursor = self.conn.cursor()
        dates_list = []
        try:
            for date_column in delta_date_columns:
                dates_list.append(self._get_max_date(date_column, **kwargs))

            if None in dates_list:
                dates_list.remove(None)

            if len(dates_list) == 0:
                self.log.info("No dates found")
                return None

            last_date = max(dates_list)
            self.log.info("Last date found: %s", last_date)
        except Exception as exc:
            self.log.error("Error getting max date from Postgres: %s", exc)
            raise exc
        finally:
            cursor.close()

        return last_date

    def _get_max_date(self, date_column: str, **kwargs):
        """
        Get max date from Postgres table
        """
        cursor = self.conn.cursor()
        sql = f"""
            SELECT MAX({date_column}) FROM {kwargs['schema']}.{kwargs['table']}
        """

        try:
            cursor.execute(sql)
            max_date = cursor.fetchone()  # type: ignore
        except Exception as exc:
            self.log.error("Error while executing SQL statement: %s", exc)
            return None

        last_date = max_date[0]

        if last_date:
            self.log.info("Last date in table: %s", last_date)
            last_date = last_date.replace(tzinfo=timezone("UTC"))

        cursor.close()
        return last_date

    def _get_postgres_types(self, columns_and_types: dict) -> dict:
        for name, _type in columns_and_types.items():
            if isinstance(_type, list):
                _type = _type[0]
            _type = _type.__name__
            if _type == "str":
                columns_and_types[name] = "varchar(255)"
            elif _type == "int":
                columns_and_types[name] = "integer"
            elif _type == "float":
                columns_and_types[name] = "float"
            elif _type == "bool":
                columns_and_types[name] = "boolean"
            elif _type in ("dict", "list"):
                columns_and_types[name] = "variant"
            elif _type == "datetime":
                columns_and_types[name] = "timestamp"
            else:
                columns_and_types[name] = "varchar(255)"
        return columns_and_types

    def _get_postgres_columns(self, **kwargs) -> list:
        columns = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{kwargs["schema"]}'
                AND table_name = '{kwargs["table"]}'
                """
            )
            columns = cursor.fetchall()
            columns = [column[0] for column in columns]
        return columns

    def _get_insert_sql(
        self, columns_and_types: dict, merge_ids: list, **kwargs
    ) -> str:
        """
        Get SQL statement to insert data into Postgres
        """
        table_name = f'{kwargs["schema"]}.{kwargs["table"]}'
        sql = f"INSERT INTO {table_name} ("

        for col in columns_and_types.keys():
            sql += f"{col}, "

        sql = sql[:-2] + ") VALUES ("

        for col in columns_and_types.keys():
            sql += f"%({col})s, "

        sql = sql[:-2] + ")"

        if merge_ids:
            sql += " ON CONFLICT ("
            for col in merge_ids:
                sql += f"{col}, "
            sql = sql[:-2] + ") DO UPDATE SET "

            for col in columns_and_types.keys():
                sql += f"{col} = EXCLUDED.{col}, "

            sql = sql[:-2]

        self.log.info("SQL statement: %s", sql)

        return sql

    def _load_data(
        self, columns_and_types: dict, data: list[dict], merge_ids: list, **kwargs
    ) -> bool:
        cursor = self.conn.cursor()
        self.log.info(
            "Loading data into table %s.%s", kwargs["schema"], kwargs["table"]
        )
        insert_sql = self._get_insert_sql(
            columns_and_types=columns_and_types, merge_ids=merge_ids, **kwargs
        )
        try:
            cursor.executemany(insert_sql, data)
            self.conn.commit()
            return True
        except Exception as error:
            self.log.error(
                "Error loading data into table %s.%s", kwargs["schema"], kwargs["table"]
            )
            self.log.error(error)
            self.conn.rollback()
            raise error
        finally:
            cursor.close()
