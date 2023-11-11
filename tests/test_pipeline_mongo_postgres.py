# pylint: disable=missing-module-docstring, missing-function-docstring, too-few-public-methods. redefined-outer-name, protected-access, unused-import
from unittest.mock import MagicMock
from datetime import datetime

import pytest  # pylint: disable=import-error

from ..src.pipeline.mongo_to_postgres import MongoToPostgres


@pytest.fixture(scope="module")
def obj():
    yield MongoToPostgres()


class TestMongoToPostgres:
    """
    Testing pipeline from mongo to postgres.
    """

    def test_run(self, obj):
        success = obj.run(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="postgres_test",
            postgres_schema="public",
            postgres_user="postgres",
            postgres_password="postgres",
            mongodb_host="localhost",
            mongodb_port=27017,
            mongodb_database="mydatabase",
            mongodb_auth=False,
            mongodb_collection="users",
            mongodb_user="",
            mongodb_password="",
            mongodb_filter=None,
            mongodb_aggregation_clause=None,
            delta_date_columns=["loaddate"],
            batch_size=10000,
            columns_to_drop=None,
            columns_to_rename=None,
            merge_ids=None,
            load_table="employees_pipeline",
            load_schema="public",
            load_database="postgres_test",
        )
        # trunk-ignore(bandit/B101)
        assert success
        assert obj.number_of_rows_extracted
        assert obj.number_of_rows_transformed
        assert obj.number_of_rows_loaded
        assert obj.execution_time
        assert obj.percentage_rows_loaded
