# pylint: disable=relative-beyond-top-level
from ..extract.extract_mongodb import FromMongodb

from ..transform.transform_to_postgres import TransformPostgres
from ..load.load_postgres import ToPostgres

from .interface.pipeline_interface import PipelineInterface


class MongoToPostgres(PipelineInterface):
    """'
    This class is used to run the pipeline from MongoDB to Postgres
    """

    @PipelineInterface.get_execution_time
    def run(self, **kwargs):
        """
        Run the pipeline
        # Kwargs arguments:
        ## Required
        - delta_date_columns: list,
            - List of columns to be used to filter data
        - merge_ids: list,
            - List of columns to be used to merge data

        ### Mongodb_conn
        - mongodb_auth
        - mongodb_host
        - mongodb_port
        - mongodb_user
        - mongodb_password
        - mongodb_database
        - mongodb_aggregation_clause
        - mongodb_filter
        - mongodb_collection

        ### Postgres_conn
        - postgres_host
        - postgres_port
        - postgres_user
        - postgres_password
        - postgres_database
        - postgres_schema
        - postgres_table

        ## Optional
        - batch_size: int = 10000,
            - Batch size to be used to extract and load data
        - columns_to_drop: list = [],
            - List of columns to be dropped from data
        - columns_to_rename: dict = {},
            - Dictionary of columns to be renamed from data
        """
        mongodb = FromMongodb(
            auth=kwargs["mongodb_auth"],
            host=kwargs["mongodb_host"],
            port=kwargs["mongodb_port"],
            user=kwargs["mongodb_user"],
            password=kwargs["mongodb_password"],
            database=kwargs["mongodb_database"],
        )

        postgres = ToPostgres(
            host=kwargs["postgres_host"],
            port=kwargs["postgres_port"],
            user=kwargs["postgres_user"],
            password=kwargs["postgres_password"],
            database=kwargs["postgres_database"],
        )

        last_date = postgres.get_last_date(
            delta_date_columns=kwargs["delta_date_columns"],
            table=kwargs["load_table"],
            schema=kwargs["load_schema"],
            database=kwargs["load_database"],
        )

        extracted_data = mongodb.extract(
            delta_date_columns=kwargs["delta_date_columns"],
            batch_size=kwargs["batch_size"],
            collection=kwargs["mongodb_collection"],
            aggregation_clause=kwargs["mongodb_aggregation_clause"],
            filter=kwargs["mongodb_filter"],
            last_date=last_date,
        )

        self.number_of_rows_extracted = len(extracted_data)

        transformed_data = TransformPostgres().transform(
            data=extracted_data,
            columns_to_drop=kwargs["columns_to_drop"],
            columns_to_rename=kwargs["columns_to_rename"],
        )

        self.number_of_rows_transformed = len(transformed_data)

        postgres.load(
            data=transformed_data,
            merge_ids=kwargs["merge_ids"],
            table=kwargs["load_table"],
            schema=kwargs["load_schema"],
            database=kwargs["load_database"],
        )

        self.number_of_rows_loaded = len(transformed_data)
        self.percentage_rows_loaded = (
            self.number_of_rows_loaded / self.number_of_rows_transformed
        ) * 100

        return True
