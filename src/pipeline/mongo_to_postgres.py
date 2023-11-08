# pylint: disable=relative-beyond-top-level
import time

from ..extract.extract_mongodb import FromMongodb
from ..transform.transform_to_postgres import TransformPostgres
from ..load.load_postgres import ToPostgres

from .interface.pipeline_interface import PipelineInterface


class MongoToPostgres(PipelineInterface):
    """'
    This class is used to run the pipeline from MongoDB to Postgres.
    """

    # @PipelineInterface._get_execution_time
    def run(self, **kwargs) -> str:
        """
        Run the pipeline recursively until all data is loaded.
        # Kwargs arguments:
        ## Required
        - delta_date_columns: list,
            - List of columns to be used to filter data
        - merge_ids: list,
            - List of columns to be used to merge data.
        - load_database: str,
            - Database to load data
        - load_schema: str,
            - Schema to load data
        - load_table: str,
            - Table to load data.

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
        start_time = time.time()
        try:
            is_success = self._pipeline(**kwargs)
            if is_success:
                self.log.info(
                    "Pipeline ran successfully. Rows: %s extracted, %s transformed, %s loaded",
                    self.number_of_rows_extracted,
                    self.number_of_rows_transformed,
                    self.number_of_rows_loaded,
                )
                self._pipeline(**kwargs)
            else:
                self.log.info(
                    "Pipeline Finished. Rows: %s extracted, %s transformed, %s loaded",
                    self.number_of_rows_extracted,
                    self.number_of_rows_transformed,
                    self.number_of_rows_loaded,
                )
        except Exception as exc:
            self.log.error("Error running pipeline: %s", exc)
            print(exc)
            raise RuntimeError(exc) from exc

        end_time = time.time()
        self.execution_time = end_time - start_time
        self.log.info("Execution time of method run: %s", self.execution_time)
        return "Pipeline ran successfully."

    # @PipelineInterface._get_execution_time
    def _pipeline(self, **kwargs) -> bool:
        """
        Create the pipeline.
        """
        start_time = time.time()
        self.number_of_executions += 1
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

        last_date = postgres.get_last_load_date(
            delta_date_columns=kwargs["delta_date_columns"],
            table=kwargs["load_table"],
            schema=kwargs["load_schema"],
            database=kwargs["load_database"],
        )

        self.log.info(last_date)

        extracted_data = mongodb.extract(
            delta_date_columns=kwargs["delta_date_columns"],
            batch_size=kwargs["batch_size"],
            collection=kwargs["mongodb_collection"],
            aggregation_clause=kwargs["mongodb_aggregation_clause"],
            filter=kwargs["mongodb_filter"],
            last_date=last_date,
        )

        if not extracted_data or len(extracted_data) == 0:
            self.log.info("No data extracted")
            return False

        self.number_of_rows_extracted = len(extracted_data)

        transformed_columns, transformed_data = TransformPostgres().transform(
            data=extracted_data,
            columns_to_drop=kwargs["columns_to_drop"],
            columns_to_rename=kwargs["columns_to_rename"],
        )
        self.log.info("Transformed columns: %s ", transformed_columns)

        if not transformed_data or len(transformed_data) == 0:
            self.log.info("No data transformed")
            return False

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
        end_time = time.time()
        execution_time = end_time - start_time
        self.log.info("Execution time of method _pipeline: %s", execution_time)
        self.log.info("Number of rows extracted: %s", self.number_of_rows_extracted)
        self.log.info("Number of rows transformed: %s", self.number_of_rows_transformed)
        self.log.info("Number of rows loaded: %s", self.number_of_rows_loaded)
        self.log.info("Pipeline ran successfully.")

        return True
