import time
from abc import ABC, abstractmethod

from logging import getLogger, shutdown


class PipelineInterface(ABC):
    """
    Basic interface for every pipeline.
    """

    def __init__(self):
        """
        Initialize the pipeline.
        """
        self.__start_log()
        self.number_of_rows_extracted = 0
        self.number_of_rows_transformed = 0
        self.number_of_rows_loaded = 0
        self.execution_time = 0
        self.percentage_rows_loaded = 0

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the pipeline recursively until all data is loaded
        # Kwargs arguments:
        ## Required
        - delta_date_columns: list,
            - List of columns to be used to filter data
        - merge_ids: list,
            - List of columns to be used to merge data.

        ## Optional
        - batch_size: int = 10000,
            - Batch size to be used to extract and load data
        - columns_to_drop: list = [],
            - List of columns to be dropped from data
        - columns_to_rename: dict = {},
            - Dictionary of columns to be renamed from data

        ## Dependent on the implementation
        ### Extract class
        - Extract conn args
        - Extract database,schema,table or equivalent

        ### Load class
        - Load conn args
        - load database,schema,table or equivalent
        """

    @abstractmethod
    # Need to apply on the exectuion @_get_execution_time
    def _pipeline(self, **kwargs) -> bool:
        """
        Create the pipeline
        # Kwargs arguments:
        ## Required
        - delta_date_columns: list,
            - List of columns to be used to filter data
        - merge_ids: list,
            - List of columns to be used to merge data.

        ## Optional
        - batch_size: int = 10000,
            - Batch size to be used to extract and load data
        - columns_to_drop: list = [],
            - List of columns to be dropped from data
        - columns_to_rename: dict = {},
            - Dictionary of columns to be renamed from data

        ## Dependent on the implementation
        ### Extract class
        - Extract conn args
        - Extract database,schema,table or equivalent

        ### Load class
        - Load conn args
        - load database,schema,table or equivalent
        """

    def get_stats(self) -> dict:
        """
        Get stats from pipeline.
        """
        return {
            "number_of_rows_extracted": self.number_of_rows_extracted,
            "number_of_rows_transformed": self.number_of_rows_transformed,
            "number_of_rows_loaded": self.number_of_rows_loaded,
            "execution_time": self.execution_time,
            "percentage_of_rows_loaded": self.percentage_rows_loaded,
        }

    @staticmethod
    def _get_execution_time(func):
        """
        Get the time it took to run a function.
        """

        def wrapper(instance, *args, **kwargs):
            start_time = time.time()
            result = func(instance, *args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            instance.execution_time = elapsed_time
            instance.log.info(
                "[Instance Timer] %s took %i.6f seconds.", func.__name__, elapsed_time
            )
            return result

        return wrapper

    def __start_log(self) -> None:
        """
        Start logging for class.
        """
        self.log = getLogger(__name__)
        self.log.info("-----------------------------------------")
        self.log.info("Initializing %s class", self.__class__.__name__)
        self.log.info("-----------------------------------------")

    def __del__(self) -> None:
        self.log.info("Connection %s closed", self.__class__.__name__)
        shutdown()
