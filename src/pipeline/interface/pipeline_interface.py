from abc import ABC, abstractmethod


class PipelineInterface(ABC):
    """
    This class is used to run the pipeline
    """

    @abstractmethod
    def __init__(self, **kwargs):
        """
        Initialize the pipeline
        # Kwargs arguments:
        ## Dependent on the implementation
        - Extract conn args
        - Load conn args
        """

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the pipeline
        # Kwargs arguments:
        ## Required
        - delta_date_columns: list,
            - List of columns to be used to filter data
        - extract_database: str,
            - Database to be used to extract data
        - extract_schema: str,
            - Schema to be used to extract data
        - extract_table: str,
            - Table to be used to extract data
        - load_database: str,
            - Database to be used to load data
        - load_schema: str,
            - Schema to be used to load data
        - load_table: str,
            - Table to be used to load data
        - merge_ids: list,
            - List of columns to be used to merge data

        ## Optional
        - batch_size: int = 10000,
            - Batch size to be used to extract and load data
        - columns_to_drop: list = [],
            - List of columns to be dropped from data
        - columns_to_rename: dict = {},
            - Dictionary of columns to be renamed from data
        """

    @abstractmethod
    def get_stats(self):
        """
        Get stats from pipeline
        """
