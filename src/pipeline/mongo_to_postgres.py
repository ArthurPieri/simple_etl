# pylint: disable=relative-beyond-top-level

# from ..extract.extract_mongodb import FromMongodb
# from ..transform.transform_to_postgres import TransformPostgres
# from ..load.load_postgres import ToPostgres

from .interface.pipeline_interface import PipelineInterface


class MongoToPostgres(PipelineInterface):
    """'
    This class is used to run the pipeline from MongoDB to Postgres
    """

    def __init__(self):
        ...

    def run(self, **kwargs):
        ...

    def get_stats(self):
        ...
