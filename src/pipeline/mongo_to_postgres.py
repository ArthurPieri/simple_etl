# pylint: disable=relative-beyond-top-level

from ..extract.extract_mongodb import FromMongodb
from ..transform.transform_to_postgres import TransformPostgres
from ..load.load_postgres import ToPostgres

from .interface.pipeline_interface import PipelineInterface


class MongoToPostgres(PipelineInterface, FromMongodb, TransformPostgres, ToPostgres):
    """'
    This class is used to run the pipeline from MongoDB to Postgres
    """

    # check if it is possible to get a list and create a new table with it
