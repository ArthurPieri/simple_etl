from abc import ABC, abstractmethod


class PipelineInterface(ABC):
    """
    This class is used to run the pipeline
    """

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the pipeline
        """

    @abstractmethod
    def extract(self, **kwargs):
        """
        Extract data from source
        """

    @abstractmethod
    def transform(self, **kwargs):
        """
        Transform data from source
        """

    @abstractmethod
    def load(self, **kwargs):
        """
        Load data into destination
        """
