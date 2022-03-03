"""
Classification class definition.
"""
from abc import ABC, abstractmethod
from .config_builder import BaseConfigBuilder

class BaseClassification(ABC):
    """
    BaseClassification abstract class used to schedule and/or check the status of a classification
    process of Weaviate objects. Sync/Async Client should implement each its own Classification.
    """

    @abstractmethod
    def schedule(self) -> BaseConfigBuilder:
        """
        Schedule a Classification of the Objects within Weaviate.

        Returns
        -------
        weaviate.base.BaseConfigBuilder
            A ConfigBuilder that should be configured to the desired
            classification task
        """

    @abstractmethod
    def get(self, classification_uuid: str) -> dict:
        """
        Polls the current state of the given classification.

        Parameters
        ----------
        classification_uuid : str
            Identifier of the classification.

        Returns
        -------
        dict
            A dict containing the Weaviate answer.
        """

    @abstractmethod
    def is_complete(self, classification_uuid: str) -> bool:
        """
        Checks if a started classification job has completed.

        Parameters
        ----------
        classification_uuid : str
            Identifier of the classification.

        Returns
        -------
        bool
            True if given classification has finished, False otherwise.
        """

    @abstractmethod
    def is_failed(self, classification_uuid: str) -> bool:
        """
        Checks if a started classification job has failed.

        Parameters
        ----------
        classification_uuid : str
            Identifier of the classification.

        Returns
        -------
        bool
            True if the classification failed, False otherwise.
        """

    @abstractmethod
    def is_running(self, classification_uuid: str) -> bool:
        """
        Checks if a started classification job is running.

        Parameters
        ----------
        classification_uuid : str
            Identifier of the classification.

        Returns
        -------
        bool
            True if the classification is running, False otherwise.
        """
