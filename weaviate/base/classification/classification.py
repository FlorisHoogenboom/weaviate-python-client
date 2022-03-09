"""
BaseClassification abstract class definition.
"""
import uuid
from typing import Union
from abc import ABC, abstractmethod
from weaviate.util import get_valid_uuid
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
        """

    @abstractmethod
    def get(self, classification_uuid: Union[str, uuid.UUID]):
        """
        Polls the current state of the given classification.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.
        """

    @abstractmethod
    def is_complete(self, classification_uuid:  Union[str, uuid.UUID]):
        """
        Checks if a started classification job has completed.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.
        """

    @abstractmethod
    def is_failed(self, classification_uuid:  Union[str, uuid.UUID]):
        """
        Checks if a started classification job has failed.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.
        """

    @abstractmethod
    def is_running(self, classification_uuid:  Union[str, uuid.UUID]):
        """
        Checks if a started classification job is running.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.
        """


def pre_get(classification_uuid:  Union[str, uuid.UUID]):

    path = f'/classifications/{get_valid_uuid(classification_uuid)}'

    return path
