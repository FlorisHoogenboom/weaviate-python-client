"""
Classification class definition.
"""
import uuid
from typing import Union
from weaviate.exceptions import UnsuccessfulStatusCodeError, AiohttpConnectionError
from weaviate.base.classification import(
    BaseClassification,
    pre_get,
)
from .config_builder import ConfigBuilder
from ..requests import Requests


class Classification(BaseClassification):
    """
    Classification class used to schedule and/or check the status of a classification process
    of Weaviate objects.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a Classification class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests

    def schedule(self) -> ConfigBuilder:
        """
        Schedule a Classification of the Objects within Weaviate.

        Returns
        -------
        weaviate.asynchronous.ConfigBuilder
            A ConfigBuilder that should be configured to the desired classification task.
        """

        return ConfigBuilder(self._requests, self)

    async def get(self, classification_uuid: Union[str, uuid.UUID]) -> dict:
        """
        Polls the current state of the given classification.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.

        Returns
        -------
        dict
            A dict containing the Weaviate answer.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path = pre_get(classification_uuid=classification_uuid)

        try:
            response = await self._requests.get(
                path=path,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Classification status could not be retrieved due to connection error.'
            ) from conn_err
        if response.status == 200:
            return await response.json()
        raise UnsuccessfulStatusCodeError(
            "Get classification status.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def is_complete(self, classification_uuid: Union[str, uuid.UUID]) -> bool:
        """
        Checks if a started classification job has completed.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.

        Returns
        -------
        bool
            True if given classification has finished, False otherwise.
        """

        return await self._check_status(classification_uuid, 'completed')

    async def is_failed(self, classification_uuid: Union[str, uuid.UUID]) -> bool:
        """
        Checks if a started classification job has failed.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.

        Returns
        -------
        bool
            True if the classification failed, False otherwise.
        """

        return await self._check_status(classification_uuid, "failed")

    async def is_running(self, classification_uuid: Union[str, uuid.UUID]) -> bool:
        """
        Checks if a started classification job is running.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.

        Returns
        -------
        bool
            True if the classification is running, False otherwise.
        """

        return await self._check_status(classification_uuid, "running")

    async def _check_status(self, classification_uuid: Union[str, uuid.UUID], status: str) -> bool:
        """
        Check for a status of a classification.

        Parameters
        ----------
        classification_uuid : str or uuid.UUID
            Identifier of the classification.
        status : str
            Status to check for.

        Returns
        -------
        bool
            True if 'status' is satisfied, False otherwise.
        """

        response = await self.get(classification_uuid)

        if response["status"] == status:
            return True
        return False
