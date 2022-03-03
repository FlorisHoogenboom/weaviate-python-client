"""
ConfigBuilder class definition.
"""
import asyncio
from weaviate.exceptions import AiohttpConnectionError, UnsuccessfulStatusCodeError
from weaviate.base import BaseConfigBuilder
from weaviate.asynchronous import AsyncRequests


class AsyncConfigBuilder(BaseConfigBuilder):
    """
    AsyncConfigBuilder class that is used to configure a classification process.
    """

    def __init__(self, requests: AsyncRequests, classification: 'AsyncClassification'):
        """
        Initialize a AsyncConfigBuilder class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.AsyncRequests
            AsyncRequests object to an active and running weaviate instance.
        classification : weaviate.asynchronous.AsyncClassification
            AsyncClassification object to be configured using this AsyncConfigBuilder instance.
        """

        super().__init__()
        self._requests = requests
        self._classification = classification

    async def _start(self) -> dict:
        """
        Start the classification based on the configuration set.

        Returns
        -------
        dict
            Classification result.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            Unexpected error.
        """

        try:
            response = await self._requests.post(
                path='/classifications',
                data_json=self._config,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Classification may not started due to connection error.'
            ) from conn_err
        if response.status == 201:
            return await response.json()
        raise UnsuccessfulStatusCodeError(
            "Start classification.",
            status_code=response.status,
            response_message=response.text,
        )

    async def do(self) -> dict:
        """
        Start the classification.

        Returns
        -------
        dict
            Classification result.
        """

        self._validate_config()

        response = await self._start()
        if not self._wait_for_completion:
            return response

        # wait for completion
        classification_uuid = response["id"]
        while await self._classification.is_running(classification_uuid):
            asyncio.sleep(2.0)
        return await self._classification.get(classification_uuid)
