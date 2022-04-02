"""
ConfigBuilder class definition.
"""
import time
import ujson
from weaviate.exceptions import RequestsConnectionError, UnsuccessfulStatusCodeError
from weaviate.base.classification import BaseConfigBuilder
from ..requests import Requests


class ConfigBuilder(BaseConfigBuilder):
    """
    ConfigBuilder class that is used to configure a classification process.
    """

    def __init__(self, requests: Requests, classification: 'Classification'):
        """
        Initialize a ConfigBuilder class instance.

        Parameters
        ----------
        requests : weaviate.synchronous.Requests
            Requests object to an active and running Weaviate instance.
        classification : weaviate.synchronous.Classification
            Classification object to be configured using this ConfigBuilder instance.
        """

        super().__init__()
        self._requests = requests
        self._classification = classification

    def _start(self) -> dict:
        """
        Start the classification based on the configuration set.

        Returns
        -------
        dict
            Classification result.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            Unexpected error.
        """

        try:
            response = self._requests.post(
                path='/classifications',
                data_json=self._config,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Classification may not started due to connection error.'
            ) from conn_err
        if response.status_code == 201:
            return ujson.loads(response.content)
        raise UnsuccessfulStatusCodeError(
            "Start classification.",
            status_code=response.status_code,
            response_message=response.text,
        )

    def do(self) -> dict:
        """
        Start the classification.

        Returns
        -------
        dict
            Classification result.
        """

        self._validate_config()

        response = self._start()
        if not self._wait_for_completion:
            return response

        # wait for completion
        classification_uuid = response["id"]
        while self._classification.is_running(classification_uuid):
            time.sleep(2.0)
        return self._classification.get(classification_uuid)
