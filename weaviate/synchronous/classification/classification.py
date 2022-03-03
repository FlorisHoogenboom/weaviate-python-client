"""
Classification class definition.
"""
from weaviate.exceptions import UnsuccessfulStatusCodeError, RequestsConnectionError
from weaviate.util import get_valid_uuid
from ..requests import SyncRequests
from .config_builder import SyncConfigBuilder

class SyncClassification:
    """
    SyncClassification class used to schedule and/or check the status of a classification process
    of Weaviate objects.
    """

    def __init__(self, requests: SyncRequests):
        """
        Initialize a SyncClassification class instance.

        Parameters
        ----------
        connection : weaviate.sync.SyncRequests
            SyncRequests object to an active and running Weaviate instance.
        """

        self._requests = requests

    def schedule(self) -> SyncConfigBuilder:
        """
        Schedule a Classification of the Objects within Weaviate.

        Returns
        -------
        weaviate.classification.config_builder.SyncConfigBuilder
            A SyncConfigBuilder that should be configured to the desired
            classification task
        """

        return SyncConfigBuilder(self._requests, self)

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

        Raises
        ------
        ValueError
            If not a proper uuid.
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path = f'/classifications/{get_valid_uuid(classification_uuid)}'

        try:
            response = self._requests.get(
                path=path,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Classification status could not be retrieved due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return response.json()
        raise UnsuccessfulStatusCodeError(
            "Get classification status.",
            status_code=response.status_code,
            response_message=response.text,
        )

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

        return self._check_status(classification_uuid, 'completed')

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

        return self._check_status(classification_uuid, "failed")

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

        return self._check_status(classification_uuid, "running")

    def _check_status(self, classification_uuid: str, status: str) -> bool:
        """
        Check for a status of a classification.

        Parameters
        ----------
        classification_uuid : str
            Identifier of the classification.
        status : str
            Status to check for.

        Returns
        -------
        bool
            True if 'status' is satisfied, False otherwise.
        """

        response = self.get(classification_uuid)

        if response["status"] == status:
            return True
        return False
