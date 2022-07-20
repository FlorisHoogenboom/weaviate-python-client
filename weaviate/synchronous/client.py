"""
Client class definition.
"""
from typing import Optional, Union
import ujson
from weaviate.base.connection import Connection, ClientTimeout
from weaviate.auth import AuthCredentials
from weaviate.exceptions import UnsuccessfulStatusCodeError, RequestsConnectionError
from .requests import Requests
from .classification import Classification
from .schema import Schema
from .contextionary import Contextionary
from .batch import Batch
from .data import DataObject
from .gql import Query


class Client:
    """
    A python native Client class that encapsulates Weaviate functionalities in one object.
    A Client instance creates all the needed objects to interact with Weaviate, and
    connects all of them to the same Weaviate instance. See below the Attributes of the
    Client instance. For the per attribute functionality see that attribute's
    documentation.

    Attributes
    ----------
    classification : weaviate.classification.Classification
        A Classification object instance connected to the same Weaviate instance as the
        Client.
    schema : weaviate.schema.Schema
        A Schema object instance connected to the same Weaviate instance as the Client.
    contextionary : weaviate.contextionary.Contextionary
        A Contextionary object instance connected to the same Weaviate instance as the
        Client.
    batch : weaviate.batch.Batch
        A Batch object instance connected to the same Weaviate instance as the Client.
    data_object : weaviate.date.DataObject
        A DataObject object instance connected to the same Weaviate instance as the Client.
    query : weaviate.gql.Query
        A Query object instance connected to the same Weaviate instance as the Client.
    """

    def __init__(self,
            url: str='http://localhost:8080',
            auth_client_secret: Optional[AuthCredentials]=None,
            timeout_config: ClientTimeout=ClientTimeout(20),
            proxies: Union[dict, str, None]=None,
            trust_env: bool=False,
            additional_headers: Optional[dict]=None,
        ):
        """
        Initialize a Client class instance.

        Parameters
        ----------
        url : str
            URL to a running Weaviate instance.
            By default 'http://localhost:8080'.
        auth_client_secret : weaviate.auth.AuthCredentials or None, optional
            User login credentials for the Weaviate instance at 'url', by default None.
        timeout_config : weaviate.ClientTimeout, optional
            Set the timeout configuration for all requests to the Weaviate server.
            By default weaviate.ClientTimeout(20).
        proxies : dict, str or None, optional
            Proxies to be used for requests. Are used by both 'requests' and 'aiohttp'. Can be
            passed as a dict ('requests' format:
            https://docs.python-requests.org/en/stable/user/advanced/#proxies), str (HTTP/HTTPS
            protocols are going to use this proxy) or None.
            By default None.
        trust_env : bool, optional
            Whether to read proxies from the ENV variables: (HTTP_PROXY or http_proxy, HTTPS_PROXY
            or https_proxy). By default False.
            NOTE: 'proxies' has priority over 'trust_env', i.e. if 'proxies' is NOT None,
            'trust_env' is ignored.
        additional_headers : dict or None
            Additional headers to include in the requests, used to set OpenAI key. OpenAI key looks
            like this: {'X-OpenAI-Api-Key': 'KEY'}

        Examples
        --------
        Without Auth.

        >>> client = Client(
        ...     url = 'http://localhost:8080'
        ... )

        With Auth.

        >>> my_credentials = weaviate.auth.AuthClientPassword(USER_NAME, MY_PASSWORD)
        >>> client = Client(
        ...     url = 'http://localhost:8080',
        ...     auth_client_secret = my_credentials
        ... )

        Raises
        ------
        TypeError
            If arguments are of a wrong data type.
        """

        if not isinstance(url, str):
            raise TypeError(
                f"'url' must be of type str. Given type: {type(url)}"
            )

        self._connection = Connection(
            url=url.strip('/'),
            auth_client_secret=auth_client_secret,
            timeout_config=timeout_config,
            proxies=proxies,
            trust_env=trust_env,
            include_aiohttp=False,
            additional_headers=additional_headers,
        )
        self._requests = Requests(self._connection)

        self.classification = Classification(self._requests)
        self.schema = Schema(self._requests)
        self.batch = Batch(self._requests)
        self.data_object = DataObject(self._requests)
        self.query = Query(self._requests)
        self.contextionary = Contextionary(self._requests)

    def is_ready(self) -> bool:
        """
        Ping Weaviate's ready state

        Returns
        -------
        bool
            True if Weaviate is ready to accept requests,
            False otherwise.
        """

        try:
            response = self._requests.get(
                path="/.well-known/ready",
            )
        except RequestsConnectionError:
            return False
        if response.status_code == 200:
            return True
        return False

    def is_live(self) -> bool:
        """
        Ping Weaviate's live state.

        Returns
        --------
        bool
            True if weaviate is live and should not be killed,
            False otherwise.
        """

        try:
            response = self._requests.get(
                path="/.well-known/live",
            )
        except RequestsConnectionError:
            return False
        if response.status_code == 200:
            return True
        return False

    def get_meta(self) -> dict:
        """
        Get the meta endpoint description of weaviate.

        Returns
        -------
        dict
            The dict describing the weaviate configuration.

        Raises
        ------
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        try:
            response = self._requests.get(
                path="/meta",
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Could not get meta data due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return ujson.loads(response.content)
        raise UnsuccessfulStatusCodeError(
            "Meta endpoint.",
            status_code=response.status_code,
            response_message=response.text,
        )

    def get_open_id_configuration(self) -> Optional[dict]:
        """
        Get the openid-configuration.

        Returns
        -------
        dict
            The configuration or None if not configured.

        Raises
        ------
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        try:
            response = self._requests.get(
                path="/.well-known/openid-configuration",
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Could not get openid-configuration due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return ujson.loads(response.content)
        if response.status_code == 404:
            return None
        raise UnsuccessfulStatusCodeError(
            "OpenID configuration endpoint",
            status_code=response.status_code,
            response_message=response.text,
        )

    @property
    def timeout_config(self) -> ClientTimeout:
        """
        Getter/Setter for 'timeout_config'.
        Parameters
        ----------
        timeout_config : weaviate.ClientTimeout
            For Getter only: Set the timeout configuration for all requests to the Weaviate server.

        Returns
        -------
        weaviate.ClientTimeout
            For Getter only: Timeout configuration used for both 'requests' and 'aiohttp' requests.
        """

        return self._connection.timeout_config

    @timeout_config.setter
    def timeout_config(self, timeout_config: ClientTimeout):
        """
        Setter for `timeout_config`. (docstring should be only in the Getter)
        """

        self._connection.timeout_config = timeout_config

    def __repr__(self) -> str:

        return f'Client(url={self._connection.base_url})'
