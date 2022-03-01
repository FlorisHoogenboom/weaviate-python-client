"""
Connection class definition.
"""
import asyncio
import time
import datetime
from typing import Optional, Union
from numbers import Real
from aiohttp.client import ClientSession, _RequestContextManager, ClientTimeout
from weaviate.exceptions import (
    AuthenticationError,
    WeaviateConnectionError,
    UnsuccessfulStatusCodeError,
)
from weaviate.auth import AuthCredentials



class Connection:
    """
    Connection class used to communicate to a Weaviate instance. Has all needed RESTful API
    implementations. If Authentication is used, it automatically gets a new token in case it
    expired.
    """

    def __init__(self,
            url: str,
            auth_client_secret: Optional[AuthCredentials] = None,
            timeout_config: Optional[Union[Real, ClientTimeout]] = 20,
            session_proxy: Optional[str] = None,
            trust_env: bool = False,
        ):
        """
        Initialize a Connection class instance.

        Parameters
        ----------
        url : str
            URL to a running weaviate instance.
        auth_client_secret : weaviate.auth.AuthCredentials or None, optional
            User login credentials to a weaviate instance, by default None
        timeout_config : float/int, aiohttp.ClientTimeout or None, optional
            Set the timeout configuration for all requests to Weaviate server. It can be
            aiohttp.ClientTimeout or a float/int (used as 'total' for aiohttp.ClientTimeout).
            None or 0 disables the timeout check.
            By default 20.
        session_proxy : str or None, optional
            The proxy to use for the requests Session. (Can be read from environment variables see
            'trust_env' argument description).
            By default None.
        trust_env : bool, optional
            Get proxies information from HTTP_PROXY/HTTPS_PROXY environment variables if the
            parameter is True. Get proxy credentials from ~/.netrc file if present.
            By default False.

        Raises
        ------
        ValueError
            If no authentication credentials provided but the Weaviate server has an OpenID
            configured.
        """

        if not (auth_client_secret is None or isinstance(auth_client_secret, AuthCredentials)):
            raise TypeError(
                "'auth_client_secret' must be of type 'AuthCredentials' or None. "
                f"Given type: {type(auth_client_secret)}."
            )
        
        if not (session_proxy is None or isinstance(session_proxy, str)):
            raise TypeError(
                f"'session_proxy' must be of type 'str' or None. Given type: {type(session_proxy)}."
            )

        self._api_version_path = '/v1'
        self._session = ClientSession(trust_env=trust_env)
        self.url = url
        self.timeout_config = timeout_config
        self._auth_expires = 0  # unix time when auth expires
        self._auth_bearer = None
        self._auth_client_secret = auth_client_secret
        self._is_authentication_required = False
        self._proxy = {}

        if session_proxy:
            self._proxy['proxy'] = session_proxy

        run_sync(self._log_in)()

    async def _log_in(self) -> None:
        """
        Log in to the Weaviate server only if the Weaviate server has an OpenID configured.

        Raises
        ------
        ValueError
            If no authentication credentials provided but the Weaviate server has an OpenID
            configured.
        """

        response = await self._session.get(
            self.url + self._api_version_path + "/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=self._timeout_config,
        )
        response.close()

        if response.status == 200:
            if self._auth_client_secret is None:
                raise ValueError(
                    f"No login credentials provided. The weaviate instance at {self.url} requires "
                    "login credential, use argument 'auth_client_secret'."
                )
            self._is_authentication_required = True
            self._refresh_authentication()
        elif response.status != 404:
            raise UnsuccessfulStatusCodeError(
                "Failed to get OpenID Configuration.",
                response=response,
            )

    def __del__(self):
        """
        Destructor for Connection class instance.
        """

        run_sync(self._session.close)()

    # Requests a new bearer
    async def _refresh_authentication(self) -> None:
        """
        Request a new bearer.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If cannot connect to weaviate.
        weaviate.exceptions.AuthenticationError
            If cannot authenticate, http status code unsuccessful.
        """

        if self._auth_expires < _get_epoch_time():
            # collect data for the request
            try:
                response = await self._session.get(
                    self.url + self._api_version_path + "/.well-known/openid-configuration",
                    headers={"content-type": "application/json"},
                    timeout=self._timeout_config,
                )
            except WeaviateConnectionError as error:
                raise WeaviateConnectionError("Cannot connect to weaviate.") from error
            if response.status != 200:
                response.close()
                raise AuthenticationError("Cannot authenticate.", response=response)

            response_json = await response.json()
            response.close()

            await self._set_bearer(
                client_id=response_json['clientId'],
                href=response_json['href'],
            )

    async def _set_bearer(self, client_id: str, href: str) -> None:
        """
        Set bearer for a refreshed authentication.

        Parameters
        ----------
        client_id : str
            The client ID of the OpenID Connect.
        href : str
            The URL of the OpenID Connect issuer.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If cannot connect to the third party authentication service.
        weaviate.exceptions.AuthenticationError
            If status not OK in connection to the third party authentication service.
        weaviate.exceptions.AuthenticationError
            If the grant_types supported by the thirdparty authentication service are insufficient.
        weaviate.exceptions.AuthenticationError
            If authentication access denied.
        """

        # request additional information
        try:
            response_third_part = await self._session.get(
                url=href,
                headers={"content-type": "application/json"},
                timeout=self._timeout_config,
            )
        except WeaviateConnectionError as error:
            raise WeaviateConnectionError(
                "Can't connect to the third party authentication service. "
                "Check that it is running."
            ) from error

        if response_third_part.status != 200:
            response_third_part.close()
            raise AuthenticationError(
                "Status not OK in connection to the third party authentication service.",
                response=response_third_part,
            )

        response_third_part_json = await response_third_part.json()
        response_third_part.close()

        # Validate third part auth info
        if 'client_credentials' not in response_third_part_json['grant_types_supported']:
            raise AuthenticationError(
                "The grant_types supported by the thirdparty authentication service are "
                "insufficient. Please add 'client_credentials'."
            )

        request_body = self._auth_client_secret.get_credentials()
        request_body["client_id"] = client_id

        response = await self._session.post(
            url=response_third_part_json['token_endpoint'],
            json=request_body,
            timeout=self._timeout_config
        )

        if response.status == 401:
            response.close()
            raise AuthenticationError(
                "Authentication access denied. Are the credentials correct?"
            )
        response_json = await response.json()
        response.close()

        self._auth_bearer = response_json['access_token']
        # -2 for some lagtime
        self._auth_expires = int(_get_epoch_time() + response_json['expires_in'] - 2)

    async def _get_request_header(self) -> dict:
        """
        Returns the correct headers for a request.

        Returns
        -------
        dict
            Request header as a dict.
        """

        header = {"content-type": "application/json"}

        if self._is_authentication_required:
            await self._refresh_authentication()
            header["Authorization"] = "Bearer " + self._auth_bearer

        return header

    async def delete(self, path: str, data_json: Optional[dict]=None) -> _RequestContextManager:
        """
        Make a DELETE request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.
        data_json : dict or None, optional
            JSON formatted object is used as payload for DELETE request. By default None.

        Returns
        -------
        aiohttp.client._RequestContextManager
            The opened request context manager.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If the DELETE request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.delete(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
        )

    async def patch(self, path: str, data_json: dict) -> _RequestContextManager:
        """
        Make a PATCH request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.
        data_json : dict
            JSON formatted object is used as payload for PATCH request.

        Returns
        -------
        aiohttp.client._RequestContextManager
            The opened request context manager.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If the PATCH request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.patch(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
        )

    async def post(self, path: str, data_json: Optional[dict]=None) -> _RequestContextManager:
        """
        Make a POST request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.
        data_json : dict or None, optional
            JSON formatted object is used as payload for POST request. By default None.

        Returns
        -------
        aiohttp.client._RequestContextManager
            The opened request context manager.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If the POST request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.post(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
        )

    async def put(self, path: str, data_json: dict) -> _RequestContextManager:
        """
        Make a PUT request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.
        data_json : dict
            JSON formatted object is used as payload for PUT request.

        Returns
        -------
        aiohttp.client._RequestContextManager
            The opened request context manager.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If the PUT request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.put(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
        )

    async def get(self, path: str, params: Optional[dict]=None) -> _RequestContextManager:
        """
        Make a GET request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.
        params : dict or None, optional
            Additional request parameters, by default None

        Returns
        -------
        aiohttp.client._RequestContextManager
            The opened request context manager.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If the GET request could not be made.
        """

        if params is None:
            params = {}
        request_url = self.url + self._api_version_path + path

        return self._session.get(
            url=request_url,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
            params=params,
        )

    async def head(self, path: str) -> _RequestContextManager:
        """
        Make a HEAD request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.

        Returns
        -------
        aiohttp.client._RequestContextManager
            The opened request context manager.

        Raises
        ------
        aiohttp.client.ClientConnectorError
            If the HEAD request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.head(
            url=request_url,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
        )

    @property
    def timeout_config(self) -> Optional[Real]:
        """
        Getter/Setter for 'timeout_config'.

        Parameters
        ----------
        timeout_config : float/int, aiohttp.ClientTimeout or None
            Set the timeout configuration for all requests to Weaviate server. It can be a float or
            int. None or 0 disables the timeout check.

        Returns
        -------
        aiohttp.ClientTimeout
            For Getter only: HTTP Client requests Timeout configuration.
        """

        return self._timeout_config

    @timeout_config.setter
    def timeout_config(self, timeout_config: Optional[Union[Real, ClientTimeout]]):
        """
        Setter for 'timeout_config'. (docstring should be only in the Getter)
        """

        self._timeout_config = _get_valid_timeout_config(timeout_config)


def _get_valid_timeout_config(
        timeout_config: Optional[Union[Real, ClientTimeout]],
    ) -> ClientTimeout:
    """
    Validate and return Timeout configuration.

    Parameters
    ----------
    timeout_config : Real, aiohttp.ClientTimeout or None
            The timeout configuration to check for correct type and value.

    Returns
    -------
    float or None
        Validated 'timeout_config'.

    Raises
    ------
    TypeError
        If 'timeout_config' is not of type Real, aiohttp.ClientTimeout or NoneType.
    ValueError
        If 'timeout_config' is a negative number.
    """

    if isinstance(timeout_config, ClientTimeout):
        return timeout_config

    if timeout_config is None:
        return ClientTimeout(total=None)

    if not isinstance(timeout_config, Real) or isinstance(timeout_config, bool):
        raise TypeError(
            "'timeout_config' must be of type float/int, aiohttp.ClientTimeout or be None. "
            f"Given type: {type(timeout_config)}"
        )
    if timeout_config < 0.0:
        raise ValueError(
            f"'timeout_config' cannot be a negative number. Given value: {timeout_config}."
        )
    return ClientTimeout(total=timeout_config)


def _get_epoch_time() -> int:
    """
    Get the current epoch time as an integer.

    Returns
    -------
    int
        Current epoch time.
    """

    dts = datetime.datetime.utcnow()
    return round(time.mktime(dts.timetuple()) + dts.microsecond / 1e6)
