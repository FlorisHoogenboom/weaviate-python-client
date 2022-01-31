"""
Connection class definition.
"""
import os
import time
import datetime
from typing import Tuple, Optional, Union
from numbers import Real
import requests
from weaviate.exceptions import (
    AuthenticationError,
    WeaviateConnectionError,
    UnsuccessfulStatusCodeError,
)
from weaviate.auth import AuthCredentials
from weaviate.util import _get_valid_timeout_config


class Connection:
    """
    Connection class used to communicate to a Weaviate instance. Has all needed RESTful API
    implementations. If Authentication is used, it automatically gets a new token in case it
    expired.
    """

    def __init__(self,
            url: str,
            auth_client_secret: Optional[AuthCredentials]=None,
            timeout_config: Optional[Union[Tuple[Real, Real], Real]]=20,
            session_proxies: Optional[dict]=None,
        ):
        """
        Initialize a Connection class instance.

        Parameters
        ----------
        url : str
            URL to a running weaviate instance.
        auth_client_secret : weaviate.auth.AuthCredentials or None, optional
            User login credentials to a weaviate instance, by default None
        timeout_config : tuple(Real, Real), Real or None, optional
            Set the timeout configuration for all requests to the Weaviate server. It can be a
            real number or, a tuple of two real numbers: (connect timeout, read timeout).
            If only one real number is passed then both connect and read timeout will be set to
            that value, by default 20.
        session_proxies : dict or None, optional
            The HTTP and/or HTTPS proxies to use for the requests Session. Can be passed as a dict
            or None to read from the ENV variables: (HTTP_PROXY or http_proxy, HTTPS_PROXY or
            https_proxy).

        Raises
        ------
        ValueError
            If no authentication credentials provided but the Weaviate server has an OpenID
            configured.
        """

        if not (isinstance(auth_client_secret, AuthCredentials) or auth_client_secret is None):
            raise TypeError(
                "The 'auth_client_secret' should be of type AuthCredentials or None. "
                f"Given type: {type(auth_client_secret)}."
            )

        self._api_version_path = '/v1'
        self._session = requests.Session()
        self.url = url
        self.timeout_config = timeout_config
        self._auth_expires = 0  # unix time when auth expires
        self._auth_bearer = None
        self._auth_client_secret = auth_client_secret
        self._is_authentication_required = False

        self._set_session_proxies(proxies=session_proxies)
        self._log_in()

    def _set_session_proxies(self, proxies: Optional[dict]) -> None:
        """
        Set Session proxies.

        Parameters
        ----------
        proxies : dict or None
            The HTTP/HTTPS proxies.
        """

        if proxies is not None:
            self._session.proxies = proxies
            return

        http_proxy = (os.environ.get('http_proxy'), os.environ.get('HTTP_PROXY'))
        https_proxy = (os.environ.get('https_proxy'), os.environ.get('HTTPS_PROXY'))

        if not any(http_proxy + https_proxy):
            return

        proxies = {}
        if any(http_proxy):
            proxies['http'] = http_proxy[0] if http_proxy[0] else http_proxy[1]
        if any(https_proxy):
            proxies['https'] = https_proxy[0] if https_proxy[0] else https_proxy[1]

        self._session.proxies = proxies


    def _log_in(self) -> None:
        """
        Log in to the Weaviate server only if the Weaviate server has an OpenID configured.

        Raises
        ------
        ValueError
            If no authentication credentials provided but the Weaviate server has an OpenID
            configured.
        """

        response = self._session.get(
            self.url + self._api_version_path + "/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=self._timeout_config
        )

        if response.status_code == 200:
            if self._auth_client_secret is None:
                raise ValueError(
                    f"No login credentials provided. The weaviate instance at {self.url} requires "
                    "login credential, use argument 'auth_client_secret'."
                )
            self._is_authentication_required = True
            self._refresh_authentication()
        elif response.status_code != 404:
            raise UnsuccessfulStatusCodeError(
                "Failed to get OpenID Configuration!",
                response=response,
            )

    def __del__(self):
        """
        Destructor for Connection class instance.
        """

        self._session.close()

    # Requests a new bearer
    def _refresh_authentication(self) -> None:
        """
        Request a new bearer.

        Raises
        ------
        requests.exception.ConnectionError
            If cannot connect to weaviate.
        weaviate.exceptions.AuthenticationError
            If cannot authenticate, http status code unsuccessful.
        """

        if self._auth_expires < _get_epoch_time():
            # collect data for the request
            try:
                response = self._session.get(
                    self.url + self._api_version_path + "/.well-known/openid-configuration",
                    headers={"content-type": "application/json"},
                    timeout=(30, 45)
                    )
            except WeaviateConnectionError as error:
                raise WeaviateConnectionError("Cannot connect to weaviate.") from error
            if response.status_code != 200:
                raise AuthenticationError("Cannot authenticate!", response=response)

            # Set the client ID
            client_id = response.json()['clientId']

            self._set_bearer(client_id=client_id, href=response.json()['href'])

    def _set_bearer(self, client_id: str, href: str) -> None:
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
        requests.exception.ConnectionError
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
            response_third_part = requests.get(
                href,
                headers={"content-type": "application/json"},
                timeout=(30, 45)
                )
        except WeaviateConnectionError as error:
            raise WeaviateConnectionError(
                "Can't connect to the third party authentication service. "
                "Check that it is running."
            ) from error

        if response_third_part.status_code != 200:
            raise AuthenticationError(
                "Status not OK in connection to the third party authentication service.",
                response=response_third_part,
            )

        # Validate third part auth info
        if 'client_credentials' not in response_third_part.json()['grant_types_supported']:
            raise AuthenticationError(
                "The grant_types supported by the thirdparty authentication service are "
                "insufficient. Please add 'client_credentials'."
            )

        request_body = self._auth_client_secret.get_credentials()
        request_body["client_id"] = client_id

        response = requests.post(
            response_third_part.json()['token_endpoint'],
            request_body,
            timeout=(30, 45)
            )

        if response.status_code == 401:
            raise AuthenticationError(
                "Authentication access denied. Are the credentials correct?"
            )
        self._auth_bearer = response.json()['access_token']
        # -2 for some lagtime
        self._auth_expires = int(_get_epoch_time() + response.json()['expires_in'] - 2)

    def _get_request_header(self) -> dict:
        """
        Returns the correct headers for a request.

        Returns
        -------
        dict
            Request header as a dict.
        """

        header = {"content-type": "application/json"}

        if self._is_authentication_required:
            self._refresh_authentication()
            header["Authorization"] = "Bearer " + self._auth_bearer

        return header

    def delete(self, path: str, data_json: Optional[dict]=None) -> requests.Response:
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
        requests.Response
            The request response.

        Raises
        ------
        requests.ConnectionError
            If the DELETE request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.delete(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config
        )

    def patch(self, path: str, data_json: dict) -> requests.Response:
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
        requests.Response
            The request response.

        Raises
        ------
        requests.ConnectionError
            If the PATCH request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.patch(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config
        )

    def post(self, path: str, data_json: Optional[dict]=None) -> requests.Response:
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
        requests.Response
            The request response.

        Raises
        ------
        requests.ConnectionError
            If the POST request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.post(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config
        )

    def put(self, path: str, data_json: dict) -> requests.Response:
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
        requests.Response
            The request response.

        Raises
        ------
        requests.ConnectionError
            If the PUT request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.put(
            url=request_url,
            json=data_json,
            headers=self._get_request_header(),
            timeout=self._timeout_config
        )

    def get(self, path: str, params: Optional[dict]=None) -> requests.Response:
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
        requests.Response
            The request response.

        Raises
        ------
        requests.ConnectionError
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

    def head(self, path: str) -> requests.Response:
        """
        Make a HEAD request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.

        Returns
        -------
        requests.Response
            The request response.

        Raises
        ------
        requests.ConnectionError
            If the HEAD request could not be made.
        """

        request_url = self.url + self._api_version_path + path

        return self._session.head(
            url=request_url,
            headers=self._get_request_header(),
            timeout=self._timeout_config,
        )

    @property
    def timeout_config(self) -> Optional[Tuple[Real, Real]]:
        """
        Getter/Setter for 'timeout_config'.

        Parameters
        ----------
        timeout_config : tuple(Real, Real) or Real or None
            For Getter only: Set the timeout configuration for all requests to the Weaviate server.
            It can be None, a real number or a tuple of two real numbers:
                    (connect timeout, read timeout).
            If only one real number is passed then both connect and read timeout will be set to
            that value.
            If None then the it will wait forever, until the server responds (NOT recommended).

        Returns
        -------
        Tuple[Real, Real] or None
            For Getter only: Requests Timeout configuration.
        """

        return self._timeout_config

    @timeout_config.setter
    def timeout_config(self, timeout_config: Optional[Union[Tuple[Real, Real], Real]]):
        """
        Setter for 'timeout_config'. (docstring should be only in the Getter)
        """

        self._timeout_config = _get_valid_timeout_config(timeout_config)

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
