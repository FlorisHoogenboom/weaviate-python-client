"""
Module to connect and authenticate (if needed) to a Weaviate instance.
Contains ClientTimeout and Proxies definitions that are used for both synchronous
requests ('requests' library) and asynchronous one ('aiohttp' library).
"""
import os
import time
import datetime
from typing import Optional, Union, Any
from numbers import Real
import requests
import aiohttp
from weaviate.exceptions import (
    AuthenticationError,
    RequestsConnectionError,
    UnsuccessfulStatusCodeError,
)
from weaviate.auth import AuthCredentials


class Proxies:
    """
    Proxies class used as a wrapper for both the 'requests' and the 'aiohttp' libraries.
    """

    def __init__(self,
            proxies: Union[dict, str, None],
            trust_env: bool=False,
            include_aiohttp: bool=False,
        ):
        """
        Initialize a Proxies class instance.

        Parameters
        ----------
        proxies : Union[dict, str, None]
            The proxies to be used.
        trust_env : bool, optional
            Whether to read proxies form ENV VARs. By default False.
        include_aiohttp : bool, optional
            Whether to create proxy compatible with 'aiohttp' library, by default False.
            (This should be true only for the AsyncClient).
        """

        self._trust_env = trust_env
        self._proxies_requests = _get_proxies_requests(proxies=proxies, trust_env=trust_env)
        self._include_aiohttp = include_aiohttp
        if include_aiohttp:
            self._proxy_aiohttp = _get_proxy_aiohttp(proxy=proxies)

    def get_proxies_requests(self) -> dict:
        """
        Get proxies compatible with 'requests' library.

        Returns
        -------
        dict
            The proxies compatible with 'requests' library.
        """

        return self._proxies_requests

    def get_proxy_aiohttp(self) -> Optional[str]:
        """
        Get proxy compatible with 'aiohttp' library.

        Returns
        -------
        Optional[str]
            The proxy compatible with 'aiohttp' library.
        """

        if self._include_aiohttp:
            return self._proxies_requests
        raise AttributeError(
            "The 'aiohttp' proxy attribute is not set. This means that method is NOT called "
            "the AsyncClient. If it is, please report the Issue."
        )


class ClientTimeout:
    """
    ClientTimeout class used as a wrapper for both the 'requests' and the 'aiohttp' libraries.
    """

    def __init__(self, timeout_config: Any):
        """
        Initialize a ClientTimeout class instance.

        Parameters
        ----------
        timeout_config : Any, optional
            Timeout configurations to use for all the REST requests.
            It should be either 'requests' or 'aiohttp' library compatible.
            aiohttp: https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts
            requests: https://docs.python-requests.org/en/stable/user/advanced/#timeouts

        Raises
        ------
        ValueError
            If 'timeout_config' is a tuple/list and has a length different than 2.
        TypeError
            If 'timeout_config' is not None or of type: tuple, list, Real or aiohttp.ClientTimeout.
        """


        if timeout_config is None or isinstance(timeout_config, Real):
            self._requests_timeout = timeout_config
            self._aiohttp_timeout = aiohttp.ClientTimeout(
                total=timeout_config,
            )
        elif isinstance(timeout_config, (tuple, list)):
            if len(timeout_config) != 2:
                raise ValueError(
                    "If 'timeout_config' is a tuple/list, it must be of length two. "
                    f"Given length: {len(timeout_config)}."
                )
            self._requests_timeout = (timeout_config[0], timeout_config[1])
            self._aiohttp_timeout = aiohttp.ClientTimeout(
                total=sum(timeout_config),
                connect=timeout_config[0],
            )

        elif isinstance(timeout_config, aiohttp.ClientTimeout):
            self._requests_timeout = (
                timeout_config.connect if timeout_config.connect else None,
                timeout_config.total if timeout_config.total else None,
            )
            self._aiohttp_timeout = timeout_config
        else:
            raise TypeError(
                f"Unsupported type {type(timeout_config)} for 'timeout_config'. "
                "It can be one of these: None, tuple/list or aiohttp.ClientTimeout."
            )

    def get_timeout_requests(self) -> Union[tuple, Real, None]:
        """
        Get timeout configuration compatible with 'requests' library.

        Returns
        -------
        Union[tuple, Real, None]
        """

        return self._requests_timeout

    def get_timeout_aiohttp(self) -> aiohttp.ClientTimeout:
        """
        Get timeout configuration compatible with 'aiohttp' library.

        Returns
        -------
        aiohttp.ClientTimeout
        """

        return self._aiohttp_timeout


class Connection:
    """
    Connection class used to connect and authenticate to a Weaviate instance. If Authentication
    is used, it automatically gets a new token in case it expired.
    """

    def __init__(self,
            url: str,
            auth_client_secret: Optional[AuthCredentials]=None,
            timeout_config: ClientTimeout=ClientTimeout(20),
            proxies: Union[dict, str, None]=None,
            trust_env: bool=False,
            include_aiohttp: bool=False,
        ):
        """
        Initialize a Connection class instance.

        Parameters
        ----------
        url : str
            URL to a running Weaviate instance.
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
        include_aiohttp : bool, optional
            Whether to create proxy compatible with 'aiohttp' library, by default False.
            (This should be true only for the AsyncClient).

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

        self._api_version_path = '/v1'
        self._url = url
        self._timeout_config = timeout_config
        self._auth_expires = 0  # unix time when auth expires
        self._auth_bearer = None
        self._auth_client_secret = auth_client_secret
        self._is_authentication_required = False
        self._proxies = Proxies(
            proxies=proxies,
            trust_env=trust_env,
            include_aiohttp=include_aiohttp,
        )

        self.log_in()

    def log_in(self) -> None:
        """
        Log in to the Weaviate server only if the Weaviate server has an OpenID configured.

        Raises
        ------
        ValueError
            If no authentication credentials provided but the Weaviate server has an OpenID
            configured.
        """

        response = requests.get(
            self._url + self._api_version_path + "/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=self._timeout_config.get_timeout_requests(),
            proxies=self._proxies.get_proxies_requests(),
        )

        if response.status_code == 200:
            if self._auth_client_secret is None:
                raise ValueError(
                    f"No login credentials provided. The weaviate instance at {self._url} requires "
                    "login credential, use argument 'auth_client_secret'."
                )
            self._is_authentication_required = True
            self.refresh_authentication()
        elif response.status_code != 404:
            raise UnsuccessfulStatusCodeError(
                "Failed to get OpenID Configuration.",
                status_code=response.status_code,
                response_message=response.text,
            )

    def refresh_authentication(self) -> None:
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
                response = requests.get(
                    self._url + self._api_version_path + "/.well-known/openid-configuration",
                    headers={"content-type": "application/json"},
                    timeout=self._timeout_config.get_timeout_requests(),
                    proxies=self._proxies.get_proxies_requests(),
                )
            except RequestsConnectionError as error:
                raise RequestsConnectionError("Cannot connect to weaviate.") from error
            if response.status_code != 200:
                raise AuthenticationError("Cannot authenticate.", response=response)

            response_json = response.json()

            self.set_bearer(
                client_id=response_json['clientId'],
                href=response_json['href'],
            )

    def set_bearer(self, client_id: str, href: str) -> None:
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
            response_third_part = requests.get(
                url=href,
                headers={"content-type": "application/json"},
                timeout=self._timeout_config.get_timeout_requests(),
                proxies=self._proxies.get_proxies_requests(),
            )
        except RequestsConnectionError as error:
            raise RequestsConnectionError(
                "Can't connect to the third party authentication service. "
                "Check that it is running."
            ) from error

        if response_third_part.status_code != 200:
            raise AuthenticationError(
                "Status not OK in connection to the third party authentication service.",
                response=response_third_part,
            )

        response_third_part_json = response_third_part.json()

        # Validate third part auth info
        if 'client_credentials' not in response_third_part_json['grant_types_supported']:
            raise AuthenticationError(
                "The grant_types supported by the thirdparty authentication service are "
                "insufficient. Please add 'client_credentials'."
            )

        request_body = self._auth_client_secret.get_credentials()
        request_body["client_id"] = client_id

        response = requests.post(
            url=response_third_part_json['token_endpoint'],
            json=request_body,
            timeout=self._timeout_config.get_timeout_requests(),
            proxies=self._proxies.get_proxies_requests(),
        )

        if response.status_code == 401:
            raise AuthenticationError(
                "Authentication access denied. Are the credentials correct?"
            )

        response_json = response.json()
        self._auth_bearer = response_json['access_token']
        # -2 for some lagtime
        self._auth_expires = int(_get_epoch_time() + response_json['expires_in'] - 2)

    def get_request_header(self) -> dict:
        """
        Returns the correct headers for a request.

        Returns
        -------
        dict
            Request header as a dict.
        """

        header = {"content-type": "application/json"}

        if self._is_authentication_required:
            self.refresh_authentication()
            header["Authorization"] = "Bearer " + self._auth_bearer

        return header

    def get_url(self, path: str) -> str:
        """
        Construct and return the full weaviate URL to the resource 'path'.

        Parameters
        ----------
        path : str
            The relative path to the Weaviate resource, e.g. '/objects'.

        Returns
        -------
        str
            The URL to the Weaviate resource.
        """

        return self._url + self._api_version_path + path

    @property
    def base_url(self) -> str:
        """
        Get the set Weaviate URL.

        Returns
        -------
        str
            The set Weaviate URL.
        """

        return self._url

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

        return self._timeout_config

    @timeout_config.setter
    def timeout_config(self, timeout_config: ClientTimeout):
        """
        Setter for 'timeout_config'. (docstring should be only in the Getter)
        """

        if not isinstance(timeout_config, ClientTimeout):
            raise TypeError(
                "'timeout_config' must be of type weaviate.ClientTimeout. "
                f"Given type: {type(timeout_config)}."
            )

        self._timeout_config = timeout_config

    @property
    def proxies(self) -> Proxies:
        """
        Get Proxies instance, so it can be used in the Requests/AsyncRequests classes.

        Returns
        -------
        weaviate.connect.Proxies
            Proxies configuration used for both 'requests' and 'aiohttp' requests.
        """

        return self._proxies


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


def _get_proxies_requests(proxies: Union[dict, str, None]=None, trust_env: bool=False) -> dict:
    """
    Get proxies as dict, compatible with 'requests' library.
    NOTE: 'proxies' has priority over 'trust_env', i.e. if 'proxies' is NOT None, 'trust_env'
    is ignored.

    Parameters
    ----------
    proxies : dict, str or None
        The proxies to use for requests. If it is a dict it should follow 'requests' library
        format (https://docs.python-requests.org/en/stable/user/advanced/#proxies). If it is
        a URL (str), a dict will be constructed with both 'http' and 'https' pointing to that
        URL. If None, no proxies will be used.
    trust_env : bool, optional
        If True, the proxies will be read from ENV VARs (case insensitive):
            HTTP_PROXY/HTTPS_PROXY.
        By default False.
        NOTE: It is ignored if 'proxies' is NOT None.

    Returns
    -------
    dict
        A dictionary with proxies, either set from 'proxies' or read from ENV VARs.
    """

    if proxies is not None:
        if isinstance(proxies, str):
            return {
                'http': proxies,
                'https': proxies,
            }
        if isinstance(proxies, dict):
            return proxies
        raise TypeError(
            "If 'proxies' is not None, it must be of type dict or str. "
            f"Given type: {type(proxies)}."
        )

    if not trust_env:
        return {}

    http_proxy = (os.environ.get('HTTP_PROXY'), os.environ.get('http_proxy'))
    https_proxy = (os.environ.get('HTTPS_PROXY'), os.environ.get('https_proxy'))

    if not any(http_proxy + https_proxy):
        return {}

    proxies = {}
    if any(http_proxy):
        proxies['http'] = http_proxy[0] if http_proxy[0] else http_proxy[1]
    if any(https_proxy):
        proxies['https'] = https_proxy[0] if https_proxy[0] else https_proxy[1]

    return proxies


def _get_proxy_aiohttp(proxy: Union[dict, str, None]) -> Optional[str]:
    """
    Get proxy as str or None, compatible with 'aiohttp' library.

    Parameters
    ----------
    proxy : dict, str or None
        The proxy to use for requests. If it is a dict then the 'https'/'http' key-value is
        returned (otherwise an exception is raised). If it is a str (URL), it is used as proxy.
        If None, None is returned.

    Returns
    -------
    Optional[str]
        The proxy as str or None.

    Raises
    ------
    TypeError
        If 'proxy' is not of type: NoneType, dict or str.
    ValueError
        If 'proxy' is of type dict and does not have the key 'https' or 'http'.
    """

    if not proxy:
        return None

    if isinstance(proxy, str):
        return proxy

    if not isinstance(proxy, dict):
        raise TypeError(
            "If 'proxy' is not None, it must be of type dict or str. "
            f"Given type: {type(proxy)}."
            )

    http_proxy = proxy.get('http')
    https_proxy = proxy.get('https')

    proxy = https_proxy if https_proxy else http_proxy

    if proxy is None:
        raise ValueError(
            "Could not find 'http' or 'https' key in 'proxy'!"
        )
    return proxy
