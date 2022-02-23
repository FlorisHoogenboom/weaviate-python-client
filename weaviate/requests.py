"""
Connection class definition.
"""

from typing import Optional
from requests import Response, Session
from .connection import Connection


class Requests:
    """
    Connection class used to communicate to a Weaviate instance. Has all needed RESTful API
    implementations. If Authentication is used, it automatically gets a new token in case it
    expired.
    """

    def __init__(self,
            connection: Connection,
        ):
        """
        Initialize a Connection class instance.

        Parameters
        ----------
        connection: weaviate.connection.Connection

        Raises
        ------
        ValueError
            If no authentication credentials provided but the Weaviate server has an OpenID
            configured.
        """

        self._connection = connection
        self._session = Session()

    def __del__(self):
        """
        Destructor for Connection class instance.
        """

        self._session.close()

    def delete(self, path: str, data_json: Optional[dict]=None) -> Response:
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
            The opened request context manager.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the DELETE request could not be made.
        """

        return self._session.delete(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_requests(),
            proxies=self._connection.proxies.get_proxies_requests(),
        )

    def patch(self, path: str, data_json: dict) -> Response:
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
            The opened request context manager.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the PATCH request could not be made.
        """

        return self._session.patch(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_requests(),
            proxies=self._connection.proxies.get_proxies_requests(),
        )

    def post(self, path: str, data_json: Optional[dict]=None) -> Response:
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
            The opened request context manager.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the POST request could not be made.
        """

        return self._session.post(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_requests(),
            proxies=self._connection.proxies.get_proxies_requests(),
        )

    def put(self, path: str, data_json: dict) -> Response:
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
        requests.exceptions.ConnectionError
            If the PUT request could not be made.
        """

        return self._session.put(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_requests(),
            proxies=self._connection.proxies.get_proxies_requests(),
        )

    def get(self, path: str, params: Optional[dict]=None) -> Response:
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
            The opened request context manager.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the GET request could not be made.
        """

        if params is None:
            params = {}

        return self._session.get(
            url=self._connection.get_url(path),
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_requests(),
            proxies=self._connection.proxies.get_proxies_requests(),
            params=params,
        )

    def head(self, path: str) -> Response:
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
            The opened request context manager.

        Raises
        ------
        requests.exceptions.ConnectionError
            If the HEAD request could not be made.
        """

        return self._session.head(
            url=self._connection.get_url(path),
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_requests(),
            proxies=self._connection.proxies.get_proxies_requests(),
        )
