"""
Connection class definition.
"""

from typing import Optional
from aiohttp.client import ClientSession, ClientResponse
from weaviate.base.connection import Connection


class AsyncRequests:
    """
    AsyncRequests class used to make asynchronous requests to a Weaviate instance by using a
    Connection. It has all needed RESTful API implementations.
    """

    def __init__(self,
            connection: Connection,
        ):
        """
        Initialize a AsyncRequests class instance.

        Parameters
        ----------
        connection: weaviate.connection.Connection
        """

        self._connection = connection
        self._session = ClientSession()

    async def close(self):
        """
        Close connection for AsyncRequests class instance.
        """

        await self._session.close()

    async def delete(self, path: str, data_json: Optional[dict]=None) -> ClientResponse:
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
        aiohttp.ClientResponse
            The opened request context manager.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the DELETE request could not be made.
        """

        return await self._session.delete(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_aiohttp(),
            proxy=self._connection.proxies.get_proxy_aiohttp(),
        )

    async def patch(self, path: str, data_json: dict) -> ClientResponse:
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
        aiohttp.ClientResponse
            The opened request context manager.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the PATCH request could not be made.
        """

        return await self._session.patch(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_aiohttp(),
            proxy=self._connection.proxies.get_proxy_aiohttp(),
        )

    async def post(self, path: str, data_json: Optional[dict]=None) -> ClientResponse:
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
        aiohttp.ClientResponse
            The opened request context manager.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the POST request could not be made.
        """

        return await self._session.post(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_aiohttp(),
            proxy=self._connection.proxies.get_proxy_aiohttp(),
        )

    async def put(self, path: str, data_json: dict) -> ClientResponse:
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
        aiohttp.ClientResponse
            The opened request context manager.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the PUT request could not be made.
        """

        return await self._session.put(
            url=self._connection.get_url(path),
            json=data_json,
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_aiohttp(),
            proxy=self._connection.proxies.get_proxy_aiohttp(),
        )

    async def get(self, path: str, params: Optional[dict]=None) -> ClientResponse:
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
        aiohttp.ClientResponse
            The opened request context manager.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the GET request could not be made.
        """

        if params is None:
            params = {}

        return await self._session.get(
            url=self._connection.get_url(path),
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_aiohttp(),
            proxy=self._connection.proxies.get_proxy_aiohttp(),
            params=params,
        )

    async def head(self, path: str) -> ClientResponse:
        """
        Make a HEAD request to the server.

        Parameters
        ----------
        path : str
            Sub-path to the resources. Must be a valid sub-path.
            e.g. '/meta' or '/objects', without version.

        Returns
        -------
        aiohttp.ClientResponse
            The opened request context manager.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the HEAD request could not be made.
        """

        return await self._session.head(
            url=self._connection.get_url(path),
            headers=self._connection.get_request_header(),
            timeout=self._connection.timeout_config.get_timeout_aiohttp(),
            proxy=self._connection.proxies.get_proxy_aiohttp(),
        )
