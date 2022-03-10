"""
Helper functions for this module.
"""
from abc import ABC, abstractmethod
from weaviate.exceptions import AiohttpConnectionError, UnsuccessfulStatusCodeError
from ..requests import Requests


async def make_query_request(requests: Requests, query: str) -> dict:
    """
    Make Query request to Weaviate.

    Parameters
    ----------
    requests : Requests
        A Requests object instance.
    query : str
        The query as a string.

    Returns
    -------
    dict
        The response of the query.

    Raises
    ------
    aiohttp.ClientConnectionError
        If the network connection to weaviate fails.
    weaviate.UnsuccessfulStatusCodeError
        If weaviate reports a none OK status.
    """

    try:
        response = await requests.post(
            path="/graphql",
            data_json={"query": query},
        )
    except AiohttpConnectionError as conn_err:
        raise AiohttpConnectionError(
            'Query was not successful.'
        ) from conn_err
    if response.status == 200:
        return await response.json()
    raise UnsuccessfulStatusCodeError(
        "Query was not successful",
        status_code=response.status,
        response_message=await response.text(),
    )

class SendRequest(ABC):
    """
    SendRequest abstract class from which all GraphQl query types should inherit from.
    I contain the `do` method that is common to all query types.
    """

    _requests: Requests

    @abstractmethod
    def build(self) -> str:
        """
        Build method to create the query string. No need to implement in in here,
        it should be inherited from the XXXBuilder.

        Returns
        -------
        str
            The GraphQL query as a string.
        """

    async def do(self) -> dict:
        """
        Builds and runs the query.

        Returns
        -------
        dict
            The response of the query.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to weaviate fails.
        weaviate.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        return await make_query_request(
            requests=self._requests,
            query=self.build(),
        )
