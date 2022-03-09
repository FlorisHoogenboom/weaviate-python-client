"""
GraphQL query module.
"""
from typing import List, Union
from weaviate.exceptions import UnsuccessfulStatusCodeError, RequestsConnectionError
from weaviate.base.gql.query import BaseQuery, pre_raw
from .get import SyncGetBuilder
from .aggregate import SyncAggregateBuilder
from ..requests import SyncRequests


class Query(BaseQuery):
    """
    Query class used to make `get` and/or `aggregate` GraphQL queries.
    """

    def __init__(self, requests: SyncRequests):
        """
        Initialize a Classification class instance.

        Parameters
        ----------
        requests : weaviate.synchronous.SyncRequests
            SyncRequests object to an active and running Weaviate instance.
        """

        self._requests = requests

    def get(self,
            class_name: str,
            properties: Union[List[str], str]=[],
        ) -> SyncGetBuilder:
        """
        Instantiate a GetBuilder for GraphQL `get` requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to interact with.
        properties : list of str or str
            Properties of the objects to get.

        Returns
        -------
        GetBuilder
            A GetBuilder to make GraphQL `get` requests from weaviate.
        """

        return SyncGetBuilder(class_name, properties, self._requests)

    def aggregate(self, class_name: str) -> SyncAggregateBuilder:
        """
        Instantiate an AggregateBuilder for GraphQL `aggregate` requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to be aggregated.

        Returns
        -------
        AggregateBuilder
            An AggregateBuilder to make GraphQL `aggregate` requests from weaviate.
        """

        return SyncAggregateBuilder(class_name, self._requests)

    def raw(self, gql_query: str) -> dict:
        """
        Allows to send simple graph QL string queries.
        Be cautious of injection risks when generating query strings.

        Parameters
        ----------
        gql_query : str
            GraphQL query as a string.

        Returns
        -------
        dict
            Data response of the query.

        Examples
        --------
        >>> query = \"""
        ... {
        ...     Get {
        ...         Article(limit: 2) {
        ...         title
        ...         hasAuthors {
        ...             ... on Author {
        ...                 name
        ...                 }
        ...             }
        ...         }
        ...     }
        ... }
        ... \"""
        >>> client.query.raw(query)
        {
        "data": {
            "Get": {
            "Article": [
                {
                "hasAuthors": [
                    {
                    "name": "Jonathan Wilson"
                    }
                ],
                "title": "Sergio Ag\u00fcero has been far more than a great goalscorer for
                            Manchester City"
                },
                {
                "hasAuthors": [
                    {
                    "name": "Emma Elwick-Bates"
                    }
                ],
                "title": "At Swarovski, Giovanna Engelbert Is Crafting Jewels As Exuberantly
                            Joyful As She Is"
                }
            ]
            }
        },
        "errors": null
        }

        Raises
        ------
        TypeError
            If 'gql_query' is not of type str.
        requests.ConnectionError
            If the network connection to weaviate fails.
        weaviate.UnexpectedStatusCodeException
            If weaviate reports a none OK status.
        """

        json_query = pre_raw(
            gql_query=gql_query,
        )

        try:
            response = self._requests.post(
                path="/graphql",
                data_json=json_query,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Query not executed.'
            ) from conn_err
        if response.status_code == 200:
            return response.json()
        raise UnsuccessfulStatusCodeError(
            "GQL query failed",
            status_code=response.status_code,
            response_message=response.text,
        )
