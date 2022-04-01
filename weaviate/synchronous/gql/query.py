"""
GraphQL query module.
"""
from typing import List, Union
from weaviate.base.gql.query import BaseQuery
from .get import GetBuilder
from .aggregate import AggregateBuilder
from .explore import ExploreBuilder
from .util import make_query_request
from ..requests import Requests


class Query(BaseQuery):
    """
    Query class used to make 'get' and/or 'aggregate' GraphQL queries.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a Classification class instance.

        Parameters
        ----------
        requests : weaviate.synchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests

    def get(self,
            class_name: str,
            properties: Union[List[str], str, None]=None,
        ) -> GetBuilder:
        """
        Instantiate a GetBuilder for GraphQL 'get' requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to interact with.
        properties : list of str, str or None, optional
            Properties of the objects to get. By default None.
            NOTE: If it is None, them you MUST provide 'additional_properties'.

        Returns
        -------
        GetBuilder
            A GetBuilder to make GraphQL 'get' requests from Weaviate.
        """

        return GetBuilder(
            class_name=class_name,
            properties=properties,
            requests=self._requests,
        )

    def aggregate(self, class_name: str) -> AggregateBuilder:
        """
        Instantiate an AggregateBuilder for GraphQL 'aggregate' requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to be aggregated.

        Returns
        -------
        AggregateBuilder
            An AggregateBuilder to make GraphQL 'aggregate' requests from Weaviate.
        """

        return AggregateBuilder(
            class_name=class_name,
            requests=self._requests,
        )

    def explore(self,
            properties: Union[List[str], str],
        ) -> ExploreBuilder:
        """
        Instantiate an ExploreBuilder for GraphQL 'explore' requests.

        Parameters
        ----------
        properties : list of str or str
            Property/ies of the Explore filter to be returned. Currently there are 3 choices:
                'beacon', 'certainty', 'className'.

        Returns
        -------
        ExploreBuilder
            An ExploreBuilder to make GraphQL 'explore' requests from Weaviate.
        """

        return ExploreBuilder(
            properties=properties,
            requests=self._requests,
        )

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
            If the network connection to Weaviate fails.
        weaviate.UnexpectedStatusCodeException
            If Weaviate reports a none OK status.
        """

        return make_query_request(
            requests=self._requests,
            query=gql_query,
        )
