"""
GraphQL abstract query module.
"""
from abc import ABC, abstractmethod
from typing import List, Union, Dict

class BaseQuery(ABC):
    """
    BaseQuery abstract class used to make 'get' and/or 'aggregate' GraphQL queries.
    """

    @abstractmethod
    def get(self,
            class_name: str,
            properties: Union[List[str], str]=[],
        ):
        """
        Instantiate a GetBuilder for GraphQL 'get' requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to interact with.
        properties : list of str, str or None
            Properties of the objects to get. By default [].
        """

    @abstractmethod
    def aggregate(self, class_name: str):
        """
        Instantiate an AggregateBuilder for GraphQL 'aggregate' requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to be aggregated.
        """

    @abstractmethod
    def raw(self, gql_query: str):
        """
        Allows to send simple graph QL string queries.
        Be cautious of injection risks when generating query strings.

        Parameters
        ----------
        gql_query : str
            GraphQL query as a string.
        """


def pre_raw(gql_query: str) -> Dict[str, str]:
    """
    Pre-process the raw query before making the request to Weaviate.

    Parameters
    ----------
    gql_query : str
        The raw query.

    Returns
    -------
    Dict[str, str]
        The query as payload to the Weaviate request.
    """
    if not isinstance(gql_query, str):
        raise TypeError("Query is expected to be a string")

    return  {
        "query": gql_query
    }
