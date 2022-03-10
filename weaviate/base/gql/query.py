"""
GraphQL abstract query module.
"""
from abc import ABC, abstractmethod
from typing import List, Union

class BaseQuery(ABC):
    """
    BaseQuery abstract class used to make 'get' and/or 'aggregate' GraphQL queries.
    """

    @abstractmethod
    def get(self,
            class_name: str,
            properties: Union[List[str], str, None]=None,
        ):
        """
        Instantiate a GetBuilder for GraphQL 'get' requests.

        Parameters
        ----------
        class_name : str
            Class name of the objects to interact with.
        properties : list of str, str or None
            Properties of the objects to get. By default None.
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
    def explore(self, properties: Union[List[str], str]):
        """
        Instantiate an AggregateBuilder for GraphQL 'aggregate' requests.

        Parameters
        ----------
        properties : list of str or str
            Property/ies of the Explore filter to be returned. Currently there are 3 choices:
                'beacon', 'certainty', 'className'.
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
