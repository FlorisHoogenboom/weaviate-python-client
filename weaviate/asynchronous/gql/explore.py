"""
GraphQL 'Explore' clause.
"""
from typing import Union, List
from weaviate.base.gql.explore import BaseExploreBuilder
from .util import SendRequest
from ..requests import Requests


class ExploreBuilder(BaseExploreBuilder, SendRequest):
    """
    ExploreBuilder class used to explore Weaviate objects.
    """

    def __init__(self,
            requests: Requests,
            properties: Union[List[str], str],
        ):
        """
        Initialize a ExploreBuilder class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.Requests
            Requests object to an active and running Weaviate instance.
        properties : list of str or str
            Property/ies of the Explore filter to be returned. Currently there are 3 choices:
                'beacon', 'certainty', 'className'.

        Raises
        ------
        TypeError
            If argument/s is/are of wrong type.
        """

        super().__init__(properties=properties)
        self._requests = requests
