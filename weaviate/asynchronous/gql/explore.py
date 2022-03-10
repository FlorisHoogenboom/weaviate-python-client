"""
GraphQL `Explore` command.
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
            properties: Union[List[str], str],
            requests: Requests,
        ):
        """
        Initialize a ExploreBuilder class instance.

        Parameters
        ----------
        properties : list of str or str
            Property/ies of the Explore filter to be returned. Currently there are 3 choices:
                'beacon', 'certainty', 'className'.
        requests : weaviate.synchronous.Requests
            Requests object to an active and running Weaviate instance.

        Raises
        ------
        TypeError
            If argument/s is/are of wrong type.
        """

        super().__init__(properties=properties)
        self._requests = requests
