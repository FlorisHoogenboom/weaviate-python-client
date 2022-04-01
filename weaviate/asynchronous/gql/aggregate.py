"""
GraphQL 'Aggregate' clause.
"""
from weaviate.base.gql.aggregate import BaseAggregateBuilder
from .util import SendRequest
from ..requests import Requests



class AggregateBuilder(BaseAggregateBuilder, SendRequest):
    """
    AggregateBuilder class used to aggregate Weaviate objects.
    """

    def __init__(self, class_name: str, requests: Requests):
        """
        Initialize a AggregateBuilder class instance.

        Parameters
        ----------
        class_name : str
            Class name of the objects to be aggregated.
        requests : weaviate.synchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        super().__init__(class_name=class_name)
        self._requests = requests
