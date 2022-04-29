"""
GraphQL 'Get' clause.
"""
from typing import Union, List
from weaviate.base.gql.get import BaseGetBuilder
from .util import SendRequest
from ..requests import Requests


class GetBuilder(BaseGetBuilder, SendRequest):
    """
    GetBuilder class used to create GraphQL queries.
    """

    def __init__(self,
            requests: Requests,
            class_name: str,
            properties: Union[List[str], str, None],
        ):
        """
        Initialize a GetBuilder class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.Requests
            Requests object to an active and running Weaviate instance.
        class_name : str
            Class name of the objects to interact with.
        properties : list of str, str or None
            Properties of the objects to interact with.

        Raises
        ------
        TypeError
            If argument/s is/are of wrong type.
        """

        super().__init__(class_name, properties)
        self._requests = requests
