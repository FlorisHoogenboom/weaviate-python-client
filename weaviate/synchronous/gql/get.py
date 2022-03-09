"""
GraphQL `Get` command.
"""
from json import dumps
from typing import List, Union
from weaviate.base import BaseGetBuilder
from ..requests import SyncRequests


class SyncGetBuilder(BaseGetBuilder):
    """
    GetBuilder class used to create GraphQL queries.
    """

    def __init__(self,
            class_name: str,
            properties: Union[List[str], str],
            requests: SyncRequests
        ):
        """
        Initialize a GetBuilder class instance.

        Parameters
        ----------
        class_name : str
            Class name of the objects to interact with.
        properties : str or list of str, optional
            Properties of the objects to interact with. By default [].
        requests : weaviate.synchronous.SyncRequests
            SyncRequests object to an active and running Weaviate instance.

        Raises
        ------
        TypeError
            If argument/s is/are of wrong type.
        """

        super().__init__(class_name, properties)
        self._requests = requests

    