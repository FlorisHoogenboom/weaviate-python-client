"""
Module for BatchRequest definition. Here should be implemented the Object/Reference batch requests.
Both Object/Reference batch requests must inherit from the abstract class `BatchRequests`. Any
functionality that is shared between batch requests must be defined in the BatchRequest class,
either as an abstract method or be implemented directly.
"""
import copy
from uuid import uuid4
from abc import ABC, abstractmethod
from typing import List, Sequence, Optional
from weaviate.util import get_valid_uuid, get_vector


class BatchRequest(ABC):
    """
    BatchRequest abstract class used as a interface for batch requests.
    """

    def __init__(self):
        self._items = []

    def __len__(self):
        return len(self._items)

    def is_empty(self) -> bool:
        """
        Check if BatchRequest is empty.

        Returns
        -------
        bool
            Whether the BatchRequest is empty.
        """

        return len(self._items) == 0

    def empty(self) -> None:
        """
        Remove all the items from the BatchRequest.
        """

        self._items = []

    def pop(self, index: int=-1) -> dict:
        """
        Remove and return item at index (default last).

        Parameters
        ----------
        index : int, optional
            The index of the item to pop, by default -1 (last item).

        Returns
        -------
        dict
            The popped item.

        Raises
        -------
        IndexError
            If batch is empty or index is out of range.
        """

        return self._items.pop(index)

    @abstractmethod
    def add(self, *args, **kwargs):
        """
        This method should me implemented by all inheriting classes.
        """

    @abstractmethod
    def get_request_body(self):
        """
        This method should me implemented by all inheriting classes.
        """


class ReferenceBatchRequest(BatchRequest):
    """
    Collect Weaviate-object references to add them in one request to Weaviate.
    NOTE: This request will miss some validations in order to be faster.
    """

    def add(self,
            from_class_name: str,
            from_uuid: str,
            from_property_name: str,
            to_uuid: str,
            to_class_name: str,
        ) -> None:
        """
        Add one Weaviate-object reference to this batch. Does NOT validate the consistency of the
        reference against the class schema. Checks the arguments' type and UUIDs' format.

        Parameters
        ----------
        from_class_name : str
            The name of the class that should reference another object.
        from_uuid : str
            The UUID or URL of the object that should reference another object.
        from_property_name : str
            The name of the property that contains the reference.
        to_uuid : str
            The UUID or URL of the object that is actually referenced.
        to_class_name : str
            The name of the class that should be referenced

        Raises
        ------
        TypeError
            If arguments are not of type str.
        ValueError
            If 'uuid' is not valid or cannot be extracted.
        """

        if (
            not isinstance(from_class_name, str)
            or not isinstance(from_uuid, str)
            or not isinstance(from_property_name, str)
            or not isinstance(to_uuid, str)
            or not isinstance(to_class_name, str)
        ):
            raise TypeError(
                'All arguments must be of type str.'
            )

        from_uuid = get_valid_uuid(from_uuid)
        to_uuid = get_valid_uuid(to_uuid)

        self._items.append(
            {
            'from': 'weaviate://localhost/'
                + from_class_name
                + '/'
                + from_uuid
                + '/'
                + from_property_name,
            'to': 'weaviate://localhost/'
                + to_class_name
                + '/'
                + to_uuid,
            }
        )

    def get_request_body(self) -> List[dict]:
        """
        Get request body as a list of dictionaries, where each dictionary
        is a Weaviate-object reference.

        Returns
        -------
        List[dict]
            A list of Weaviate-objects references as dictionaries.
        """

        return self._items


class ObjectBatchRequest(BatchRequest):
    """
    Collect objects for one batch request to weaviate.
    Caution this batch will not be validated through weaviate.
    """

    def add(self,
            properties: dict,
            class_name: str,
            uuid: Optional[str]=None,
            vector: Optional[Sequence]=None,
        ) -> str:
        """
        Add one object to this batch. Does NOT validate the consistency of the object against
        the client's schema. Checks the arguments' type and UUIDs' format.

        Parameters
        ----------
        class_name : str
            The name of the class this object belongs to.
        properties : dict
            Object properties to be added, as a dict datatype.
        uuid : Optional[str], optional
            UUID of the object as a string, by default None
        vector: Optional[Sequence], optional
            The embedding of the object that should be created. Used only class objects that do not
            have a vectorization module. Supported types are `list`, `numpy.ndarray`,
            `torch.Tensor` and `tf.Tensor`,
            by default None.

        Returns
        -------
        str
            The UUID of the added object as string.

        Raises
        ------
        TypeError
            If an argument passed is not of an appropriate type.
        ValueError
            If 'uuid' is not of a proper form.
        """

        if not isinstance(class_name, str):
            raise TypeError(
                f"'class_name' must be of type str. Given type: {type(class_name)}."
            )
        if not isinstance(properties, dict):
            raise TypeError(
                f"'properties' must be of type dict. Given type: {type(properties)}."
            )

        batch_item = {
            "class": class_name,
            "properties": copy.deepcopy(properties)
        }
        if uuid is not None:
            batch_item["id"] = get_valid_uuid(uuid)
        else:
            batch_item["id"] = uuid4().hex

        if vector is not None:
            batch_item["vector"] = get_vector(vector)

        self._items.append(batch_item)

        return batch_item["id"]

    def get_request_body(self) -> dict:
        """
        Get the request body as it is needed for the Weaviate server.

        Returns
        -------
        dict
            The request body as a dict.
        """

        return {
            "fields": ["ALL"],
            "objects": self._items
        }
