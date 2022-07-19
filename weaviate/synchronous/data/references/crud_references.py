"""
Reference class definition.
"""
import uuid
from typing import Union, List
from weaviate.base.data.references import (
    BaseReference,
    pre_replace,
    pre_delete_and_add,
)
from weaviate.exceptions import RequestsConnectionError, UnsuccessfulStatusCodeError
from ...requests import Requests


class Reference(BaseReference):
    """
    Reference class used to manipulate references within objects.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a Reference class instance.

        Parameters
        ----------
        requests : weaviate.synchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests

    def delete(self,
            from_uuid: Union[str, uuid.UUID],
            from_class_name: str,
            from_property_name: str,
            to_uuid: Union[str, uuid.UUID],
            to_class_name: str,
        ) -> dict:
        """
        Remove a reference to another object. Equal to removing one direction of an edge from the
        graph.

        Parameters
        ----------
        from_uuid : str or uuid.UUID
            The UUID of the object for which to delete the reference.
        from_class_name : str
            The class name of the referencing object for which to delete the reference.
        from_property_name : str
            The property that contains the reference that should be deleted.
        to_uuid : str or uuid.UUID
            The UUID of the referenced object.
        to_class_name : str
            The class name of the referenced object.

        Examples
        --------
        Assume we have two classes, Author and Book.

        >>> # Create the objects first
        >>> author_uuid = client.data_object.create(
        ...     data_object = {'name': 'Ray Bradbury'},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab',
        ... )
        >>> book_uuid = client.data_object.create(
        ...     data_object = {'title': 'The Martian Chronicles'},
        ...     class_name = 'Book',
        ...     uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d',
        ... )
        >>> # Add the cross references
        >>> ## Author -> Book
        >>> client.data_object.reference.add(
        ...     from_uuid = author_uuid,
        ...     from_class_name = 'Author',
        ...     from_property_name = 'wroteBooks',
        ...     to_uuid = book_uuid,
        ...     to_class_name = 'Book',
        ... )
        >>> client.data_object.get(author_uuid, 'Author')
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617177700595,
            "id": "e067f671-1202-42c6-848b-ff4d1eb804ab",
            "lastUpdateTimeUnix": 1617177700595,
            "properties": {
                "name": "Ray Bradbury",
                "wroteBooks": [
                {
                    "beacon": "weaviate://localhost/Book/a9c1b714-4f8a-4b01-a930-38b046d69d2d",
                    "href": "/v1/objects/Book/a9c1b714-4f8a-4b01-a930-38b046d69d2d"
                }
                ]
            },
            "vectorWeights": null
        }
        >>> # delete the reference
        >>> client.data_object.reference.delete(
        ...     from_uuid = author_uuid,
        ...     from_class_name = 'Author',
        ...     from_property_name = 'wroteBooks',
        ...     to_uuid = book_uuid,
        ...     to_class_name = 'Book',
        ... )
        >>> client.data_object.get(author_uuid, 'Author')
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617177700595,
            "id": "e067f671-1202-42c6-848b-ff4d1eb804ab",
            "lastUpdateTimeUnix": 1617177864970,
            "properties": {
                "name": "Ray Bradbury",
                "wroteBooks": []
            },
            "vectorWeights": null
        }

        Raises
        ------
        TypeError
            If one of the parameters is of the wrong type.
        ValueError
            If one of the parameters has a wrong value.
        requests.ConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path, beacon = pre_delete_and_add(
            from_uuid=from_uuid,
            from_class_name=from_class_name,
            from_property_name=from_property_name,
            to_uuid=to_uuid,
            to_class_name=to_class_name,
        )
        try:
            response = self._requests.delete(
                path=path,
                data_json=beacon,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Reference was not deleted due to connection error.'
            ) from conn_err
        if response.status_code == 204:
            return
        raise UnsuccessfulStatusCodeError(
            "Delete property reference to object",
            status_code=response.status_code,
            response_message=response.text,
        )

    def replace(self,
            from_uuid: Union[str, uuid.UUID],
            from_class_name: str,
            from_property_name: str,
            to_uuids: Union[list, str, uuid.UUID],
            to_class_names: Union[List[str], str],
        ):
        """
        Allows to replace ALL references in that property with a new set of references.
        NOTE: All old references will be deleted.

        Parameters
        ----------
        from_uuid : str or uuid.UUID
            The UUID of the object for which to replace the reference/s.
        from_class_name : str
            The class name of the referencing object for which to replace all the references.
        from_property_name : str
            The property that contains the reference that should be replaced.
        to_uuids : list, str or uuid.UUID
            The UUIDs of the objects that should be referenced. If 'str' it is converted internally
            into a list of str.
        to_class_name : Union[List[str], str
            The class name/s of the referenced object/s. If it is of type 'str' this class is going
            to be used for all the 'to_uuids', if it is List[str] it should the class name of each
            UUID from 'to_uuids'.

        Examples
        --------
        You have data object 1 with reference property 'wroteBooks' and currently has one reference
        to data object 7. Now you say, I want to replace the references of data object 1.wroteBooks
        to this list 3,4,9. After the replace, the data object 1.wroteBooks is now 3,4,9, but no
        longer contains 7.

        >>> author_uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        >>> client.data_object.get(author_uuid, 'Author')
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617177700595,
            "id": "e067f671-1202-42c6-848b-ff4d1eb804ab",
            "lastUpdateTimeUnix": 1617177700595,
            "properties": {
                "name": "Ray Bradbury",
                "wroteBooks": [
                {
                    "beacon": "weaviate://localhost/Book/a9c1b714-4f8a-4b01-a930-38b046d69d2d",
                    "href": "/v1/objects/Book/a9c1b714-4f8a-4b01-a930-38b046d69d2d"
                }
                ]
            },
            "vectorWeights": null
        }
        Currently there is only one 'Book' reference.
        Replace all the references of the Author for property name 'wroteBooks'.
        >>> client.data_object.reference.replace(
        ...     from_uuid = author_uuid,
        ...     from_class_name = 'Author',
        ...     from_property_name = 'wroteBooks',
        ...     to_uuids = [
        ...         '8429f68f-860a-49ea-a50b-1f8789515882',
        ...         '3e2e6795-298b-47e9-a2cb-3d8a77a24d8a'
        ...     ],
        ...     to_class_names = 'Book', # CAN BE ['Book', 'Book'] TOO
        ... )
        >>> client.data_object.get(author_uuid, 'Author')
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617181292677,
            "id": "e067f671-1202-42c6-848b-ff4d1eb804ab",
            "lastUpdateTimeUnix": 1617181409405,
            "properties": {
                "name": "Ray Bradbury",
                "wroteBooks": [
                {
                    "beacon": "weaviate://localhost/Book/8429f68f-860a-49ea-a50b-1f8789515882",
                    "href": "/v1/objects/Book/8429f68f-860a-49ea-a50b-1f8789515882"
                },
                {
                    "beacon": "weaviate://localhost/Book/3e2e6795-298b-47e9-a2cb-3d8a77a24d8a",
                    "href": "/v1/objects/Book/3e2e6795-298b-47e9-a2cb-3d8a77a24d8a"
                }
                ]
            },
            "vectorWeights": null
        }
        All the previous references were removed and now we have only those specified in the
        '.replace()' method.

        Raises
        ------
        TypeError
            If one of the parameters is of the wrong type.
        ValueError
            If one of the parameters has a wrong value.
        requests.ConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path, beacons = pre_replace(
            from_uuid=from_uuid,
            from_class_name=from_class_name,
            from_property_name=from_property_name,
            to_uuids=to_uuids,
            to_class_names=to_class_names,
        )
        try:
            response = self._requests.put(
                path=path,
                data_json=beacons,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Reference was not replaced due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return
        raise UnsuccessfulStatusCodeError(
            "Update property reference to object.",
            status_code=response.status_code,
            response_message=response.text,
        )

    def add(self,
            from_uuid: Union[str, uuid.UUID],
            from_class_name: str,
            from_property_name: str,
            to_uuid: Union[str, uuid.UUID],
            to_class_name: str,
        ) -> dict:
        """
        Allows to link an object to an object uni-directionally.

        Parameters
        ----------
        from_uuid : str or uuid.UUID
            The UUID of the object for which to add the reference.
        from_class_name : str
            The class name of the referencing object for which to add the reference.
        from_property_name : str
            The property for which to create the reference.
        to_uuid : str or uuid.UUID
            The UUID of the referenced object.
        to_class_name : str
            The class name of the referenced object.

        Examples
        --------
        Assume we have two classes, Author and Book.

        >>> # Create the objects first
        >>> author_uuid = client.data_object.create(
        ...     data_object = {'name': 'Ray Bradbury'},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        >>> book_uuid = client.data_object.create(
        ...     data_object = {'title': 'The Martian Chronicles'},
        ...     class_name = 'Book',
        ...     uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d'
        ... )
        >>> # Add the cross references
        >>> ## Author -> Book
        >>> client.data_object.reference.add(
        ...     from_uuid = author_uuid,
        ...     from_class_name = 'Author',
        ...     from_property_name = 'wroteBooks',
        ...     to_uuid = book_uuid,
        ...     to_class_name = 'Book',
        ... )
        >>> client.data_object.get(author_uuid, 'Author')
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617177700595,
            "id": "e067f671-1202-42c6-848b-ff4d1eb804ab",
            "lastUpdateTimeUnix": 1617177700595,
            "properties": {
                "name": "Ray Bradbury",
                "wroteBooks": [
                {
                    "beacon": "weaviate://localhost/Book/a9c1b714-4f8a-4b01-a930-38b046d69d2d",
                    "href": "/v1/objects/Book/a9c1b714-4f8a-4b01-a930-38b046d69d2d"
                }
                ]
            },
            "vectorWeights": null
        }

        Raises
        ------
        TypeError
            If one of the parameters is of the wrong type.
        ValueError
            If one of the parameters has a wrong value.
        requests.ConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path, beacon = pre_delete_and_add(
            from_uuid=from_uuid,
            from_class_name=from_class_name,
            from_property_name=from_property_name,
            to_uuid=to_uuid,
            to_class_name=to_class_name,
        )
        try:
            response = self._requests.post(
                path=path,
                data_json=beacon,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Reference was not added due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return
        raise UnsuccessfulStatusCodeError(
            "Add property reference to object.",
            status_code=response.status_code,
            response_message=response.text,
        )
