"""
AsyncReference class definition.
"""
from typing import Union
from weaviate.base import BaseReference
from weaviate.exceptions import AiohttpConnectionError, UnsuccessfulStatusCodeError
from ...requests import AsyncRequests


class AsyncReference(BaseReference):
    """
    SyncReference class used to manipulate references within objects.
    """

    def __init__(self, requests: AsyncRequests):
        """
        Initialize a AsyncReference class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.AsyncRequests
            AsyncRequests object to an active and running weaviate instance.
        """

        self._requests = requests

    async def delete(self, from_uuid: str, from_property_name: str, to_uuid: str):
        """
        Remove a reference to another object. Equal to removing one direction of an edge from the
        graph.

        Parameters
        ----------
        from_uuid : str
            The ID of the object that references another object.
        from_property_name : str
            The property from which the reference should be deleted.
        to_uuid : str
            The UUID of the referenced object.

        Examples
        --------
        Assume we have two classes, Author and Book.

        >>> # Create the objects first
        >>> await client.data_object.create(
        ...     data_object = {'name': 'Ray Bradbury'},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        >>> await client.data_object.create(
        ...     data_object = {'title': 'The Martian Chronicles'},
        ...     class_name = 'Book',
        ...     uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d'
        ... )
        >>> # Add the cross references
        >>> ## Author -> Book
        >>> await client.data_object.reference.add(
        ...     from_uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab', # Author UUID
        ...     from_property_name = 'wroteBooks',
        ...     to_uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d' # Book UUID
        ... )
        >>> await client.data_object.get('e067f671-1202-42c6-848b-ff4d1eb804ab') # Author UUID
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
                    "beacon": "weaviate://localhost/a9c1b714-4f8a-4b01-a930-38b046d69d2d",
                    "href": "/v1/objects/a9c1b714-4f8a-4b01-a930-38b046d69d2d"
                }
                ]
            },
            "vectorWeights": null
        }
        >>> # delete the reference
        >>> await client.data_object.reference.delete(
        ...     from_uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab', # Author UUID
        ...     from_property_name = 'wroteBooks',
        ...     to_uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d' # Book UUID
        ... )
        >>> await client.data_object.get('e067f671-1202-42c6-848b-ff4d1eb804ab') # Author UUID
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
        aiohttp.ClientConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path, beacon = super().delete(
            from_uuid=from_uuid,
            from_property_name=from_property_name,
            to_uuid=to_uuid,
        )
        try:
            response = await self._requests.delete(
                path=path,
                data_json=beacon,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Reference was not deleted due to connection error.'
            ) from conn_err
        if response.status == 204:
            return
        raise UnsuccessfulStatusCodeError(
            "Delete property reference to object",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def replace(self, from_uuid: str, from_property_name: str, to_uuids: Union[list, str]):
        """
        Allows to replace ALL references in that property with a new set of references.
        NOTE: All old references will be deleted.

        Parameters
        ----------
        from_uuid : str
            The object that should have the reference as part of its properties.
            Should be in the form of an UUID or in form of an URL.
            E.g.
            'http://localhost:8080/v1/objects/fc7eb129-f138-457f-b727-1b29db191a67'
            or
            'fc7eb129-f138-457f-b727-1b29db191a67'
        from_property_name : str
            The name of the property within the object.
        to_uuids : list or str
            The UUIDs of the objects that should be referenced.
            Should be a list of str in the form of an UUID or str in form of an URL.
            E.g.
            ['http://localhost:8080/v1/objects/fc7eb129-f138-457f-b727-1b29db191a67', ...]
            or
            ['fc7eb129-f138-457f-b727-1b29db191a67', ...]
            If 'str' it is converted internally into a list of str.

        Examples
        --------
        You have data object 1 with reference property 'wroteBooks' and currently has one reference
        to data object 7. Now you say, I want to replace the references of data object 1.wroteBooks
        to this list 3,4,9. After the replace, the data object 1.wroteBooks is now 3,4,9, but no
        longer contains 7.

        >>> await client.data_object.get('e067f671-1202-42c6-848b-ff4d1eb804ab') # Author UUID
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
                    "beacon": "weaviate://localhost/a9c1b714-4f8a-4b01-a930-38b046d69d2d",
                    "href": "/v1/objects/a9c1b714-4f8a-4b01-a930-38b046d69d2d"
                }
                ]
            },
            "vectorWeights": null
        }
        Currently there is only one 'Book' reference.
        Replace all the references of the Author for property name 'wroteBooks'.
        >>> await client.data_object.reference.replace(
        ...     from_uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab', # Author UUID
        ...     from_property_name = 'wroteBooks',
        ...     to_uuids = [
        ...         '8429f68f-860a-49ea-a50b-1f8789515882',
        ...         '3e2e6795-298b-47e9-a2cb-3d8a77a24d8a'
        ...     ]
        ... )
        >>> await client.data_object.get('e067f671-1202-42c6-848b-ff4d1eb804ab') # Author UUID
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
                    "beacon": "weaviate://localhost/8429f68f-860a-49ea-a50b-1f8789515882",
                    "href": "/v1/objects/8429f68f-860a-49ea-a50b-1f8789515882"
                },
                {
                    "beacon": "weaviate://localhost/3e2e6795-298b-47e9-a2cb-3d8a77a24d8a",
                    "href": "/v1/objects/3e2e6795-298b-47e9-a2cb-3d8a77a24d8a"
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
        aiohttp.ClientConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path, beacons = super().replace(
            from_uuid=from_uuid,
            from_property_name=from_property_name,
            to_uuids=to_uuids,
        )
        try:
            response = await self._requests.put(
                path=path,
                data_json=beacons,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Reference was not replaced due to connection error.'
            ) from conn_err
        if response.status == 200:
            return
        raise UnsuccessfulStatusCodeError(
            "Update property reference to object.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def add(self, from_uuid: str, from_property_name: str, to_uuid: str):
        """
        Allows to link an object to an object unidirectionally.

        Parameters
        ----------
        from_uuid : str
            The ID of the object that should have the reference as part
            of its properties. Should be a plane UUID or an URL.
            E.g.
            'http://localhost:8080/v1/objects/fc7eb129-f138-457f-b727-1b29db191a67'
            or
            'fc7eb129-f138-457f-b727-1b29db191a67'
        from_property_name : str
            The name of the property within the object.
        to_uuid : str
            The UUID of the object that should be referenced.
            Should be a plane UUID or an URL.
            E.g.
            'http://localhost:8080/v1/objects/fc7eb129-f138-457f-b727-1b29db191a67'
            or
            'fc7eb129-f138-457f-b727-1b29db191a67'

        Examples
        --------
        Assume we have two classes, Author and Book.

        >>> # Create the objects first
        >>> client.data_object.create(
        ...     data_object = {'name': 'Ray Bradbury'},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        >>> client.data_object.create(
        ...     data_object = {'title': 'The Martian Chronicles'},
        ...     class_name = 'Book',
        ...     uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d'
        ... )
        >>> # Add the cross references
        >>> ## Author -> Book
        >>> client.data_object.reference.add(
        ...     from_uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab', # Author UUID
        ...     from_property_name = 'wroteBooks',
        ...     to_uuid = 'a9c1b714-4f8a-4b01-a930-38b046d69d2d' # Book UUID
        ... )
        >>> client.data_object.get('e067f671-1202-42c6-848b-ff4d1eb804ab') # Author UUID
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
                    "beacon": "weaviate://localhost/a9c1b714-4f8a-4b01-a930-38b046d69d2d",
                    "href": "/v1/objects/a9c1b714-4f8a-4b01-a930-38b046d69d2d"
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
        aiohttp.ClientConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path, beacons = super().add(
            from_uuid=from_uuid,
            from_property_name=from_property_name,
            to_uuid=to_uuid,
        )
        try:
            response = await self._requests.post(
                path=path,
                data_json=beacons,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Reference was not added due to connection error.'
            ) from conn_err
        if response.status == 200:
            return
        raise UnsuccessfulStatusCodeError(
            "Add property reference to object.",
            status_code=response.status,
            response_message=await response.text(),
        )
