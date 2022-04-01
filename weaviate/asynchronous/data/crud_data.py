"""
DataObject class definition.
"""
import uuid as uuid_lib
from numbers import Real
from typing import Union, Optional, List, Sequence
from weaviate.base.data import (
    BaseDataObject,
    pre_replace,
    pre_create,
    pre_delete_exists,
    pre_get,
    pre_update,
    pre_validate,
)
from weaviate.exceptions import (
    ObjectAlreadyExistsError,
    AiohttpConnectionError,
    UnsuccessfulStatusCodeError,
)
from .references.crud_references import Reference
from ..requests import Requests


class DataObject(BaseDataObject):
    """
    DataObject class used to manipulate object to/from Weaviate. This class has CRUD methods.

    Attributes
    ----------
    reference : weaviate.asynchronous.Reference
        A Reference object to create objects cross-references.
    """

    async def __init__(self, requests: Requests):
        """
        Initialize a DataObject class instance.

        Parameters
        ----------
        requests : weaviate.synchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests
        self.reference = Reference(self._requests)

    async def create(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID, None]=None,
            vector: Optional[Sequence[Real]]=None,
        ) -> str:
        """
        Create a new object in Weaviate.

        Parameters
        ----------
        data_object : dict
            The new object to add to Weaviate. It represents the class instance properties only.
        class_name : str
            The class name associated with the object given.
        uuid : str, uuid.UUID or None, optional
            The object's UUID. The object to will have this UUID if it is provided, otherwise
            Weaviate will generate an UUID for this object, by default None.
        vector: Sequence[Real] or None, optional
            The embedding of the object that should be created. Used only for class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor',by default None.

        Examples
        --------
        Schema contains a class Author with only 'name' and 'age' primitive property.

        >>> await client.data_object.create(
        ...     data_object = {'name': 'Neil Gaiman', 'age': 60},
        ...     class_name = 'Author',
        ... )
        '46091506-e3a0-41a4-9597-10e3064d8e2d'
        >>> await client.data_object.create(
        ...     data_object = {'name': 'Andrzej Sapkowski', 'age': 72},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        'e067f671-1202-42c6-848b-ff4d1eb804ab'

        Returns
        -------
        str
            The UUID of the created object.

        Raises
        ------
        TypeError
            If one of the arguments is of wrong data type.
        ValueError
            If one of the arguments has an invalid value.
        weaviate.exception.ObjectAlreadyExistsException
            If an object with the given uuid already exists within Weaviate.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If creating the object in Weaviate failed for a different reason other than connection,
            more information is given in the error message.
        """

        path, weaviate_obj = pre_create(
            data_object=data_object,
            class_name=class_name,
            uuid=uuid,
            vector=vector,
        )
        try:
            response = await self._requests.post(
                path=path,
                data_json=weaviate_obj,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Object was not added due to connection error.'
            ) from conn_err
        if response.status == 200:
            return str(await response.json()["id"])

        object_does_already_exist = False
        try:
            if 'already exists' in await response.json()['error'][0]['message']:
                object_does_already_exist = True
        except KeyError:
            pass
        if object_does_already_exist:
            raise ObjectAlreadyExistsError(weaviate_obj["id"])
        raise UnsuccessfulStatusCodeError(
            "Creating object.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def update(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID],
            vector: Optional[Sequence[Real]]=None,
        ) -> None:
        """
        Update the given object's property/ies. Only the specified property/ies are updated, the
        unspecified ones remain unchanged.

        Parameters
        ----------
        data_object : dict
            The object's property/ies that should be updated. Fields not specified by in the
            'data_object' remain unchanged. Fields that are None will not be changed.
        class_name : str
            The class name of the object that should be updated.
        uuid : str or uuid.UUID
            The object's UUID which should be updated.
        vector: Sequence[Real] or None, optional
            The embedding of the object that should be updated. Used only for class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor', by default None.

        Examples
        --------
        >>> author_id = await client.data_object.create(
        ...     data_object = {'name': 'Philip Pullman', 'age': 64},
        ...     class_name = 'Author'
        ... )
        >>> await client.data_object.get(author_id)
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617111215172,
            "id": "bec2bca7-264f-452a-a5bb-427eb4add068",
            "lastUpdateTimeUnix": 1617111215172,
            "properties": {
                "age": 64,
                "name": "Philip Pullman"
            },
            "vectorWeights": null
        }
        >>> await client.data_object.update(
        ...     data_object = {'age': 74},
        ...     class_name = 'Author',
        ...     uuid = author_id
        ... )
        >>> await client.data_object.get(author_id)
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617111215172,
            "id": "bec2bca7-264f-452a-a5bb-427eb4add068",
            "lastUpdateTimeUnix": 1617111215172,
            "properties": {
                "age": 74,
                "name": "Philip Pullman"
            },
            "vectorWeights": null
        }

        Raises
        ------
        TypeError
            If one of the arguments is of wrong data type.
        ValueError
            If one of the arguments has an invalid value.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If updating the object in Weaviate failed for a different reason than connection,
            more information is given in the error message.
        """

        path, weaviate_obj = pre_update(
            data_object=data_object,
            class_name=class_name,
            uuid=uuid,
            vector=vector,
        )

        try:
            response = await self._requests.patch(
                path=path,
                data_json=weaviate_obj,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Object was not updated due to connection error.'
            ) from conn_err
        if response.status == 204:
            return
        raise UnsuccessfulStatusCodeError(
            "Update of the object not successful.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def replace(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID],
            vector: Optional[Sequence[Real]]=None,
        ) -> None:
        """
        Replace an already existing object with a new one. This method replaces the whole object.

        Parameters
        ----------
        data_object : dict
            The new object to be replaced with.
        class_name : str
            The class name of the object that should be replaced.
        uuid : str or uuid.UUID
            The object's UUID which should be replaced.
        vector: Sequence[Real] or None, optional
            The embedding of the object that should be replaced. Used only for class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor', by default None.

        Examples
        --------
        >>> author_id = await client.data_object.create(
        ...     data_object = {'name': 'H. Lovecraft', 'age': 46},
        ...     class_name = 'Author'
        ... )
        >>> await client.data_object.get(author_id)
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617112817487,
            "id": "d842a0f4-ad8c-40eb-80b4-bfefc7b1b530",
            "lastUpdateTimeUnix": 1617112817487,
            "properties": {
                "age": 46,
                "name": "H. Lovecraft"
            },
            "vectorWeights": null
        }
        >>> await client.data_object.replace(
        ...     data_object = {'name': 'H.P. Lovecraft'},
        ...     class_name = 'Author',
        ...     uuid = author_id
        ... )
        >>> await client.data_object.get(author_id)
        {
            "additional": {},
            "class": "Author",
            "id": "d842a0f4-ad8c-40eb-80b4-bfefc7b1b530",
            "lastUpdateTimeUnix": 1617112838668,
            "properties": {
                "name": "H.P. Lovecraft"
            },
            "vectorWeights": null
        }

        Raises
        ------
        TypeError
            If one of the arguments is of wrong data type.
        ValueError
            If one of the arguments has an invalid value.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If replacing the object in Weaviate failed for a different reason than connection,
            more information is given in the error message.
        """

        path, weaviate_obj = pre_replace(
            data_object=data_object,
            class_name=class_name,
            uuid=uuid,
            vector=vector,
        )
        try:
            response = await self._requests.put(
                path=path,
                data_json=weaviate_obj,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Object was not replaced due to connection error.'
            ) from conn_err
        if response.status == 200:
            return
        raise UnsuccessfulStatusCodeError(
            "Replace object.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def get_by_id(self,
            uuid: Union[str, uuid_lib.UUID],
            additional_properties: Optional[Union[List[str], str]]=None,
            with_vector: bool=False,
        ) -> Optional[dict]:
        """
        Get an object as dict.

        Parameters
        ----------
        uuid : str
            The identifier of the object that should be retrieved.
        additional_properties : list of str, str or None, optional
            Additional property/ies that should be included in the request, by default None.
        with_vector: bool
            If True the 'vector' property will be returned too, by default False.

        Examples
        --------
        >>> await client.data_object.get_by_id("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617112817487,
            "id": "d842a0f4-ad8c-40eb-80b4-bfefc7b1b530",
            "lastUpdateTimeUnix": 1617112817487,
            "properties": {
                "age": 46,
                "name": "H.P. Lovecraft"
            },
            "vectorWeights": null
        }

        Returns
        -------
        dict or None
            The object as a 'dict' if it exists exists. None in case the object does not exist.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        return await self.get(
            uuid=uuid,
            additional_properties=additional_properties,
            with_vector=with_vector,
        )

    async def get(self,
            uuid: Union[str, uuid_lib.UUID, None]=None,
            additional_properties: Optional[Union[List[str], str]]=None,
            with_vector: bool=False,
            limit: Optional[int]=None,
            offset: Optional[int]=None,
        ) -> Optional[Union[List[dict], dict]]:
        """
        Gets objects from Weaviate, the default maximum number of objects depends of Weaviate
        server's 'QUERY_DEFAULTS_LIMIT'. If 'uuid' is None a maximum of 'QUERY_DEFAULTS_LIMIT'
        objects are returned, use 'limit' argument to query more than 'QUERY_DEFAULTS_LIMIT'.
        If 'uuid' is specified the result is the same as for method '.get_by_uuid(...)'. One could
        use the 'offset' argument to specify a starting index for object retrival.
        NOTE: If 'offset' is 10 and 'limit' is 100, then objects 11-100 are returned (if there are
        that many). If 'offset' is larger than number of objects in Weaviate, then en empty list is
        returned.

        Parameters
        ----------
        uuid : str, uuid.UUID or None, optional
            The identifier of the object that should be retrieved.
        additional_properties : list of str, str or None, optional
            Additional properties that should be included in the request, by default None
        with_vector: bool, optional
            If True the 'vector' property will be returned too, by default False.
        limit : int or None, optional
            The maximum number of objects to be returned.
        offset : int or None
            The starting index for object retrival.


        Returns
        -------
        List[dict], dict or None
            A list of all objects (in case NO 'uuid' was provided), or empty list if no object were
            found. A 'dict' if an 'uuid' was provided, or None if object was not found.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path, params = pre_get(
            uuid=uuid,
            additional_properties=additional_properties,
            with_vector=with_vector,
            limit=limit,
            offset=offset,
        )
        try:
            response = await self._requests.get(
                path=path,
                params=params,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Could not get object/s due to connection error.'
            ) from conn_err
        if response.status == 200:
            if uuid is None:
                return await response.json()['objects']
            return await response.json()
        if uuid is not None and response.status == 404:
            return None
        raise UnsuccessfulStatusCodeError(
            "Get object/s.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def delete(self, uuid: Union[str, uuid_lib.UUID]) -> None:
        """
        Delete an existing object from Weaviate.

        Parameters
        ----------
        uuid : str or uuid.UUID
            The ID of the object that should be deleted.

        Examples
        --------
        >>> await client.data_object.get("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
        {
            "additional": {},
            "class": "Author",
            "creationTimeUnix": 1617112817487,
            "id": "d842a0f4-ad8c-40eb-80b4-bfefc7b1b530",
            "lastUpdateTimeUnix": 1617112817487,
            "properties": {
                "age": 46,
                "name": "H.P. Lovecraft"
            },
            "vectorWeights": null
        }
        >>> await client.data_object.delete("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
        >>> await client.data_object.get("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
        None

        Raises
        ------
        TypeError
            If 'uuid' is not of type 'str'.
        ValueError
            If 'uuid' is not properly formed.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path = pre_delete_exists(
            uuid=uuid,
        )
        try:
            response = await self._requests.delete(
                path=path,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Object could not be deleted due to connection error.'
            ) from conn_err
        if response.status == 204:
            return
        raise UnsuccessfulStatusCodeError(
            "Delete object.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def exists(self, uuid: Union[str, uuid_lib.UUID]) -> bool:
        """
        Check if the object exist in Weaviate.

        Parameters
        ----------
        uuid : str or uuid.UUID
            The UUID of the object that may or may not exist within Weaviate.

        Examples
        --------
        >>> await client.data_object.exists('e067f671-1202-42c6-848b-ff4d1eb804ab')
        False
        >>> await client.data_object.create(
        ...     data_object = {'name': 'Andrzej Sapkowski', 'age': 72},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        >>> await client.data_object.exists('e067f671-1202-42c6-848b-ff4d1eb804ab')
        True

        Returns
        -------
        bool
            True if object exists, False otherwise.

        Raises
        ------
        TypeError
            If 'uuid' is not of type 'str'.
        ValueError
            If 'uuid' is not properly formed.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path = pre_delete_exists(
            uuid=uuid,
        )
        try:
            response = await self._requests.head(
                path=path,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Could not check if object exists due to connection error.'
            ) from conn_err

        if response.status == 204:
            return True
        if response.status == 404:
            return False
        raise UnsuccessfulStatusCodeError(
            "Object exists.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def validate(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID, None]=None,
            vector: Optional[Sequence[Real]]=None
        ) -> dict:
        """
        Validate an object against Weaviate.

        Parameters
        ----------
        data_object : dict
            Object to be validated.
        class_name : str
            Name of the class of the object that should be validated.
        uuid : str, uuid.UUID or None, optional
            The UUID of the object that should be validated against Weaviate, by default None.
        vector: Sequence[Real] or None, optional
            The embedding of the object that should be validated. Used only class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor', by default None.

        Examples
        --------
        Assume we have a Author class only 'name' property, NO 'age'.

        >>> await client.data_object.validate(
        ...     data_object = {'name': 'H. Lovecraft'},
        ...     class_name = 'Author'
        ... )
        {'error': None, 'valid': True}
        >>> await client.data_object.validate(
        ...     data_object = {'name': 'H. Lovecraft', 'age': 46},
        ...     class_name = 'Author'
        ... )
        {
            "error": [
                {
                "message": "invalid object: no such prop with name 'age' found in class 'Author'
                    in the schema. Check your schema files for which properties in this class are
                    available"
                }
            ],
            "valid": False
        }

        Returns
        -------
        dict
            Validation result. E.g. {"valid": bool, "error": None or list}

        Raises
        ------
        TypeError
            If argument is of wrong type.
        ValueError
            If argument contains an invalid value.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If validating the object against Weaviate failed with a different reason.
        """

        path, weaviate_obj = pre_validate(
            data_object=data_object,
            class_name=class_name,
            uuid=uuid,
            vector=vector,
        )
        try:
            response = await self._requests.post(
                path=path,
                data_json=weaviate_obj,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Object was not validated due to connection error.'
            ) from conn_err

        result: dict = {
            "error": None
        }

        if response.status == 200:
            result["valid"] = True
            return result
        if response.status == 422:
            result["valid"] = False
            result["error"] = await response.json()["error"]
            return result
        raise UnsuccessfulStatusCodeError(
            "Validate object.",
            status_code=response.status,
            response_message=await response.text(),
        )
