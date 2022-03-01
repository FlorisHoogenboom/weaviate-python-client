"""
DataObject class definition.
"""
from numbers import Real
from typing import Union, Optional, List, Sequence
from weaviate.connect import Connection
from weaviate.exceptions import (
    ObjectAlreadyExistsError,
    WeaviateConnectionError,
    UnsuccessfulStatusCodeError,
)
from weaviate.util import (
    get_vector,
    get_valid_uuid,
    capitalize_first_letter,
)
from weaviate.data.references import Reference


class DataObject:
    """
    DataObject class used to manipulate object to/from weaviate. This class has CRUD methods.

    Attributes
    ----------
    reference : weaviate.data.references.Reference
        A Reference object to create objects cross-references.
    """

    def __init__(self, connection: Connection):
        """
        Initialize a DataObject class instance.

        Parameters
        ----------
        connection : weaviate.connect.Connection
            Connection object to an active and running weaviate instance.
        """

        self._connection = connection
        self.reference = Reference(self._connection)

    def create(self,
            data_object: dict,
            class_name: str,
            uuid: str=None,
            vector: Sequence[Real]=None,
        ) -> str:
        """
        Create a new object in Weaviate.

        Parameters
        ----------
        data_object : dict
            The new object to add to Weaviate. It represents the class instance properties only.
        class_name : str
            The class name associated with the object given.
        uuid : str, optional
            The object's UUID. The object to will have this uuid if it is provided, otherwise
            weaviate will generate a UUID for this object, by default None.
        vector: Sequence, optional
            The embedding of the object that should be created. Used only for class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor', by default None.

        Examples
        --------
        Schema contains a class Author with only 'name' and 'age' primitive property.

        >>> client.data_object.create(
        ...     data_object = {'name': 'Neil Gaiman', 'age': 60},
        ...     class_name = 'Author',
        ... )
        '46091506-e3a0-41a4-9597-10e3064d8e2d'
        >>> client.data_object.create(
        ...     data_object = {'name': 'Andrzej Sapkowski', 'age': 72},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        'e067f671-1202-42c6-848b-ff4d1eb804ab'

        Returns
        -------
        str
            Returns the UUID of the created object.

        Raises
        ------
        TypeError
            If one of the arguments is of wrong data type.
        ValueError
            If one of the arguments has an invalid value.
        weaviate.exception.ObjectAlreadyExistsException
            If an object with the given uuid already exists within weaviate.
        requests.exceptions.ConnectionError
            If the network connection to weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If creating the object in Weaviate failed for a different reason other than connection,
            more information is given in the error message.
        """

        if not isinstance(class_name, str):
            raise TypeError(
                f"'class_name' must be of type 'str'. Given type: {type(class_name)}."
            )
        
        if not isinstance(data_object, dict):
            raise TypeError(
                f"'data_object' must be of type 'dict'. Given type: {type(data_object)}."
            )

        weaviate_obj = {
            "class": capitalize_first_letter(class_name),
            "properties": data_object,
        }
        if uuid is not None:
            weaviate_obj["id"] = get_valid_uuid(uuid)

        if vector is not None:
            weaviate_obj["vector"] = get_vector(vector)

        path = "/objects"
        try:
            response = self._connection.post(
                path=path,
                data_json=weaviate_obj,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Object was not added due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return str(response.json()["id"])

        object_does_already_exist = False
        try:
            if 'already exists' in response.json()['error'][0]['message']:
                object_does_already_exist = True
        except KeyError:
            pass
        if object_does_already_exist:
            raise ObjectAlreadyExistsError(weaviate_obj["id"])
        raise UnsuccessfulStatusCodeError("Creating object.", response)

    def update(self,
            data_object: dict,
            class_name: str,
            uuid: str,
            vector: Sequence[Real]=None,
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
        uuid : str
            The object's UUID which should be updated.
        vector: Sequence, optional
            The embedding of the object that should be updated. Used only for class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor', by default None.

        Examples
        --------
        >>> author_id = client.data_object.create(
        ...     data_object = {'name': 'Philip Pullman', 'age': 64},
        ...     class_name = 'Author'
        ... )
        >>> client.data_object.get(author_id)
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
        >>> client.data_object.update(
        ...     data_object = {'age': 74},
        ...     class_name = 'Author',
        ...     uuid = author_id
        ... )
        >>> client.data_object.get(author_id)
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If updating the object in Weaviate failed for a different reason than connection,
            more information is given in the error message.
        """

        if not isinstance(class_name, str):
            raise TypeError(
                f"'class_name' must be of type 'str'. Given type: {type(class_name)}"
            )

        if not isinstance(data_object, dict):
            raise TypeError(
                f"'data_object' must be of type 'dict'. Given type: {type(data_object)}."
            )

        weaviate_obj = {
            "id": get_valid_uuid(uuid),
            "class": capitalize_first_letter(class_name),
            "properties": data_object,
        }

        if vector is not None:
            weaviate_obj['vector'] = get_vector(vector)

        path = f"/objects/{uuid}"

        try:
            response = self._connection.patch(
                path=path,
                data_json=weaviate_obj,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Object was not updated due to connection error.'
            ) from conn_err
        if response.status_code == 204:
            return
        raise UnsuccessfulStatusCodeError("Update of the object not successful.", response)

    def replace(self,
            data_object: dict,
            class_name: str,
            uuid: str,
            vector: Sequence[Real]=None,
        ) -> None:
        """
        Replace an already existing object with a new one. This method replaces the whole object.

        Parameters
        ----------
        data_object : dict
            The new object to be replaced with.
        class_name : str
            The class name of the object that should be replaced.
        uuid : str
            The object's UUID which should be replaced.
        vector: Sequence, optional
            The embedding of the object that should be replaced. Used only for class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor', by default None.

        Examples
        --------
        >>> author_id = client.data_object.create(
        ...     data_object = {'name': 'H. Lovecraft', 'age': 46},
        ...     class_name = 'Author'
        ... )
        >>> client.data_object.get(author_id)
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
        >>> client.data_object.replace(
        ...     data_object = {'name': 'H.P. Lovecraft'},
        ...     class_name = 'Author',
        ...     uuid = author_id
        ... )
        >>> client.data_object.get(author_id)
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If replacing the object in Weaviate failed for a different reason than connection,
            more information is given in the error message.
        """

        if not isinstance(class_name, str):
            raise TypeError(
                f"'class_name' must be of type 'str'. Given type: {type(class_name)}"
            )

        if not isinstance(data_object, dict):
            raise TypeError(
                f"'data_object' must be of type 'dict'. Given type: {type(data_object)}."
            )

        weaviate_obj = {
            "id": get_valid_uuid(uuid),
            "class": capitalize_first_letter(class_name),
            "properties": data_object,
        }

        if vector is not None:
            weaviate_obj['vector'] = get_vector(vector)

        path = f"/objects/{uuid}"
        try:
            response = self._connection.put(
                path=path,
                data_json=weaviate_obj,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Object was not replaced due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            return
        raise UnsuccessfulStatusCodeError("Replace object.", response)

    def get_by_id(self,
            uuid: str,
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
        >>> client.data_object.get_by_id("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        return self.get(
            uuid=uuid,
            additional_properties=additional_properties,
            with_vector=with_vector,
        )

    def get(self,
            uuid: Optional[str]=None,
            additional_properties: Optional[Union[List[str], str]]=None,
            with_vector: bool=False,
            limit: Optional[int]=None,
            offset: Optional[int]=None,
        ) -> Optional[Union[List[dict], dict]]:
        """
        Gets objects from weaviate, the default maximum number of objects depends of Weaviate
        server's 'QUERY_DEFAULTS_LIMIT'. If 'uuid' is None a maximum of 'QUERY_DEFAULTS_LIMIT'
        objects are returned, use 'limit' argument to query more than 'QUERY_DEFAULTS_LIMIT'.
        If 'uuid' is specified the result is the same as for method '.get_by_uuid(...)'. One could
        use the 'offset' argument to specify a starting index for object retrival.
        NOTE: If 'offset' is 10 and 'limit' is 100, then objects 11-100 are returned (if there are
        that many). If 'offset' is larger than number of objects in Weaviate, then en empty list is
        returned.

        Parameters
        ----------
        uuid : str, optional
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        params = _get_params(
            additional_properties=additional_properties,
            with_vector=with_vector,
            limit=limit,
            offset=offset,
        )

        if uuid is not None:
            path = "/objects/" + get_valid_uuid(uuid)
        else:
            path = "/objects"

        try:
            response = self._connection.get(
                path=path,
                params=params,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Could not get object/s due to connection error.'
            ) from conn_err
        if response.status_code == 200:
            if uuid is None:
                return response.json()['objects']
            return response.json()
        if uuid is not None and response.status_code == 404:
            return None
        raise UnsuccessfulStatusCodeError("Get object/s.", response)

    def delete(self, uuid: str) -> None:
        """
        Delete an existing object from weaviate.

        Parameters
        ----------
        uuid : str
            The ID of the object that should be deleted.

        Examples
        --------
        >>> client.data_object.get("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
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
        >>> client.data_object.delete("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
        >>> client.data_object.get("d842a0f4-ad8c-40eb-80b4-bfefc7b1b530")
        None

        Raises
        ------
        TypeError
            If 'uuid' is not of type 'str'.
        ValueError
            If 'uuid' is not properly formed.
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path = f"/objects/{get_valid_uuid(uuid)}"
        try:
            response = self._connection.delete(
                path=path,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Object could not be deleted due to connection error.'
            ) from conn_err
        if response.status_code == 204:
            return
        raise UnsuccessfulStatusCodeError("Delete object.", response)

    def exists(self, uuid: str) -> bool:
        """
        Check if the object exist in weaviate.

        Parameters
        ----------
        uuid : str
            The UUID of the object that may or may not exist within weaviate.

        Examples
        --------
        >>> client.data_object.exists('e067f671-1202-42c6-848b-ff4d1eb804ab')
        False
        >>> client.data_object.create(
        ...     data_object = {'name': 'Andrzej Sapkowski', 'age': 72},
        ...     class_name = 'Author',
        ...     uuid = 'e067f671-1202-42c6-848b-ff4d1eb804ab'
        ... )
        >>> client.data_object.exists('e067f671-1202-42c6-848b-ff4d1eb804ab')
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path = f'/objects/{get_valid_uuid(uuid)}'
        try:
            response = self._connection.head(
                path=path,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Could not check if object exists due to connection error.'
            ) from conn_err

        if response.status_code == 204:
            return True
        if response.status_code == 404:
            return False
        raise UnsuccessfulStatusCodeError("Object exists.", response)

    def validate(self,
            data_object: dict,
            class_name: str,
            uuid: Optional[str]=None,
            vector: Optional[Sequence[Real]]=None
        ) -> dict:
        """
        Validate an object against weaviate.

        Parameters
        ----------
        data_object : dict
            Object to be validated.
        class_name : str
            Name of the class of the object that should be validated.
        uuid : str or None, optional
            The UUID of the object that should be validated against weaviate.
            by default None.
        vector: Sequence[Real] or None, optional
            The embedding of the object that should be validated. Used only class objects that
            do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor',
            by default None.

        Examples
        --------
        Assume we have a Author class only 'name' property, NO 'age'.

        >>> client.data_object.validate(
        ...     data_object = {'name': 'H. Lovecraft'},
        ...     class_name = 'Author'
        ... )
        {'error': None, 'valid': True}
        >>> client.data_object.validate(
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If validating the object against Weaviate failed with a different reason.
        """

        if not isinstance(class_name, str):
            raise TypeError(
                f"'class_name' must be of type 'str'. Given type: {type(class_name)}."
            )

        if not isinstance(data_object, dict):
            raise TypeError(
                f"'data_object' must be of type 'dict'. Given type: {type(data_object)}."
            )

        weaviate_obj = {
            "class": capitalize_first_letter(class_name),
            "properties": data_object,
        }

        if uuid is not None:
            if not isinstance(uuid, str):
                raise TypeError("UUID must be of type 'str'.")
            weaviate_obj['id'] = uuid

        if vector is not None:
            weaviate_obj['vector'] = get_vector(vector)

        path = "/objects/validate"
        try:
            response = self._connection.post(
                path=path,
                data_json=weaviate_obj,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Object was not validated due to connection error.'
            ) from conn_err

        result: dict = {
            "error": None
        }

        if response.status_code == 200:
            result["valid"] = True
            return result
        if response.status_code == 422:
            result["valid"] = False
            result["error"] = response.json()["error"]
            return result
        raise UnsuccessfulStatusCodeError("Validate object.", response)


def _get_params(
        additional_properties: Optional[Union[List[str], str]],
        with_vector: bool,
        limit: Optional[int],
        offset: Optional[int],
    ) -> dict:
    """
    Get underscore properties in the format accepted by weaviate.

    Parameters
    ----------
    additional_properties : list of str, str or None
        Additional property/ies to include in object description.
    with_vector: bool
        If True the 'vector' property will be returned too.
    limit : int or None
        The maximum number of objects to be returned.
    offset : int or None
        The starting index for object retrival.

    Returns
    -------
    dict
        A dictionary including weaviate-accepted additional properties
        and/or 'vector' property.
    """

    params = {}

    if additional_properties:
        if isinstance(additional_properties, list):
            params['include'] = additional_properties
        else:
            params['include'] = [additional_properties]

    if with_vector:
        if 'include' in params:
            params['include'].append('vector')
        else:
            params['include'] = 'vector'

    if limit:
        params['limit'] = limit

    if limit:
        params['offset'] = offset

    return params
