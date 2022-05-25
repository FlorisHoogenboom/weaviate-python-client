"""
BaseDataObject class definition.
"""
import uuid as uuid_lib
from abc import ABC, abstractmethod
from numbers import Real
from typing import Union, Optional, List, Sequence, Tuple
from weaviate.util import (
    get_vector,
    get_valid_uuid,
    capitalize_first_letter,
)


class BaseDataObject(ABC):
    """
    BaseDataObject abstract class used to manipulate object to/from Weaviate.
    """

    @abstractmethod
    def create(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID, None]=None,
            vector: Optional[Sequence[Real]]=None,
        ):
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
        """

    @abstractmethod
    def update(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID],
            vector: Optional[Sequence[Real]]=None,
        ):
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
        """

    @abstractmethod
    def replace(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID],
            vector: Optional[Sequence[Real]]=None,
        ):
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
        """

    @abstractmethod
    def get_by_id(self,
            uuid: Union[str, uuid_lib.UUID],
            additional_properties: Optional[Union[List[str], str]]=None,
            with_vector: bool=False,
        ):
        """
        Get an object as dict.

        Parameters
        ----------
        uuid : str or uuid.UUID
            The UUID of the object that should be retrieved.
        additional_properties : list of str, str or None, optional
            Additional property/ies that should be included in the request, by default None.
        with_vector: bool
            If True the 'vector' property will be returned too, by default False.
        """

    @abstractmethod
    def get(self,
            uuid: Union[str, uuid_lib.UUID, None]=None,
            additional_properties: Optional[Union[List[str], str]]=None,
            with_vector: bool=False,
            limit: Optional[int]=None,
            offset: Optional[int]=None,
        ):
        """
        Gets objects from Weaviate, the default maximum number of objects depends of Weaviate
        server's 'QUERY_DEFAULTS_LIMIT'. If 'uuid' is None a maximum of 'QUERY_DEFAULTS_LIMIT'
        objects are returned, use 'limit' argument to query more than 'QUERY_DEFAULTS_LIMIT'.
        If 'uuid' is specified the result is the same as for method '.get_by_uuid(...)'. One could
        use the 'offset' argument to specify a starting index for object retrieval.
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
            The starting index for object retrieval.
        """

    @abstractmethod
    def delete(self, uuid: Union[str, uuid_lib.UUID]):
        """
        Delete an existing object from Weaviate.

        Parameters
        ----------
        uuid : str or uuid.UUID
            The UUID of the object that should be deleted.
        """

    @abstractmethod
    def exists(self, uuid: Union[str, uuid_lib.UUID]):
        """
        Check if the object exist in Weaviate.

        Parameters
        ----------
        uuid : str or uuid.UUID
            The UUID of the object that may or may not exist within Weaviate.
        """

    @abstractmethod
    def validate(self,
            data_object: dict,
            class_name: str,
            uuid: Union[str, uuid_lib.UUID, None]=None,
            vector: Optional[Sequence[Real]]=None
        ):
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
        """


def pre_create(
        data_object: dict,
        class_name: str,
        uuid: Union[str, uuid_lib.UUID, None],
        vector: Optional[Sequence[Real]],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    data_object : dict
        The new object to add to Weaviate. It represents the class instance properties only.
    class_name : str
        The class name associated with the object given.
    uuid : str, uuid.UUID or None
        The object's UUID. The object to will have this UUID if it is provided, otherwise
        Weaviate will generate an UUID for this object.
    vector: Sequence[Real] or None
        The embedding of the object that should be created. Used only for class objects that
        do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
        'torch.Tensor' and 'tf.Tensor'.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    if not isinstance(class_name, str):
        raise TypeError(
            f"'class_name' must be of type str. Given type: {type(class_name)}."
        )

    if not isinstance(data_object, dict):
        raise TypeError(
            f"'data_object' must be of type dict. Given type: {type(data_object)}."
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

    return path, weaviate_obj


def pre_update(
        data_object: dict,
        class_name: str,
        uuid: Union[str, uuid_lib.UUID],
        vector: Optional[Sequence[Real]],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    data_object : dict
        The object's property/ies that should be updated. Fields not specified by in the
        'data_object' remain unchanged. Fields that are None will not be changed.
    class_name : str
        The class name of the object that should be updated.
    uuid : str or uuid.UUID
        The object's UUID which should be updated.
    vector: Sequence[Real] or None
        The embedding of the object that should be updated. Used only for class objects that
        do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
        'torch.Tensor' and 'tf.Tensor'.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    if not isinstance(class_name, str):
        raise TypeError(
            f"'class_name' must be of type str. Given type: {type(class_name)}"
        )

    if not isinstance(data_object, dict):
        raise TypeError(
            f"'data_object' must be of type dict. Given type: {type(data_object)}."
        )

    weaviate_obj = {
        "id": get_valid_uuid(uuid),
        "class": capitalize_first_letter(class_name),
        "properties": data_object,
    }

    if vector is not None:
        weaviate_obj['vector'] = get_vector(vector)

    path = f"/objects/{uuid}"

    return path, weaviate_obj


def pre_replace(
        data_object: dict,
        class_name: str,
        uuid: Union[str, uuid_lib.UUID],
        vector: Optional[Sequence[Real]],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    data_object : dict
        The new object to be replaced with.
    class_name : str
        The class name of the object that should be replaced.
    uuid : str or uuid.UUID
        The object's UUID which should be replaced.
    vector: Sequence[Real] or None
        The embedding of the object that should be replaced. Used only for class objects that
        do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
        'torch.Tensor' and 'tf.Tensor'.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    if not isinstance(class_name, str):
        raise TypeError(
            f"'class_name' must be of type str. Given type: {type(class_name)}"
        )

    if not isinstance(data_object, dict):
        raise TypeError(
            f"'data_object' must be of type dict. Given type: {type(data_object)}."
        )

    weaviate_obj = {
        "id": get_valid_uuid(uuid),
        "class": capitalize_first_letter(class_name),
        "properties": data_object,
    }

    if vector is not None:
        weaviate_obj['vector'] = get_vector(vector)

    path = f"/objects/{uuid}"

    return path, weaviate_obj


def pre_validate(
        data_object: dict,
        class_name: str,
        uuid: Union[str, uuid_lib.UUID, None],
        vector: Optional[Sequence[Real]],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    data_object : dict
        Object to be validated.
    class_name : str
        Name of the class of the object that should be validated.
    uuid : str, uuid.UUID or None
        The UUID of the object that should be validated against Weaviate.
    vector: Sequence[Real] or None
        The embedding of the object that should be validated. Used only class objects that
        do not have a vectorization module. Supported types are 'list', 'numpy.ndarray',
        'torch.Tensor' and 'tf.Tensor'.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    if not isinstance(class_name, str):
        raise TypeError(
            f"'class_name' must be of type str. Given type: {type(class_name)}."
        )

    if not isinstance(data_object, dict):
        raise TypeError(
            f"'data_object' must be of type dict. Given type: {type(data_object)}."
        )

    weaviate_obj = {
        "class": capitalize_first_letter(class_name),
        "properties": data_object,
    }

    if uuid is not None:
        weaviate_obj['id'] = get_valid_uuid(uuid)

    if vector is not None:
        weaviate_obj['vector'] = get_vector(vector)

    path = "/objects/validate"

    return path, weaviate_obj


def pre_get(
        uuid: Union[str, uuid_lib.UUID, None],
        additional_properties: Optional[Union[List[str], str]],
        with_vector: bool,
        limit: Optional[int],
        offset: Optional[int],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    uuid : str, uuid.UUID or None
        The identifier of the object that should be retrieved.
    additional_properties : list of str, str or None
        Additional properties that should be included in the request.
    with_vector: bool,
        If True the 'vector' property will be returned too.
    limit : int or None
        The maximum number of objects to be returned.
    offset : int or None
        The starting index for object retrieval.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the request parameters.
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

    return path, params


def pre_delete_exists(uuid: Union[str, uuid_lib.UUID]) -> str:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    uuid : str or uuid.UUID
        The UUID of the object that should be deleted or check if exists.

    Returns
    -------
    str
        The path to the Weaviate resource.
    """

    path = f"/objects/{get_valid_uuid(uuid)}"

    return path


def _get_params(
        additional_properties: Optional[Union[List[str], str]],
        with_vector: bool,
        limit: Optional[int],
        offset: Optional[int],
    ) -> dict:
    """
    Get underscore properties in the format accepted by Weaviate.

    Parameters
    ----------
    additional_properties : list of str, str or None
        Additional property/ies to include in object description.
    with_vector: bool
        If True the 'vector' property will be returned too.
    limit : int or None
        The maximum number of objects to be returned.
    offset : int or None
        The starting index for object retrieval.

    Returns
    -------
    dict
        A dictionary including Weaviate-accepted additional properties
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
