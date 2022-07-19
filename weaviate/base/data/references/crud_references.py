"""
BaseReference class definition.
"""
import uuid
from typing import Union, Tuple, List
from abc import ABC, abstractmethod
from weaviate.util import (
    get_valid_uuid,
    generate_local_beacon,
    capitalize_first_letter,
)


class BaseReference(ABC):
    """
    BaseReference abstract class used to manipulate references within objects.
    """

    @abstractmethod
    def delete(self,
            from_uuid: Union[str, uuid.UUID],
            from_class_name: str,
            from_property_name: str,
            to_uuid: Union[str, uuid.UUID],
            to_class_name: str,
        ):
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
        """

    @abstractmethod
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
        """

    @abstractmethod
    def add(self,
            from_uuid: Union[str, uuid.UUID],
            from_class_name: str,
            from_property_name: str,
            to_uuid: Union[str, uuid.UUID],
            to_class_name: str,
        ):
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
        """


def _validate_for_str(value: str, var_name: str) -> None:
    """
    Validate the property name type.

    Parameters
    ----------
    property_name : str
        Property name to be validated.

    Raises
    ------
    TypeError
        If 'property_name' is not of type str.
    """

    if not isinstance(value, str):
        raise TypeError(
            f"'{var_name}' must be of type str. Given type: {type(value)}."
        )


def pre_delete_and_add(
        from_uuid: Union[str, uuid.UUID],
        from_class_name: str,
        from_property_name: str,
        to_uuid: Union[str, uuid.UUID],
        to_class_name: str,
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate (for both 'delete' and 'add').

    Parameters
    ----------
    from_uuid : str or uuid.UUID
        The UUID of the object for which to add the reference.
    from_class_name : str
        The class name of the referencing object for which to delete/add the reference.
    from_property_name : str
        The property for which to create the reference.
    to_uuid : str or uuid.UUID
        The UUID of the referenced object.
    to_class_name : str
        The class name of the referenced object.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    from_uuid = get_valid_uuid(from_uuid)
    _validate_for_str(value=from_class_name, var_name='from_class_name')
    _validate_for_str(value=from_property_name, var_name='from_property_name')
    _validate_for_str(value=to_class_name, var_name='to_class_name')
    beacon = generate_local_beacon(uuid=to_uuid, class_name=to_class_name)
    from_class_name = capitalize_first_letter(from_class_name)
    path = f"/objects/{from_class_name}/{from_uuid}/references/{from_property_name}"

    return path, beacon


def pre_replace(
        from_uuid: Union[str, uuid.UUID],
        from_class_name: str,
        from_property_name: str,
        to_uuids: Union[list, str, uuid.UUID],
        to_class_names: Union[List[str], str],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

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

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    # Validate and create Beacon
    from_uuid = get_valid_uuid(from_uuid)
    _validate_for_str(value=from_class_name, var_name='from_class_name')
    _validate_for_str(value=from_property_name, var_name='from_property_name')
    from_class_name = capitalize_first_letter(from_class_name)

    beacons = []

    if not isinstance(to_uuids, list):
        to_uuids = [to_uuids]
    if isinstance(to_class_names, str):
        to_class_names = [to_class_names] * len(to_uuids)
    if len(to_uuids) != len(to_class_names):
        raise ValueError(
            "'to_class_names' and 'to_uuids' have different lengths, they must match."
        )

    for to_uuid, to_class_name in zip(to_uuids, to_class_names):
        _validate_for_str(value=to_class_name, var_name='to_class_name[i]')
        beacons.append(
            generate_local_beacon(uuid=to_uuid, class_name=to_class_name)
        )

    path = f"/objects/{from_class_name}/{from_uuid}/references/{from_property_name}"

    return path, beacons
