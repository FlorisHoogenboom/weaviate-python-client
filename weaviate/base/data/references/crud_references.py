"""
BaseReference class definition.
"""
import uuid
from typing import Union, Tuple
from abc import ABC, abstractmethod
from weaviate.util import get_valid_uuid


class BaseReference(ABC):
    """
    BaseReference abstract class used to manipulate references within objects.
    """

    @abstractmethod
    def delete(self,
            from_uuid: Union[str, uuid.UUID],
            from_property_name: str,
            to_uuid: Union[str, uuid.UUID],
        ):
        """
        Remove a reference to another object. Equal to removing one direction of an edge from the
        graph.

        Parameters
        ----------
        from_uuid : str or uuid.UUID
            The UUID of the object for which to delete the reference.
        from_property_name : str
            The property that contains the reference that should be deleted.
        to_uuid : str or uuid.UUID
            The UUID of the referenced object.
        """

    @abstractmethod
    def replace(self,
            from_uuid: Union[str, uuid.UUID],
            from_property_name: str,
            to_uuids: Union[list, str, uuid.UUID],
        ):
        """
        Allows to replace ALL references in that property with a new set of references.
        NOTE: All old references will be deleted.

        Parameters
        ----------
        from_uuid : str or uuid.UUID
            The UUID of the object for which to replace the reference/s.
        from_property_name : str
            The property that contains the reference that should be replaced.
        to_uuids : list, str or uuid.UUID
            The UUIDs of the objects that should be referenced. If 'str' it is converted internally
            into a list of str.
        """

    @abstractmethod
    def add(self,
            from_uuid: Union[str, uuid.UUID],
            from_property_name: str,
            to_uuid: Union[str, uuid.UUID],
        ):
        """
        Allows to link an object to an object unidirectionally.

        Parameters
        ----------
         from_uuid : str or uuid.UUID
            The UUID of the object for which to add the reference.
        from_property_name : str
            The property for which to create the reference.
        to_uuid : str or uuid.UUID
            The UUID of the referenced object.
        """

        # Validate and create Beacon
        from_uuid = get_valid_uuid(from_uuid)
        to_uuid = get_valid_uuid(to_uuid)
        _validate_property_name(from_property_name)
        beacons = _get_beacon(to_uuid)

        path = f"/objects/{from_uuid}/references/{from_property_name}"

        return path, beacons


def _get_beacon(to_uuid: Union[str, uuid.UUID]) -> dict:
    """
    Get a weaviate-style beacon.

    Parameters
    ----------
    to_uuid : str or uuid.UUID
        The UUID to create beacon for.

    Returns
    -------
    dict
        Weaviate-style beacon as a dict.
    """

    return {
        "beacon": f"weaviate://localhost/{to_uuid}"
    }


def _validate_property_name(property_name: str) -> None:
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

    if not isinstance(property_name, str):
        raise TypeError(
            f"'from_property_name' must be of type str. Given type: {type(property_name)}."
        )


def pre_delete(
        from_uuid: Union[str, uuid.UUID],
        from_property_name: str,
        to_uuid: Union[str, uuid.UUID],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    from_uuid : str or uuid.UUID
        The UUID of the object for which to delete the reference.
    from_property_name : str
        The property that contains the reference that should be deleted.
    to_uuid : str or uuid.UUID
        The UUID of the referenced object.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    # Validate and create Beacon
    from_uuid = get_valid_uuid(from_uuid)
    to_uuid = get_valid_uuid(to_uuid)
    _validate_property_name(from_property_name)
    beacon = _get_beacon(to_uuid)

    path = f"/objects/{from_uuid}/references/{from_property_name}"

    return path, beacon


def pre_replace(
        from_uuid: Union[str, uuid.UUID],
        from_property_name: str,
        to_uuids: Union[list, str, uuid.UUID],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    from_uuid : str or uuid.UUID
        The UUID of the object for which to replace the reference/s.
    from_property_name : str
        The property that contains the reference that should be replaced.
    to_uuids : list, str or uuid.UUID
        The UUIDs of the objects that should be referenced. If 'str' it is converted internally
        into a list of str.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    if not isinstance(to_uuids, list):
        to_uuids = [to_uuids]

    # Validate and create Beacon
    from_uuid = get_valid_uuid(from_uuid)
    _validate_property_name(from_property_name)
    beacons = []
    for to_uuid in to_uuids:
        to_uuid = get_valid_uuid(to_uuid)
        beacons.append(_get_beacon(to_uuid))

    path = f"/objects/{from_uuid}/references/{from_property_name}"

    return path, beacons


def pre_add(
        from_uuid: Union[str, uuid.UUID],
        from_property_name: str,
        to_uuid: Union[str, uuid.UUID],
    ) -> Tuple[str, dict]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    from_uuid : str or uuid.UUID
        The UUID of the object for which to add the reference.
    from_property_name : str
        The property for which to create the reference.
    to_uuid : str or uuid.UUID
        The UUID of the referenced object.

    Returns
    -------
    Tuple[str, dict]
        The path to the Weaviate resource and the payload.
    """

    # Validate and create Beacon
    from_uuid = get_valid_uuid(from_uuid)
    to_uuid = get_valid_uuid(to_uuid)
    _validate_property_name(from_property_name)
    beacons = _get_beacon(to_uuid)

    path = f"/objects/{from_uuid}/references/{from_property_name}"

    return path, beacons
