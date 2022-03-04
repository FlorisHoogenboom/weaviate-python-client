"""
BaseReference class definition.
"""
from typing import Union
from abc import ABC, abstractmethod
from weaviate.util import get_valid_uuid


class BaseReference(ABC):
    """
    BaseReference abstract class used to manipulate references within objects.
    """

    @abstractmethod
    def delete(self, from_uuid: str, from_property_name: str, to_uuid: str):
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
        """

        # Validate and create Beacon
        from_uuid = get_valid_uuid(from_uuid)
        to_uuid = get_valid_uuid(to_uuid)
        _validate_property_name(from_property_name)
        beacon = _get_beacon(to_uuid)

        path = f"/objects/{from_uuid}/references/{from_property_name}"

        return path, beacon

    @abstractmethod
    def replace(self, from_uuid: str, from_property_name: str, to_uuids: Union[list, str]) -> None:
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

    @abstractmethod
    def add(self, from_uuid: str, from_property_name: str, to_uuid: str) -> None:
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
        """

        # Validate and create Beacon
        from_uuid = get_valid_uuid(from_uuid)
        to_uuid = get_valid_uuid(to_uuid)
        _validate_property_name(from_property_name)
        beacons = _get_beacon(to_uuid)

        path = f"/objects/{from_uuid}/references/{from_property_name}"

        return path, beacons


def _get_beacon(to_uuid: str) -> dict:
    """
    Get a weaviate-style beacon.

    Parameters
    ----------
    to_uuid : str
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
