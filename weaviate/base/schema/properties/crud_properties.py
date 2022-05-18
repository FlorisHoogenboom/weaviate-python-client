"""
BaseProperty class definition.
"""
from abc import ABC, abstractmethod
from weaviate.util import capitalize_first_letter
from ..validate_schema import check_property


class BaseProperty(ABC):
    """
    Property class used to create object properties.
    """

    @abstractmethod
    def create(self, schema_class_name: str, schema_property: dict):
        """
        Create a class property.

        Parameters
        ----------
        schema_class_name : str
            The name of the schema class to which the property should be added.
        schema_property : dict
            The property that should be added.
        """


def pre_create(schema_class_name: str, schema_property: dict) -> str:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    schema_class_name : str
        The name of the schema class to which the property should be added.
    schema_property : dict
        The property that should be added.

    Returns
    -------
    str
        The path to the Weaviate resource.
    """

    if not isinstance(schema_class_name, str):
        raise TypeError(
            f"'schema_class_name' must be of type str. Given type: {type(schema_class_name)}."
        )

    if not isinstance(schema_property, str):
        raise TypeError(
            f"'schema_property' must be of type dict. Given type: {type(schema_property)}."
        )

    check_property(
        class_property=schema_property,
        class_name=schema_class_name,
    )

    schema_class_name = capitalize_first_letter(schema_class_name)

    path = f"/schema/{schema_class_name}/properties"

    return path
