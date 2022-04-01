"""
BaseSchema class definition.
"""
from abc import ABC, abstractmethod
from typing import Optional, Tuple
from weaviate.exceptions import SchemaValidationError
from weaviate.util import capitalize_first_letter
from .validate_schema import validate_schema, check_class


PRIMITIVE_WEAVIATE_TYPES = set(
    [
        "string",
        "string[]",
        "int",
        "int[]",
        "boolean",
        "boolean[]",
        "number",
        "number[]",
        "date",
        "date[]",
        "text",
        "text[]",
        "geoCoordinates",
        "blob",
        "phoneNumber"
    ]
)


class BaseSchema(ABC):
    """
    BaseSchema abstract class used to interact and manipulate schemas or classes.
    """

    @abstractmethod
    def create(self, schema: dict):
        """
        Create the schema at the Weaviate instance.

        Parameters
        ----------
        schema : dict
            The schema to be created.
        """

    @abstractmethod
    def create_class(self, schema_class: dict):
        """
        Create a single class as part of the schema in Weaviate.

        Parameters
        ----------
        schema_class : dict or str
            The schema class to be created.
        """

    @abstractmethod
    def delete_class(self, class_name: str) -> None:
        """
        Delete a schema class from Weaviate. This deletes all associated data.

        Parameters
        ----------
        class_name : str
            The class that should be deleted from schema.
        """

    @abstractmethod
    def delete_all(self) -> None:
        """
        Remove the entire schema from the Weaviate instance and all data associated with it.
        """

    @abstractmethod
    def contains(self, schema: Optional[dict]=None):
        """
        Check if Weaviate already contains a schema.

        Parameters
        ----------
        schema : dict or None, optional
            The (sub-)schema to check if is part of the Weaviate existing schema. If a 'schema' is
            not None, it checks if this specific schema is already loaded. If the given schema is a
            subset of the loaded schema it will still return True. If 'schema' is None it checks
            for any existing schema, by default None.
        """

    @abstractmethod
    def update_config(self, class_name: str, config: dict):
        """
        Update a schema configuration for a specific class.

        Parameters
        ----------
        class_name : str
            The class for which to update the schema configuration.
        config : dict
            The configurations to update (MUST follow schema format).
        """

    @abstractmethod
    def get(self, class_name: Optional[str]=None):
        """
        Get the schema from Weaviate.

        Parameters
        ----------
        class_name : str or None, optional
            The class for which to return the schema. If NOT provided the whole schema is returned,
            otherwise only the schema of this class is returned. By default None.
        """


def pre_create(schema: dict) -> None:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    schema : dict
        The schema to be created.
    """

    if not isinstance(schema, dict):
        raise TypeError(
            f"'schema' must be of type dict. Given type: {type(schema)}."
        )
    validate_schema(schema=schema)


def pre_create_class(schema_class: dict) -> None:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    schema_class : dict or str
        The schema class to be created.
    """

    if not isinstance(schema_class, dict):
        raise TypeError(
            f"'schema_class' must be of type 'dict'. Given type: {type(schema_class)}."
        )
    check_class(schema_class)


def pre_delete_class(class_name: str) -> str:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    class_name : str
        The class that should be deleted from schema (removes all the data object of that class).

    Returns
    -------
    str
        The path to the resource.
    """

    if not isinstance(class_name, str):
        raise TypeError(
            f"'class_name' must be of type 'str'. Given type: {type(class_name)}."
        )

    path = f"/schema/{capitalize_first_letter(class_name)}"

    return path


def pre_contains(schema: Optional[dict]) -> None:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    schema : dict or None, optional
        The (sub-)schema to check if is part of the Weaviate existing schema. If a 'schema' is
        not None, it checks if this specific schema is already loaded. If the given schema is a
        subset of the loaded schema it will still return True. If 'schema' is None it checks
        for any existing schema.
    """

    if schema is not None:
        if not isinstance(schema, dict):
            raise TypeError(
                f"'schema' must be None or of type dict. Given type: {type(schema)}"
            )

def pre_update_config(class_name: str) -> Tuple[str, str]:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    class_name : str
        The class for which to update the schema configuration.

    Returns
    -------
    Tuple[str, str]
        The path to the resource and the class name.
    """

    class_name = capitalize_first_letter(class_name)
    path = f"/schema/{class_name}"

    return path, class_name


def pre_get(class_name: Optional[str]) -> str:
    """
    Pre-process before making a call to Weaviate.

    Parameters
    ----------
    class_name : Optional[str]
        The class for which to return the schema. If NOT provided the whole schema is returned,
        otherwise only the schema of this class is returned.

    Returns
    -------
    str
        The path to the resource.
    """

    path = '/schema'
    if class_name is not None:
        if not isinstance(class_name, str):
            raise TypeError(
                f"'class_name' must be of type str. Given type: {type(class_name)}."
            )
        path += capitalize_first_letter(class_name)

    return path


def get_class_schema_with_primitives_and_path(schema_class: dict) -> Tuple[dict, str]:
    """
    Validate and construct a new class schema with only primitive properties.

    Parameters
    ----------
    schema_class : dict
        Class schema passed by user.

    Returns
    -------
    Tuple[dict, str]
        Class schema with only primitive properties and the Weaviate path to the resource.
    """

    path = '/schema'

    schema_class_to_return = {
        'class': capitalize_first_letter(schema_class['class']),
        'properties': []
    }

    if 'description' in schema_class:
        schema_class_to_return['description'] = schema_class['description']

    if 'vectorIndexType' in schema_class:
        schema_class_to_return['vectorIndexType'] = schema_class['vectorIndexType']

    if 'vectorIndexConfig' in schema_class:
        schema_class_to_return['vectorIndexConfig'] = schema_class['vectorIndexConfig']

    if 'vectorizer' in schema_class:
        schema_class_to_return['vectorizer'] = schema_class['vectorizer']

    if 'moduleConfig' in schema_class:
        schema_class_to_return['moduleConfig'] = schema_class['moduleConfig']

    if 'shardingConfig' in schema_class:
        schema_class_to_return['shardingConfig'] = schema_class['shardingConfig']

    if 'properties' in schema_class:
        schema_class_to_return['properties'] = (
            _get_primitive_properties(
                properties_list=schema_class['properties'],
            )
        )
    return schema_class_to_return, path


def get_complex_properties_from_class(schema_property: dict) -> dict:
    """
    Create the property object. All complex dataTypes should be capitalized.

    Parameters
    ----------
    schema_property : dict
        The raw property schema.

    Returns
    -------
    dict
        The pre-processed property schema.
    """

    to_return_schema_property = {
        'name': schema_property['name'],
        'dataType': [capitalize_first_letter(dtype) for dtype in  schema_property['dataType']],
    }

    if 'description' in schema_property:
        to_return_schema_property['description'] = schema_property['description']

    if 'indexInverted' in schema_property:
        to_return_schema_property['indexInverted'] = schema_property['indexInverted']

    if 'moduleConfig' in schema_property:
        to_return_schema_property['moduleConfig'] = schema_property['moduleConfig']

    return to_return_schema_property


def is_sub_schema(sub_schema: dict, schema: dict) -> bool:
    """
    Check for a subset in a schema.

    Parameters
    ----------
    sub_schema : dict
        The smaller schema that should be contained in the 'schema'.
    schema : dict
        The schema for which to check if 'sub_schema' is a part of. Must have the 'classes' key.

    Returns
    -------
    bool
        True is 'sub_schema' is a subset of the 'schema'.
        False otherwise.
    """

    schema_classes = schema.get("classes", [])
    if 'classes' in sub_schema:
        sub_schema_classes = sub_schema["classes"]
    else:
        sub_schema_classes = [sub_schema]
    return _compare_class_sets(sub_schema_classes, schema_classes)


def is_primitive_property(data_type_list: list) -> bool:
    """
    Check if the property is primitive.

    Parameters
    ----------
    data_type_list : list
        Data types to be checked if are primitive.

    Returns
    -------
    bool
        True if it only consists of primitive data types,
        False otherwise.
    """

    if len(set(data_type_list) - PRIMITIVE_WEAVIATE_TYPES) == 0:
        return True
    return False


def _get_primitive_properties(properties_list: list) -> list:
    """
    Filter the list of properties for only primitive properties.

    Parameters
    ----------
    properties_list : list
        A list of properties to exctract the primitive properties.

    Returns
    -------
    list
        A list of properties containing only primitives.
    """

    primitive_properties = []
    for property_ in properties_list:
        if not is_primitive_property(property_["dataType"]):
            # property is complex and therefore will be ignored
            continue
        primitive_properties.append(property_)
    return primitive_properties


def update_nested_dict(dict_1: dict, dict_2: dict) -> dict:
    """
    Update 'dict_1' with elements from 'dict_2' in a nested manner.
    If a value of a key is a dict, it is going to be updated and not replaced by a the whole dict.

    Parameters
    ----------
    dict_1 : dict
        The dictionary to be updated.
    dict_2 : dict
        The dictionary that contains values to be updated.

    Returns
    -------
    dict
        The updated 'dict_1'.
    """
    for key, value in dict_2.items():
        if key not in dict_1:
            dict_1[key] = value
            continue
        if isinstance(value, dict):
            update_nested_dict(dict_1[key], value)
        else:
            dict_1.update({key : value})
    return dict_1


def _compare_class_sets(sub_set: list, set_: list) -> bool:
    """
    Check for a subset in a set of classes.

    Parameters
    ----------
    sub_set : list
        The smaller set that should be contained in the 'set'.
    schema : dict
        The set for which to check if 'sub_set' is a part of.

    Returns
    -------
    bool
        True is 'sub_set' is a subset of the 'set'.
        False otherwise.
    """

    for sub_set_class in sub_set:
        found = False
        for set_class in set_:
            if 'class' not in sub_set_class:
                raise SchemaValidationError(
                    "The sub-schema class/es MUST have a 'class' keyword each."
                )

            sub_set_class_name = capitalize_first_letter(sub_set_class["class"])
            set_class_name = capitalize_first_letter(set_class["class"])
            if sub_set_class_name == set_class_name:
                if _compare_properties(sub_set_class["properties"], set_class["properties"]):
                    found = True
                    break
        if not found:
            return False
    return True


def _compare_properties(sub_set: list, set_: list) -> bool:
    """
    Check for a subset in a set of properties.

    Parameters
    ----------
    sub_set : list
        The smaller set that should be contained in the 'set'.
    schema : dict
        The set for which to check if 'sub_set' is a part of.

    Returns
    -------
    bool
        True is 'sub_set' is a subset of the 'set'.
        False otherwise.
    """

    for sub_set_property in sub_set:
        found = False
        for set_property in set_:
            if sub_set_property["name"] == set_property["name"]:
                found = True
                break
        if not found:
            return False
    return True
