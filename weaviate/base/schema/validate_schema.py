"""
Schema validation module.
"""
from typing import Any, Optional
from weaviate.exceptions import SchemaValidationError


ALLOWED_CLASS_KEYS = [
    'class',
    'description',
    'vectorIndexType',
    'vectorIndexConfig',
    'vectorizer',
    'moduleConfig',
    'properties',
    'invertedIndexConfig',
    'shardingConfig',
]


ALLOWED_PROPERTY_KEYS = [
    'name',
    'description',
    'dataType',
    'moduleConfig',
    'indexInverted'
]


def validate_schema(schema: dict) -> None:
    """
    Validate schema.

    Parameters
    ----------
    schema : dict
        Schema to be validated.

    Raises
    ------
    weaviate.exceptions.SchemaValidationError
        If the schema could not be validated against the standard format.
    """

    if "classes" not in schema:
        raise SchemaValidationError(
            "Schema must have 'classes' key at the first level of the 'dict'/JSON object"
        )

    _check_dict_value_type("classes", schema["classes"], list)

    for weaviate_class in schema["classes"]:
        _check_dict_value_type("class", weaviate_class, dict)
        check_class(weaviate_class)


def check_class(class_definition: dict) -> None:
    """
    Validate a class against the standard class format.

    Parameters
    ----------
    class_definition : dict
        The definition of the class to be validated.

    Raises
    ------
    weaviate.exceptions.SchemaValidationError
        If the class could not be validated against the standard class format.
    """

    # check mandatory keys
    if 'class' not in class_definition:
        raise SchemaValidationError(
            "'class' key is missing in class definition."
        )

    # check optional keys
    for key in class_definition.keys():
        # Check if key is known
        if key not in ALLOWED_CLASS_KEYS:
            raise SchemaValidationError(
                f"Class: {class_definition['class']}. '{key}' is not a known class definition key."
                f" Class definition keys: {ALLOWED_CLASS_KEYS}"
            )
        # check if key is right type
        if key in ['class', 'vectorIndexType', 'description', 'vectorizer']:
            _check_dict_value_type(key, class_definition[key], str, class_definition['class'])
        if key in ['vectorIndexConfig', 'moduleConfig', 'invertedIndexConfig', 'shardingConfig']:
            _check_dict_value_type(key, class_definition[key], dict, class_definition['class'])
        if key in ['properties']:
            _check_dict_value_type(key, class_definition[key], list, class_definition['class'])

    if 'properties' in class_definition:
        for class_property in class_definition['properties']:
            check_property(
                class_property=class_property,
                class_name=class_definition['class'],
            )


def check_property(class_property: dict, class_name: str) -> None:
    """
    Validate a class property against the standard class property.

    Parameters
    ----------
    class_property : dict
        The class property to be validated.
    class_name : str
        The class name of the property.

    Raises
    ------
    weaviate.exceptions.SchemaValidationError
        If the class property could not be validated against the standard class property format.
    """

    # mandatory fields
    if 'dataType' not in class_property:
        raise SchemaValidationError(
            f"Class: {class_name}. Property does not contain 'dataType' key-value."
        )
    if 'name' not in class_property:
        raise SchemaValidationError(
            f"Class: {class_name}. Property does not contain 'name' key-value"
        )

    for key in class_property:
        if key not in ALLOWED_PROPERTY_KEYS:
            raise SchemaValidationError(
                f"Class: {class_name}. '{key}' is not a known property definition key. "
                f"Property definition keys: {ALLOWED_PROPERTY_KEYS}"
            )

        # Test types
        if key in ['dataType']:
            _check_dict_value_type(key, class_property[key], list, class_name)
        if key in ['name', 'description']:
            _check_dict_value_type(key, class_property[key], str, class_name)
        if key in ['indexInverted']:
            _check_dict_value_type(key, class_property[key], bool, class_name)
        if key in ['moduleConfig']:
            _check_dict_value_type(key, class_property[key], dict, class_name)

    # Test dataType types
    for data_type in class_property['dataType']:
        _check_dict_value_type('dataType values', data_type, str, class_name)


def _check_dict_value_type(
        key: str,
        value: Any,
        expected_type: Any,
        class_name: Optional[str]=None,
    ) -> None:
    """
    Check if value is of an expected type.

    Parameters
    ----------
    key : str
        The key for which to check data type.
    value : Any
        The value of the 'key' for which to check data type.
    expected_type : Any
        The expected data type of the 'value'.

    Raises
    ------
    weaviate.exceptions.SchemaValidationError
        If the 'value' is of wrong data type.
    """
    class_name_msg = ''
    if class_name:
        class_name_msg = f"Class: {class_name}. "

    if not isinstance(value, expected_type):
        raise SchemaValidationError(
            f"{class_name_msg}'{key}' must be of type {expected_type}. Given type: {type(value)}."
        )
