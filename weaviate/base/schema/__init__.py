"""
Module used to manipulate schemas.
"""

__all__ = [
    'BaseSchema',
    'BaseProperty',
    'get_path_for_get_method',
    'get_class_schema_with_primitives_and_path',
    'is_sub_schema',
    'update_nested_dict',
    'is_primitive_property',
    'validate_schema',
    'check_class',
    'check_property',
]

from .crud_schema import (
    BaseSchema,
    get_path_for_get_method,
    get_class_schema_with_primitives_and_path,
    is_sub_schema,
    update_nested_dict,
    is_primitive_property,
)
from .validate_schema import (
    validate_schema,
    check_class,
    check_property,
)
from .properties import BaseProperty
