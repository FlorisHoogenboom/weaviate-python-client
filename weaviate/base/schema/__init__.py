"""
Module used to manipulate schemas.
"""


from .crud_schema import (
    BaseSchema,
    get_class_schema_with_primitives_and_path,
    is_sub_schema,
    update_nested_dict,
    is_primitive_property,
    get_complex_properties_from_class,
    pre_contains,
    pre_create,
    pre_create_class,
    pre_delete_class,
    pre_get,
    pre_update_config,
    pre_get_class_shards,
    pre_update_class_shard,
)
from .validate_schema import (
    validate_schema,
    check_class,
    check_property,
)
