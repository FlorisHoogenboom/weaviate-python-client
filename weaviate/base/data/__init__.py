"""
Data module used to create, read, update and delete object and references.
"""

from .crud_data import (
    BaseDataObject,
    pre_replace,
    pre_create,
    pre_delete_exists,
    pre_get,
    pre_update,
    pre_validate
)
