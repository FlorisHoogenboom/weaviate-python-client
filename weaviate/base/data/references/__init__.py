"""
Module for adding, deleting and updating references in-between objects.
"""

from .crud_references import (
    BaseReference,
    pre_delete_and_add,
    pre_replace,
)
