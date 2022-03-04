"""
Data module used to create, read, update and delete object and references.
"""

__all__ = [
    'AsyncDataObject',
    'AsyncReference',
]

from .crud_data import AsyncDataObject
from .references import AsyncReference
