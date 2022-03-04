"""
Data module used to create, read, update and delete object and references.
"""

__all__ = [
    'BaseDataObject',
    'BaseReference',
]

from .crud_data import BaseDataObject
from .references import BaseReference
