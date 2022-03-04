"""
Data module used to create, read, update and delete object and references.
"""

__all__ = [
    'SyncDataObject',
    'SyncReference',
]

from .crud_data import SyncDataObject
from .references import SyncReference
