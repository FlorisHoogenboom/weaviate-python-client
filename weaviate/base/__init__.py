"""
Package that contains all Base class definitions that are used by SyncClient or AsyncClient.
"""

__all__ = [
    'Connection',
    'BaseClassification',
    'BaseConfigBuilder',
    'BaseContextionary',
    'BaseProperty',
    'BaseSchema'
]

from .connection import Connection
from .classification import BaseClassification, BaseConfigBuilder
from .contextionary import BaseContextionary
from .schema import BaseProperty, BaseSchema
