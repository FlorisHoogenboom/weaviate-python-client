"""
Package that contains all Base class definitions that are used by SyncClient or AsyncClient.
"""

__all__ = [
    'Connection',
    'BaseClassification',
    'BaseConfigBuilder',
    'BaseContextionary',
    'BaseProperty',
    'BaseSchema',
    'BaseDataObject',
    'BaseReference',
    'BaseQuery',
    'BaseBatch',
]

from .connection import Connection
from .classification import BaseClassification, BaseConfigBuilder
from .contextionary import BaseContextionary
from .schema import BaseSchema
from .schema.properties import BaseProperty
from .data import BaseDataObject
from .data.references import BaseReference
from .gql import BaseQuery
from .batch import BaseBatch
