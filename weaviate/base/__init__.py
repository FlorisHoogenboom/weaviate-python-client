"""
Package that contains all Base class definitions that are used by SyncClient or AsyncClient.
"""

__all__ = [
    'Connection',
    'BaseClassification',
    'BaseConfigBuilder',
]

from .connection import Connection
from .classification import BaseClassification, BaseConfigBuilder
