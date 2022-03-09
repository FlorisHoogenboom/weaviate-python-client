"""
Module that defines the base abstract classes for classification. Sync and Async packages should
implements its own classes to be compatible with the respective libraries.
"""

from .classification import (
    BaseClassification,
    BaseConfigBuilder,
    pre_get,
)
