"""
BaseContextionary abstract class definition.
"""
from typing import Dict, Any
from numbers import Real
from abc import ABC, abstractmethod


class BaseContextionary(ABC):
    """
    BaseContextionary abstract class used to add extend the Weaviate contextionary module
    or to get vector/s of a specific concept.
    """

    @abstractmethod
    def extend(self, concept: str, definition: str, weight: Real):
        """
        Extend the text2vec-contextionary with new concepts

        Parameters
        ----------
        concept : str
            The new concept that should be added that is not in the Weaviate
            or needs to be updated, e.g. an abbreviation.
        definition : str
            The definition of the new concept.
        weight : Real
            The weight of the new definition compared to the old one,
            must be in-between the interval [0.0; 1.0]
        """

    @abstractmethod
    def get_concept_vector(self, concept: str):
        """
        Retrieves the vector representation of the given concept.

        Parameters
        ----------
        concept : str
            Concept for which the vector should be retrieved. Should be camelCased for word
            combinations.
        """


def pre_extend(concept: str, definition: str, weight: Real) -> Dict[str, Any]:
    """
    Validate all arguments for correct type and value, and construct the payload for weaviate
    request.

    Parameters
    ----------
    concept : str
        The new concept that should be added that is not in the Weaviate
        or needs to be updated, e.g. an abbreviation.
    definition : str
        The definition of the new concept.
    weight : Real
        The weight of the new definition compared to the old one,
        must be in-between the interval [0.0; 1.0]

    Returns
    -------
    Dict[str, Any]
        Payload to use to extend the contextionary.
    """

    if not isinstance(concept, str):
        raise TypeError(
            f"'concept' must be of type str. Given type: {type(concept)}."
        )
    if not isinstance(definition, str):
        raise TypeError(
            f"'definition' must be of type str. Given type: {type(definition)}."
        )
    if not isinstance(weight, Real) or isinstance(weight, bool):
        raise TypeError(
            f"'weight' must be of type float/int. Given type: {type(weight)}."
        )

    if weight > 1.0 or weight < 0.0:
        raise ValueError(
            f"'weight' is out of bounds: 0.0 <= weight <= 1.0. Given: {weight}."
        )

    return {
        "concept": concept,
        "definition": definition,
        "weight": weight
    }


def pre_get_concept_vector(concept: str) -> str:
    """
    The pre-request function that should be shared between the sync/async version of the
    BaseContextionary.

    Parameters
    ----------
    concept : str
        Concept for which the vector should be retrieved. Should be camelCased for word
        combinations.

    Returns
    -------
    str
        The Weaviate resource path.
    """

    path = "/modules/text2vec-contextionary/concepts/" + concept

    return path
