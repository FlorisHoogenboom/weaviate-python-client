"""
AsyncContextionary class definition.
"""
from numbers import Real
from weaviate.exceptions import AiohttpConnectionError, UnsuccessfulStatusCodeError
from weaviate.base.contextionary import (
    BaseContextionary,
    pre_extend,
    pre_get_concept_vector,
)
from .requests import Requests


class Contextionary(BaseContextionary):
    """
    Contextionary class used to add extend the Weaviate contextionary module
    or to get vector/s of a specific concept.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a Contextionary class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests

    async def extend(self, concept: str, definition: str, weight: Real=1.0):
        """
        Extend the text2vec-contextionary with new concepts

        Parameters
        ----------
        concept : str
            The new concept that should be added that is not in the Weaviate
            or needs to be updated, e.g. an abbreviation.
        definition : str
            The definition of the new concept.
        weight : Real, optional
            The weight of the new definition compared to the old one,
            must be in-between the interval [0.0; 1.0], by default 1.0

        Examples
        --------
        >>> await client.contextionary.extend(
        ...     concept = 'palantir',
        ...     definition = 'spherical stone objects used for communication in Middle-earth'
        ... )


        Raises
        ------
        TypeError
            If one of the arguments is of a wrong data type.
        ValueError
            If 'weight' is outside the interval [0.0; 1.0].
        aiohttp.ClientConnectionError
            If text2vec-contextionary could not be extended.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If the network connection to weaviate fails.
        """

        extension = pre_extend(
            concept=concept,
            definition=definition,
            weight=weight,
        )

        try:
            response = await self._requests.post(
                path="/modules/text2vec-contextionary/extensions",
                data_json=extension,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'text2vec-contextionary could not be extended.'
            ) from conn_err
        if response.status == 200:
            return
        raise UnsuccessfulStatusCodeError(
            "Extend text2vec-contextionary.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def get_concept_vector(self, concept: str):
        """
        Retrieves the vector representation of the given concept.

        Parameters
        ----------
        concept : str
            Concept for which the vector should be retrieved.
            May be camelCase for word combinations.

        Examples
        --------
        >>> await client.contextionary.get_concept_vector('king')
        {
            "individualWords": [
                {
                "info": {
                    "nearestNeighbors": [
                    {
                        "word": "king"
                    },
                    {
                        "distance": 5.7498446,
                        "word": "kings"
                    },
                    ...,
                    {
                        "distance": 6.1396513,
                        "word": "queen"
                    }
                    ],
                    "vector": [
                    -0.68988,
                    ...,
                    -0.561865
                    ]
                },
                "present": true,
                "word": "king"
                }
            ]
        }

        Returns
        -------
        dict
            A dictionary containing info and the vector/s of the concept.
            The vector might be empty if the text2vec-contextionary does not contain it.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path = pre_get_concept_vector(concept=concept)
        try:
            response = await self._requests.get(
                path=path,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'text2vec-contextionary vector was not retrieved due to connection error.'
            ) from conn_err
        else:
            if response.status == 200:
                return await response.json()
            raise UnsuccessfulStatusCodeError(
                "text2vec-contextionary vector.",
                status_code=response.status,
                response_message=await response.text(),
            )
