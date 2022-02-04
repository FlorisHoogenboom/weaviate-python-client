"""
Contextionary class definition.
"""
from numbers import Real
from weaviate.exceptions import WeaviateConnectionError, UnsuccessfulStatusCodeError
from weaviate.connect import Connection


class Contextionary:
    """
    Contextionary class used to add extend the Weaviate contextionary module
    or to get vector/s of a specific concept.
    """

    def __init__(self, connection: Connection):
        """
        Initialize a Contextionary class instance.

        Parameters
        ----------
        connection : weaviate.connect.Connection
            Connection object to an active and running Weaviate instance.
        """

        self._connection = connection

    def extend(self, concept: str, definition: str, weight: Real=1.0) -> None:
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
        >>> client.contextionary.extend(
        ...     concept = 'palantir',
        ...     definition = 'spherical stone objects used for communication in Middle-earth'
        ... )


        Raises
        ------
        TypeError
            If one of the arguments is of a wrong data type.
        ValueError
            If 'weight' is outside the interval [0.0; 1.0].
        requests.exceptions.ConnectionError
            If text2vec-contextionary could not be extended.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If the network connection to weaviate fails.
        """

        if not isinstance(concept, str):
            raise TypeError(
                f"'concept' must be of type 'str'. Given type: {type(concept)}."
            )
        if not isinstance(definition, str):
            raise TypeError(
                f"'definition' must be of type 'str'. Given type: {type(definition)}."
            )
        if not isinstance(weight, Real):
            raise TypeError(
                f"'weight' must be of type 'float'/'int'. Given type: {type(weight)}."
            )

        if weight > 1.0 or weight < 0.0:
            raise ValueError(
                f"'weight' is out of limits! 0.0 <= weight <= 1.0, Given: {weight}."
            )

        extension = {
            "concept": concept,
            "definition": definition,
            "weight": weight
        }

        try:
            response = self._connection.post(
                path="/modules/text2vec-contextionary/extensions",
                data_json=extension,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'text2vec-contextionary could not be extended.'
            ) from conn_err
        if response.status_code == 200:
            return
        raise UnsuccessfulStatusCodeError("Extend text2vec-contextionary!", response)

    def get_concept_vector(self, concept: str) -> dict:
        """
        Retrieves the vector representation of the given concept.

        Parameters
        ----------
        concept : str
            Concept for which the vector should be retrieved.
            May be camelCase for word combinations.

        Examples
        --------
        >>> client.contextionary.get_concept_vector('king')
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
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If weaviate reports a none OK status.
        """

        path = "/modules/text2vec-contextionary/concepts/" + concept
        try:
            response = self._connection.get(
                path=path,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'text2vec-contextionary vector was not retrieved due to connection error!'
            ) from conn_err
        else:
            if response.status_code == 200:
                return response.json()
            raise UnsuccessfulStatusCodeError("text2vec-contextionary vector!", response)
