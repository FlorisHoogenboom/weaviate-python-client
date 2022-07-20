"""
BaseExploreBuilder abstract class definition.
"""
from abc import ABC
from typing import List, Union
from weaviate.util import image_encoder_b64
from .filter import (
    NearText,
    NearVector,
    NearObject,
    NearImage,
    Filter,
)


class BaseExploreBuilder(ABC):
    """
    BaseExploreBuilder abstract class used to create GraphQL queries.
    """

    def __init__(self,
            properties: Union[List[str], str],
        ):
        """
        Initialize a BaseExploreBuilder class instance.

        Parameters
        ----------
        properties : list of str or str
            Property/ies of the Explore filter to be returned. Currently there are 4 choices:
                'beacon', 'distance', 'className' and 'certainty' (ONLY with 'cosine' distance
                used in schema).

        Raises
        ------
        TypeError
            If argument/s is/are of wrong type.
        """

        if not isinstance(properties, (list, str)):
            raise TypeError(
                "'properties' must be of type str or list of str. "
                f"Given type: {type(properties)}."
            )
        if isinstance(properties, str):
            properties = [properties]
        for prop in properties:
            if not isinstance(prop, str):
                raise TypeError(
                    "All the 'properties' must be of type str."
                )
        self._properties: List[str] = properties
        self._limit: str = ''
        self._offset: str = ''
        self._near: Union[Filter, str] = ''
        self._contains_near = False

    def with_near_text(self, content: dict) -> 'BaseExploreBuilder':
        """
        Set 'nearText' filter. This filter can be used with text modules (text2vec).
        E.g.: text2vec-contextionary, text2vec-transformers.
        NOTE: The 'autocorrect' field is enabled only with the 'text-spellcheck' Weaviate module.

        Parameters
        ----------
        content : dict
            The content of the 'nearText' filter to set. See examples below.

        Examples
        --------
        Content full prototype:

        >>> content = {
        ...     'concepts': <list of str or str>,
        ...     # certainty ONLY with `cosine` distance specified in the schema
        ...     'certainty': <float>, # Optional, either 'certainty' OR 'distance'
        ...     'distance': <float>, # Optional, either 'certainty' OR 'distance'
        ...     'moveAwayFrom': {                      # Optional
        ...         'concepts': <list of str or str>,
        ...         'force': <float>
        ...     },
        ...     'moveTo': {                            # Optional
        ...         'concepts': <list of str or str>,
        ...         'force': <float>
        ...     },
        ...     'autocorrect': <bool>,                 # Optional
        ... }

        Full content:

        >>> content = {
        ...     'concepts': ["fashion"],
        ...     'distance': 0.7,
        ...     'moveAwayFrom': {
        ...         'concepts': ["finance"],
        ...         'force': 0.45
        ...     },
        ...     'moveTo': {
        ...         'concepts': ["haute couture"],
        ...         'force': 0.85
        ...     },
        ...     'autocorrect': True
        ... }

        Partial content:

        >>> content = {
        ...     'concepts': ["fashion"],
        ...     'distance': 0.7,
        ...     'moveTo': {
        ...         'concepts': ["haute couture"],
        ...         'force': 0.85
        ...     }
        ... }

        Minimal content:

        >>> content = {
        ...     'concepts': "fashion"
        ... }

        Returns
        -------
        weaviate.base.gql.explore.BaseExploreBuilder
            The updated BaseExploreBuilder.

        Raises
        ------
        AttributeError
            If another 'near<Media>' filter was already set.
        """

        if not self._near:
            raise AttributeError(
                "Cannot use multiple 'near<Media>' filters."
            )
        self._near = NearText(content)
        self._contains_near = True
        return self

    def with_near_vector(self, content: dict) -> 'BaseExploreBuilder':
        """
        Set 'nearVector' filter.

        Parameters
        ----------
        content : dict
            The content of the 'nearVector' filter to set. See examples below.

        Examples
        --------
        Content full prototype:

        >>> content = {
        ...     'vector' : <list of float>,
        ...     # certainty ONLY with `cosine` distance specified in the schema
        ...     'certainty': <float>, # Optional, either 'certainty' OR 'distance'
        ...     'distance': <float>, # Optional, either 'certainty' OR 'distance'
        ... }

        NOTE: Supported types for 'vector' are 'list', 'numpy.ndarray', 'torch.Tensor'
                and 'tf.Tensor'.

        Full content:

        >>> content = {
        ...     'vector' : [.1, .2, .3, .5],
        ...     'distance': 0.75
        ... }

        Minimal content:

        >>> content = {
        ...     'vector' : [.1, .2, .3, .5]
        ... }

        Or

        >>> content = {
        ...     'vector' : torch.tensor([.1, .2, .3, .5])
        ... }

        Or

        >>> content = {
        ...     'vector' : torch.tensor([[.1, .2, .3, .5]]) # it is going to be squeezed.
        ... }

        Returns
        -------
        weaviate.base.gql.explore.BaseExploreBuilder
            The updated BaseExploreBuilder.

        Raises
        ------
        AttributeError
            If another 'near<Media>' filter was already set.
        """

        if not self._near:
            raise AttributeError(
                "Cannot use multiple 'near<Media>' filters."
            )
        self._near = NearVector(content)
        self._contains_near = True
        return self

    def with_near_object(self, content: dict) -> 'BaseExploreBuilder':
        """
        Set 'nearObject' filter.

        Parameters
        ----------
        content : dict
            The content of the 'nearObject' filter to set. See examples below.

        Examples
        --------
        Content prototype:

        >>> {
        ...     'id': "e5dc4a4c-ef0f-3aed-89a3-a73435c6bbcf",
        ...     # certainty ONLY with `cosine` distance specified in the schema
        ...     'certainty': <float>, # Optional, either 'certainty' OR 'distance'
        ...     'distance': <float>, # Optional, either 'certainty' OR 'distance'
        ... }
        >>> # alternatively
        >>> {
        ...     'beacon': "weaviate://localhost/Book/e5dc4a4c-ef0f-3aed-89a3-a73435c6bbcf"
        ...     # certainty ONLY with `cosine` distance specified in the schema
        ...     'certainty': <float>, # Optional, either 'certainty' OR 'distance'
        ...     'distance': <float>, # Optional, either 'certainty' OR 'distance'
        ... }

        Returns
        -------
        weaviate.base.gql.explore.BaseExploreBuilder
            The updated BaseExploreBuilder.

        Raises
        ------
        AttributeError
            If another 'near<Media>' filter was already set.
        """

        if not self._near:
            raise AttributeError(
                "Cannot use multiple 'near<Media>' filters."
            )
        self._near = NearObject(content)
        self._contains_near = True
        return self

    def with_near_image(self, content: dict, encode: bool=False) -> 'BaseExploreBuilder':
        """
        Set 'nearImage' filter.

        Parameters
        ----------
        content : dict
            The content of the 'nearObject' filter to set. See examples below.
        encode : bool, optional
            Whether to encode the 'content["image"]' to base64 and convert to string. If True, the
            'content["image"]' can be an image path or a file opened in binary read mode. If False,
            the 'content["image"]' MUST be a base64 encoded string (NOT bytes, i.e. NOT binary
            string that looks like this: b'BASE64ENCODED' but simple 'BASE64ENCODED').
            By default True.

        Examples
        --------
        Content prototype:

        >>> {
        ...     'image': <image>,
        ...     # certainty ONLY with `cosine` distance specified in the schema
        ...     'certainty': <float>, # Optional, either 'certainty' OR 'distance'
        ...     'distance': <float>, # Optional, either 'certainty' OR 'distance'
        ... }

        With 'encoded' True:

        >>> content = {
        ...     'image': "my_image_path.png",
        ...     'distance': 0.7 # Optional
        ... }
        >>> query = client.query.explore()\\
        ...     .with_near_image(content, encode=True) # <- encode MUST be set to True

        OR

        >>> my_image_file = open("my_image_path.png", "br")
        >>> content = {
        ...     'image': my_image_file,
        ...     'distance': 0.7 # Optional
        ... }
        >>> query = client.query.explore()\\
        ...     .with_near_image(content, encode=True) # <- encode MUST be set to True
        >>> my_image_file.close()

        With 'encoded' False:

        >>> from weaviate.util import image_encoder_b64, image_decoder_b64
        >>> encoded_image = image_encoder_b64("my_image_path.png")
        >>> content = {
        ...     'image': encoded_image,
        ...     'distance': 0.7 # Optional
        ... }
        >>> query = client.query.explore()\\
        ...     .with_near_image(content, encode=False) # <- encode MUST be set to False

        OR

        >>> from weaviate.util import image_encoder_b64, image_decoder_b64
        >>> with open("my_image_path.png", "br") as my_image_file:
        ...     encoded_image = image_encoder_b64(my_image_file)
        >>> content = {
        ...     'image': encoded_image,
        ...     'distance': 0.7 # Optional
        ... }
        >>> query = client.query.explore()\\
        ...     .with_near_image(content, encode=False) # <- encode MUST be set to False

        Encode Image yourself:

        >>> import base64
        >>> with open("my_image_path.png", "br") as my_image_file:
        ...     encoded_image = base64.b64encode(my_image_file.read()).decode("utf-8")
        >>> content = {
        ...     'image': encoded_image,
        ...     'distance': 0.7 # Optional
        ... }
        >>> query = client.query.explore()\\
        ...     .with_near_image(content, encode=False) # <- encode MUST be set to False

        Returns
        -------
        weaviate.base.gql.explore.BaseExploreBuilder
            The updated BaseExploreBuilder.

        Raises
        ------
        AttributeError
            If another 'near<Media>' filter was already set.
        """

        if not self._near:
            raise AttributeError(
                "Cannot use multiple 'near<Media>' filters."
            )
        if encode:
            content['image'] = image_encoder_b64(content['image'])
        self._near = NearImage(content)
        self._contains_near = True
        return self

    def with_limit(self, limit: int) -> 'BaseExploreBuilder':
        """
        The limit of objects returned.

        Parameters
        ----------
        limit : int
            The max number of objects returned.

        Returns
        -------
        weaviate.base.gql.explore.BaseExploreBuilder
            The updated BaseExploreBuilder.

        Raises
        ------
        ValueError
            If 'limit' is non-positive.
        """

        if limit < 1:
            raise ValueError(
                f"'limit' must be a positive integer (limit >=1). Given value: {limit}."
            )

        self._limit = f'limit: {limit} '
        return self

    def with_offset(self, offset: int) -> 'BaseExploreBuilder':
        """
        The offset of objects returned, i.e. the starting index of the returned objects should be
        used in conjunction with the 'with_limit' method.

        Parameters
        ----------
        offset : int
            The offset used for the returned objects.

        Returns
        -------
        weaviate.gql.explore.BaseExploreBuilder
            The updated BaseExploreBuilder.

        Raises
        ------
        ValueError
            If 'offset' is non-positive.
        """

        if offset < 0:
            raise ValueError(
                f"'offset' must be a non-negative integer (offset >=0). Given value: {offset}."
            )

        self._offset = f'offset: {offset} '
        return self

    def build(self) -> str:
        """
        Build query filter as a string.

        Returns
        -------
        str
            The GraphQL query as a string.
        """

        if not self._contains_near:
            raise AttributeError(
                "No 'near<Media>' filter provided. Cannot perform Explore without 'near<Media>' "
                "filter."
            )

        properties = " ".join(self._properties)

        query = (
            '{Explore(' +
            self._limit +
            self._offset +
            str(self._near) +
            ') {' +
            properties +
            '}}'
        )
        return query
