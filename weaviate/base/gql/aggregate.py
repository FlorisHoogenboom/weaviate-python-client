"""
BaseAggregateBuilder abstract class definition.
"""
from abc import ABC
from typing import List, Union
from weaviate.util import capitalize_first_letter
from .filter import (
    Where,
    NearObject,
    NearText,
    NearVector,
    AggregateGroupBy,
)

class BaseAggregateBuilder(ABC):
    """
    BaseAggregateBuilder abstract class used to aggregate Weaviate objects.
    """

    def __init__(self, class_name: str):
        """
        Initialize a BaseAggregateBuilder class instance.

        Parameters
        ----------
        class_name : str
            Class name of the objects to be aggregated.
        """

        self._class_name = capitalize_first_letter(class_name)
        self._object_limit: str = ''
        self._meta_count: str = ''
        self._fields: List[str] = []
        self._where: Union[Where, str] = ''
        self._group_by: Union[AggregateGroupBy, str] = ''
        self._uses_filter = False
        self._near: Union[NearObject, NearText, NearVector, str] = ''

    def with_meta_count(self) -> 'BaseAggregateBuilder':
        """
        Set Meta Count to True.

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.
        """

        self._meta_count = "meta{count} "
        return self

    def with_object_limit(self, limit: int) -> 'BaseAggregateBuilder':
        """
        Set objectLimit to limit vector search results only when with near<MEDIA> filter.

        Parameters
        ----------
        limit : int
            The object limit.

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.
        """

        self._object_limit = f"objectLimit: {limit} "
        return self

    def with_fields(self, field: str) -> 'BaseAggregateBuilder':
        """
        Include a field in the aggregate query.

        Parameters
        ----------
        field : str
            Field to include in the aggregate query.
            e.g. '<property_name> { count }'

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.
        """

        self._fields.append(field)
        return self

    def with_where(self, content: dict) -> 'BaseAggregateBuilder':
        """
        Set 'where' filter.

        Parameters
        ----------
        content : dict
            The where filter to include in the aggregate query. See examples below.

        Examples
        --------
        The 'content' prototype is like this:

        >>> content = {
        ...     'operator': '<operator>',
        ...     'operands': [
        ...         {
        ...             'path': [path],
        ...             'operator': '<operator>'
        ...             '<valueType>': <value>
        ...         },
        ...         {
        ...             'path': [<matchPath>],
        ...             'operator': '<operator>',
        ...             '<valueType>': <value>
        ...         }
        ...     ]
        ... }

        This is a complete 'where' filter but it does not have to be like this all the time.

        Single operand:

        >>> content = {
        ...     'path': ["wordCount"],    # Path to the property that should be used
        ...     'operator': 'GreaterThan',  # operator
        ...     'valueInt': 1000       # value (which is always = to the type of the path property)
        ... }

        Or

        >>> content = {
        ...     'path': ["id"],
        ...     'operator': 'Equal',
        ...     'valueString': "e5dc4a4c-ef0f-3aed-89a3-a73435c6bbcf"
        ... }

        Multiple operands:

        >>> content = {
        ...     'operator': 'And',
        ...     'operands': [
        ...         {
        ...             'path': ["wordCount"],
        ...             'operator': 'GreaterThan',
        ...             'valueInt': 1000
        ...         },
        ...         {
        ...             'path': ["wordCount"],
        ...             'operator': 'LessThan',
        ...             'valueInt': 1500
        ...         }
        ...     ]
        ... }

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.
        """

        self._where = Where(content)
        self._uses_filter = True
        return self

    def with_group_by(self, properties: Union[str, List[str]]) -> 'BaseAggregateBuilder':
        """
        Add a group by filter to the query. Might requires the user to set
        an additional group by clause using '.with_fields(...)'.

        Parameters
        ----------
        properties : str or list of str
            Property or list of properties that are included in the groupBy filter.

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.
        """

        self._group_by = AggregateGroupBy(properties)
        self._uses_filter = True
        return self

    def with_near_text(self, content: dict) -> 'BaseAggregateBuilder':
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
        ...     'certainty': <float>, # Optional
        ...     'moveAwayFrom': { # Optional
        ...         'concepts': <list of str or str>,
        ...         'force': <float>
        ...     },
        ...     'moveTo': { # Optional
        ...         'concepts': <list of str or str>,
        ...         'force': <float>
        ...     },
        ...     'autocorrect': <bool>, # Optional
        ... }

        Full content:

        >>> content = {
        ...     'concepts': ["fashion"],
        ...     'certainty': 0.7,
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
        ...     'certainty': 0.7,
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
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.

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
        self._uses_filter = True
        return self

    def with_near_vector(self, content: dict) -> 'BaseAggregateBuilder':
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
        ...     'certainty': <float> # Optional
        ... }

        NOTE: Supported types for 'vector' are 'list', 'numpy.ndarray', 'torch.Tensor'
                and 'tf.Tensor'.

        Full content:

        >>> content = {
        ...     'vector' : [.1, .2, .3, .5],
        ...     'certainty': 0.75
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
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.

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
        self._uses_filter = True
        return self

    def with_near_object(self, content: dict) -> 'BaseAggregateBuilder':
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
        ...     'certainty': 0.7 # Optional
        ... }
        >>> # alternatively
        >>> {
        ...     'beacon': "weaviate://localhost/e5dc4a4c-ef0f-3aed-89a3-a73435c6bbcf"
        ...     'certainty': 0.7 # Optional
        ... }

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.

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
        self._uses_filter = True
        return self

    def build(self) -> str:
        """
        Build the query and return the string.

        Returns
        -------
        str
            The GraphQL query as a string.
        """

        # Path
        query = f"{{Aggregate{{{self._class_name}"

        # Filter
        if self._uses_filter:
            query += (
                '(' +
                str(self._object_limit) +
                str(self._where) +
                str(self._near) +
                str(self._group_by) +
                ')'
            )
        # Body
        query += (
            "{" +
            self._meta_count +
            ' '.join(self._fields) +
            '}}}'
        )
        return query
