"""
BaseAggregateBuilder abstract class definition.
"""
import json
from abc import ABC
from typing import List, Optional, Union
from weaviate.util import capitalize_first_letter
from .filter import Where

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
        self._meta_count: str = ''
        self._fields: List[str] = []
        self._where: Union[Where, str] = ''
        self._group_by_properties: Optional[List[str]] = None
        self._uses_filter = False

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
        The `content` prototype is like this:

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

        This is a complete `where` filter but it does not have to be like this all the time.

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

    def with_group_by_filter(self, properties: List[str]) -> 'BaseAggregateBuilder':
        """
        Add a group by filter to the query. Might requires the user to set
        an additional group by clause using `with_fields(..)`.

        Parameters
        ----------
        properties : list of str
            list of properties that are included in the group by filter.
            Generates a filter like: 'groupBy: ["property1", "property2"]'
            from a list ["property1", "property2"]

        Returns
        -------
        weaviate.base.gql.aggregate.BaseAggregateBuilder
            Updated BaseAggregateBuilder.
        """

        self._group_by_properties = properties
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
            query += "("
            query += str(self._where)
            if self._group_by_properties is not None:
                query += f"groupBy: {json.dumps(self._group_by_properties)}"
            query += ")"

        # Body
        query += (
            "{" +
            self._meta_count +
            ' '.join(self._fields) +
            "}}}"
        )

        return query
