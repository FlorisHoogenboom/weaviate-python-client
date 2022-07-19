"""
GraphQL filters for 'Get' and 'Aggregate' commands.
GraphQL abstract class for GraphQL commands to inherit from.
"""
from json import dumps
from copy import deepcopy
from typing import Any, Union, List
from abc import ABC, abstractmethod
from weaviate.util import get_vector


class Filter(ABC):
    """
    A base abstract class for all filters.
    """

    def __init__(self, content: dict):
        """
        Initialize a Filter class instance.

        Parameters
        ----------
        content : dict
            The content of the 'Filter' clause.
        """


        if not isinstance(content, dict):
            raise TypeError(
                f"{self.__class__.__name__} filter 'content' must be of type dict. "
                f"Given type: {type(content)}."
            )
        self._content = deepcopy(content)

    @abstractmethod
    def __str__(self) -> str:
        """
        Should be implemented in each inheriting class.
        """


class NearText(Filter):
    """
    NearText class used to filter Weaviate objects. Can be used with text models only (text2vec).
    E.g.: text2vec-contextionary, text2vec-transformers.
    """

    def __init__(self, content: dict):
        """
        Initialize a NearText class instance.

        Parameters
        ----------
        content : dict
            The content of the 'nearText' clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If 'content'  has key "certainty" but the value is not float.
        """

        super().__init__(content)

        _check_concept(self, self._content)

        if "certainty" in self._content:
            _check_type(
                var_name='certainty',
                value=self._content["certainty"],
                dtype=float
            )

        if "moveTo" in self._content:
            _check_direction_clause(self, self._content["moveTo"])

        if "moveAwayFrom" in self._content:
            _check_direction_clause(self, self._content["moveAwayFrom"])

        if "autocorrect" in self._content:
            _check_type(
                self=self,
                var_name='autocorrect',
                value=self._content["autocorrect"],
                dtype=bool,
            )

    def __str__(self):
        near_text = f'nearText: {{concepts: {dumps(self._content["concepts"])}'

        if 'certainty' in self._content:
            near_text += f' certainty: {self._content["certainty"]}'

        if 'moveTo' in self._content:
            move_to = self._content["moveTo"]
            near_text += f' moveTo: {{force: {move_to["force"]}'
            if 'concepts' in move_to:
                near_text += f' concepts: {move_to["concepts"]}'
            if 'objects' in move_to:
                near_text += _move_clause_objects_to_str(move_to['objects'])
            near_text += '}'

        if 'moveAwayFrom' in self._content:
            move_away_from = self._content["moveAwayFrom"]
            near_text += f' moveAwayFrom: {{force: {move_away_from["force"]}'
            if 'concepts' in move_away_from:
                near_text += f' concepts: {dumps(move_away_from["concepts"])}'
            if 'objects' in move_to:
                near_text += _move_clause_objects_to_str(move_away_from['objects'])
            near_text += '}'

        if 'autocorrect' in self._content:
            near_text += f' autocorrect: {_bool_to_str(self._content["autocorrect"])}'

        return near_text + '} '


class NearVector(Filter):
    """
    NearVector class used to filter Weaviate objects.
    """

    def __init__(self, content: dict):
        """
        Initialize a NearVector class instance.

        Parameters
        ----------
        content : list
            The content of the 'nearVector' clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        KeyError
            If 'content' does not contain "vector".
        TypeError
            If 'content["vector"]' is not of type list.
        AttributeError
            If invalid 'content' keys are provided.
        ValueError
            If 'content'  has key "certainty" but the value is not float.
        """

        super().__init__(content)

        if "vector" not in self._content:
            raise KeyError(
                f"{self.__class__.__name__}: "
                f"'vector' required key is missing from 'content' argument. Given: {content}."
            )

        # Check optional fields
        if "certainty" in self._content:
            _check_type(
                self, self,
                var_name='certainty',
                value=self._content["certainty"],
                dtype=float
            )

        self._content['vector'] = get_vector(self._content['vector'])

    def __str__(self):
        near_vector = f'nearVector: {{vector: {dumps(self._content["vector"])}'
        if 'certainty' in self._content:
            near_vector += f' certainty: {self._content["certainty"]}'
        return near_vector + '} '


class NearObject(Filter):
    """
    NearObject class used to filter Weaviate objects.
    """

    def __init__(self, content: dict):
        """
        Initialize a NearVector class instance.

        Parameters
        ----------
        content : list
            The content of the 'nearVector' clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If 'content'  has key "certainty" but the value is not float.
        TypeError
            If 'id'/'beacon' key does not have a value of type str.
        """

        super().__init__(content)

        if ('id' in self._content) and ('beacon' in self._content):
            raise ValueError(
                f"{self.__class__.__name__}: "
                "'content' argument should contain EITHER 'id' OR 'beacon', not both. "
                f"Given: {content}."
            )

        if 'id' in self._content:
            self.obj_id = 'id'
        else:
            self.obj_id = 'beacon'

        _check_type(
            self=self,
            var_name=self.obj_id,
            value=self._content[self.obj_id],
            dtype=str
        )

        if "certainty" in self._content:
            _check_type(
                self=self,
                var_name='certainty',
                value=self._content["certainty"],
                dtype=float
            )

    def __str__(self):

        near_object = f'nearObject: {{{self.obj_id}: "{self._content[self.obj_id]}"'
        if 'certainty' in self._content:
            near_object += f' certainty: {self._content["certainty"]}'
        return near_object + '} '


class Ask(Filter):
    """
    Ask class used to filter Weaviate objects by asking a question.
    """

    def __init__(self, content: dict):
        """
        Initialize a Ask class instance.

        Parameters
        ----------
        content : list
            The content of the 'ask' clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If 'content'  has key "certainty" but the value is not float.
        TypeError
            If 'content'  has key "properties" but the type is not list or str.
        """

        super().__init__(content)

        if 'question' not in self._content:
            raise ValueError(
                f"{self.__class__.__name__}: "
                f"'question' required key is missing from 'content' argument. Given: {content}."
            )

        _check_type(
            self=self,
            var_name='question',
            value=self._content["question"],
            dtype=str
        )
        if 'certainty' in self._content:
            _check_type(
                self=self,
                var_name='certainty',
                value=self._content["certainty"],
                dtype=float
            )

        if "autocorrect" in self._content:
            _check_type(
                self=self,
                var_name='autocorrect',
                value=self._content["autocorrect"],
                dtype=bool
            )

        if "rerank" in self._content:
            _check_type(
                self=self,
                var_name='rerank',
                value=self._content["rerank"],
                dtype=bool
            )

        if 'properties' in self._content:
            _check_type(
                self=self,
                var_name='properties',
                value=self._content["properties"],
                dtype=(list, str)
            )
            if isinstance(self._content['properties'], str):
                self._content['properties'] = [self._content['properties']]

    def __str__(self):
        ask = f'ask: {{question: {dumps(self._content["question"])}'
        if 'certainty' in self._content:
            ask += f' certainty: {self._content["certainty"]}'
        if 'properties' in self._content:
            ask += f' properties: {dumps(self._content["properties"])}'
        if 'autocorrect' in self._content:
            ask += f' autocorrect: {_bool_to_str(self._content["autocorrect"])}'
        if 'rerank' in self._content:
            ask += f' rerank: {_bool_to_str(self._content["rerank"])}'
        return ask + '} '


class NearImage(Filter):
    """
    NearObject class used to filter Weaviate objects.
    """

    def __init__(self, content: dict):
        """
        Initialize a NearImage class instance.

        Parameters
        ----------
        content : list
            The content of the 'nearImage' clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        TypeError
            If 'content["image"]' is not of type str.
        ValueError
            If 'content'  has key "certainty" but the value is not float.
        """

        super().__init__(content)

        if 'image' not in self._content:
            raise ValueError(
                f"{self.__class__.__name__}: "
                f"'image' required key is missing from 'content' argument.  Given: {content}."
            )

        _check_type(
            self=self,
            var_name='image',
            value=self._content["image"],
            dtype=str
        )
        if "certainty" in self._content:
            _check_type(
                self=self,
                var_name='certainty',
                value=self._content["certainty"],
                dtype=float,
            )

    def __str__(self):
        near_image = f'nearImage: "{{image: {self._content["image"]}"'
        if 'certainty' in self._content:
            near_image += f' certainty: {self._content["certainty"]}'
        return near_image + '} '


class Sort:
    """
    Sort filter class used to sort weaviate objects.
    """

    def __init__(self, content: Union[dict, List[dict]]):
        """
        Initialize a Where filter class instance.

        Parameters
        ----------
        content : dict or list of dict
            The content of the 'sort' filter clause or a single clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If a mandatory key is missing in the filter content.
        """

        self._content = []
        self.add(content=content)

    def add(self, content: Union[dict, list]) -> None:
        """
        Add more sort clauses to the already existing sort clauses.

        Parameters
        ----------
        content : list or dict
            The content of the 'sort' filter clause or a single clause to be added to the already
            existing ones.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If a mandatory key is missing in the filter content.
        """

        if isinstance(content, dict):
            content = [content]

        if not isinstance(content, list):
            raise TypeError(
                f"'content' must be of type dict or list. Given type: {type(content)}."
            )

        if len(content) == 0:
            raise ValueError(
                "'content' cannot be an empty list."
            )

        for clause in content:
            if 'path' not in clause or 'order' not in clause:
                raise ValueError(
                    "'sort' required field/s is/are missing: 'path' and/or 'order'."
                )

            _check_type(
                var_name='path',
                value=clause["path"],
                dtype=list,
            )
            _check_type(
                var_name='order',
                value=clause["order"],
                dtype=str,
            )

            self._content.append(
                {
                    'path': clause['path'],
                    'order': clause['order'],
                }
            )

    def __str__(self) -> str:

        sort = f'sort: ['
        for clause in self._content:
            sort += f"{{ path: {dumps(clause['path'])} order: {clause['order']} }} "
        sort += '] '

        return sort


class GetGroup(Filter):
    """
    GetGroup filter class used to group Weaviate objects.
    """

    def __init__(self, content: dict):
        """
        Initialize a GetGroup filter class instance.

        Parameters
        ----------
        content : dict
            The content of the 'sort' filter clause (only for Get).

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If a mandatory key is missing in the filter content.
        """

        super().__init__(content)

        if 'type' not in self._content:
            raise ValueError(
                f"{self.__class__.__name__}: "
                f"'type' required key is missing from 'content' argument.  Given: {content}."
            )

        if 'force' not in self._content:
            raise ValueError(
                f"{self.__class__.__name__}: "
                f"'force' required key is missing from 'content' argument.  Given: {content}."
            )

        _check_type(
            self=self,
            var_name='type',
            value=self._content["type"],
            dtype=str,
        )

        _check_type(
            self=self,
            var_name='force',
            value=self._content["force"],
            dtype=float,
        )

    def __str__(self) -> str:
        group = f'group: {{type: {self._content["type"]}, force: {self._content["force"]}}} '
        return group


class AggregateGroupBy:
    """
    AggregateGroupBy filter class used to group Weaviate objects.
    """

    def __init__(self, content: Union[str, List[str]]):
        """
        Initialize a AggregateGroupBy filter class instance.

        Parameters
        ----------
        content : str or list of str
            The content of the 'groupBy' filter clause (only for Aggregate).

        Raises
        ------
        TypeError
            If 'content' is not of type str or list.
        ValueError
            If a mandatory key is missing in the filter content.
        """

        if not isinstance(content, (str, list)):
            raise TypeError(
                f"{self.__class__.__name__}: "
                f"'content' key-value must be of type str or list. Given type: {type(content)}."
            )

        if isinstance(content, str):
            self._content = [content]
        else:
            self._content = content.copy()

            for property in self._content:
                if not isinstance(property, str):
                    raise TypeError(
                        f"{self.__class__.__name__}: "
                        "If 'content' is of type list all elements must be of type str. "
                        f"Found type: {type(property)}."
                    )

    def __str__(self) -> str:
        group = f'groupBy: {dumps(self._content)} '
        return group


class Where(Filter):
    """
    Where filter class used to filter Weaviate objects.
    """

    def __init__(self, content: dict):
        """
        Initialize a Where filter class instance.

        Parameters
        ----------
        content : dict
            The content of the 'where' filter clause.

        Raises
        ------
        TypeError
            If 'content' is not of type dict.
        ValueError
            If a mandatory key is missing in the filter content.
        """

        super().__init__(content)

        if "path" in self._content:
            self.is_filter = True
            self._parse_filter(self._content)
        elif "operands" in self._content:
            self.is_filter = False
            self._parse_operator(self._content)
        else:
            raise ValueError(
                f"{self.__class__.__name__}: "
                "'path' or 'operands' required key is missing from 'content' argument. "
                f"Given: {self._content}."
            )

    def _parse_filter(self, content: dict) -> None:
        """
        Set filter fields for the Where filter.

        Parameters
        ----------
        content : dict
            The content of the 'where' filter clause.

        Raises
        ------
        ValueError
            If 'content' is missing required fields.
        """

        if "operator" not in content:
            raise ValueError(
                f"{self.__class__.__name__}: "
                f"'operator' required key is missing from 'content' argument. Given: {content}."
            )

        self.path = dumps(content["path"])
        self.operator = content["operator"]
        self.value_type = _find_value_type(content)
        self.value = content[self.value_type]

    def _parse_operator(self, content: dict) -> None:
        """
        Set operator fields for the Where filter.

        Parameters
        ----------
        content : dict
            The content of the 'where' filter clause.

        Raises
        ------
        ValueError
            If 'content' is missing required fields.
        """

        if "operator" not in content:
            raise ValueError(
                f"{self.__class__.__name__}: "
                f"'operator' required key is missing from 'content' argument. Given: {content}."
            )
        _content = deepcopy(content)
        self.operator = _content["operator"]
        self.operands = []
        for operand in _content["operands"]:
            self.operands.append(Where(operand))

    def __str__(self):
        if self.is_filter:
            gql = f'where: {{path: {self.path} operator: {self.operator} {self.value_type}: '
            if self.value_type in ["valueInt", "valueNumber"]:
                gql += f'{self.value}}}'
            elif self.value_type == "valueBoolean":
                gql += f'{_bool_to_str(self.value)}}}'
            else:
                gql += f'{dumps(self.value)}}}'
            return gql + ' '

        operands_str = []
        for operand in self.operands:
            # remove the 'where: ' from the operands and the last space
            operands_str.append(str(operand)[7:-1])
        operands = ", ".join(operands_str)
        return f'where: {{operator: {self.operator} operands: [{operands}]}} '


def _bool_to_str(value: bool) -> str:
    """
    Convert a bool value to string (lowercased) to match JSON formatting.

    Parameters
    ----------
    value : bool
        The value to be converted

    Returns
    -------
    str
        The string interpretation of the value in JSON format.
    """

    if value is True:
        return 'true'
    return 'false'


def _check_direction_clause(self: Filter, direction: dict) -> dict:
    """
    Validate the direction sub clause.

    Parameters
    ----------
    self : Filter
        The filter object from which we call this function. Used to print the class name in error
        messages.
    direction : dict
        A sub clause of the 'nearText' filter.

    Raises
    ------
    TypeError
        If 'direction' is not a dict.
    TypeError
        If the value of the "force" key is not float.
    ValueError
        If no "force" key in the 'direction'.
    """

    _check_type(
        var_name='moveXXX',
        value=direction,
        dtype=dict
    )

    if ('concepts' not in direction) and ('objects' not in direction):
        raise ValueError(
            f"{self.__class__.__name__}: "
            "The 'move' clause should contain 'concepts' OR/AND 'objects'."
        )

    if 'concepts' in direction:
        _check_concept(direction)
    if 'objects' in direction:
        _check_objects(direction)
    if not "force" in direction:
        raise ValueError(
            f"{self.__class__.__name__}: "
            "'move' clause needs to state a 'force'."
        )
    _check_type(
        var_name='force',
        value=direction["force"],
        dtype=float,
    )


def _check_concept(self: Filter, content: dict) -> None:
    """
    Validate the concept sub clause.

    Parameters
    ----------
    self : Filter
        The filter object from which we call this function. Used to print the class name in error
        messages.
    content : dict
        A (sub) clause to check for 'concepts'.

    Raises
    ------
    ValueError
        If no "concepts" key in the 'content' dict.
    TypeError
        If the value of the  "concepts" is of wrong type.
    """

    if "concepts" not in content:
        raise ValueError(
            f"{self.__class__.__name__}: "
            "No concepts in content."
        )

    _check_type(
        var_name='concepts',
        value=content["concepts"],
        dtype=(list, str),
    )
    if isinstance(content["concepts"], str):
        content["concepts"] = [content["concepts"]]


def _check_objects(self: Filter, content: dict) -> None:
    """
    Validate the 'objects' sub clause of the 'move' clause.

    Parameters
    ----------
    self : Filter
        The filter object from which we call this function. Used to print the class name in error
        messages.
    content : dict
        A (sub) clause to check for 'objects'.

    Raises
    ------
    ValueError
        If no "concepts" key in the 'content' dict.
    TypeError
        If the value of the  "concepts" is of wrong type.
    """

    _check_type(
        var_name='objects',
        value=content["objects"],
        dtype=(list, dict)
    )
    if isinstance(content["objects"], dict):
        content["objects"] = [content["objects"]]

    if len(content["objects"]) ==  0:
        raise ValueError(
            "'moveXXX' clause specifies 'objects' but no value provided."
        )

    for obj in content["objects"]:
        if len(obj) != 1 or ('id' not in obj and 'beacon' not in obj):
            raise ValueError(
                f"{self.__class__.__name__}: "
                "Each object from the 'move' clause should have ONLY 'id' OR 'beacon'."
            )


def _check_type(self: Filter, var_name: str, value: Any, dtype: type) -> None:
    """
    Check 'certainty

    Parameters
    ----------
    self : Filter
        The filter object from which we call this function. Used to print the class name in error
        messages.
    var_name : str
        The variable name for which to check the type (used for error message).
    value : Any
        The value for which to check the type.
    dtype : type
        The expected data type of the 'value'.

    Raises
    ------
    TypeError
        If the 'value' type does not match the expected 'dtype'.
    """

    if not isinstance(value, dtype):
        raise TypeError(
            f"{self.__class__.__name__}: "
            f"'{var_name}' key-value must be of type {dtype}. Given type: {type(value)}."
        )


def _find_value_type(self: Filter, content: dict) -> str:
    """
    Find the correct type of the content.

    Parameters
    ----------
    self : Filter
        The filter object from which we call this function. Used to print the class name in error
        messages.
    content : dict
        The content for which to find the appropriate data type.

    Returns
    -------
    str
        The correct data type.

    Raises
    ------
    ValueError
        If missing required fields.
    """

    if "valueString" in content:
        to_return = "valueString"
    elif "valueText" in content:
        to_return = "valueText"
    elif "valueInt" in content:
        to_return = "valueInt"
    elif "valueNumber" in content:
        to_return = "valueNumber"
    elif "valueDate" in content:
        to_return = "valueDate"
    elif "valueBoolean" in content:
        to_return = "valueBoolean"
    elif "valueGeoRange" in content:
        to_return = "valueGeoRange"
    else:
        raise ValueError(
            f"{self.__class__.__name__}: "
            "'value<Type>' required key is missing from one clause of the 'content' argument: "
            f"{content}."
        )
    return to_return


def _move_clause_objects_to_str(objects: list) -> str:
    """
    Convert 'moveXXX' clause to str in GraphQL format.

    Parameters
    ----------
    objects : list
        The objects to use for the 'moveXXX' clause.

    Returns
    -------
    str
        The 'objects' field of the 'moveXXX' clause as str in GraphQL format.
    """

    to_return = ' objects: ['
    for obj in objects:
        if 'id' in obj:
            id_beacon = 'id'
        else:
            id_beacon = 'beacon'
        to_return += f'{{{id_beacon}: {dumps(obj[id_beacon])}}} '
    return to_return + ']'
