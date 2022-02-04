"""
Property class definition.
"""
from weaviate.exceptions import WeaviateConnectionError, UnsuccessfulStatusCodeError
from weaviate.schema.validate_schema import check_property
from weaviate.util import _get_dict_from_object, _capitalize_first_letter
from weaviate.connect import Connection


class Property:
    """
    Property class used to create object properties.
    """

    def __init__(self, connection: Connection):
        """
        Initialize a Property class instance.

        Parameters
        ----------
        connection : weaviate.connect.Connection
            Connection object to an active and running weaviate instance.
        """

        self._connection = connection

    def create(self, schema_class_name: str, schema_property: dict) -> None:
        """
        Create a class property.

        Parameters
        ----------
        schema_class_name : str
            The name of the schema class to which the property should be added.
        schema_property : dict
            The property that should be added.

        Examples
        --------
        >>> property_age = {
        ...     "dataType": [
        ...         "int"
        ...     ],
        ...     "description": "The Author's age",
        ...     "name": "age"
        ... }
        >>> client.schema.property.create('Author', property_age)

        Raises
        ------
        TypeError
            If 'schema_class_name' is not of type 'str'.
        requests.exceptions.ConnectionError
            If the network connection to weaviate fails.
        weaviate.exceptions.SchemaValidationException
            If the 'schema_property' is not valid.
        weaviate.exceptions.UnexpectedStatusCodeException
            If weaviate reports a none OK status.
        """

        if not isinstance(schema_class_name, str):
            raise TypeError(
                f"'schema_class_name' must be of type 'str'. Given type: {type(schema_class_name)}"
            )

        loaded_schema_property = _get_dict_from_object(schema_property)

        # check if valid property
        check_property(
            class_property=loaded_schema_property,
            class_name=schema_class_name,
        )

        schema_class_name = _capitalize_first_letter(schema_class_name)

        path = f"/schema/{schema_class_name}/properties"
        try:
            response = self._connection.post(
                path=path,
                data_json=loaded_schema_property,
            )
        except WeaviateConnectionError as conn_err:
            raise WeaviateConnectionError(
                'Property was created due to connection error.'
            ) from conn_err
        if response.status_code != 200:
            raise UnsuccessfulStatusCodeError("Add property to class!", response)
