"""
SyncProperty class definition.
"""
from weaviate.exceptions import RequestsConnectionError, UnsuccessfulStatusCodeError
from weaviate.util import capitalize_first_letter
from weaviate.base import BaseProperty
from ...requests import SyncRequests


class SyncProperty(BaseProperty):
    """
    SyncProperty class used to create object properties.
    """

    def __init__(self, requests: SyncRequests):
        """
        Initialize a SyncProperty class instance.

        Parameters
        ----------
        requests : weaviate.synchronous.SyncRequests
            SyncRequests object to an active and running weaviate instance.
        """

        self._requests = requests

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

        super().create(
            schema_class_name=schema_class_name,
            schema_property=schema_property,
        )

        schema_class_name = capitalize_first_letter(schema_class_name)

        path = f"/schema/{schema_class_name}/properties"
        try:
            response = self._requests.post(
                path=path,
                data_json=schema_property,
            )
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Property was created due to connection error.'
            ) from conn_err
        if response.status_code != 200:
            raise UnsuccessfulStatusCodeError(
                "Add property to class.",
                status_code=response.status_code,
                response_message=response.text,
            )
