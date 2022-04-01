"""
Property class definition.
"""
from weaviate.exceptions import AiohttpConnectionError, UnsuccessfulStatusCodeError
from weaviate.base.schema.properties import BaseProperty, pre_create
from ...requests import Requests


class Property(BaseProperty):
    """
    Property class used to create object properties.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a SyncProperty class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests

    async def create(self, schema_class_name: str, schema_property: dict) -> None:
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
        >>> await client.schema.property.create('Author', property_age)

        Raises
        ------
        TypeError
            If 'schema_class_name' is not of type 'str'.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.SchemaValidationException
            If the 'schema_property' is not valid.
        weaviate.exceptions.UnexpectedStatusCodeException
            If Weaviate reports a none OK status.
        """

        path = pre_create(
            schema_class_name=schema_class_name,
            schema_property=schema_property,
        )
        try:
            response = await self._requests.post(
                path=path,
                data_json=schema_property,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Property was created due to connection error.'
            ) from conn_err
        if response.status != 200:
            raise UnsuccessfulStatusCodeError(
                "Add property to class.",
                status_code=response.status,
                response_message=await response.text(),
            )
