"""
Schema class definition.
"""
import asyncio
from typing import Optional
from weaviate.base.schema import (
    BaseSchema,
    get_class_schema_with_primitives_and_path,
    is_sub_schema,
    update_nested_dict,
    is_primitive_property,
    get_complex_properties_from_class,
    check_class,
    pre_create,
    pre_contains,
    pre_create_class,
    pre_delete_class,
    pre_get,
    pre_update_config,
)
from weaviate.exceptions import AiohttpConnectionError, UnsuccessfulStatusCodeError
from .properties.crud_properties import Property
from ..requests import Requests


class Schema(BaseSchema):
    """
    Schema class used to interact and manipulate schemas or classes.

    Attributes
    ----------
    property : weaviate.synchronous.schema.Property
        A Property object to create new schema property/ies.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a Schema class instance.

        Parameters
        ----------
        requests : weaviate.asynchronous.Requests
            Requests object to an active and running Weaviate instance.
        """

        self._requests = requests
        self.property = Property(self._requests)

    async def create(self, schema: dict):
        """
        Create the schema at the Weaviate instance.

        Parameters
        ----------
        schema : dict
            The schema to be created.

        Examples
        --------
        >>> author_class_schema = {
        ...     "class": "Author",
        ...     "description": "An Author class to store the author information",
        ...     "properties": [
        ...         {
        ...             "name": "name",
        ...             "dataType": ["string"],
        ...             "description": "The name of the author",
        ...         },
        ...         {
        ...             "name": "wroteArticles",
        ...             "dataType": ["Article"],
        ...             "description": "The articles of the author",
        ...         }
        ...     ]
        ... }
        >>> await client.schema.create(author_class_schema)

        Raises
        ------
        TypeError
            If the 'schema' is neither a string nor a dict.
        ValueError
            If 'schema' can not be converted into a Weaviate schema.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reported a none OK status.
        weaviate.exceptions.SchemaValidationError
            If the 'schema' could not be validated against the standard format.
        """

        pre_create(
            schema=schema,
        )

        await self._create_classes_with_primitives(
            schema_classes_list=schema["classes"],
        )
        await self._create_complex_properties_from_classes(
            schema_classes_list=schema["classes"],
        )

    async def create_class(self, schema_class: dict):
        """
        Create a single class as part of the schema in Weaviate.

        Parameters
        ----------
        schema_class : dict or str
            The schema class to be created.

        Examples
        --------
        >>> author_schema_class = {
        ...     "class": "Author",
        ...     "description": "An Author class to store the author information",
        ...     "properties": [
        ...         {
        ...             "name": "name",
        ...             "dataType": ["string"],
        ...             "description": "The name of the author",
        ...         },
        ...         {
        ...             "name": "wroteArticles",
        ...             "dataType": ["Article"],
        ...             "description": "The articles of the author",
        ...         }
        ...     ]
        ... }
        >>> await client.schema.create_class(author_schema_class)

        Raises
        ------
        TypeError
            If the 'schema_class' is neither a string nor a dict.
        ValueError
            If 'schema_class' can not be converted into a Weaviate schema.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reported a none OK status.
        weaviate.exceptions.SchemaValidationError
            If the 'schema_class' could not be validated against the standard format.
        """

        pre_create_class(
            schema_class=schema_class,
        )

        await self._create_class_with_premitives(
            schema_class=schema_class,
        )
        await self._create_complex_properties_from_class(
            schema_class=schema_class,
        )

    async def delete_class(self, class_name: str):
        """
        Delete a schema class from Weaviate. This deletes all associated data.

        Parameters
        ----------
        class_name : str
            The class that should be deleted from Weaviate.

        Examples
        --------
        >>> await client.schema.delete_class('Author')

        Raises
        ------
        TypeError
            If 'class_name' argument not of type 'str'.
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reported a none OK status.
        """

        path = pre_delete_class(
            class_name=class_name,
        )

        try:
            response = await self._requests.delete(
                path=path,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Deletion of class failed due to connection error.'
            ) from conn_err
        if response.status != 200:
            raise UnsuccessfulStatusCodeError(
                "Delete class from schema.",
                status_code=response.status,
                response_message=await response.text(),
            )

    async def delete_all(self):
        """
        Remove the entire schema from the Weaviate instance and all data associated with it.

        Examples
        --------
        >>> await client.schema.delete_all()
        """

        schema = await self.get()
        class_names = [_class["class"] for _class in schema.get("classes", [])]
        tasks = [asyncio.create_task(self.delete_class(class_name)) for class_name in class_names]
        await asyncio.gather(*tasks, return_exceptions=False)

    async def contains(self, schema: Optional[dict]=None):
        """
        Check if Weaviate already contains a schema.

        Parameters
        ----------
        schema : dict or None, optional
            The (sub-)schema to check if is part of the Weaviate existing schema. If a 'schema' is
            not None, it checks if this specific schema is already loaded. If the given schema is a
            subset of the loaded schema it will still return True. If 'schema' is None it checks
            for any existing schema, by default None.

        Examples
        --------
        >>> schema = await client.schema.get()
        >>> client.schema.contains(schema)
        True
        >>> schema = await client.schema.get()
        >>> schema['classes'].append(
            {
                "class": "Animal",
                "description": "An Animal",
                "properties": [
                    {
                        "name": "type",
                        "dataType": ["string"],
                        "description": "The animal type",
                    }
                ]
            }
        )
        >>> await client.schema.contains(schema)
        False

        Returns
        -------
        bool
            True if a schema is present, False otherwise.
        """

        pre_contains(
            schema=schema,
        )

        loaded_schema = await self.get()

        if schema is not None:
            sub_schema = schema
            return is_sub_schema(sub_schema, loaded_schema)

        if len(loaded_schema["classes"]) == 0:
            return False
        return True

    async def update_config(self, class_name: str, config: dict):
        """
        Update a schema configuration for a specific class.

        Parameters
        ----------
        class_name : str
            The class for which to update the schema configuration.
        config : dict
            The configurations to update (MUST follow schema format).

        Example
        -------
        In the example below we have a Weaviate instance with a class 'Test'.

        >>> await client.schema.get('Test')
        {
            'class': 'Test',
            ...
            'vectorIndexConfig': {
                'ef': -1,
                ...
            },
            ...
        }
        >>> await client.schema.update_config(
        ...     class_name='Test',
        ...     config={
        ...         'vectorIndexConfig': {
        ...             'ef': 100,
        ...         }
        ...     }
        ... )
        >>> await client.schema.get('Test')
        {
            'class': 'Test',
            ...
            'vectorIndexConfig': {
                'ef': 100,
                ...
            },
            ...
        }

        NOTE: When updating schema configuration, the 'config' MUST be sub-set of the schema,
        starting at the top level. In the example above we update 'ef' value, and for this we
        included the 'vectorIndexConfig' top level too.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to Weaviate fails.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reports a none OK status.
        """

        path, class_name = pre_update_config(class_name=class_name)
        class_schema = await self.get(class_name)
        new_class_schema = update_nested_dict(class_schema, config)
        check_class(new_class_schema)

        try:
            response = await self._requests.put(
                path=path,
                data_json=new_class_schema,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                "Class schema configuration could not be updated die to connection error."
            ) from conn_err
        if response.status != 200:
            raise UnsuccessfulStatusCodeError(
                "Update class schema configuration.",
                status_code=response.status,
                response_message=await response.text(),
            )

    async def get(self, class_name: Optional[str]=None):
        """
        Get the schema from Weaviate.

        Parameters
        ----------
        class_name : str or None, optional
            The class for which to return the schema. If NOT provided the whole schema is returned,
            otherwise only the schema of this class is returned. By default None.

        Returns
        -------
        dict
            A dict containing the schema. The schema may be empty.
            To see if a schema has already been loaded use 'contains' method.

        Examples
        --------
        No schema present in client

        >>> async client.schema.get()
        {'classes': []}

        Schema present in client

        >>> async client.schema.get()
        {
            "classes": [
                {
                "class": "Animal",
                "description": "An Animal",
                "invertedIndexConfig": {
                    "cleanupIntervalSeconds": 60
                },
                "properties": [
                    {
                    "dataType": [
                        "string"
                    ],
                    "description": "The animal type",
                    "name": "type"
                    }
                ],
                "vectorIndexConfig": {
                    "cleanupIntervalSeconds": 300,
                    "maxConnections": 64,
                    "efConstruction": 128,
                    "vectorCacheMaxObjects": 500000
                },
                "vectorIndexType": "hnsw",
                "vectorizer": "text2vec-contextionary"
                }
            ]
        }

        >>> async client.schema.get('Animal')
        {
            "class": "Animal",
            "description": "An Animal",
            "invertedIndexConfig": {
                "cleanupIntervalSeconds": 60
            },
            "properties": [
                {
                "dataType": [
                    "string"
                ],
                "description": "The animal type",
                "name": "type"
                }
            ],
            "vectorIndexConfig": {
                "cleanupIntervalSeconds": 300,
                "maxConnections": 64,
                "efConstruction": 128,
                "vectorCacheMaxObjects": 500000
            },
            "vectorIndexType": "hnsw",
            "vectorizer": "text2vec-contextionary"
        }

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reported a none OK status.
        """

        path = pre_get(class_name=class_name)

        try:
            response = await self._requests.get(
                path=path,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Schema could not be retrieved due to connection error.'
            ) from conn_err
        if response.status == 200:
            return response.json()
        raise UnsuccessfulStatusCodeError(
            "Get schema.",
            status_code=response.status,
            response_message=await response.text(),
        )

    async def _create_complex_properties_from_class(self, schema_class: dict):
        """
        Add cross-references to already existing class.

        Parameters
        ----------
        schema_class : dict
            Description of the class that should be added.

        Raises
        ------
        aiohttp.ClientConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reported a none OK status.
        """

        if 'properties' not in schema_class:
            # Class has no properties nothing to do
            return

        for _property in schema_class['properties']:
            if is_primitive_property(_property['dataType']):
                continue

            schema_property = get_complex_properties_from_class(
                schema_property=_property,
            )

            await self.property.create(
                schema_class_name=schema_class['class'],
                schema_property=schema_property,
            )

    async def _create_complex_properties_from_classes(self, schema_classes_list: list):
        """
        Add crossreferences to already existing classes.

        Parameters
        ----------
        schema_classes_list : list
            A list of classes as they are found in a schema json description.
        """

        tasks = []
        for schema_class in schema_classes_list:
            tasks.append(
                asyncio.create_task(self._create_complex_properties_from_class(schema_class))
            )
        asyncio.gather(*tasks, return_exceptions=False)

    async def _create_class_with_premitives(self, schema_class: dict):
        """
        Create class with only primitives.

        Parameters
        ----------
        schema_class : dict
            A single Weaviate formated class

        Raises
        ------
        requests.ConnectionError
            If the network connection to Weaviate failed.
        weaviate.exceptions.UnsuccessfulStatusCodeError
            If Weaviate reported a none OK status.
        """

        # Create the class
        schema_class, path = get_class_schema_with_primitives_and_path(
            schema_class=schema_class,
        )
        try:
            response = await self._requests.post(
                path=path,
                data_json=schema_class,
            )
        except AiohttpConnectionError as conn_err:
            raise AiohttpConnectionError(
                'Class may not have been created properly due to connection error.'
            ) from conn_err
        if response.status != 200:
            raise UnsuccessfulStatusCodeError(
                "Create class",
                status_code=response.status,
                response_message=await response.text(),
            )

    async def _create_classes_with_primitives(self, schema_classes_list: list):
        """
        Create all the classes in the list and primitive properties.
        This function does not create references,
        to avoid references to classes that do not yet exist.

        Parameters
        ----------
        schema_classes_list : list
            A list of classes as they are found in a schema json description.
        """

        tasks = []
        for weaviate_class in schema_classes_list:
            tasks.append(
                asyncio.create_task(self._create_class_with_premitives(weaviate_class))
            )
