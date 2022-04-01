"""
Batch class definitions.
"""
import sys
import time
from typing import Optional, Sequence
from requests import ReadTimeout, Response
from weaviate.exceptions import (
    RequestsConnectionError,
    UnsuccessfulStatusCodeError,
    BatchObjectCreationError,
)
from weaviate.base.batch import (
    BaseBatch,
    BatchType,
    BatchRequest,
    ObjectBatchRequest,
    ReferenceBatchRequest,
    check_batch_result,
)
from ..requests import Requests


class Batch(BaseBatch):
    """
    Batch class used to add multiple objects or object references at once into weaviate.
    To add data to the Batch use these methods of this class: `add_data_object` and
    `add_reference`. This object also stores 2 recommended batch size variables, one for objects
    and one for references. The recommended batch size is updated with every batch creation, and
    is the number of data objects/references that can be sent/processed by the Weaviate server in
    `creation_time` interval (see `configure` or `__call__` method on how to set this value, by
    default it is set to 10). The initial value is None/batch_size and is updated with every batch
    create methods. The values can be accessed with the getters: `recommended_num_objects` and
    `recommended_num_references`.
    NOTE: If the UUID of one of the objects already exists then the existing object will be
    replaced by the new object.

    This class can be used in 3 ways:

    Case I:
        Everything should be done by the user, i.e. the user should add the
        objects/object-references and create them whenever the user wants. To create one of the
        data type use these methods of this class: `create_objects`, `create_references` and
        `flush`. This case has the Batch instance's batch_size set to None (see docs for the
        `configure` or `__call__` method). Can be used in a context manager, see below.

    Case II:
        Batch auto-creates when full. This can be achieved by setting the Batch instance's
        batch_size set to a positive integer (see docs for the `configure` or `__call__` method).
        The batch_size in this case corresponds to the sum of added objects and references.
        This case does not require the user to create the batch/s, but it can be done. Also to
        create non-full batches (last batche/s) that do not meet the requirement to be auto-created
        use the `flush` method. Can be used in a context manager, see below.

    Case III:
        Similar to Case II but uses dynamic batching, i.e. auto-creates either objects or
        references when one of them reached the `recommended_num_objects` or
        `recommended_num_references` respectively. See docs for the `configure` or `__call__`
        method for how to enable it.

    Context-manager support: Can be use with the `with` statement. When it exists the context-
        manager it calls the `flush` method for you. Can be combined with `configure`/`__call__`
        method, in order to set it to the desired Case.

    Examples
    --------
    Here are examples for each CASE described above. Here `client` is an instance of the
    `weaviate.Client`.

    >>> object_1 = '154cbccd-89f4-4b29-9c1b-001a3339d89d'
    >>> object_2 = '154cbccd-89f4-4b29-9c1b-001a3339d89c'
    >>> object_3 = '254cbccd-89f4-4b29-9c1b-001a3339d89a'
    >>> object_4 = '254cbccd-89f4-4b29-9c1b-001a3339d89b'

    For Case I:

    >>> client.batch.shape
    (0, 0)
    >>> client.batch.add_data_object({}, 'MyClass')
    >>> client.batch.add_data_object({}, 'MyClass')
    >>> client.batch.add_reference(object_1, 'MyClass', 'myProp', object_2)
    >>> client.batch.shape
    (2, 1)
    >>> client.batch.create_objects()
    >>> client.batch.shape
    (0, 1)
    >>> client.batch.create_references()
    >>> client.batch.shape
    (0, 0)
    >>> client.batch.add_data_object({}, 'MyClass')
    >>> client.batch.add_reference(object_3, 'MyClass', 'myProp', object_4)
    >>> client.batch.shape
    (1, 1)
    >>> client.batch.flush()
    >>> client.batch.shape
    (0, 0)

    Or with a context manager:

    >>> with client.batch as batch:
    ...     batch.add_data_object({}, 'MyClass')
    ...     batch.add_reference(object_3, 'MyClass', 'myProp', object_4)
    >>> # flush was called
    >>> client.batch.shape
    (0, 0)

    For Case II:

    >>> client.batch(batch_size=3)
    >>> client.batch.shape
    (0, 0)
    >>> client.batch.add_data_object({}, 'MyClass')
    >>> client.batch.add_reference(object_1, 'MyClass', 'myProp', object_2)
    >>> client.batch.shape
    (1, 1)
    >>> client.batch.add_data_object({}, 'MyClass') # sum of data_objects and references reached
    >>> client.batch.shape
    (0, 0)

    Or with a context manager and `__call__` method:

    >>> with client.batch(batch_size=3) as batch:
    ...     batch.add_data_object({}, 'MyClass')
    ...     batch.add_reference(object_3, 'MyClass', 'myProp', object_4)
    ...     batch.add_data_object({}, 'MyClass')
    ...     batch.add_reference(object_1, 'MyClass', 'myProp', object_4)
    >>> # flush was called
    >>> client.batch.shape
    (0, 0)

    Or with a context manager and setter:

    >>> client.batch.batch_size = 3
    >>> with client.batch as batch:
    ...     batch.add_data_object({}, 'MyClass')
    ...     batch.add_reference(object_3, 'MyClass', 'myProp', object_4)
    ...     batch.add_data_object({}, 'MyClass')
    ...     batch.add_reference(object_1, 'MyClass', 'myProp', object_4)
    >>> # flush was called
    >>> client.batch.shape
    (0, 0)

    For Case III:
    Same as Case II but you need to configure or enable 'dynamic' batching.

    >>> client.batch.configure(batch_size=3, dynamic=True) # 'batch_size' must be an valid int

    Or:

    >>> client.batch.batch_size = 3
    >>> client.batch.dynamic = True

    See the documentation of the `configure`( or `__call__`) and the setters for more information
    on how/why and what you need to configure/set in order to use a particular Case.
    """

    def __init__(self, requests: Requests):
        """
        Initialize a Batch class instance. This defaults to manual creation configuration.
        See docs for the `configure` or `__call__` method for different types of configurations.

        Parameters
        ----------
        requests : weaviate.synchronous.Requests
            Requests object to an active and running weaviate instance.
        """

        super().__init__()

        # set all protected attributes
        self._requests = requests

    def __call__(self,
            **kwargs,
        ) -> 'Batch':
        """
        Configure the instance to your needs. (`__call__` and `configure` methods are the same).
        NOTE: Changes only the attributes of the passed arguments.

        Parameters
        ----------
        batch_size : Optional[int], optional
            The batch size to be use. This value sets the Batch functionality, if `batch_size` is
            None then no auto-creation is done (`callback` and `dynamic` are ignored). If it is a
            positive number auto-creation is enabled and the value represents: 1) in case `dynamic`
            is False -> the number of data in the Batch (sum of objects and references) when to
            auto-create; 2) in case `dynamic` is True -> the initial value for both
            `recommended_num_objects` and `recommended_num_references`, initial value None
        creation_time : Real, optional
            The time interval it should take the Batch to be created, used ONLY for computing
            `recommended_num_objects` and `recommended_num_references`, initial value 10.0
        timeout_retries : int, optional
            Number of times to retry to create a Batch that failed with TimeOut error,
            initial value 0
        callback : Optional[Callable[[dict], None]], optional
            A callback function on the results of each (objects and references) batch types. It is
            used only when `batch_size` is NOT None, initial value None
        dynamic : bool, optional
            Whether to use dynamic batching or not, initial value False
        rolling_frame_size : int
            The size of the rolling frame for the object/reference creation time. It is used for a
            better estimation of the recommended number of objects/references to be created in the
            'creation_time' span. recommended number of objects/references to be created in the
            'creation_time' span, initial value 5

        Returns
        -------
        Batch
            Updated self.

        Raises
        ------
        TypeError
            If one of the arguments is of a wrong type.
        ValueError
            If the value of one of the arguments is wrong.
        """

        super().__call__(
            **kwargs,
        )

        if self._batch_config.type != BatchType.MANUAL:
            self._auto_create()
        return self

    def add_data_object(self,
            data_object: dict,
            class_name: str,
            uuid: Optional[str]=None,
            vector: Optional[Sequence]=None
        ) -> None:
        """
        Add one object to this batch.
        NOTE: If the UUID of one of the objects already exists then the existing object will be
        replaced by the new object.

        Parameters
        ----------
        data_object : dict
            Object to be added as a dict datatype.
        class_name : str
            The name of the class this object belongs to.
        uuid : str, optional
            UUID of the object as a string, by default None
        vector: Sequence, optional
            The embedding of the object that should be created. Used only class objects that do not
            have a vectorization module. Supported types are `list`, 'numpy.ndarray`,
            `torch.Tensor` and `tf.Tensor`,
            by default None.

        Raises
        ------
        TypeError
            If an argument passed is not of an appropriate type.
        ValueError
            If 'uuid' is not of a propper form.
        """

        super().add_data_object(
            data_object=data_object,
            class_name=class_name,
            uuid=uuid,
            vector=vector,
        )

        if self._batch_config.type != BatchType.MANUAL:
            self._auto_create()

    def add_reference(self,
            from_object_uuid: str,
            from_object_class_name: str,
            from_property_name: str,
            to_object_uuid: str
        ) -> None:
        """
        Add one reference to this batch.

        Parameters
        ----------
        from_object_uuid : str
            The UUID or URL of the object that should reference another object.
        from_object_class_name : str
            The name of the class that should reference another object.
        from_property_name : str
            The name of the property that contains the reference.
        to_object_uuid : str
            The UUID or URL of the object that is actually referenced.

        Raises
        ------
        TypeError
            If arguments are not of type str.
        ValueError
            If 'uuid' is not valid or cannot be extracted.
        """

        super().add_reference(
            from_object_class_name=from_object_class_name,
            from_object_uuid=from_object_uuid,
            from_property_name=from_property_name,
            to_object_uuid=to_object_uuid,
        )

        if self._batch_config.type != BatchType.MANUAL:
            self._auto_create()

    def _create_data(self,
            data_type: str,
            batch_request: BatchRequest,
        ) -> Response:
        """
        Create data in batches, either Objects or References. This does NOT guarantee
        that each batch item (only Objects) is added/created. This can lead to a successfull
        batch creation but unsuccessfull per batch item creation. See the Examples below.

        Parameters
        ----------
        data_type : str
            The data type of the BatchRequest, used to save time for not checking the type of the
            BatchRequest.
        batch_request : weaviate.batch.BatchRequest
            Contains all the data objects that should be added in one batch.
            Note: Should be a sub-class of BatchRequest since BatchRequest
            is just an abstract class, e.g. ObjectBatchRequest, ReferenceBatchRequest

        Returns
        -------
        requests.Response
            The requests response.

        Raises
        ------
        requests.ConnectionError
            If the network connection to weaviate fails.
        weaviate.UnexpectedStatusCodeException
            If weaviate reports a none OK status.
        """

        try:
            for i in range(self._batch_config.timeout_retries + 1):
                try:
                    response = self._requests.post(
                        path='/batch/' + data_type,
                        data_json=batch_request.get_request_body(),
                    )
                except ReadTimeout:
                    if i == self._batch_config.timeout_retries:
                        raise
                    print(
                        f'[ERROR] Batch ReadTimeout Exception occurred! Retring in {2 * (i + 1)}s. '
                            f'[{i+1}/{self._batch_config.timeout_retries}]',
                        file=sys.stderr)
                    time.sleep(2 * (i + 1))
                else:
                    break
        except RequestsConnectionError as conn_err:
            raise RequestsConnectionError(
                'Batch was not added to weaviate.'
            ) from conn_err
        except ReadTimeout as timeout_error:
            timeout_config = self._requests.timeout_config
            if isinstance(timeout_config, tuple):
                timeout_config = timeout_config[1]
            message = (
                f"The '{data_type}' creation was cancelled because it took "
                f"longer than the configured timeout of {timeout_config}s. "
                f"Try reducing the batch size (currently {len(batch_request)}) to a lower value. "
                "Aim to, on average, complete batch request within less than 10s"
            )
            raise ReadTimeout(message) from timeout_error
        if response.status_code == 200:
            return response
        raise UnsuccessfulStatusCodeError(
            f"Create {data_type} in batch",
            status_code=response.status_code,
            response_message=response.text,
        )

    def create_objects(self) -> list:
        """
        Creates multiple Objects at once in Weaviate.
        NOTE: If the UUID of one of the objects already exists then the existing object will be
        replaced by the new object.

        Examples
        --------
        Here 'client' is an instance of the 'weaviate.Client'.

        Add objects to the object batch.

        >>> client.batch.add_data_object({}, 'ExistingClass')
        >>> result = client.batch.create_objects(batch)
        >>> import json
        >>> print(json.dumps(result, indent=4))
        [
            {
                "class": "ExistingClass",
                "creationTimeUnix": 1614852753746,
                "id": "b7b1cfbe-20da-496c-b932-008d35805f26",
                "properties": {},
                "vector": [
                    -0.05244319,
                    ...
                    0.076136276
                ],
                "deprecations": null,
                "result": {}
            }
        ]

        To check the results of batch creation when using the auto-creation Batch, use a 'callback'
        (see the docs `configure` or `__call__` method for more information).

        Returns
        -------
        list
            A list with the status of every object that was created.

        Raises
        ------
        requests.ConnectionError
            If the network connection to weaviate fails.
        weaviate.UnexpectedStatusCodeException
            If weaviate reports a none OK status.
        """

        if len(self._objects_batch) != 0:
            response = self._create_data(
                data_type='objects',
                batch_request=self._objects_batch,
            )
            results = response.json()
            if check_batch_result(results):
                raise BatchObjectCreationError(
                    "One or more batch objects creation failed. If the error is caught, the "
                    "per-object result is accessible using this error's '.batch_results' "
                    "attribute. The batch objects can also be accessed using this error's "
                    "'.batch_objects' attribute.",
                    batch_results=results,
                    batch_objects=self._objects_batch.get_request_body()['objects']
                )

            object_creation_time = response.elapsed.total_seconds() / len(self._objects_batch)

            self._batch_config.add_object_creation_time_to_frame(
                creation_time=object_creation_time,
            )
            self._objects_batch = ObjectBatchRequest()
            return results
        return []

    def create_references(self) -> list:
        """
        Creates multiple References at once in Weaviate.
        Adding References in batch is faster but it ignors validations like class name
        and property name, resulting in a SUCCESSFUL reference creation of a nonexistent object
        types and/or a nonexistent properties. If the consistency of the References is wanted
        use 'client.data_object.reference.add' to have additional validation against the
        weaviate schema. See Examples below.

        Examples
        --------
        Here `client` is an instance of the `weaviate.Client`.

        Object that does not exist in weaviate.

        >>> object_1 = '154cbccd-89f4-4b29-9c1b-001a3339d89d'

        Objects that exist in weaviate.

        >>> object_2 = '154cbccd-89f4-4b29-9c1b-001a3339d89c'
        >>> object_3 = '254cbccd-89f4-4b29-9c1b-001a3339d89a'
        >>> object_4 = '254cbccd-89f4-4b29-9c1b-001a3339d89b'

        >>> client.batch.add_reference(object_1, 'NonExistingClass', 'existsWith', object_2)
        >>> client.batch.add_reference(object_3, 'ExistingClass', 'existsWith', object_4)

        Both references were added to the batch request without error because they meet the
        required citeria (See the documentation of the 'weaviate.Batch.add_reference' method
        for more information).

        >>> result = client.batch.create_references()

        As it can be noticed the reference batch creation is successful (no error thrown). Now we
        can inspect the 'result'.

        >>> import json
        >>> print(result, indent=4))
        [
            {
                "from": "weaviate://localhost/NonExistingClass/
                                                154cbccd-89f4-4b29-9c1b-001a3339d89a/existsWith",
                "to": "weaviate://localhost/154cbccd-89f4-4b29-9c1b-001a3339d89b",
                "result": {
                    "status": "SUCCESS"
                }
            },
            {
                "from": "weaviate://localhost/ExistingClass/
                                                254cbccd-89f4-4b29-9c1b-001a3339d89a/existsWith",
                "to": "weaviate://localhost/254cbccd-89f4-4b29-9c1b-001a3339d89b",
                "result": {
                    "status": "SUCCESS"
                }
            }
        ]

        Both references were added successfully but one of them is corrupted (links two objects
        of nonexisting class and one of the objects is not yet created). To make use of the
        validation, crete each references individually (see the client.data_object.reference.add
        method).

        Returns
        -------
        list
            A list with the status of every reference added.

        Raises
        ------
        requests.ConnectionError
            If the network connection to weaviate fails.
        weaviate.UnexpectedStatusCodeException
            If weaviate reports a none OK status.
        """

        if len(self._reference_batch) != 0:
            response = self._create_data(
                data_type='references',
                batch_request=self._reference_batch,
            )
            ref_per_second = len(self._reference_batch) / response.elapsed.total_seconds()
            self._recommended_num_references = round(ref_per_second * self._creation_time)
            self._reference_batch = ReferenceBatchRequest()
            return response.json()
        return []

    def _auto_create(self) -> None:
        """
        Auto create both objects and references in the batch. This protected method works with a
        fixed batch size (BatchType.AUTO) and with dynamic batching (BatchType.DYNAMIC). For a 
        'fixed' batching type it auto-creates when the sum of both objects and references equals
        batch_size. For dynamic batching it creates both batch requests when only one is full.
        """

        # greater or equal in case the self._batch_size is changed manually
        if self._batch_config.type == BatchType.AUTO:
            if sum(self.shape) >= self._batch_config.size:
                self.flush()
            return

        if self._batch_config.type == BatchType.DYNAMIC:
            if (
                self.num_objects() >= self._batch_config.recommended_num_objects
                or self.num_references() >= self._batch_config.recommended_num_references
            ):
                self.flush()
            return
        # just in case
        raise ValueError(f"Unsupported batching type '{self._batch_config.type}'.")

    def flush(self) -> None:
        """
        Flush both objects and references to the Weaviate server and call the callback function
        if one is provided. (See the docs for `configure` or `__call__` for how to set one.)
        """

        result_objects = self.create_objects()
        self.create_references()
        if self._batch_config.callback is not None:
            if result_objects:
                self._batch_config.callback(result_objects)
