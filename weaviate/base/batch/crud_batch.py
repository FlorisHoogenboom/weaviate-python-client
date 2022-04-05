"""
Batch class definitions.
"""
from numbers import Real
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Sequence
from weaviate.util import capitalize_first_letter
from .requests import BatchRequest, ObjectBatchRequest, ReferenceBatchRequest
from .batch_config import BatchType, BatchConfig


class BaseBatch(ABC):
    """
    BaseBatch class used to add multiple objects or object references at once into weaviate.
    """

    def __init__(self):
        """
        Initialize a Batch class instance. This defaults to manual creation configuration.
        See docs for the 'configure' or '__call__' method for different types of configurations.

        Parameters
        ----------
        connection : weaviate.connect.Connection
            Connection object to an active and running weaviate instance.
        """

        # set all protected attributes
        self._objects_batch = ObjectBatchRequest()
        self._reference_batch = ReferenceBatchRequest()

        ## user configurable, need to be public should implement a setter/getter
        self._batch_config = BatchConfig()

    def configure(self,
            **kwargs,
        ):
        """
        Configure the instance to your needs. ('__call__' and 'configure' methods are the same).
        NOTE: Changes only the attributes of the passed arguments.

        Parameters
        ----------
        batch_size : Optional[int], optional
            The batch size to be use. This value sets the Batch functionality, if 'batch_size' is
            None then no auto-creation is done ('dynamic' is ignored). If it is a positive number,
            auto-creation is enabled and the value represents:
                1) in case 'dynamic' is False -> the number of data in the Batch (sum of objects
                and references) when to auto-create;
                2) in case 'dynamic' is True -> is used as the initial value for both
                'recommended_num_objects' and 'recommended_num_references', (initial value None)
        creation_time : Real, optional
            The time interval it should take the Batch to be created, used ONLY for computing
            'recommended_num_objects' and 'recommended_num_references', initial value 10.0
        timeout_retries : int, optional
            Number of times to retry to create a Batch that failed with TimeOut error,
            initial value 0
        callback : Optional[Callable[[list, list], None]], optional
            A callback function on the results of each object batch only. The callback has to have
            two arguments: one for the list of results, and one for the list of objects themselves
            in case it is needed to get the object that failed.
            NOTE: If 'raise_object_error' is True, then the error is going to be raised before
            calling the 'callback' function. Set 'raise_object_error' to False if 'callback' is
            needed to be called and handle the errors in there.
        dynamic : bool, optional
            Whether to use dynamic batching or not, initial value False
        rolling_frame_size : int
            The size of the rolling frame for the object/reference creation time. It is used for a
            better estimation of the recommended number of objects/references to be created in the
            'creation_time' span. recommended number of objects/references to be created in the
            'creation_time' span, initial value 5
        raise_object_error: bool
            Whether to raise 'BatchObjectCreationError' in case one of the object creation failed.
            If False, one could use the 'callback' argument to check which objects failed creation.
            This influences the 'callback' argument, see the 'callback' argument description.
            Initial value True.

        Raises
        ------
        TypeError
            If one of the arguments is of a wrong type.
        ValueError
            If the value of one of the arguments is wrong.
        """

        return self.__call__(
            **kwargs,
        )

    @abstractmethod
    def __call__(self,
            **kwargs
        ):
        """
        Configure the instance to your needs. ('__call__' and 'configure' methods are the same).
        NOTE: Changes only the attributes of the passed arguments.

        Parameters
        ----------
        batch_size : Optional[int], optional
            The batch size to be use. This value sets the Batch functionality, if 'batch_size' is
            None then no auto-creation is done ('dynamic' is ignored). If it is a positive number,
            auto-creation is enabled and the value represents:
                1) in case 'dynamic' is False -> the number of data in the Batch (sum of objects
                and references) when to auto-create;
                2) in case 'dynamic' is True -> is used as the initial value for both
                'recommended_num_objects' and 'recommended_num_references', (initial value None)
        creation_time : Real, optional
            The time interval it should take the Batch to be created, used ONLY for computing
            'recommended_num_objects' and 'recommended_num_references', initial value 10.0
        timeout_retries : int, optional
            Number of times to retry to create a Batch that failed with TimeOut error,
            initial value 0
        callback : Optional[Callable[[list, list], None]], optional
            A callback function on the results of each object batch only. The callback has to have
            two arguments: one for the list of results, and one for the list of objects themselves
            in case it is needed to get the object that failed.
            NOTE: If 'raise_object_error' is True, then the error is going to be raised before
            calling the 'callback' function. Set 'raise_object_error' to False if 'callback' is
            needed to be called and handle the errors in there.
        dynamic : bool, optional
            Whether to use dynamic batching or not, initial value False
        rolling_frame_size : int
            The size of the rolling frame for the object/reference creation time. It is used for a
            better estimation of the recommended number of objects/references to be created in the
            'creation_time' span. recommended number of objects/references to be created in the
            'creation_time' span, initial value 5
        raise_object_error: bool
            Whether to raise 'BatchObjectCreationError' in case one of the object creation failed.
            If False, one could use the 'callback' argument to check which objects failed creation.
            This influences the 'callback' argument, see the 'callback' argument description.
            Initial value True.

        Raises
        ------
        TypeError
            If one of the arguments is of a wrong type.
        ValueError
            If the value of one of the arguments is wrong.
        """

        batch_size = kwargs.get('batch_size', self._batch_config.size)
        creation_time = kwargs.get('creation_time', self._batch_config.creation_time)
        timeout_retries = kwargs.get('timeout_retries', self._batch_config.timeout_retries)
        callback = kwargs.get('callback', self._batch_config.callback)
        dynamic = kwargs.get('dynamic', self._batch_config.size == BatchType.DYNAMIC)
        rolling_frame_size = kwargs.get('rolling_frame_size', self._batch_config.rolling_frame_size)
        raise_object_error = kwargs.get('raise_object_error', self._batch_config.raise_object_error)

        _check_positive_num(creation_time, 'creation_time', Real)
        _check_bool(raise_object_error, 'raise_object_error')

        self._batch_config.creation_time = creation_time
        self._batch_config.timeout_retries = timeout_retries
        self._batch_config.callback = callback
        self._batch_config.rolling_frame_size = rolling_frame_size
        self._batch_config.raise_object_error = raise_object_error

        # set Batch to manual import
        if batch_size is None:
            self._batch_config.size = None
            self._batch_config.type = BatchType.MANUAL
            return

        _check_positive_num(batch_size, 'batch_size', int)
        _check_bool(dynamic, 'dynamic')

        self._batch_config.size = batch_size

        if dynamic is False:
            self._batch_config.type = BatchType.AUTO
        else:
            self._batch_config.type = BatchType.DYNAMIC
            self._batch_config.init_recommended_num_objects(
                init_value=batch_size,
            )
            self._batch_config.init_recommended_num_references(
                init_value=batch_size,
            )

    @abstractmethod
    def add_data_object(self,
            data_object: dict,
            class_name: str,
            uuid: Optional[str]=None,
            vector: Optional[Sequence]=None
        ):
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
            have a vectorization module. Supported types are 'list', 'numpy.ndarray',
            'torch.Tensor' and 'tf.Tensor',
            by default None.

        Raises
        ------
        TypeError
            If an argument passed is not of an appropriate type.
        ValueError
            If 'uuid' is not of a propper form.
        """

        self._objects_batch.add(
            class_name=capitalize_first_letter(class_name),
            data_object=data_object,
            uuid=uuid,
            vector=vector,
        )

    @abstractmethod
    def add_reference(self,
            from_object_uuid: str,
            from_object_class_name: str,
            from_property_name: str,
            to_object_uuid: str
        ):
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

        self._reference_batch.add(
            from_object_class_name=capitalize_first_letter(from_object_class_name),
            from_object_uuid=from_object_uuid,
            from_property_name=from_property_name,
            to_object_uuid=to_object_uuid,
        )

    @abstractmethod
    def _create_data(self,
            data_type: str,
            batch_request: BatchRequest,
        ):
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
        """

    @abstractmethod
    def create_objects(self):
        """
        Creates multiple Objects at once in Weaviate.
        NOTE: If the UUID of one of the objects already exists then the existing object will be
        replaced by the new object.
        """

    @abstractmethod
    def create_references(self):
        """
        Creates multiple References at once in Weaviate.
        Adding References in batch is faster but it ignors validations like class name
        and property name, resulting in a SUCCESSFUL reference creation of a nonexistent object
        types and/or a nonexistent properties. If the consistency of the References is wanted
        use 'client.data_object.reference.add' to have additional validation against the
        weaviate schema.
        """

    @abstractmethod
    def _auto_create(self):
        """
        Auto create both objects and references in the batch. This protected method works with a
        fixed batch size and with dynamic batching. For a 'fixed' batching type it auto-creates
        when the sum of both objects and references equals batch_size. For dynamic batching it
        creates both batch requests when only one is full.
        """

    @abstractmethod
    def flush(self):
        """
        Flush both objects and references to the Weaviate server and call the callback function
        if one is provided. (See the docs for 'configure' or '__call__' for how to set one.)
        """

    def num_objects(self) -> int:
        """
        Get current number of objects in the batch.

        Returns
        -------
        int
            The number of objects in the batch.
        """

        return len(self._objects_batch)

    def num_references(self) -> int:
        """
        Get current number of references in the batch.

        Returns
        -------
        int
            The number of references in the batch.
        """

        return len(self._reference_batch)

    def pop_object(self, index: int=-1) -> dict:
        """
        Remove and return the object at index (default last).

        Parameters
        ----------
        index : int, optional
            The index of the object to pop, by default -1 (last item).

        Returns
        -------
        dict
            The popped object.

        Raises
        -------
        IndexError
            If batch is empty or index is out of range.
        """

        return self._objects_batch.pop(index)

    def pop_reference(self, index: int=-1) -> dict:
        """
        Remove and return the reference at index (default last).

        Parameters
        ----------
        index : int, optional
            The index of the reference to pop, by default -1 (last item).

        Returns
        -------
        dict
            The popped reference.

        Raises
        -------
        IndexError
            If batch is empty or index is out of range.
        """

        return self._reference_batch.pop(index)

    def empty_objects(self) -> None:
        """
        Remove all the objects from the batch.
        """

        self._objects_batch.empty()

    def empty_references(self) -> None:
        """
        Remove all the references from the batch.
        """

        self._reference_batch.empty()

    def is_empty_objects(self) -> bool:
        """
        Check if batch contains any objects.

        Returns
        -------
        bool
            Whether the Batch object list is empty.
        """

        return self._objects_batch.is_empty()

    def is_empty_references(self) -> bool:
        """
        Check if batch contains any references.

        Returns
        -------
        bool
            Whether the Batch reference list is empty.
        """

        return self._reference_batch.is_empty()

    @property
    def shape(self) -> Tuple[int, int]:
        """
        Get current number of objects and references in the batch.

        Returns
        -------
        Tuple[int, int]
            The number of objects and references, respectively, in the batch as a tuple,
            i.e. returns (number of objects, number of references).
        """

        return (len(self._objects_batch), len(self._reference_batch))

    @property
    def batch_size(self) -> Optional[int]:
        """
        Getter for 'batch_size'.

        Returns
        -------
        Optional[int]
            The current value of the batch_size. It is NOT the current number of data in the Batch.
            See the documentation for 'configure' or '__call__' for more information.
        """

        return self._batch_config.size

    @property
    def dynamic(self) -> bool:
        """
        Getter for 'dynamic'.

        Returns
        -------
        bool
            Wether the dynamic batching is enabled.
        """

        return self._batch_config.type == BatchType.DYNAMIC

    @property
    def recommended_num_objects(self) -> Optional[int]:
        """
        The recommended number of objects per batch. If None then it could not be computed.

        Returns
        -------
        Optional[int]
            The recommended number of objects per batch. If None then it could not be computed.
        """

        return self._batch_config.recommended_num_objects

    @property
    def recommended_num_references(self) -> Optional[int]:
        """
        The recommended number of references per batch. If None then it could not be computed.

        Returns
        -------
        Optional[int]
            The recommended number of references per batch. If None then it could not be computed.
        """

        return self._batch_config.recommended_num_references

    @property
    def creation_time(self) -> Real:
        """
        Getter for 'creation_time'.

        Returns
        -------
        Real
            The 'creation_time' value.
        """

        return self._batch_config.creation_time

    @property
    def timeout_retries(self) -> int:
        """
        Getter for 'timeout_retries'.

        Returns
        -------
        int
            The 'timeout_retries' value.
        """

        return self._batch_config.timeout_retries


def _check_positive_num(value: Real, arg_name: str, data_type: type) -> None:
    """
    Check if the 'value' of the 'arg_name' is a positive number.

    Parameters
    ----------
    value : Union[int, float]
        The value to check.
    arg_name : str
        The name of the variable from the original function call. Used for error message.
    data_type : type
        The data type to check for.

    Raises
    ------
    TypeError
        If the 'value' is not of type 'data_type'.
    ValueError
        If the 'value' has a non positive value.
    """

    if not isinstance(value, data_type) or isinstance(value, bool):
        raise TypeError(f"'{arg_name}' must be of type {data_type}.")
    if value <= 0:
        raise ValueError(f"'{arg_name}' must be positive, i.e. greater that zero (>0).")


def _check_bool(value: bool, arg_name: str) -> None:
    """
    Check if bool.

    Parameters
    ----------
    value : bool
        The value to check.
    arg_name : str
        The name of the variable from the original function call. Used for error message.

    Raises
    ------
    TypeError
        If the 'value' is not of type bool.
    """

    if not isinstance(value, bool):
        raise TypeError(f"'{arg_name}' must be of type bool.")


def check_batch_result(results: dict) -> bool:
    """
    Check batch results for errors.

    Parameters
    ----------
    results : dict
        The Weaviate batch creation return value.

    Returns
    -------
    bool
        If any object failed to create.
    """

    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result']:
                if 'error' in result['result']['errors']:
                    return True
    return False
