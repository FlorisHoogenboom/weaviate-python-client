"""
BatchConfig and BatchType class definitions.
"""

from numbers import Real
from typing import Callable, Optional
from enum import Enum, auto
from collections import deque


class BatchType(Enum):
    """
    Enum class for batching type. Each item represents a batching method.
    """

    MANUAL = auto()
    AUTO = auto()
    DYNAMIC = auto()


class BatchConfig:
    """
    BatchConfig class to store all the configuration, user adjustable, and computed ones.
    """

    def __init__(self):
        """
        Initialize a BatchConfig class instance.
        """

        # user adjustable configurations
        self.callback: Optional[Callable] = None
        self.size: Optional[int] = None
        self.creation_time: Real = 10.0
        self.type: BatchType = BatchType.MANUAL
        self._timeout_retries: int = 0
        self._rolling_frame_size = 5
        self.raise_object_error = True

        # user un-adjustable configurations
        self._recommended_num_objects: Optional[int] = None
        self._recommended_num_references: Optional[int] = None
        self._objects_per_second_frame = deque(
            maxlen=self._rolling_frame_size
        )
        self._references_per_second_frame = deque(
            maxlen=self._rolling_frame_size
        )

    @property
    def rolling_frame_size(self) -> int:
        """
        Settter and Getter for 'rolling_frame_size'.

        Parameters
        ----------
        rolling_frame_size : int
            Setter ONLY: The rolling frame size of the data creation time.
            NOTE: MUST be a positive integer.

        Returns
        ----------
        int
            Getter ONLY: The 'rolling_frame_size'.

        Raises
        ------
        TypeError
            Setter ONLY: If the 'rolling_frame_size' argument is no of type int.
        ValueError
            Setter ONLY: If the 'rolling_frame_size' argument is non-positive.
        """

        return self._rolling_frame_size

    @rolling_frame_size.setter
    def rolling_frame_size(self, rolling_frame_size: int) -> None:
        """
        Set new rolling_frame_size.

        Parameters
        ----------
        rolling_frame_size : int
            The rolling frame size of the data creation time.
            NOTE: MUST be a positive integer.

        Raises
        ------
        TypeError
            If the 'rolling_frame_size' argument is no of type int.
        ValueError
            If the 'rolling_frame_size' argument is non-positive.
        """

        if not isinstance(rolling_frame_size, int):
            raise TypeError(
                "'rolling_frame_size' must be of type int. Given type: "
                f"{type(rolling_frame_size)}"
            )

        if rolling_frame_size < 1:
            raise ValueError(
                "'rolling_frame_size' must be a positive integer, given: "
                f"{rolling_frame_size}"
            )

        self._rolling_frame_size = rolling_frame_size

        self._object_creation_time_frame = deque(
            self._object_creation_time_frame,
            maxlen=rolling_frame_size,
        )
        self._reference_creation_time_frame = deque(
            self._reference_creation_time_frame,
            maxlen=rolling_frame_size,
        )

    def add_object_creation_time_to_frame(self, creation_time: Real) -> None:
        """
        Add Object creation time to the rolling frame.

        Parameters
        ----------
        creation_time : Real
            The creation time of a single Object.
        """

        self._object_creation_time_frame.append(creation_time)

        avg_object_creation_time = (
            sum(self._object_creation_time_frame) / len(self._object_creation_time_frame)
        )

        _recommended_num_objects = round(self.creation_time / avg_object_creation_time)

        self._recommended_num_objects = max(1, _recommended_num_objects)

    def add_reference_creation_time_to_frame(self, creation_time: Real) -> None:
        """
        Add Reference creation time to the rolling frame.

        Parameters
        ----------
        creation_time : Real
            The creation time of a single Reference.
        """

        self._reference_creation_time_frame.append(creation_time)

        avg_reference_creation_time = (
            sum(self._reference_creation_time_frame) / len(self._reference_creation_time_frame)
        )

        _recommended_num_references = round(self.creation_time / avg_reference_creation_time)

        self._recommended_num_references = max(1, _recommended_num_references)

    @property
    def timeout_retries(self) -> int:
        """
        Setter and Getter for 'timeout_retries'.

        Propreties
        ----------
        value : int
            Setter ONLY: The new value for 'timeout_retries'.

        Returns
        -------
        int
            Getter ONLY: The 'timeout_retries' value.

        Raises
        ------
        TypeError
            Setter ONLY: If the new value is not of type int.
        ValueError
            Setter ONLY: If the new value has a non positive value.
        """

        return self._timeout_retries

    @timeout_retries.setter
    def timeout_retries(self, timeout_retries: int):
        """
        Set value for 'timeout_retries'.

        Propreties
        ----------
        timeout_retries : int
            The new value for 'timeout_retries'.

        Raises
        ------
        TypeError
            If the new value is not of type int.
        ValueError
            If the new value has a non positive value.
        """

        if not isinstance(timeout_retries, Real) or isinstance(timeout_retries, bool):
            raise TypeError(
                f"'timeout_retries' must be of type float/int Given type: {type(timeout_retries)}."
            )
        if timeout_retries < 0:
            raise ValueError(
                "'timeout_retries' must be positive, i.e. greater or equal that zero (>=0)."
            )
        self._timeout_retries = timeout_retries

    @property
    def recommended_num_objects(self) -> Optional[int]:
        """
        Getter for 'recommended_num_objects'.

        Returns
        -------
        Optional[int]
            The recommended number of objects to use per batch.
        """

        return self._recommended_num_objects

    def init_recommended_num_objects(self, init_value: int) -> None:
        """
        Initialize 'recommended_num_objects' if it is None.

        Parameters
        ----------
        init_value : int
            The initial value for 'recommended_num_objects'.
        """

        if self._recommended_num_objects is None:
            self._recommended_num_objects = init_value

    @property
    def recommended_num_references(self) -> Optional[int]:
        """
        Getter for 'recommended_num_references'.

        Returns
        -------
        Optional[int]
            The recommended number of references to use per batch.
        """

        return self._recommended_num_references

    def init_recommended_num_references(self, init_value: int) -> None:
        """
        Initialize 'recommended_num_references' if it is None.

        Parameters
        ----------
        init_value : int
            The initial value for 'recommended_num_references'.
        """

        if self._recommended_num_references is None:
            self._recommended_num_references = init_value
