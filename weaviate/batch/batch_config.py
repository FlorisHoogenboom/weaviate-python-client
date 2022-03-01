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
    BatchConfig class to store all the configuration, user adjustable and computed ones.
    """

    def __init__(self):
        """
        Initialize a BatchConfig class instance.
        """

        # user adjustable configurations
        self._callback: Optional[Callable] = None
        self._size: Optional[int] = None
        self._creation_time: Real = 10.0
        self._timeout_retries: int = 0
        self._type: BatchType = BatchType.MANUAL
        self._rolling_frame_size = 5

        # user un-adjustable configurations
        self._recommended_num_objects: int = 1
        self._recommended_num_references: int = 1
        self._objects_per_second_frame = deque(
            maxlen=self._rolling_frame_size
        )
        self._references_per_second_frame = deque(
            maxlen=self._rolling_frame_size
        )

    @property
    def creation_time_frame_size(self) -> int:
        """
        Settter and Getter for `creation_time_frame_size`. 

        Parameters
        ----------
        creation_time_frame_size : int
            Setter ONLY: The rolling frame size of the data creation time.
            NOTE: MUST be a positive integer.

        Returns
        ----------
        int
            Getter ONLY: The `creation_time_frame_size`.

        Raises
        ------
        TypeError
            Setter ONLY: If the `creation_time_frame_size` argument is no of type `int`.
        ValueError
            Setter ONLY: If the `creation_time_frame_size` argument is non-positive.
        """

        return self._creation_time_frame_size

    @creation_time_frame_size.setter
    def creation_time_frame_size(self, creation_time_frame_size: int) -> None:
        """
        Set new creation_time_frame_size. 

        Parameters
        ----------
        creation_time_frame_size : int
            The rolling frame size of the data creation time.
            NOTE: MUST be a positive integer.
        
        Raises
        ------
        TypeError
            If the `creation_time_frame_size` argument is no of type `int`.
        ValueError
            If the `creation_time_frame_size` argument is non-positive.
        """


        if not isinstance(creation_time_frame_size, int):
            raise TypeError(
                "The `creation_time_frame_size` must be of type `int`, given: "
                f"{type(creation_time_frame_size)}"
            )

        if creation_time_frame_size < 1:
            raise ValueError(
                "The `creation_time_frame_size` must be a positive integer, given: "
                f"{creation_time_frame_size}"
            )

        self._creation_time_frame_size = creation_time_frame_size

        self._object_creation_time_frame = deque(
            self._object_creation_time_frame,
            maxlen=creation_time_frame_size,
        )
        self._reference_creation_time_frame = deque(
            self._reference_creation_time_frame,
            maxlen=creation_time_frame_size,
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

        self.recommended_num_objects = self