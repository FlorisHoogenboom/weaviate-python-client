"""
Helper functions.
"""
import os
import base64
import uuid as uuid_lib
from typing import Union, Sequence, Any, List
from numbers import Real
from io import BufferedReader


def image_encoder_b64(image_file_or_path: Union[str, BufferedReader]) -> str:
    """
    Encode an image in a Weaviate understandable format from a binary read file or by providing
    the image path.

    Parameters
    ----------
    image_file_or_path : str, io.BufferedReader
        The binary read file or the path to the file.

    Returns
    -------
    str
        Encoded image.

    Raises
    ------
    TypeError
        If the argument is of a wrong data type.
    """


    if isinstance(image_file_or_path, str):
        with open(image_file_or_path, 'br') as file:
            content = file.read()
    elif isinstance(image_file_or_path, BufferedReader):
        content = image_file_or_path.read()
    else:
        raise TypeError(
            "'image_or_image_path' should be an image path or a binary-read file "
            "io.BufferedReader"
        )
    return base64.b64encode(content).decode("utf-8")


def image_decoder_b64(encoded_image: str) -> bytes:
    """
    Decode an image from a Weaviate format image.

    Parameters
    ----------
    encoded_image : str
        The encoded image.

    Returns
    -------
    bytes
        Decoded image as a binary string.
    """

    return base64.b64decode(encoded_image.encode('utf-8'))


def generate_local_beacon(uuid: Union[str, uuid_lib.UUID], class_name: str) -> dict:
    """
    Generates a beacon with the given UUID.

    Parameters
    ----------
    uuid : str or uuid.UUID
        The UUID for which to create a local beacon.
    class_name : str
        The class name of the object for which to generate beacon.

    Returns
    -------
    dict
        The local beacon.

    Raises
    ------
    TypeError
        If 'uuid' is not of type str or uuid.UUID.
    ValueError
        If the 'uuid' is not valid.
    """

    if not isinstance(class_name, str):
        raise TypeError(
            f"'class_name' must be of type str. Given type: {type(class_name)}"
        )

    _uuid = get_valid_uuid(uuid=uuid)

    return {
        "beacon": f"weaviate://localhost/{capitalize_first_letter(class_name)}/{_uuid}"
    }


def get_valid_uuid(uuid: Union[str, uuid_lib.UUID]) -> str:
    """
    Validate and extract the UUID.

    Parameters
    ----------
    uuid : str or uuid.UUID
        The UUID to be validated and extracted. Should be either as a string (with or without
        hyphens) or an instance of uuid.UUID.

    Returns
    -------
    str
        The extracted UUID.

    Raises
    ------
    TypeError
        If 'uuid' is not of type str.
    ValueError
        If 'uuid' is not valid or cannot be extracted.
    """

    if isinstance(uuid, uuid_lib.UUID):
        return uuid.hex

    if not isinstance(uuid, str):
        raise TypeError(
            f"'uuid' must be of type str or uuid.UUID. Given type: {type(uuid)}"
        )

    try:
        return uuid_lib.UUID(uuid).hex
    except ValueError:
        raise ValueError(
            "Not valid 'uuid' or 'uuid' can not be extracted from value"
        ) from None


def get_vector(vector: Sequence[Real]) -> List[Real]:
    """
    Get embedding vector in Weaviate compatible format.

    Parameters
    ----------
    vector: Sequence
        The embedding of an object. Used only for class objects that do not have a vectorization
        module. Supported types are 'list', 'numpy.ndarray', 'torch.Tensor' and 'tf.Tensor'.

    Returns
    -------
    List[Real]
        The embedding as a list.

    Raises
    ------
    TypeError
        If 'vector' is not of a supported type.
    """

    if isinstance(vector, list):
        # if vector is already a list
        return vector
    try:
        # if vector is numpy.ndarray or torch.Tensor
        return vector.squeeze().tolist()
    except AttributeError:
        try:
            # if vector is tf.Tensor
            return vector.numpy().squeeze().tolist()
        except AttributeError:
            raise TypeError(
                "The type of the 'vector' argument is not supported. "
                "Supported types are 'list', 'numpy.ndarray', 'torch.Tensor' and 'tf.Tensor'"
            ) from None


def generate_uuid5(identifier: Any, namespace: Any = "") -> str:
    """
    Generate an UUIDv5, may be used to consistently generate the same UUID for a specific
    identifier and namespace.

    Parameters
    ----------
    identifier : Any
        The identifier/object that should be used as basis for the UUID.
    namespace : Any, optional
        Allows to namespace the identifier, by default ""

    Returns
    -------
    str
        The UUID as a string.
    """

    return str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, str(identifier) + str(namespace)))


def capitalize_first_letter(string: str) -> str:
    """
    Capitalize only the first letter of the 'string'.

    Parameters
    ----------
    string : str
        The string to be capitalized.

    Returns
    -------
    str
        The capitalized string.
    """

    if len(string) == 1:
        return string.capitalize()
    return string[0].capitalize() + string[1:]
