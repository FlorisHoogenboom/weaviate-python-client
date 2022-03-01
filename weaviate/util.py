"""
Helper functions.
"""
import os
import base64
import uuid as uuid_lib
from typing import Union, Sequence, Any
from numbers import Real
from io import BufferedReader
import validators


def image_encoder_b64(image_or_image_path: Union[str, BufferedReader]) -> str:
    """
    Encode a image in a Weaviate understandable format from a binary read file or by providing
    the image path.

    Parameters
    ----------
    image_or_image_path : str, io.BufferedReader
        The binary read file or the path to the file.

    Returns
    -------
    str
        Encoded image.

    Raises
    ------
    ValueError
        If the argument is str and does not point to an existing file.
    TypeError
        If the argument is of a wrong data type.
    """


    if isinstance(image_or_image_path, str):
        if not os.path.isfile(image_or_image_path):
            raise ValueError("No file found at location " + image_or_image_path)
        with open(image_or_image_path, 'br') as file:
            content = file.read()

    elif isinstance(image_or_image_path, BufferedReader):
        content = image_or_image_path.read()
    else:
        raise TypeError(
            "'image_or_image_path' should be a image path or a binary read file io.BufferedReader."
        )
    return base64.b64encode(content).decode("utf-8")


def image_decoder_b64(encoded_image: str) -> bytes:
    """
    Decode image from a Weaviate format image.

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


def generate_local_beacon(uuid: str) -> dict:
    """
    Generates a beacon with the given uuid.

    Parameters
    ----------
    uuid : str
        The UUID for which to create a local beacon.

    Returns
    -------
    dict
        The local beacon.

    Raises
    ------
    TypeError
        If 'to_uuid' is not of type str.
    ValueError
        If the 'to_uuid' is not valid.
    """

    if not isinstance(uuid, str):
        raise TypeError(
            f"'to_object_uuid' must be of type 'str'. Given type: {type(uuid)}."
        )
    if not validators.uuid(uuid):
        raise ValueError("UUID does not have the propper form.")
    return {"beacon": "weaviate://localhost/" + uuid}


def is_weaviate_object_url(url: str) -> bool:
    """
    Checks if the input follows a normal Weaviate 'beacon' like this:
    'weaviate://localhost/28f3f61b-b524-45e0-9bbe-2c1550bf73d2'

    Parameters
    ----------
    input : str
        The URL to be validated.

    Returns
    -------
    bool
        True if 'input' is a Weaviate object URL.
        False otherwise.
    """

    if not isinstance(url, str):
        return False
    if not url.startswith("weaviate://"):
        return False
    url = url[11:]
    split = url.split("/")
    if len(split) != 2:
        return False
    if split[0] != "localhost":
        if not validators.domain(split[0]):
            return False
    if not validators.uuid(split[1]):
        return False
    return True


def is_object_url(url: str) -> bool:
    """
    Validates an url like 'http://localhost:8080/v1/objects/1c9cd584-88fe-5010-83d0-017cb3fcb446'
    or '/v1/objects/1c9cd584-88fe-5010-83d0-017cb3fcb446' references a object. It only validates
    the path format and UUID, not the host or the protocol.

    Parameters
    ----------
    input : str
        The URL to be validated.

    Returns
    -------
    bool
        True if the 'input' is a valid path to an object.
        False otherwise.
    """

    split = url.split("/")
    if len(split) < 3:
        return False
    if not validators.uuid(split[-1]):
        return False
    if not split[-2] == "objects":
        return False
    if not split[-3] == "v1":
        return False
    return True


def get_valid_uuid(uuid: str) -> str:
    """
    Validate and extract the UUID.

    Parameters
    ----------
    uuid : str
        The UUID to be validated and extracted.
        Should be in the form of an UUID or in form of an URL (weaviate 'beacon' or 'href').
        E.g.
        'http://localhost:8080/v1/objects/fc7eb129-f138-457f-b727-1b29db191a67'
        or
        'weaviate://localhost/28f3f61b-b524-45e0-9bbe-2c1550bf73d2'
        or
        'fc7eb129-f138-457f-b727-1b29db191a67'

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

    if not isinstance(uuid, str):
        raise TypeError("'uuid' must be of type str but was: " + str(type(uuid)))

    _is_weaviate_url = is_weaviate_object_url(uuid)
    _is_object_url = is_object_url(uuid)
    _uuid = uuid
    if _is_weaviate_url or _is_object_url:
        _uuid = uuid.split("/")[-1]
    if not validators.uuid(_uuid):
        raise ValueError("Not valid 'uuid' or 'uuid' can not be extracted from value")
    return _uuid


def get_vector(vector: Sequence[Real]) -> list:
    """
    Get weaviate compatible format of the embedding vector.

    Parameters
    ----------
    vector: Sequence
        The embedding of an object. Used only for class objects that do not have a vectorization
        module. Supported types are 'list', 'numpy.ndarray', 'torch.Tensor' and 'tf.Tensor'.

    Returns
    -------
    list
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
        # if vetcor is numpy.ndarray or torch.Tensor
        return vector.squeeze().tolist()
    except AttributeError:
        try:
            # if vector is tf.Tensor
            return vector.numpy().squeeze().tolist()
        except AttributeError:
            raise TypeError("The type of the 'vector' argument is not supported!\n"
                "Supported types are 'list', 'numpy.ndarray', 'torch.Tensor' "
                "and 'tf.Tensor'") from None


def get_domain_from_weaviate_url(url: str) -> str:
    """
    Get the domain from a weaviate URL.

    Parameters
    ----------
    url : str
        The weaviate URL.
        Of this form: 'weaviate://localhost/objects/28f3f61b-b524-45e0-9bbe-2c1550bf73d2'

    Returns
    -------
    str
        The domain.
    """

    return url[11:].split("/")[0]


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

    return str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, str(namespace) + str(identifier)))


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
