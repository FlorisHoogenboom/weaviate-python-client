"""
Weaviate Exceptions.
"""
from typing import Optional
from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError
from aiohttp.client_exceptions import ClientConnectionError as AiohttpConnectionError


class WeaviateBaseError(Exception):
    """
    Weaviate base exception that all Weaviate exceptions should inherit from.
    This error can be used to catch any Weaviate exceptions.
    """

    def __init__(self, message: str = ''):
        """
        Weaviate base exception initializer.

        Parameters
        ----------
        message: str, optional
            An error message specific to the context in which the error occurred.
        """

        self.message = message
        super().__init__(message)


class UnsuccessfulStatusCodeError(WeaviateBaseError):
    """
    Is raised in case the request status code returned from Weaviate server is not handled in the
    client implementation.
    """

    def __init__(self, message: str, status_code: int, response_message: str):
        """
        Unsuccessful Status Code exception initializer.

        Parameters
        ----------
        message: str
            An error message specific to the context in which the error occurred.
        response: requests.Response
            The request response for which the status code was unsuccessful.
        """

        error_message = (
            f"{message} Unsuccessful status code: {status_code}, "
            f"with response body: '{response_message}'"
        )
        super().__init__(error_message)


class ObjectAlreadyExistsError(WeaviateBaseError):
    """
    Object Already Exists Error.
    """


class AuthenticationError(WeaviateBaseError):
    """
    Authentication Failed Error.
    """

    def __init__(self, message: str, response: Optional[Response] = None):
        """
        Authentication error exception initializer.

        Parameters
        ----------
        message : str
            The error message context.
        response : Response or None, optional
            The authentication request response, by default None.
        """

        if response is not None:
            error_message = (
                f"{message} with status code: {response.status_code}, "
                f"with response body: '{response.text}'"
            )
        else:
            error_message = message

        super().__init__(error_message)


class SchemaValidationError(WeaviateBaseError):
    """
    Schema Validation Error.
    """
