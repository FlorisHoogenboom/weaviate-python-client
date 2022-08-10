import unittest
from unittest.mock import Mock
import ujson
from weaviate.exceptions import (
    WeaviateBaseError,
    UnsuccessfulStatusCodeError,
    ObjectAlreadyExistsError,
    AuthenticationError,
    SchemaValidationError,
    BatchObjectCreationError,
)


class TestExceptions(unittest.TestCase):

    def test_weaviate_base(self):
        """
        Test the `WeaviateBaseError` exception.
        """

        weaviate_base_error = WeaviateBaseError()
        self.assertEqual(str(weaviate_base_error), '')
        self.assertEqual(weaviate_base_error.message, '')

        weaviate_base_error = WeaviateBaseError('Test error!')
        self.assertEqual(str(weaviate_base_error), 'Test error!')
        self.assertEqual(weaviate_base_error.message, 'Test error!')

    def test_unsuccessful_status_code(self):
        """
        Test the `UnsuccessfulStatusCodeError` exception.
        """

        exception = UnsuccessfulStatusCodeError(
            message="Test message!",
            status_code=404,
            response_message='Test response!'
        )
        error_message = (
            f"Test message! Unsuccessful status code: 404, "
            f"with response body: 'Test response!'"
        )

        self.assertEqual(str(exception), error_message)
        self.assertIsInstance(exception, WeaviateBaseError)

    def test_object_already_exists(self):
        """
        Test the `ObjectAlreadyExistsError` exception.
        """

        exception = ObjectAlreadyExistsError("Test")
        self.assertEqual(str(exception), "Test")
        self.assertIsInstance(exception, WeaviateBaseError)

    def test_authentication_failed(self):
        """
        Test the `AuthenticationError` exception.
        """

        exception = AuthenticationError("Test")
        self.assertEqual(str(exception), "Test")
        self.assertIsInstance(exception, WeaviateBaseError)

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = 'Test response text'
        exception = AuthenticationError("Test!", mock_response)
        error_message = (
            f"Test! with status code: 404, "
            f"with response body: 'Test response text'"
        )
        self.assertEqual(str(exception), error_message)
        self.assertIsInstance(exception, WeaviateBaseError)

    def test_schema_validation(self):
        """
        Test the `SchemaValidationError` exception.
        """

        exception = SchemaValidationError("Test")
        self.assertEqual(str(exception), "Test")
        self.assertIsInstance(exception, WeaviateBaseError)

    def test_batch_object_creation(self):
        """
        Test the `BatchObjectCreationError` exception.
        """

        error_message = lambda message, batch_results: (
            message +
            ' The batch creation result is displayed here as well in case error was not caught: '
            + ujson.dumps(batch_results)
        )

        exception = BatchObjectCreationError('Test!', {}, [])
        self.assertEqual(str(exception), error_message('Test!', {}))
        self.assertEqual(exception.batch_objects, [])
        self.assertEqual(exception.batch_results, {})
        self.assertIsInstance(exception, WeaviateBaseError)

        exception = BatchObjectCreationError('Test!', {'errors':['Test']}, ['obj1', 'obj2'])
        self.assertEqual(str(exception), error_message('Test!', {'errors':['Test']}))
        self.assertEqual(exception.batch_objects, ['obj1', 'obj2'])
        self.assertEqual(exception.batch_results, {'errors':['Test']})
        self.assertIsInstance(exception, WeaviateBaseError)
