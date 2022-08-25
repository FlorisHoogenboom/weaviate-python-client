import unittest
from unittest.mock import Mock
from weaviate.base.contextionary import (
    pre_extend,
    pre_get_concept_vector,
)
from test.util import mock_connection_method, check_error_message, check_startswith_error_message


class TestText2VecContextionary(unittest.TestCase):

    def test_pre_extend(self):
        """
        Test `pre_extend` function.
        """

        some_concept = {
            "concept" : "lsd",
            "definition" : "In probability and statistics, the logarithmic series distribution is a discrete probability distribution derived from the Maclaurin series expansion"
        }

        # error messages
        concept_type_err_msg = lambda dt: f"'concept' must be of type str. Given type: {dt}."
        definition_type_err_msg = lambda dt: f"'definition' must be of type str. Given type: {dt}."
        weight_type_err_msg = lambda dt: f"'weight' must be of type float/int. Given type: {dt}."
        weight_value_err_msg = lambda val: f"'weight' is out of bounds: 0.0 <= weight <= 1.0. Given: {val}."


        ## test exceptions
        with self.assertRaises(TypeError) as error:
            pre_extend(concept=1, definition=some_concept["definition"], weight=1.0)
        check_error_message(self, error, concept_type_err_msg(int))

        with self.assertRaises(TypeError) as error:
            pre_extend(concept=some_concept["concept"], definition=True, weight=1.0)
        check_error_message(self, error, definition_type_err_msg(bool))

        with self.assertRaises(TypeError) as error:
            pre_extend(**some_concept, weight='1')
        check_error_message(self, error, weight_type_err_msg(str))

        with self.assertRaises(ValueError) as error:
            pre_extend(**some_concept, weight=1.1)
        check_error_message(self, error, weight_value_err_msg(1.1))

        with self.assertRaises(ValueError) as error:
            pre_extend(**some_concept, weight=-1.0)
        check_error_message(self, error, weight_value_err_msg(-1.0))
        
        ## test valid call without specifying 'weight'
        res = pre_extend(
            concept=some_concept['concept'],
            definition=some_concept['definition'],
        )
        some_concept["weight"] = 1.0
        self.assertEqual(res, some_concept)

        ## test valid call with specifying 'weight as error'
        res = pre_extend(
            concept=some_concept['concept'],
            definition=some_concept['definition'],
            weight=.123
        )
        some_concept["weight"] = .123
        self.assertEqual(res, some_concept)

    def test_pre_get_concept_vector(self):
        """
        Test `pre_get_concept_vector` method.
        """

        # test valid call
        self.assertEqual(
            pre_get_concept_vector(concept='TEST'),
            "/modules/text2vec-contextionary/concepts/TEST"
        )

        self.assertEqual(
            pre_get_concept_vector(concept='TEST2'),
            "/modules/text2vec-contextionary/concepts/TEST2"
        )
