import unittest
from unittest.mock import patch, Mock
from weaviate import AuthClientPassword, AuthClientCredentials, ClientTimeout
from weaviate.exceptions import (
    UnsuccessfulStatusCodeError,
    AuthenticationError,
    RequestsConnectionError,
)
from weaviate.wcs.crud_wcs import WCS, WCSConnection
from test.util import check_error_message, check_startswith_error_message


class TestWCSConnection(unittest.TestCase):

    @patch('weaviate.wcs.crud_wcs.Connection.refresh_authentication')
    def test_log_in(self, mock_refresh_authentication):
        """
        Test the `log_in` method.
        """

        # invalid calls
        auth_err_msg = (
            "No login credentials provided, or wrong type of credentials. "
            "Accepted type of credentials: weaviate.auth.AuthClientPassword."
        )
        with self.assertRaises(AuthenticationError) as error:
            WCSConnection(
                auth_client_secret=AuthClientCredentials('TEST'),
                timeout_config=ClientTimeout(20),
                proxies=None,
                trust_env=False,
            )
        check_error_message(self, error, auth_err_msg)
        mock_refresh_authentication.assert_not_called()

        # valid calls
        auth_url = (
            'https://auth.wcs.api.semi.technology/auth/realms/SeMI/'
            '.well-known/openid-configuration'
        )

        wcs_connection = WCSConnection(
            auth_client_secret=AuthClientPassword('test', 'TEST'),
            timeout_config=ClientTimeout(20),
            proxies=None,
            trust_env=False,
        )
        
        self.assertTrue(wcs_connection._is_authentication_required)
        self.assertEqual(wcs_connection._auth_url, auth_url)
        mock_refresh_authentication.assert_called()

    @patch('weaviate.wcs.crud_wcs.WCSConnection.log_in')
    def test__get_client_id_and_href(self, mock_log_in):
        """
        Test the `_get_client_id_and_href` method.
        """

        auth_url = (
            'https://auth.wcs.api.semi.technology/auth/realms/SeMI/'
            '.well-known/openid-configuration'
        )

        # valid calls
        wcs_connection = WCSConnection(
            auth_client_secret=AuthClientPassword('test', 'TEST'),
            timeout_config=ClientTimeout(20),
            proxies=None,
            trust_env=False,
        )
        
        result = wcs_connection._get_client_id_and_href()
        self.assertEqual(result, ('wcs', auth_url))

        wcs_connection = WCSConnection(
            auth_client_secret=AuthClientPassword('test1', 'TEST2'),
            timeout_config=ClientTimeout(20),
            proxies=None,
            trust_env=False,
        )
        
        result = wcs_connection._get_client_id_and_href()
        self.assertEqual(result, ('wcs', auth_url))


class TestWCS(unittest.TestCase):

    @patch('weaviate.wcs.crud_wcs.Requests')
    @patch('weaviate.wcs.crud_wcs.WCSConnection')
    def test___init__(self, mock_wcs_connection, mock_requests):
        """
        Test the `__init__` method.
        """

        # invalid calls
        ## error messages
        login_error_message = (
                "No login credentials provided, or wrong type of credentials! "
                "Accepted type of credentials: weaviate.auth.AuthClientPassword"
            )

        with self.assertRaises(AuthenticationError) as error:
            WCS(None)
        check_error_message(self, error, login_error_message)
        mock_wcs_connection.assert_not_called()

        # valid calls
        auth = AuthClientPassword('test_user', 'test_pass')
        timeout = ClientTimeout(20)
        wcs = WCS(auth, timeout_config=timeout)
        mock_wcs_connection.assert_called_with(
            auth_client_secret=auth,
            timeout_config=timeout,
            proxies=None,
            trust_env=False,
        )
        mock_requests.assert_called()
        self.assertEqual(wcs._email, 'test_user')

        auth = AuthClientPassword('test@semi.technology', 'test_pass')
        timeout = ClientTimeout(120)
        wcs = WCS(auth, timeout_config=timeout, proxies={'http': 'test'}, trust_env=True)
        mock_wcs_connection.assert_called_with(
            auth_client_secret=auth,
            timeout_config=timeout,
            proxies={'http': 'test'},
            trust_env=True,
        )
        mock_requests.assert_called()
        self.assertEqual(wcs._email, 'test@semi.technology')

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.WCS.get_cluster_config')
    def test_is_ready(self, mock_get_cluster_config):
        """
        Test the `is_ready` method.
        """

        wcs = WCS(AuthClientPassword('test_user', 'test_pass'))

        # invalid calls
        ## error messages
        value_error_msg = "No cluster with name: 'test_name'. Check the name again."

        mock_get_cluster_config.return_value = {}
        with self.assertRaises(ValueError) as error:
            wcs.is_ready('TEST_NAME')
        check_error_message(self, error, value_error_msg)
        mock_get_cluster_config.assert_called_with('test_name')

        # valid calls
        mock_get_cluster_config.return_value = {'status': {'state': {'percentage' : 99}}}
        self.assertEqual(wcs.is_ready('test_name'), False)
        mock_get_cluster_config.assert_called_with('test_name')

        mock_get_cluster_config.return_value = {'status': {'state': {'percentage' : 100}}}
        self.assertEqual(wcs.is_ready('test_name2'), True)
        mock_get_cluster_config.assert_called_with('test_name2')

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.post')
    @patch('weaviate.wcs.crud_wcs.WCS.get_cluster_config')
    def test_create(self, mock_get_cluster_config, mock_post):
        """
        Test the `create` method.
        """

        wcs = WCS(AuthClientPassword('test@semi.technology', 'test_pass'))
        progress = lambda name, prog = 99: {
            'meta': {'PublicURL': f'{name}.semi.network'},
            'status': {
                'state': {'percentage': prog}
            }
        }

        mock_get_cluster_config.side_effect = progress
        config = {
            'id': 'Test_name',
            'email': 'test@semi.technology',
            'configuration': {
                'tier': 'sandbox', 
                "requiresAuthentication": False
            }
        }

        # invalid calls

        ## error messages
        connection_error_message = 'WCS cluster was not created due to connection error.'
        unexpected_error_message = 'Creating WCS instance.'
        key_error_message = lambda  m: (
            "A module should have a required key: 'name',  and optional keys: 'tag', 'repo' and/or 'inferenceUrl'!"
            f" Given keys: {m.keys()}"
        )
        type_error_message = lambda t: (
            "Wrong type for the 'modules' argument. Accepted types are: NoneType, str, dict or "
            f"list but given: {t}"
        )

        key_type_error_msg = "The type of each value of the module's dict should be str."
        module_type_msg = "Wrong type for one of the modules. Should be either str or dict but given: "
        config_type_error_msg = "'config' must be either None or of type dict. Given type:"

        # config error
        with self.assertRaises(TypeError) as error:
            wcs.create(config=[{'name': 'TEST!'}])
        check_startswith_error_message(self, error, config_type_error_msg)

        # modules error
        ## no `name` key
        modules = {}
        with self.assertRaises(KeyError) as error:
            wcs.create(cluster_name='Test_name', cluster_type='test_type', modules=modules)
        check_error_message(self, error, f'"{key_error_message(modules)}"') # KeyError adds extra quotes

        ## extra key
        modules = {'name': 'Test Name', 'tag': 'Test Tag', 'invalid': 'Test'}
        with self.assertRaises(KeyError) as error:
            wcs.create(cluster_name='Test_name', cluster_type='test_type', modules=modules)
        check_error_message(self, error, f'"{key_error_message(modules)}"')# KeyError adds extra quotes

        ## module config type
        with self.assertRaises(TypeError) as error:
            wcs.create(cluster_name='Test_name', cluster_type='test_type', modules=12234)
        check_error_message(self, error, type_error_message(int))

        ## module config type when list
        with self.assertRaises(TypeError) as error:
            wcs.create(cluster_name='Test_name', cluster_type='test_type', modules=['test1', None])
        check_startswith_error_message(self, error, module_type_msg)

        ## wrong key value type
        with self.assertRaises(TypeError) as error:
            wcs.create(cluster_name='Test_name', cluster_type='test_type', modules=[{'name': 123}])
        check_startswith_error_message(self, error, key_type_error_msg)

        # connection error
        mock_post.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.create(cluster_name='Test_name', cluster_type='test_type')
        check_error_message(self, error, connection_error_message)
        mock_post.assert_called_with(
            path='/clusters',
            data_json={
                'email': 'test@semi.technology',
                'id': 'test_name',
                'configuration': {
                    'tier': 'test_type',
                    "requiresAuthentication": False,
                    'modules': []
                }
            },
        )

        mock_post.side_effect = None
        mock_post.return_value = Mock(status_code=404)

        # unexpected error
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.create(config=config)
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_post.assert_called_with(
            path='/clusters',
            data_json=config,
        )


        # valid calls
        mock_post.return_value = Mock(status_code=202, content='{"id": "my-cluster"}')
        result = wcs.create(
            cluster_name='my-cluster',
            modules='test',
            wait_for_completion=False,
        )
        mock_post.assert_called_with(
            path='/clusters',
            data_json={
                'id': 'my-cluster',
                'email': 'test@semi.technology',
                'configuration': {
                    'tier': 'sandbox',
                    "requiresAuthentication": False,
                    'modules': [{'name': 'test'}]
                }
            },
        )
        self.assertEqual(result, 'https://my-cluster.semi.network')

        mock_post.return_value = Mock(status_code=202, content='{"id": "my-url"}')
        modules = ['test', {'name': 'test2', 'repo': 'test_repo', 'tag': 'TAG', 'inferenceUrl': 'URL'}]
        result = wcs.create(
            cluster_name='My-url',
            weaviate_version='v1.14.1',
            modules=modules,
            with_auth=True,
            wait_for_completion=False,
        )
        mock_post.assert_called_with(
            path='/clusters',
            data_json={
                'id': 'my-url',
                'email': 'test@semi.technology',
                'configuration': {
                    'tier': 'sandbox',
                    "requiresAuthentication": True,
                    'modules': [{'name': modules[0]}, modules[1]],
                    'release': {'weaviate': '1.14.1'}
                }
            },
        )
        self.assertEqual(result, 'https://my-url.semi.network')

        mock_post.return_value = Mock(status_code=202, content='{"id": "my-url"}')
        result = wcs.create(
            cluster_name='my-url',
            modules={'name': 'test', 'tag': 'test_tag'},
            weaviate_version='1.14.1',
            wait_for_completion=False,   
        )
        mock_post.assert_called_with(
            path='/clusters',
            data_json={
                'id': 'my-url',
                'email': 'test@semi.technology',
                'configuration': {
                    'tier': 'sandbox',
                    "requiresAuthentication": False,
                    'modules': [{'name': 'test', 'tag': 'test_tag'}],
                    'release': {'weaviate': '1.14.1'}
                }
            },
        )
        self.assertEqual(result, 'https://my-url.semi.network')

        post_return = Mock(status_code=202, content='{"id": "test_id"}')
        mock_post.return_value = post_return
        result = wcs.create(
            config=config,
            wait_for_completion=False,
        )
        mock_post.assert_called_with(
            path='/clusters',
            data_json=config,
        )
        self.assertEqual(result, 'https://test_id.semi.network')

        mock_get_cluster_config.reset_mock()
        mock_get_cluster_config.side_effect = lambda x: progress('weaviate', 100) if mock_get_cluster_config.call_count == 2 else progress('weaviate')
        mock_post.return_value = Mock(status_code=202, content='{"id": "weaviate"}')
        result = wcs.create(cluster_name='weaviate', wait_for_completion=True)
        mock_post.assert_called_with(
            path='/clusters',
            data_json={
                'id': 'weaviate',
                'email': 'test@semi.technology',
                'configuration': {
                    'tier': 'sandbox',
                    "requiresAuthentication": False,
                    'modules': []
                }
            },
        )
        self.assertEqual(result, 'https://weaviate.semi.network')

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.get')
    def test_get_clusters(self, mock_get):
        """
        Test the `get_clusters` method.
        """

        wcs = WCS(AuthClientPassword('test@semi.technology', 'testPassword'))

        # invalid calls
        ## error messages
        connection_error_message = 'WCS clusters were not fetched due to connection error.'
        unexpected_error_message = 'Checking WCS instances.'

        # connection error
        mock_get.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.get_clusters()
        check_error_message(self, error, connection_error_message)
        mock_get.assert_called_with(
            path='/clusters/list',
            params={
                'email': 'test@semi.technology'
            }
        )

        # unexpected error
        mock_get.side_effect = None
        mock_get.return_value = Mock(status_code=400)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.get_clusters()
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_get.assert_called_with(
            path='/clusters/list',
            params={
                'email': 'test@semi.technology'
            }
        )

        # valid calls
        return_mock = Mock(status_code=200, content='{"clusterIDs": ["test!"]}')
        mock_get.return_value = return_mock
        result = wcs.get_clusters()
        self.assertEqual(result, ['test!'])
        mock_get.assert_called_with(
            path='/clusters/list',
            params={
                'email': 'test@semi.technology'
            }
        )

        return_mock = Mock(status_code=200, content='{"clusterIDs": ["cluster_1", "cluster_2"]}')
        mock_get.return_value = return_mock
        result = wcs.get_clusters()
        self.assertEqual(result, ["cluster_1", "cluster_2"])
        mock_get.assert_called_with(
            path='/clusters/list',
            params={
                'email': 'test@semi.technology'
            }
        )

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.get')
    def test_get_cluster_config(self, mock_get):
        """
        Test the `get_cluster_config` method.
        """

        wcs = WCS(AuthClientPassword('test_secret_username', 'test_secret_password'))

        # invalid calls
        ## error messages
        connection_error_message = 'WCS cluster info was not fetched due to connection error.'
        unexpected_error_message = 'Checking WCS instance.'

        ## connection error
        mock_get.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.get_cluster_config('test_name')
        check_error_message(self, error, connection_error_message)
        mock_get.assert_called_with(
            path='/clusters/test_name',
        )

        ## unexpected error
        mock_get.side_effect = None
        mock_get.return_value = Mock(status_code=400)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.get_cluster_config('test_name')
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_get.assert_called_with(
            path='/clusters/test_name',
        )

        # valid calls
        mock_get.return_value = Mock(status_code=200, content='{"config": "test!"}')
        result = wcs.get_cluster_config('test_name')
        self.assertEqual(result, {'config': 'test!'})
        mock_get.assert_called_with(
            path='/clusters/test_name',
        )

        mock_get.return_value = Mock(status_code=200, content='{"config": "test!"}')
        result = wcs.get_cluster_config('Test_Name')
        self.assertEqual(result, {'config': 'test!'})
        mock_get.assert_called_with(
            path='/clusters/test_name',
        )

        mock_get.return_value = Mock(status_code=404)
        result = wcs.get_cluster_config('test_name')
        self.assertEqual(result, {})
        mock_get.assert_called_with(
            path='/clusters/test_name',
        )

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.delete')
    def test_delete_cluster(self, mock_delete):
        """
        Test the `delete_cluster` method.
        """

        wcs = WCS(AuthClientPassword('test_secret_username', 'test_password'))

        # invalid calls
        ## error messages
        connection_error_message = 'WCS cluster was not deleted due to connection error.'
        unexpected_error_message = 'Deleting WCS instance.'

        ## connection error
        mock_delete.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.delete_cluster('test_name')
        check_error_message(self, error, connection_error_message)
        mock_delete.assert_called_with(
            path='/clusters/test_name',
        )

        ## unexpected error
        mock_delete.side_effect = None
        mock_delete.return_value = Mock(status_code=400)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.delete_cluster('test_name')
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_delete.assert_called_with(
            path='/clusters/test_name',
        )

        # valid calls
        mock_delete.return_value = Mock(status_code=200)
        self.assertIsNone(wcs.delete_cluster('test_name'))
        mock_delete.assert_called_with(
            path='/clusters/test_name',
        )

        mock_delete.return_value = Mock(status_code=404)
        self.assertIsNone(wcs.delete_cluster('test_name'))
        mock_delete.assert_called_with(
            path='/clusters/test_name',
        )

        mock_delete.return_value = Mock(status_code=404)
        self.assertIsNone(wcs.delete_cluster('TesT_naMe'))
        mock_delete.assert_called_with(
            path='/clusters/test_name',
        )

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.get')
    def test_get_users_of_cluster(self, mock_get):
        """
        Test the `get_users_of_cluster` method.
        """

        wcs = WCS(AuthClientPassword('test_secret_username', 'test_secret_password'))

        # invalid calls
        ## error messages
        connection_error_message = 'Could not get users of the cluster due to connection error.'
        unexpected_error_message = "Getting cluster's users."

        ## connection error
        mock_get.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.get_users_of_cluster('test_name')
        check_error_message(self, error, connection_error_message)
        mock_get.assert_called_with(
            path='/clusters/test_name/users',
        )

        ## unexpected error
        mock_get.side_effect = None
        mock_get.return_value = Mock(status_code=400)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.get_users_of_cluster('test_name')
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_get.assert_called_with(
            path='/clusters/test_name/users',
        )

        # valid calls
        mock_get.return_value = Mock(status_code=200, content='{"users": ["user1", "user2"]}')
        result = wcs.get_users_of_cluster('test_name')
        self.assertEqual(result, ["user1", "user2"])
        mock_get.assert_called_with(
            path='/clusters/test_name/users',
        )

        mock_get.return_value = Mock(status_code=200, content='{"users": ["user1"]}')
        result = wcs.get_users_of_cluster('Test_Name')
        self.assertEqual(result, ["user1"])
        mock_get.assert_called_with(
            path='/clusters/test_name/users',
        )

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.post')
    def test_add_user_to_cluster(self, mock_post):
        """
        Test the `add_user_to_cluster` method.
        """

        wcs = WCS(AuthClientPassword('test_secret_username', 'test_secret_password'))

        # invalid calls
        ## error messages
        connection_error_message = 'Could not add user of the cluster due to connection error.'
        unexpected_error_message = "Adding user to cluster."

        ## connection error
        mock_post.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.add_user_to_cluster(cluster_name='test_name', user='test_user_1')
        check_error_message(self, error, connection_error_message)
        mock_post.assert_called_with(
            path='/clusters/test_name/users/test_user_1',
        )

        ## unexpected error
        mock_post.side_effect = None
        mock_post.return_value = Mock(status_code=400)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.add_user_to_cluster(cluster_name='test_name', user='test_user')
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_post.assert_called_with(
            path='/clusters/test_name/users/test_user',
        )

        # valid calls
        mock_post.return_value = Mock(status_code=200)
        self.assertIsNone(wcs.add_user_to_cluster(cluster_name='test_name', user='test_user'))
        mock_post.assert_called_with(
            path='/clusters/test_name/users/test_user',
        )

        mock_post.return_value = Mock(status_code=200)
        self.assertIsNone(wcs.add_user_to_cluster(cluster_name='Test_nAme', user='test_user_2'))
        mock_post.assert_called_with(
            path='/clusters/test_name/users/test_user_2',
        )

    @patch('weaviate.wcs.crud_wcs.WCSConnection', Mock())
    @patch('weaviate.wcs.crud_wcs.Requests.delete')
    def test_remove_user_from_cluster(self, mock_delete):
        """
        Test the `remove_user_from_cluster` method.
        """

        wcs = WCS(AuthClientPassword('test_secret_username', 'test_password'))

        # invalid calls
        ## error messages
        connection_error_message = 'Could not remove user from the cluster due to connection error.'
        unexpected_error_message = 'Removing user from cluster.'

        ## connection error
        mock_delete.side_effect = RequestsConnectionError('Test!')
        with self.assertRaises(RequestsConnectionError) as error:
            wcs.remove_user_from_cluster(cluster_name='test_name', user='test_user')
        check_error_message(self, error, connection_error_message)
        mock_delete.assert_called_with(
            path='/clusters/test_name/users/test_user',
        )

        ## unexpected error
        mock_delete.side_effect = None
        mock_delete.return_value = Mock(status_code=400)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            wcs.remove_user_from_cluster('test_name', user='test_user')
        check_startswith_error_message(self, error, unexpected_error_message)
        mock_delete.assert_called_with(
            path='/clusters/test_name/users/test_user',
        )

        # valid calls
        mock_delete.return_value = Mock(status_code=200)
        self.assertIsNone(wcs.remove_user_from_cluster(cluster_name='test_name', user='test_user'))
        mock_delete.assert_called_with(
            path='/clusters/test_name/users/test_user',
        )

        mock_delete.return_value = Mock(status_code=200)
        self.assertIsNone(wcs.remove_user_from_cluster('test_name', 'test_user2'))
        mock_delete.assert_called_with(
            path='/clusters/test_name/users/test_user2',
        )
