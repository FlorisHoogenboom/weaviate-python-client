from operator import add
import unittest
import os
import aiohttp
from unittest.mock import patch, Mock
from weaviate.exceptions import (
    AuthenticationError,
    UnsuccessfulStatusCodeError,
    RequestsConnectionError,
)
from weaviate.base.connection import (
    Proxies,
    ClientTimeout,
    Connection,
)
from weaviate.auth import AuthClientPassword, AuthClientCredentials
from test.util import check_error_message, check_startswith_error_message


class TestProxies(unittest.TestCase):

    def test_get_proxies_requests(self):
        """
        Test the `get_proxies_requests` method.
        """

        # invalid calls
        requests_type_err_msg = lambda dt: (
            "If 'proxies' is not None, it must be of type dict or str. "
            f"Given type: {dt}."
        )

        with self.assertRaises(TypeError) as error:
            Proxies(123)
        check_error_message(self, error, requests_type_err_msg(int))

        # valid calls
        expected = {}
        proxy = Proxies(None)
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

        expected = {'http': 'http://proxy', 'https': 'http://proxy'}
        proxy = Proxies('http://proxy')
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

        expected = {'http': 'http://test', 'https': 'http://test-secure'}
        proxy = Proxies(expected)
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

        expected = {}
        proxy = Proxies(None, trust_env=True)
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

        os.environ['HTTP_PROXY'] = 'test-http'
        os.environ['HTTPS_PROXY'] = 'test-https'
        expected = {'http': 'test-http', 'https': 'test-https'}
        proxy = Proxies(None, trust_env=True)
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

        os.environ['HTTP_PROXY'] = ''
        os.environ['HTTPS_PROXY'] = ''
        os.environ['http_proxy'] = 'test-http2'
        os.environ['https_proxy'] = 'test-https2'
        expected = {'http': 'test-http2', 'https': 'test-https2'}
        proxy = Proxies(None, trust_env=True)
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

        expected = {'http': 'test', 'https': 'test2'}
        proxy = Proxies(expected, trust_env=True, include_aiohttp=False)
        self.assertEqual(proxy.get_proxies_requests(), expected)
        self.assertFalse(proxy._include_aiohttp)
        self.assertIsNone(proxy._proxies_aiohttp)

    def test_get_proxy_aiohttp(self):
        """
        Test the `get_proxy_aiohttp` method.
        """

        # invalid calls
        attribute_err_msg = (
            "The 'aiohttp' proxy attribute is not set. This means that method is NOT called "
            "from the AsyncClient. If it is, please report the Issue."
        )
        value_err_msg = (
            "Could not find 'http' or 'https' key in 'proxy'."
        )

        with self.assertRaises(AttributeError) as error:
            Proxies(None).get_proxy_aiohttp()
        check_error_message(self, error, attribute_err_msg)

        with self.assertRaises(ValueError) as error:
            Proxies({'test': 'TEST'}, include_aiohttp=True)
        check_error_message(self, error, value_err_msg)

        # valid calls
        expected = None
        proxy = Proxies(None, include_aiohttp=True)
        self.assertEqual(proxy.get_proxy_aiohttp(), expected)

        expected = None
        proxy = Proxies({}, include_aiohttp=True)
        self.assertEqual(proxy.get_proxy_aiohttp(), expected)

        expected = 'http://proxy'
        proxy = Proxies('http://proxy', include_aiohttp=True)
        self.assertEqual(proxy.get_proxy_aiohttp(), expected)

        expected = 'http://test-secure'
        proxy = Proxies({'http': 'http://test', 'https': 'http://test-secure'}, include_aiohttp=True)
        self.assertEqual(proxy.get_proxy_aiohttp(), expected)

        expected = 'http://test'
        proxy = Proxies({'http': 'http://test', 'htttttps': 'http://test-secure'}, trust_env=True, include_aiohttp=True)
        self.assertEqual(proxy.get_proxy_aiohttp(), expected)


class TestClientTimeout(unittest.TestCase):

    def test_exceptions(self):
        """
        Test the `get_timeout_requests` method.
        """

        value_err_msg = lambda l: (
            "If 'timeout_config' is a tuple/list, it must be of length two. "
            f"Given length: {l}."
        )
        type_err_msg = lambda dt: (
            f"Unsupported type {dt} for 'timeout_config'. "
            "It can be one of these: None, tuple/list or aiohttp.ClientTimeout."
        )

        with self.assertRaises(ValueError) as error:
            ClientTimeout([1, 2, 3])
        check_error_message(self, error, value_err_msg(3))

        with self.assertRaises(ValueError) as error:
            ClientTimeout((1,))
        check_error_message(self, error, value_err_msg(1))

        with self.assertRaises(TypeError) as error:
            ClientTimeout('10')
        check_error_message(self, error, type_err_msg(str))

    def test_methods(self):
        """
        Test the `get_timeout_requests` and `get_timeout_aiohttp` method.
        """

        with patch("weaviate.base.connection.aiohttp") as mock_aiohttp:
            timeout = ClientTimeout(None)
            self.assertEqual(timeout.get_timeout_requests(), None)
            mock_aiohttp.ClientTimeout.assert_called_with(
                total=None,
            )

            timeout = ClientTimeout(123)
            self.assertEqual(timeout.get_timeout_requests(), 123)
            mock_aiohttp.ClientTimeout.assert_called_with(
                total=123,
            )

            timeout = ClientTimeout((10, 20))
            self.assertEqual(timeout.get_timeout_requests(), (10, 20))
            mock_aiohttp.ClientTimeout.assert_called_with(
                total=30,
                connect=10,
            )

            timeout = ClientTimeout([2, 20])
            self.assertEqual(timeout.get_timeout_requests(), (2, 20))
            mock_aiohttp.ClientTimeout.assert_called_with(
                total=22,
                connect=2,
            )

        aiohttp_timeout = aiohttp.ClientTimeout(total=30, connect=2)
        timeout = ClientTimeout(aiohttp_timeout)
        self.assertEqual(timeout.get_timeout_requests(), (2, 30))
        self.assertEqual(timeout.get_timeout_aiohttp(), aiohttp_timeout)

        aiohttp_timeout = aiohttp.ClientTimeout(30)
        timeout = ClientTimeout(aiohttp_timeout)
        self.assertEqual(timeout.get_timeout_requests(), (None, 30))
        self.assertEqual(timeout.get_timeout_aiohttp(), aiohttp_timeout)
        aiohttp_timeout = aiohttp.ClientTimeout(connect=3)
        timeout = ClientTimeout(aiohttp_timeout)
        self.assertEqual(timeout.get_timeout_requests(), (3, None))
        self.assertEqual(timeout.get_timeout_aiohttp(), aiohttp_timeout)


class TestConnection(unittest.TestCase):

    @patch("weaviate.base.connection.Proxies")
    @patch("weaviate.base.connection.Connection.log_in")
    def test_attributes_and_some_methods(self, mock_login, mock_proxies):
        """
        Test the `__init__` method.
        """

        # invalid calls
        auth_type_err_msg = lambda dt: (
            "'auth_client_secret' must be of type 'AuthCredentials' or None. "
            f"Given type: {dt}."
        )
        headers_type_err_msg = lambda dt: (
            "'additional_headers' must be of type dict or None. "
            f"Given type: {dt}."
        )
        timeout_type_err_msg = lambda dt: (
            "'timeout_config' must be of type weaviate.ClientTimeout. "
            f"Given type: {dt}."
        )

        with self.assertRaises(TypeError) as error:
            Connection(
                url='SomeURL',
                auth_client_secret="MY_PASSWORD",
                timeout_config=ClientTimeout(20),
                proxies=None,
                trust_env=False,
                include_aiohttp=False,
                additional_headers=None,
            )
        check_error_message(self, error, auth_type_err_msg(str))
        mock_login.assert_not_called()

        with self.assertRaises(TypeError) as error:
            Connection(
                url='SomeURL',
                auth_client_secret=None,
                timeout_config=ClientTimeout(20),
                proxies=None,
                trust_env=False,
                include_aiohttp=False,
                additional_headers='None',
            )
        check_error_message(self, error, headers_type_err_msg(str))
        mock_login.assert_not_called()

        with self.assertRaises(TypeError) as error:
            Connection(
                url='SomeURL',
                auth_client_secret=AuthClientPassword('test', 'test'),
                timeout_config=ClientTimeout(20),
                proxies=None,
                trust_env=False,
                include_aiohttp=False,
                additional_headers=123,
            )
        check_error_message(self, error, headers_type_err_msg(int))
        mock_login.assert_not_called()

        with self.assertRaises(TypeError) as error:
            Connection(
                url='SomeURL',
                auth_client_secret=AuthClientCredentials('test'),
                timeout_config=20,
                proxies=None,
                trust_env=False,
                include_aiohttp=False,
                additional_headers=None,
            )
        check_error_message(self, error, timeout_type_err_msg(int))
        mock_login.assert_not_called()

        # valid calls
        mock_proxies.return_value = 'This is a test'

        auth = None
        timeout = ClientTimeout(20)
        conn = Connection(
            url='http://test.semi.technology',
            auth_client_secret=auth,
            timeout_config=timeout,
            proxies=None,
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )
        self.assertEqual(conn.base_url, 'http://test.semi.technology')
        self.assertEqual(conn.timeout_config, timeout)
        self.assertDictEqual(conn.get_request_header(), {"content-type": "application/json"})
        self.assertEqual(conn.get_url('/test'), 'http://test.semi.technology/v1/test')
        self.assertEqual(conn.get_url(''), 'http://test.semi.technology/v1')
        self.assertEqual(conn.proxies, 'This is a test') # return values from mock obj
        mock_proxies.assert_called_with(
            proxies=None,
            trust_env=False,
            include_aiohttp=False,
        )

        auth = None
        timeout = ClientTimeout(30)
        conn = Connection(
            url='http://test2.semi.technology',
            auth_client_secret=auth,
            timeout_config=timeout,
            proxies={'http': 'test'},
            trust_env=True,
            include_aiohttp=True,
            additional_headers={'test': 'Works!'},
        )
        self.assertEqual(conn.base_url, 'http://test2.semi.technology')
        self.assertEqual(conn.timeout_config, timeout)
        self.assertDictEqual(conn.get_request_header(), {"content-type": "application/json", 'test': 'Works!'})
        self.assertEqual(conn.get_url('/test2'), 'http://test2.semi.technology/v1/test2')
        self.assertEqual(conn.get_url(''), 'http://test2.semi.technology/v1')
        self.assertEqual(conn.proxies, 'This is a test') # return values from mock obj
        mock_proxies.assert_called_with(
            proxies={'http': 'test'},
            trust_env=True,
            include_aiohttp=True,
        )

        with patch("weaviate.base.connection.Connection.refresh_authentication") as mock_refresh:

            auth = AuthClientPassword('name', 'pass')
            timeout = ClientTimeout(3)
            conn = Connection(
                url='http://test3.semi.technology',
                auth_client_secret=auth,
                timeout_config=timeout,
                proxies={'http': 'test'},
                trust_env=True,
                include_aiohttp=False,
                additional_headers={'test': 'Works!'},
            )
            conn._is_authentication_required = True
            conn._auth_bearer = 'Test!!!'
            self.assertDictEqual(conn.get_request_header(), {"content-type": "application/json", 'test': 'Works!', 'Authorization': 'Bearer Test!!!'})
            mock_refresh.assert_called()
            mock_proxies.assert_called_with(
                proxies={'http': 'test'},
                trust_env=True,
                include_aiohttp=False,
            )

            auth = AuthClientCredentials('token')
            timeout = ClientTimeout(3)
            conn = Connection(
                url='http://test3.semi.technology',
                auth_client_secret=auth,
                timeout_config=timeout,
                proxies=None,
                trust_env=True,
                include_aiohttp=False,
                additional_headers=None,
            )
            conn._is_authentication_required = True
            conn._auth_bearer = 'Test!!!'
            self.assertEqual(conn.get_request_header(), {"content-type": "application/json", 'Authorization': 'Bearer Test!!!'})

    @patch("weaviate.base.connection.Connection.refresh_authentication")
    @patch("weaviate.base.connection.requests")
    def test_log_in(self, mock_requests, mock_refresh_authentication):
        """
        Test the `log_in` method.
        """

        # invalid calls
        val_err_msg = (
            "No login credentials provided. The Weaviate instance at http://test_url requires "
            "login credential, use argument 'auth_client_secret'."
        )
        status_code_err_msg = "Failed to get OpenID Configuration."

        mock_requests.get.return_value = Mock(status_code=200)
        with self.assertRaises(ValueError) as error:
            Connection(
                url='http://test_url',
                auth_client_secret=None,
                timeout_config=ClientTimeout(20),
                proxies=None,
                trust_env=False,
                include_aiohttp=False,
                additional_headers=None,
            )
        check_error_message(self, error, val_err_msg)
        mock_requests.get.assert_called_with(
            "http://test_url/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=20,
            proxies={},
        )
        mock_refresh_authentication.assert_not_called()

        mock_requests.get.return_value = Mock(status_code=500)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            Connection(
                url='http://test_url_2',
                auth_client_secret=None,
                timeout_config=ClientTimeout((2, 20)),
                proxies='test',
                trust_env=False,
                include_aiohttp=False,
                additional_headers=None,
            )
        check_startswith_error_message(self, error, status_code_err_msg)
        mock_requests.get.assert_called_with(
            "http://test_url_2/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={'http': 'test', 'https': 'test'},
        )
        mock_refresh_authentication.assert_not_called()


        # valid calls
        mock_requests.get.return_value = Mock(status_code=404)
        connection = Connection(
            url='http://test_url_2',
            auth_client_secret=None,
            timeout_config=ClientTimeout(None),
            proxies='test2',
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )
        mock_requests.get.assert_called_with(
            "http://test_url_2/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=None,
            proxies={'http': 'test2', 'https': 'test2'},
        )
        self.assertFalse(connection._is_authentication_required)


        mock_requests.get.return_value = Mock(status_code=404)
        connection = Connection(
            url='http://test_url_2',
            auth_client_secret=AuthClientCredentials('token'),
            timeout_config=ClientTimeout(None),
            proxies='test2',
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )
        mock_requests.get.assert_called_with(
            "http://test_url_2/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=None,
            proxies={'http': 'test2', 'https': 'test2'},
        )
        self.assertFalse(connection._is_authentication_required)


        mock_requests.get.return_value = Mock(status_code=200)
        connection = Connection(
            url='http://test_url_2',
            auth_client_secret=AuthClientCredentials('token'),
            timeout_config=ClientTimeout(None),
            proxies='test2',
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )
        mock_requests.get.assert_called_with(
            "http://test_url_2/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=None,
            proxies={'http': 'test2', 'https': 'test2'},
        )
        self.assertTrue(connection._is_authentication_required)

    @patch('weaviate.base.connection.Connection.log_in', Mock())
    @patch("weaviate.base.connection._get_epoch_time")
    @patch("weaviate.base.connection.Connection._get_client_id_and_href")
    @patch("weaviate.base.connection.Connection._set_bearer")
    def test_refresh_authentication(self, mock_set_bearer, mock_get_client_id_and_href, mock_get_epoch_time):
        """
        Test the `refresh_authentication` method.
        """

        connection = Connection(
            url='http://test_url_2',
            auth_client_secret=AuthClientCredentials('token'),
            timeout_config=ClientTimeout(None),
            proxies='test2',
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )

        mock_get_epoch_time.return_value = 2
        connection._auth_expires = 10
        connection.refresh_authentication()
        mock_get_client_id_and_href.assert_not_called()
        mock_set_bearer.assert_not_called()

        mock_get_epoch_time.return_value = 2
        mock_get_client_id_and_href.return_value = 'test_id', 'test_href'
        connection._auth_expires = 0
        connection.refresh_authentication()
        mock_get_client_id_and_href.assert_called()
        mock_set_bearer.assert_called_with(
            client_id='test_id',
            href='test_href'
        )

    @patch('weaviate.base.connection.Connection.log_in', Mock())
    @patch("weaviate.base.connection.requests")
    def test__get_client_id_and_href(self, mock_requests):
        """
        Test the `_get_client_id_and_href` method.
        """

        connection = Connection(
            url='http://test_url',
            auth_client_secret=AuthClientCredentials('token'),
            timeout_config=ClientTimeout((2, 20)),
            proxies='test',
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )

        # invalid calls

        conn_err_msg = "Cannot connect to Weaviate."
        auth_err_msg = "Cannot authenticate."

        mock_requests.get.side_effect = RequestsConnectionError
        with self.assertRaises(RequestsConnectionError) as error:
            connection._get_client_id_and_href()
        check_error_message(self, error, conn_err_msg)
        mock_requests.get.assert_called_with(
            "http://test_url/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={'http': 'test', 'https': 'test'},
        )

        mock_requests.get.side_effect = None
        mock_requests.get.return_value = Mock(status_code=404)
        with self.assertRaises(AuthenticationError) as error:
            connection._get_client_id_and_href()
        check_startswith_error_message(self, error, auth_err_msg)
        mock_requests.get.assert_called_with(
            "http://test_url/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={'http': 'test', 'https': 'test'},
        )

        # valid calls
        mock_requests.get.side_effect = None
        mock_requests.get.return_value = Mock(status_code=200, content='{"clientId":"TestID","href":"TestHref"}')
        client_id, href = connection._get_client_id_and_href()
        self.assertEqual(client_id, 'TestID')
        self.assertEqual(href, 'TestHref')
        mock_requests.get.assert_called_with(
            "http://test_url/v1/.well-known/openid-configuration",
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={'http': 'test', 'https': 'test'},
        )

    @patch('weaviate.base.connection.Connection.log_in', Mock())
    @patch("weaviate.base.connection.requests")
    @patch("weaviate.base.connection._get_epoch_time", return_value=10)
    def test__set_bearer(self, mock_get_epoch_time, mock_requests):
        """
        Test the `_set_bearer` method.
        """

        connection = Connection(
            url='http://test_url',
            auth_client_secret=AuthClientCredentials('token'),
            timeout_config=ClientTimeout((2, 20)),
            proxies=None,
            trust_env=False,
            include_aiohttp=False,
            additional_headers=None,
        )

        # invalid messages
        conn_err_msg = (
            "Can't connect to the third party authentication service. "
            "Check that it is running."
        )
        third_party_status_code_err_msg = "Status not OK in connection to the third party authentication service."
        credentials_err_msg = (
            "The grant_types supported by the third-party authentication service are "
            "insufficient. Please add 'client_credentials' or 'password'."
        )
        unauthorized_err_msg = "Authentication access denied. Are the credentials correct?"
        oauth_status_code_err_msg = "Could not get token."

        mock_requests.get.side_effect = RequestsConnectionError
        with self.assertRaises(RequestsConnectionError) as error:
            connection._set_bearer(client_id='TestID', href='HrefTest1')
        check_error_message(self, error, conn_err_msg)
        mock_requests.get.assert_called_with(
            url='HrefTest1',
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={},
        )


        mock_requests.get.side_effect = None
        mock_requests.get.return_value = Mock(status_code=404)
        with self.assertRaises(AuthenticationError) as error:
            connection._set_bearer(client_id='TestID2', href='HrefTest2')
        check_startswith_error_message(self, error, third_party_status_code_err_msg)
        mock_requests.get.assert_called_with(
            url='HrefTest2',
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={},
        )

        mock_get_response = Mock(status_code=200)
        mock_get_response.json.return_value = {'grant_types_supported' : []}
        mock_requests.get.return_value = mock_get_response
        with self.assertRaises(AuthenticationError) as error:
            connection._set_bearer(client_id='TestID2', href='HrefTest2')
        check_error_message(self, error, credentials_err_msg)
        mock_requests.get.assert_called_with(
            url='HrefTest2',
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={},
        )

        mock_get_response = Mock(status_code=200)
        mock_get_response.json.return_value = {
            'grant_types_supported' : ['client_credentials'],
            'token_endpoint': 'Some Token URL'
        }
        mock_requests.get.return_value = mock_get_response
        mock_requests.post.return_value = Mock(status_code=401)
        with self.assertRaises(AuthenticationError) as error:
            connection._set_bearer(client_id='TestID3', href='HrefTest__')
        check_startswith_error_message(self, error, unauthorized_err_msg)
        mock_requests.get.assert_called_with(
            url='HrefTest__',
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={},
        )
        mock_requests.post.assert_called_with(
            url='Some Token URL',
            json={'client_id': 'TestID3', "grant_type": "client_credentials", 'client_secret': 'token'},
            timeout=(2, 20),
            proxies={},
        )

        mock_get_response = Mock(status_code=200)
        mock_get_response.json.return_value = {
            'grant_types_supported' : ['client_credentials'],
            'token_endpoint': 'Some Token URL'
        }
        mock_requests.get.return_value = mock_get_response
        mock_requests.post.return_value = Mock(status_code=404)
        with self.assertRaises(UnsuccessfulStatusCodeError) as error:
            connection._set_bearer(client_id='TestID3', href='HrefTest__')
        check_startswith_error_message(self, error, oauth_status_code_err_msg)
        mock_requests.get.assert_called_with(
            url='HrefTest__',
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={},
        )
        mock_requests.post.assert_called_with(
            url='Some Token URL',
            json={'client_id': 'TestID3', "grant_type": "client_credentials", 'client_secret': 'token'},
            timeout=(2, 20),
            proxies={},
        )

        # valid calls
        mock_get_response = Mock(status_code=200)
        mock_get_response.json.return_value = {
            'grant_types_supported' : ['client_credentials'],
            'token_endpoint': 'Some Token URL'
        }
        mock_requests.get.return_value = mock_get_response
        mock_requests.post.return_value = Mock(status_code=200, content='{"access_token":"testToken","expires_in":20}')
        connection._set_bearer(client_id='TestID2', href='HrefTest2')
        mock_requests.get.assert_called_with(
            url='HrefTest2',
            headers={"content-type": "application/json"},
            timeout=(2, 20),
            proxies={},
        )
        mock_requests.post.assert_called_with(
            url='Some Token URL',
            json={'client_id': 'TestID2', "grant_type": "client_credentials", 'client_secret': 'token'},
            timeout=(2, 20),
            proxies={},
        )
        self.assertEqual(connection._auth_expires, 28)
        self.assertEqual(connection._auth_bearer, 'testToken')


    @patch("weaviate.base.connection.datetime")
    def test_get_epoch_time(self, mock_datetime):
        """
        Test the `get_epoch_time` function.
        """

        import datetime
        from weaviate.base.connection import _get_epoch_time

        zero_epoch = datetime.datetime.fromtimestamp(0)
        mock_datetime.datetime.utcnow.return_value = zero_epoch
        self.assertEqual(_get_epoch_time(), 0)

        epoch = datetime.datetime.fromtimestamp(110.56)
        mock_datetime.datetime.utcnow.return_value = epoch
        self.assertEqual(_get_epoch_time(), 111)

        epoch = datetime.datetime.fromtimestamp(110.46)
        mock_datetime.datetime.utcnow.return_value = epoch
        self.assertEqual(_get_epoch_time(), 110)
