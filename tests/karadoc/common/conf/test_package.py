import unittest
from unittest import mock
from unittest.mock import MagicMock

from karadoc.common import conf
from karadoc.common.exceptions import MissingConfigurationException
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class


@mock_settings_for_test_class()
class TestPackage(unittest.TestCase):
    @mock_settings_for_test({"connection": {"test_conn": {"a": "b"}}})
    def test_get_connection_conf(self):
        conn = conf.get_connection_conf(connection_name="test_conn")
        self.assertEqual(conn.to_dict(), {"a": "b"})

    @mock.patch("karadoc.common.conf.package.get_env")
    def test_get_connection_conf_with_missing_conf(self, get_env: MagicMock):
        """
        When getting a missing connection
        Then the error message should be intelligible
        """
        get_env.return_value = "test"
        with self.assertRaises(Exception) as cm:
            conf.get_connection_conf(connection_name="test_conn")
        the_exception = cm.exception
        self.assertIsInstance(the_exception, MissingConfigurationException)
        self.assertIn("Could not find configuration test.connection.test_conn", str(the_exception))

    @mock_settings_for_test({"remote_env": {"test_env": {"a": "b"}}})
    def test_get_remote_env_conf(self):
        conn = conf.get_remote_env_conf(remote_env_name="test_env")
        self.assertEqual(conn.to_dict(), {"a": "b"})

    @mock.patch("karadoc.common.conf.package.get_env")
    def test_get_remote_env_conf_with_missing_conf(self, get_env: MagicMock):
        """
        When getting a missing remote_env
        Then the error message should be intelligible
        """
        get_env.return_value = "test"
        with self.assertRaises(Exception) as cm:
            conf.get_remote_env_conf(remote_env_name="test_env")
        the_exception = cm.exception
        self.assertIsInstance(the_exception, MissingConfigurationException)
        self.assertIn("Could not find configuration test.remote_env.test_env", str(the_exception))

    @mock_settings_for_test({"secret": {"test_vault": {"a": "b"}}})
    def test_get_vault_conf(self):
        conn = conf.get_vault_conf(vault_name="test_vault")
        self.assertEqual(conn.to_dict(), {"a": "b"})

    @mock.patch("karadoc.common.conf.package.get_env")
    def test_get_vault_conf_with_missing_conf(self, get_env: MagicMock):
        """
        When getting a missing vault
        Then the error message should be intelligible
        """
        get_env.return_value = "test"
        with self.assertRaises(Exception) as cm:
            conf.get_vault_conf(vault_name="test_vault")
        the_exception = cm.exception
        self.assertIsInstance(the_exception, MissingConfigurationException)
        self.assertIn("Could not find configuration test.secret.test_vault", str(the_exception))
