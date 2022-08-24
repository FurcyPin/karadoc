import unittest
from unittest import mock

from dynaconf.base import Settings

from karadoc.common import conf
from karadoc.common.exceptions import MissingConfigurationException
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)


@mock_settings_for_test_class()
class TestConfBox(unittest.TestCase):
    def __is_pickable(self, obj):
        """When doing a backfill on specific tasks, Airflow creates a copy of the subdag,
        which requires the operators to be pickable (aka serializable)
        """
        import pickle

        ser_obj = pickle.dumps(obj)
        res = pickle.loads(ser_obj, encoding="utf-8")
        self.assertEqual(res, obj)

    @mock_settings_for_test(
        {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.connectors.dummy",
                }
            }
        }
    )
    def test_conf_base_case(self):
        self.assertEqual("tests.resources.connectors.dummy", conf.get_connection_conf("test_conn").get("type"))

    @mock_settings_for_test(
        {
            "connection": {
                "test_conn": {
                    "type": "tests.resources.connectors.dummy",
                }
            }
        }
    )
    def test_conf_pickable(self):
        self.__is_pickable(conf.get_connection_conf("test_conn"))

    secret_env_conf_valid_setting = {
        "secret": {"keyvault": {"client_id": "test_client_id"}},
        "connection": {
            "test_conn": {
                "type": "tests.resources.connectors.dummy",
                "sensitive_value": {"secret": {"keyvault": "name-in-keyvault"}},
            }
        },
    }

    @mock_settings_for_test(secret_env_conf_valid_setting)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_get_secret_env_conf(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_connection_conf("test_conn").get("sensitive_value")
        expected = "test_value"
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)

    @mock_settings_for_test(secret_env_conf_valid_setting)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_get_secret_env_conf_getitem(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_connection_conf("test_conn")["sensitive_value"]
        expected = "test_value"
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)

    @mock_settings_for_test(secret_env_conf_valid_setting)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_get_secret_env_conf_getattr(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_connection_conf("test_conn").sensitive_value
        expected = "test_value"
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)

    @mock_settings_for_test(
        {
            "secret": {"keyvault": {"client_id": "test_client_id"}},
            "connection": {
                "test_conn": {
                    "type": "tests.resources.connectors.dummy",
                    "sensitive_value": {"secret": {"INCORRECT_VAULT_NAME": "name-in-keyvault"}},
                }
            },
        }
    )
    def test_get_secret_with_incorrect_vault_name(self):
        with self.assertRaises(MissingConfigurationException):
            conf.get_connection_conf("test_conn").get("sensitive_value")

    @mock_settings_for_test(
        {
            "secret": None,
            "connection": {
                "test_conn": {
                    "type": "tests.resources.connectors.dummy",
                    "sensitive_value": {"secret": {"keyvault": "name-in-keyvault"}},
                }
            },
        }
    )
    def test_get_secret_with_missing_vault_conf(self):
        with self.assertRaises(MissingConfigurationException):
            conf.get_connection_conf("test_conn").get("sensitive_value")

    @mock_settings_for_test({"secret": None, "custom": {"my_conf_variable": "test_value"}})
    def test_get_custom_conf(self):
        actual = conf.get_custom_settings().get("my_conf_variable")
        expected = "test_value"
        self.assertEqual(expected, actual)

    def test_get_custom_conf_with_dynaconf_merge(self):
        """
        Given settings for a specific environment that partially override settings for the default environment
        When we get the setting for this environment
        Then the settings from the default environment that were not overridden should still be there
        """
        # Sadly, I could not find a way to mock dynaconf in a way that supports environment switching.
        # Instead we have to override dynaconf.settings with another global setting.
        import dynaconf

        old_settings = dynaconf.settings
        dynaconf.settings = Settings("tests/resources/karadoc/common/conf/test_conf_box/settings.toml")
        try:
            self.assertEqual("A", conf.get_custom_settings().get("variable_A"))
            self.assertEqual("B", conf.get_custom_settings().get("variable_B"))
            self.assertEqual("OVERRIDE", conf.get_custom_settings("test").get("variable_A"))
            self.assertEqual("B", conf.get_custom_settings("test").get("variable_B"))
        finally:
            dynaconf.settings = old_settings

    secret_custom_conf_valid_settings = {
        "secret": {"keyvault": {"client_id": "test_client_id"}},
        "custom": {"my.conf.variable.secret.keyvault": "name-in-keyvault"},
    }

    @mock_settings_for_test(secret_custom_conf_valid_settings)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_get_secret_custom_conf(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_custom_settings().get("my").get("conf").get("variable")
        expected = "test_value"
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)

    @mock_settings_for_test(secret_custom_conf_valid_settings)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_get_secret_custom_conf_getitem(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_custom_settings()["my"]["conf"]["variable"]
        expected = "test_value"
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)

    @mock_settings_for_test(secret_custom_conf_valid_settings)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_get_secret_custom_conf_getattr(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_custom_settings().my.conf.variable
        expected = "test_value"
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)

    @mock_settings_for_test(secret_custom_conf_valid_settings)
    @mock.patch("karadoc.common.conf.keyvault.get_secret")
    def test_to_flat_dict(self, get_secret):
        get_secret.return_value = "test_value"
        actual = conf.get_custom_settings().to_flat_dict()
        expected = {"my.conf.variable": "test_value"}
        self.assertEqual(expected, actual)
        get_secret.assert_called_once_with(conn_name="keyvault", secret_id="name-in-keyvault", env=None)
