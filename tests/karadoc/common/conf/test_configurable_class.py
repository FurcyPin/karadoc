import unittest

from karadoc.common.conf.configurable_class import ConfigurableClass, ConfParam
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class


@mock_settings_for_test_class()
class TestConfigurableClass(unittest.TestCase):
    def test_check_required_param_ok(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(required=True)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {"a": 1}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(0, len(actual))

    def test_check_required_param_ko(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(required=True)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(1, len(actual))
        self.assertIn("Missing required parameter a", actual[0].message)

    def test_check_required_param_default_ok(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(required=True, default=1)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {"a": 1}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(0, len(actual))

    def test_check_required_param_default_ko(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(required=True, default=1)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(1, len(actual))
        self.assertIn("Missing required parameter a", actual[0].message)

    def test_check_param_types_ok(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(type=str)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {"a": "1"}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(0, len(actual))

    def test_check_param_types_ko(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(type=str)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {"a": 1}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(1, len(actual))
        self.assertIn("Incorrect parameter type for a", actual[0].message)

    @mock_settings_for_test(
        {"connection": {"test_conn": {"sensitive_value": {"secret": {"keyvault": "name-in-keyvault"}}}}}
    )
    def test_check_secret_params_ok(self):
        class MyClass(ConfigurableClass):
            sensitive_value = ConfParam(secret=True)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        from karadoc.common import conf

        conf = conf.get_connection_conf("test_conn")

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(0, len(actual))

    def test_check_secret_params_ko(self):
        class MyClass(ConfigurableClass):
            a = ConfParam(secret=True)

            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {"a": 1}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(1, len(actual))
        self.assertIn("Parameter a should be secret", actual[0].message)

    def test_check_unknown_param_ko(self):
        class MyClass(ConfigurableClass):
            def __init__(self, connection_conf):
                super().__init__(connection_conf)

        conf = {"b": 1}

        actual = list(MyClass(conf).validate_params())
        self.assertEqual(1, len(actual))
        self.assertIn("Unknown parameter b", actual[0].message)
