import unittest

from dynaconf.base import Settings

import karadoc
from karadoc.common.commands.return_code import ReturnCode
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class
from tests.karadoc.test_utils import get_resource_folder_path


@mock_settings_for_test_class({"enable_file_index_cache": False})
class TestValidate(unittest.TestCase):
    """Specific validations are checked in tests/common/validations"""

    @mock_settings_for_test({"model_dir": get_resource_folder_path(__name__) + "/model_ok"})
    def test_validate(self):
        self.assertEqual(ReturnCode.Success, karadoc.cli.run_command("validate"))

    def test_validate_multiple_envs(self):
        # Sadly, I could not find a way to mock dynaconf in a way that supports environment switching.
        # Instead, we have to override dynaconf.settings with another global setting.
        import dynaconf

        old_settings = dynaconf.settings
        dynaconf.settings = Settings(get_resource_folder_path(__name__) + "/model_with_connectors/settings.toml")
        try:
            self.assertEqual(ReturnCode.Success, karadoc.cli.run_command("validate --envs test1 test2"))
            self.assertEqual(ReturnCode.Error, karadoc.cli.run_command("validate --envs test1 test3"))
        finally:
            dynaconf.settings = old_settings
