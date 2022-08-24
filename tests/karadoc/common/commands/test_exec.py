import unittest
from typing import Type
from unittest import mock

import karadoc
from karadoc.common.commands import exec
from karadoc.common.commands.command import Command
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)
from tests.resources.karadoc.common.commands.test_exec.builtin_command_package.command_1 import (
    BuiltinCommand1,
)
from tests.resources.karadoc.common.commands.test_exec.builtin_command_package.command_2 import (
    BuiltinCommand2,
)
from tests.resources.karadoc.common.commands.test_exec.builtin_command_package.command_group.sub_command_1 import (
    BuiltInSubCommand1,
)
from tests.resources.karadoc.common.commands.test_exec.builtin_command_package.command_group.sub_command_2 import (
    BuiltInSubCommand2,
)
from tests.resources.karadoc.common.commands.test_exec.custom_command_group_package.custom_command_group.sub_command_1 import (  # noqa: E501
    CustomSubCommand1,
)
from tests.resources.karadoc.common.commands.test_exec.custom_command_package.custom_command import (
    CustomCommand,
)
from tests.resources.karadoc.common.commands.test_exec.override_command_group_package.command_group.sub_command_1 import (  # noqa: E501
    OverrideSubCommand1,
)
from tests.resources.karadoc.common.commands.test_exec.override_command_package.command_1 import (
    OverrideCommand1,
)

test_exec = "tests.resources.karadoc.common.commands.test_exec"


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
    }
)
@mock.patch.object(exec, "BUILTIN_COMMAND_PACKAGE", f"{test_exec}.builtin_command_package")
class TestExec(unittest.TestCase):
    def _assert_command_works(self, command_name: str, command_class: Type[Command]):
        def do_command(args):
            self.assertEqual("arg1", args.option)

        with mock.patch.object(command_class, "do_command", side_effect=do_command) as mocked:
            karadoc.cli.run_command(f"{command_name} --option arg1")
            mocked.assert_called_once()

    def _assert_custom_command_works(self, command_name: str, command_class: Type[Command]):
        def do_command(args):
            self.assertEqual("arg1", args.custom_option)

        with mock.patch.object(command_class, "do_command", side_effect=do_command) as mocked:
            karadoc.cli.run_command(f"{command_name} --custom-option arg1")
            mocked.assert_called_once()

    def test_default_commands(self):
        """
        GIVEN a set of default commands with no other particular change
        WHEN we call them
        THEN their do_command method should be called
        """
        with mock.patch.object(exec, "_print_command_override_warning") as _print_command_override_warning:
            self._assert_command_works("command_1", BuiltinCommand1)
            self._assert_command_works("command_2", BuiltinCommand2)
            self._assert_command_works("command_group sub_command_1", BuiltInSubCommand1)
            self._assert_command_works("command_group sub_command_2", BuiltInSubCommand2)
        _print_command_override_warning.assert_not_called()

    @mock_settings_for_test({"custom_command_packages": [(f"{test_exec}.custom_command_package")]})
    def test_custom_command(self):
        """
        GIVEN a custom command
        WHEN it is called via the command line
        THEN it's do_command method should be called
         AND the other built-in commands should still be work
        """
        with mock.patch.object(exec, "_print_command_override_warning") as _print_command_override_warning:
            self._assert_custom_command_works("custom_command", CustomCommand)
            self._assert_command_works("command_1", BuiltinCommand1)
            self._assert_command_works("command_2", BuiltinCommand2)
            self._assert_command_works("command_group sub_command_1", BuiltInSubCommand1)
            self._assert_command_works("command_group sub_command_2", BuiltInSubCommand2)
        _print_command_override_warning.assert_not_called()

    @mock_settings_for_test({"custom_command_packages": [(f"{test_exec}.override_command_package")]})
    def test_override_command(self):
        """
        GIVEN a custom command that overrides an existing built-in command
        WHEN it is called via the command line
        THEN the do_command method of the command override should be called
         AND the other built-in commands should still be available
        """
        with mock.patch.object(exec, "_print_command_override_warning") as _print_command_override_warning:
            self._assert_custom_command_works("command_1", OverrideCommand1)
            self._assert_command_works("command_2", BuiltinCommand2)
            self._assert_command_works("command_group sub_command_1", BuiltInSubCommand1)
            self._assert_command_works("command_group sub_command_2", BuiltInSubCommand2)
        _print_command_override_warning.assert_called()

    @mock_settings_for_test({"custom_command_packages": [(f"{test_exec}.custom_command_group_package")]})
    def test_custom_command_group(self):
        """
        GIVEN a custom command group
        WHEN it is called via the command line
        THEN the do_command method of the sub command should be called
        """
        # Currently, we don't support defining custom sub_commands in a built-in command group.
        # Said differently, importing a custom command group that has the same name of a built-in command group
        # will override all sub commands of that group, rather than merge the built-in and the custom group
        # as one might expect.
        # Such feature might be added in the future if users ask for it.

        with mock.patch.object(exec, "_print_command_override_warning") as _print_command_override_warning:
            self._assert_custom_command_works("custom_command_group sub_command_1", CustomSubCommand1)
            self._assert_command_works("command_1", BuiltinCommand1)
            self._assert_command_works("command_2", BuiltinCommand2)
            self._assert_command_works("command_group sub_command_1", BuiltInSubCommand1)
            self._assert_command_works("command_group sub_command_2", BuiltInSubCommand2)
        _print_command_override_warning.assert_not_called()

    @mock_settings_for_test({"custom_command_packages": [(f"{test_exec}.override_command_group_package")]})
    def test_override_command_group(self):
        """
        GIVEN a custom command group that overrides an existing built-in command group
        WHEN it is called via the command line
        THEN the do_command method of the sub command override should be called
         AND the other commands from the overridden command group should not be available
        """
        # Currently, we don't support defining custom sub_commands in a built-in command group.
        # Said differently, importing a custom command group that has the same name of a built-in command group
        # will override all sub commands of that group, rather than merge the built-in and the custom group
        # as one might expect.
        # Such feature might be added in the future if users ask for it.

        with mock.patch.object(exec, "_print_command_override_warning") as _print_command_override_warning:
            self._assert_custom_command_works("command_group sub_command_1", OverrideSubCommand1)
            self._assert_command_works("command_1", BuiltinCommand1)
            self._assert_command_works("command_2", BuiltinCommand2)
            with self.assertRaises(SystemExit) as e:
                self._assert_command_works("command_group sub_command_2", BuiltInSubCommand2)
            self.assertSequenceEqual((2,), e.exception.args)
        _print_command_override_warning.assert_called()
