import unittest
from unittest import mock
from unittest.mock import MagicMock

from pyspark.sql import SparkSession

from karadoc import launcher
from karadoc.spark.utils import get_spark_session
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from tests.karadoc.test_utils import get_resource_folder_path


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/models",
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)
class TestLauncher(unittest.TestCase):
    def setUp(self) -> None:
        get_spark_session("test")
        self.assertIsNotNone(SparkSession._instantiatedSession)

    def tearDown(self) -> None:
        """
        - When karadoc has finished running
        - Then, no matter what happened during the command call, any instantiated SparkSession should be stopped
        """
        self.assertIsNone(SparkSession._instantiatedSession)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_no_arg(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with no arg
        - Then it should display the help message and exit with return code 0
        """
        with self.assertRaises(SystemExit) as e:
            launcher.main([])
        mock_print_help.assert_called_once()
        self.assertEqual((0,), e.exception.args)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_help(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with --help
        - Then it should display the help message and exit with return code 0
        """
        with self.assertRaises(SystemExit) as e:
            launcher.main(["--help"])
        mock_print_help.assert_called_once()
        self.assertEqual((0,), e.exception.args)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_unknown_command(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with an unknown command
        - Then it should exit with return code 2
        """
        with self.assertRaises(SystemExit) as e:
            launcher.main(["unknown_command"])
        mock_print_help.assert_not_called()
        self.assertEqual((2,), e.exception.args)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_valid_command_with_return_code(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with a valid command that returns a successful return code
        - Then it should exit with return code 0
        """
        # TODO: Once we make commands customizable, we should rewrite this test to use a custom dummy command.
        with self.assertRaises(SystemExit) as e:
            launcher.main(["validate"])
        mock_print_help.assert_not_called()
        self.assertEqual((0,), e.exception.args)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_valid_command_without_return_code(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with a valid command that returns no return code
        - Then it should exit with return code 0
        """
        # TODO: Once we make commands customizable, we should rewrite this test to use a custom dummy command.
        with self.assertRaises(SystemExit) as e:
            launcher.main(["list"])
        mock_print_help.assert_not_called()
        self.assertEqual((0,), e.exception.args)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_valid_command_with_unknown_option(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with a valid command with an unknown option
        - Then it should exit with return code 2
        """
        with self.assertRaises(SystemExit) as e:
            launcher.main(["validate", "--unknown-option"])
        mock_print_help.assert_not_called()
        self.assertEqual((2,), e.exception.args)

    @mock.patch("argparse.ArgumentParser.print_help")
    def test_valid_command_that_fails(self, mock_print_help: MagicMock):
        """
        - When karadoc is launched with a valid command which fails
        - Then it should exit with return code 1
        """
        with self.assertRaises(SystemExit) as e:
            launcher.main(["run", "--dry", "--tables", "unknown_table"])
        mock_print_help.assert_not_called()
        self.assertEqual((1,), e.exception.args)
