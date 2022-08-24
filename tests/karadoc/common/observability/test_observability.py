import logging
import re
from unittest import TestCase, mock

from karadoc.cli import run_command
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.common.observability import LogEvent
from karadoc.common.observability.conf import setup_observability
from karadoc.common.observability.console_event import ConsoleEvent
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)
from karadoc.test_utils.stdio import captured_output


@mock_settings_for_test_class({"observability": None})
class TestObservability(TestCase):
    def test_default_logging_configuration(self):
        setup_observability(reset=True)
        self.assertEqual(1, len(logging.root.handlers))
        self.assertEqual(logging.INFO, logging.root.level)

    @mock_settings_for_test({"observability": {"type": None, "level": "WARN"}})
    def test_logging_configuration_warn(self):
        setup_observability(reset=True)
        self.assertEqual(1, len(logging.root.handlers))
        self.assertEqual(logging.WARN, logging.root.level)

    @staticmethod
    def _generate_logs():
        with captured_output() as (out, err):
            setup_observability(reset=True)
            LOG = logging.getLogger(__name__)
            LOG.info("Hello")
            LOG.info(LogEvent(message="Hello", event_type="test"))
            LOG.user_output(ConsoleEvent(message="Hello", event_type="test", extra_console_message="World"))
        return err, out

    def test_simple_console_logging_info(self):
        """
        GIVEN the default logging configuration
        WHEN logging events at info level
        THEN only ConsoleEvents should be displayed on stdout,
         and only their message and extra_console_message should be displayed
        """
        err, out = self._generate_logs()
        self.assertEqual("Hello World\n", out.getvalue())
        self.assertEqual("", err.getvalue())

    @mock_settings_for_test({"observability": {"verbose": True}})
    def test_simple_console_logging_info_with_verbose_conf(self):
        """
        GIVEN the default logging configuration with observability.verbose enabled
        WHEN logging events at info level
        THEN all LogEvents should be fully written on stdout
        """
        err, out = self._generate_logs()
        self.assertIn("[USER_OUTPUT]", out.getvalue())
        self.assertIn("ConsoleEvent", out.getvalue())
        self.assertIn("Hello", out.getvalue())
        self.assertNotIn("World", out.getvalue())
        self.assertEqual("", err.getvalue())

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": "tests/resources/karadoc/common/observability/test_observability/model",
            "warehouse_dir": "test_working_dir/hive/warehouse",
        }
    )
    def test_simple_console_logging_info_without_verbose_option(self):
        """
        GIVEN the default configuration
        WHEN running a command without the -v -or --verbose option
        THEN only ConsoleEvents should be displayed on stdout and only their message should be displayed
        """
        with captured_output() as (out, err):
            run_command("run --dry --tables test_schema.test_table")
        self.assertNotIn("[USER_OUTPUT]", out.getvalue())
        self.assertNotIn("ConsoleEvent(", out.getvalue())
        self.assertEqual("", err.getvalue())

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": "tests/resources/karadoc/common/observability/test_observability/model",
            "warehouse_dir": "test_working_dir/hive/warehouse",
        }
    )
    def test_simple_console_logging_info_with_verbose_option(self):
        """
        GIVEN the default configuration
        WHEN running a command with the -v -or --verbose option
        THEN only ConsoleEvents should be displayed on stdout and only their message should be displayed
        """
        with captured_output() as (out, err):
            run_command("-v run --dry --tables test_schema.test_table")
        self.assertIn("[USER_OUTPUT]", out.getvalue())
        self.assertIn("ConsoleEvent(", out.getvalue())

        with captured_output() as (out, err):
            run_command("--verbose run --dry --tables test_schema.test_table")
        self.assertIn("[USER_OUTPUT]", out.getvalue())
        self.assertIn("ConsoleEvent(", out.getvalue())
        self.assertEqual("", err.getvalue())

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": "tests/resources/karadoc/common/observability/test_observability/model",
            "warehouse_dir": "test_working_dir/hive/warehouse",
        }
    )
    def test_simple_console_logging_info_for_job_failure(self):
        """
        GIVEN the default configuration
        WHEN running a job that fails without the -v -or --verbose option
        THEN the error message and stack trace should be displayed exactly once.
        """
        with captured_output() as (out, err), self.assertRaises(ActionFileLoadingError):
            run_command("run --tables test_schema.test_fail")
        self.assertIn("Traceback", out.getvalue())
        self._assertInOnce("ActionFileLoadingError:", out.getvalue())
        self.assertEqual("", err.getvalue())

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": "tests/resources/karadoc/common/observability/test_observability/model",
            "warehouse_dir": "test_working_dir/hive/warehouse",
            "custom_command_packages": [
                "tests.resources.karadoc.common.observability.test_observability.custom_command_package"
            ],
        }
    )
    def test_simple_console_logging_info_for_command_failure_without_jobs(self):
        """
        GIVEN the default configuration
        WHEN running a command that does not have jobs and that fails, without the -v -or --verbose option
        THEN the error message and stack trace should be displayed exactly once.
        """
        with captured_output() as (out, err), self.assertRaises(ValueError):
            run_command("custom_command_fail")
        self._assertInOnce("Command failed: custom_command_fail", out.getvalue())
        self._assertInOnce("Traceback", out.getvalue())
        self._assertInOnce("ValueError: Fail", out.getvalue())

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": "tests/resources/karadoc/common/observability/test_observability/model",
            "warehouse_dir": "test_working_dir/hive/warehouse",
            "observability.handler.dummy": {"type": "tests.resources.observability.handlers.dummy", "level": "info"},
        }
    )
    def test_logging_with_custom_logger(self):
        """
        GIVEN a configuration with a custom log handler
        WHEN running a job without the verbose option
        THEN the custom handler should handle log events
        """
        with captured_output() as (out, err):
            run_command("run --tables test_schema.test_table --dry")
        self._assertInOnce("DummyLogHandler: LogEvent[(]message='Command started:", out.getvalue())
        self._assertInOnce("DummyLogHandler: LogEvent[(]message='Command ended:", out.getvalue())
        self.assertEqual(2, len(logging.root.handlers))
        self.assertEqual(logging.INFO, logging.root.level)

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": "tests/resources/karadoc/common/observability/test_observability/model",
            "warehouse_dir": "test_working_dir/hive/warehouse",
            "observability.handler.dummy": {
                "type": "does.not.exists",
            },
        }
    )
    def test_logging_with_custom_logger_configuration_error(self):
        """
        GIVEN a configuration with a custom log handler that does not exist
        WHEN running a job without the verbose option
        THEN an error message, along with the stack trace and error message should be displayed.
        """
        with captured_output() as (out, err), self.assertRaises(ModuleNotFoundError):
            run_command("run --tables test_schema.test_table --dry")
        self._assertInOnce("ERROR: An exception occurred before logging could be configured", err.getvalue())
        self._assertInOnce("Traceback", err.getvalue())
        self._assertInOnce("ModuleNotFoundError: No module named 'does'", err.getvalue())

    def test_logging_with_dynaconf_error(self):
        """
        GIVEN a configuration that dynaconf fails to read
        WHEN running a job without the verbose option
        THEN an error message, along with the stack trace and error message should be displayed.
        """

        def fail(*args, **kwargs):
            raise ValueError("test")

        with mock.patch("dynaconf.settings.get", side_effect=fail) as mock_get, captured_output() as (
            out,
            err,
        ), self.assertRaises(ValueError):
            run_command("run --tables test_schema.test_table --dry")
        mock_get.assert_called()
        self._assertInOnce("ERROR: An exception occurred before logging could be configured", err.getvalue())
        self._assertInOnce("Traceback", err.getvalue())
        self._assertInOnce("ValueError: test", err.getvalue())

    @staticmethod
    def _count_occurrences(regex: str, string: str):
        """
        Count the number of occurrences of a regular expression in the given string
        """
        return len(re.compile(regex).findall(string))

    def _assertInOnce(self, regex: str, string: str):
        return self.assertEqual(1, self._count_occurrences(regex, string))
