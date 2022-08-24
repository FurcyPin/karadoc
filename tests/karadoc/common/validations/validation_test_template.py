import unittest
from typing import List, Optional
from unittest import mock

import karadoc
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.validations import ValidationResult, ValidationResultTemplate
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)


class ValidationTestTemplate(unittest.TestCase):
    """Generic class used to test validations"""

    def validation_test_template(
        self,
        test_model_dir: str,
        expected_template: Optional[ValidationResultTemplate],
        expected_return_code: ReturnCode,
        settings: dict = None,
    ):
        """Runs a validate command on the given `test_model_dir` with the given settings, and expect to find
        the given `expected_template` in the validation results.

        Given a model in `test_model_dir`
        When we run a validate command with the specified `settings`
        Then 'expected_template' is yielded

        :param test_model_dir: Path to the model directory to use
        :param expected_template: The expected ValidationResultTemplate yielded
        :param expected_return_code: The expected ReturnCode returned
        :param settings: Option dynaconf setting override
        :return:
        """
        if settings is None:
            settings = {}

        def inspect_results(results: List[ValidationResult]):
            if expected_template is None:
                self.assertEqual([], results)
            else:
                self.assertEqual([expected_template.check_type], [r.check_type for r in results])
                self.assertEqual([expected_template.default_severity], [r.severity for r in results])

        inspect_results_name = "karadoc.cli.commands.validate.inspect_results"
        with mock_settings_for_test_class({"enable_file_index_cache": False}), mock_settings_for_test(
            {"model_dir": test_model_dir, **settings}
        ), mock.patch(inspect_results_name, side_effect=inspect_results) as inspect_mock:
            return_code = karadoc.cli.run_command("validate")
        if expected_template is not None:
            inspect_mock.assert_called_once()
        self.assertEqual(expected_return_code, return_code)
