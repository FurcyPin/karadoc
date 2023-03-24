import shutil
import unittest
from unittest import mock

import karadoc
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class
from tests.karadoc.test_utils import get_resource_folder_path


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/models",
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)
class TestRun(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def test_run_with_missing_vars(self):
        """
        - Given a POPULATE.py with a variable
        - When we run it without passing the variable in the arguments
        - Then the run command shall fail.
        """
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("run --dry --models test_schema.var_str")
        the_exception = cm.exception
        self.assertEqual("The following variables are required and missing: [var_str]", str(the_exception.__cause__))

    def test_run_with_var_str(self):
        """
        - Given a Populate with a variable of type str
        - When it is run
        - And the variable is passed in the arguments
        - Then the run command shall not fail and use the value passed in the arguments
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_str": "2018-02-02"})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --vars var_str=2018-02-02 --models test_schema.var_str")
        check_mock.assert_called_once()

    def test_run_with_var_bool(self):
        """
        - Given a Populate with a variable of type bool
        - When it is run
        - And the variable is passed in the arguments
        - Then the run command shall not fail and use the value passed in the arguments
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_bool": True})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --vars var_bool=True --models test_schema.var_bool")
        check_mock.assert_called_once()

    def test_run_with_var_int(self):
        """
        - Given a Populate with a variable of type int
        - When it is run
        - And the variable is passed in the arguments
        - Then the run command shall not fail and use the value passed in the arguments
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_int": 1})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --vars var_int=1 --models test_schema.var_int")
        check_mock.assert_called_once()

    def test_run_with_var_float(self):
        """
        - Given a Populate with a variable of type float
        - When it is run
        - And the variable is passed in the arguments
        - Then the run command shall not fail and use the value passed in the arguments
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_float": 1.0})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --vars var_float=1. --models test_schema.var_float")
        check_mock.assert_called_once()

    def test_run_with_bad_var_key_type(self):
        """
        - Given a Populate with bad variable key type
        - When we run it
        - Then an exception should be raised
        """
        with self.assertRaises(ActionFileLoadingError) as cm:
            karadoc.cli.run_command("run --dry --models test_schema.var_bad_key_type")
        the_exception = cm.exception
        self.assertIn("Could not load POPULATE.py file for table test_schema.var_bad_key_type", str(the_exception))
        self.assertEqual(
            "Only strings are allowed as vars keys. Got key: 1 of type: int instead.", str(the_exception.__cause__)
        )

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_run_with_bad_var_key_type_and_allow_missing_var(self):
        """
        - Given a Populate with bad variable key type
        - When we run it with allow_missing_vars=True
        - Then an exception should be raised
        """
        with self.assertRaises(ActionFileLoadingError) as cm:
            karadoc.cli.run_command("run --dry --models test_schema.var_bad_value_type")
        the_exception = cm.exception
        self.assertIn("Could not load POPULATE.py file for table test_schema.var_bad_value_type", str(the_exception))
        self.assertEqual(
            "Type list of variable 'var_bad' is not supported for variables. "
            "Only the following types are supported ['str', 'bool', 'int', 'float']",
            str(the_exception.__cause__),
        )

    def test_run_with_bad_var_value_type(self):
        """
        - Given a Populate with bad variable key type
        - When we run it
        - Then an exception should be raised
        """
        with self.assertRaises(ActionFileLoadingError) as cm:
            karadoc.cli.run_command("run --dry --models test_schema.var_bad_value_type")
        the_exception = cm.exception
        self.assertIn("Could not load POPULATE.py file for table test_schema.var_bad_value_type", str(the_exception))
        self.assertEqual(
            "Type list of variable 'var_bad' is not supported for variables. "
            "Only the following types are supported ['str', 'bool', 'int', 'float']",
            str(the_exception.__cause__),
        )

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_run_with_bad_var_value_type_and_allow_missing_var(self):
        """
        - Given a Populate with bad variable key type
        - When we run it with allow_missing_vars=True
        - Then an exception should be raised
        """
        with self.assertRaises(ActionFileLoadingError) as cm:
            karadoc.cli.run_command("run --dry --models test_schema.var_bad_key_type")
        the_exception = cm.exception
        self.assertIn("Could not load POPULATE.py file for table test_schema.var_bad_key_type", str(the_exception))
        self.assertEqual(
            "Only strings are allowed as vars keys. Got key: 1 of type: int instead.", str(the_exception.__cause__)
        )

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_run_with_allowed_missing_var_str(self):
        """
        - Given a Populate with a variable of type str
        - When it is run
        - And the variable is not passed in the arguments
        - And the config option "allow_missing_vars" is set to true
        - Then the run command shall not fail and use the default vars declared in the Populate
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_str": "2018-01-01"})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --models test_schema.var_str")
        check_mock.assert_called_once()

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_run_with_allowed_missing_var_bool(self):
        """
        - Given a Populate with a variable of type bool
        - When it is run
        - And the variable is not passed in the arguments
        - And the config option "allow_missing_vars" is set to true
        - Then the run command shall not fail and use the default vars declared in the Populate
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_bool": False})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --models test_schema.var_bool")
        check_mock.assert_called_once()

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_run_with_allowed_missing_var_int(self):
        """
        - Given a Populate with a variable of type int
        - When it is run
        - And the variable is not passed in the arguments
        - And the config option "allow_missing_vars" is set to true
        - Then the run command shall not fail and use the default vars declared in the Populate
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_int": 0})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --models test_schema.var_int")
        check_mock.assert_called_once()

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_run_with_allowed_missing_var_float(self):
        """
        - Given a Populate with a variable of type float
        - When it is run
        - And the variable is not passed in the arguments
        - And the config option "allow_missing_vars" is set to true
        - Then the run command shall not fail and use the default vars declared in the Populate
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"var_float": 0.0})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --models test_schema.var_float")
        check_mock.assert_called_once()

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_multiple_run_with_vars(self):
        """Bugs were encountered when running several commands with vars, the value from the first command
        were overriding the values on the second command"""

        def inspect_job_1(job):
            self.assertEqual(job.vars, {"var_str": "2018-02-02"})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job_1) as check_mock_1:
            karadoc.cli.run_command("run --dry --vars var_str=2018-02-02 --models test_schema.var_str")
        check_mock_1.assert_called_once()

        def inspect_job_2(job):
            self.assertEqual(job.vars, {"var_str": "2018-01-01"})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job_2) as check_mock_2:
            karadoc.cli.run_command("run --dry --models test_schema.var_str")
        check_mock_2.assert_called_once()

    def test_run_with_undeclared_vars(self):
        """When passing vars, the ones that were not declared in the POPULATE file
        should not appear in the job object"""

        def inspect_job(job):
            self.assertEqual(job.vars, {})

        with mock.patch("karadoc.cli.commands.run.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("run --dry --vars day=2018-02-02 --models test_schema.no_var")
        check_mock.assert_called_once()
