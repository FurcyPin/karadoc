import shutil
import unittest
from pathlib import Path

import karadoc
from karadoc.common.run import load_populate
from karadoc.test_utils.mock_settings import mock_settings_for_test_class


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "template_package": "tests.resources.karadoc.cli.commands.test_run_with_template.model_lib.template_test",
        "model_dir": "tests/resources/karadoc/cli/commands/test_run_with_template/model",
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)
class TestRun(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def test_run_with_template(self):
        karadoc.cli.run_command("run --tables test_template.test_template")
        self.assertTrue(Path("test_working_dir/hive/warehouse/test_template.db/test_template").is_dir())

    def test_run_with_template_job_attributes(self):
        job1 = load_populate("test_template.test_template")
        self.assertEqual("test_pk", job1.primary_key)
        job2 = load_populate("test_template.test_template_2")
        self.assertIsNone(job2.primary_key)
