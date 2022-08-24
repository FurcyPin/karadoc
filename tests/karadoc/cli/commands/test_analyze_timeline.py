import os
import shutil
import unittest
from pathlib import Path

import karadoc
from karadoc.test_utils.mock_settings import mock_settings_for_test_class

test_dir = "test_working_dir/analyze_timeline"


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/cli/commands/test_analyze_timeline/model",
        "analyze_timeline_dir": test_dir,
    }
)
class TestAnalyzeTimeline(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        os.makedirs(test_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)

    def test_dry_run(self):
        karadoc.cli.run_command("analyze_timeline --dry --tables test_schema.test_table")

    def test_run(self):
        karadoc.cli.run_command("analyze_timeline --tables test_schema.test_table")
        self.assertTrue(Path(test_dir + "/test_schema.test_table.png").is_file())
