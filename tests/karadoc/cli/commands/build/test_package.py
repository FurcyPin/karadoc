import os
import shutil
import unittest
from pathlib import Path

import karadoc
from karadoc.test_utils.mock_settings import mock_settings_for_test_class

test_dir = "test_working_dir/target"


@mock_settings_for_test_class({
    'enable_file_index_cache': False,
    'libs': 'tests/resources/karadoc/cli/commands/build/test_package/libs',
    'model_dir': 'tests/resources/karadoc/cli/commands/build/test_package/model',
})
class TestAnalyzeTimeline(unittest.TestCase):

    def setUp(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        os.makedirs(test_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)

    def test_build_package(self):
        karadoc.cli.run_command(f'build package --target {test_dir}')
        self.assertTrue((Path(test_dir) / "libs.zip").is_file())
        import zipfile
        with zipfile.ZipFile(Path(test_dir) / "libs.zip", 'r') as zip_ref:
            zip_ref.extractall(test_dir)
            self.assertTrue((Path(test_dir) / "karadoc").is_dir())
            self.assertTrue((Path(test_dir) / "model_lib").is_dir())

        self.assertTrue((Path(test_dir) / "model.zip").is_file())
        with zipfile.ZipFile(Path(test_dir) / "model.zip", 'r') as zip_ref:
            zip_ref.extractall(test_dir)
            self.assertTrue((Path(test_dir) / "test_schema.db").is_dir())

        self.assertTrue((Path(test_dir) / "conf.zip").is_file())
        with zipfile.ZipFile(Path(test_dir) / "conf.zip", 'r') as zip_ref:
            zip_ref.extractall(test_dir)
            self.assertTrue((Path(test_dir) / "spark").is_dir())
