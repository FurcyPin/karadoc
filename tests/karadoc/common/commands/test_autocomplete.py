import os
import subprocess
from pathlib import Path
from typing import List

from tests.karadoc.test_utils import get_resource_folder_path


def simulate_autocomplete(command: str) -> List[str]:
    command = "coverage run -m karadoc.launcher " + command
    run_command = command  # + " 8>&1 9>&2 1>&9 2>&1"
    stdout_filename = "test_working_dir/autocomplete_output"
    model_dir_path = get_resource_folder_path(__name__) + "/model"

    env = dict(os.environ)
    env["IFS"] = "\013"
    env["_ARGCOMPLETE"] = "1"
    env["_ARGCOMPLETE_SUPPRESS_SPACE"] = "1"
    env["_ARGCOMPLETE_STDOUT_FILENAME"] = stdout_filename
    env["COMP_LINE"] = command
    env["COMP_POINT"] = str(len(command))
    env["COMP_TYPE"] = "63"
    env["DYNACONF_MODEL_DIR"] = Path(model_dir_path).absolute()
    try:
        with subprocess.Popen(run_command, shell=True, env=env) as process:
            process.wait()
            with open(stdout_filename, "r") as stdout_file:
                res = stdout_file.read()
    finally:
        if os.path.exists(stdout_filename):
            os.remove(stdout_filename)
    completions = res.split("\013")
    return completions


def test_autocompletion():
    completions = simulate_autocomplete("run -")
    assert "--help" in completions


def test_autocompletion_tables_option():
    """This test performs end-to-end testing, but for some reason coverage data is not recorded"""
    assert simulate_autocomplete("run --tables ") == ["schema_1.", "schema_2."]
    assert simulate_autocomplete("run --tables sc") == ["schema_1.", "schema_2."]
    assert simulate_autocomplete("run --tables schema_1") == ["schema_1."]
    assert simulate_autocomplete("run --tables schema_1.") == ["schema_1.table_1", "schema_1.table_2"]
    assert simulate_autocomplete("run --tables schema_1.ta") == ["schema_1.table_1", "schema_1.table_2"]
    assert simulate_autocomplete("run --tables schema_1.table_1") == ["schema_1.table_1"]
