import os


def test_imports_on_startup():
    """When running a command, we must make sure that the following packages are NOT imported.
    They would slow down autocomplete, and they should not have to be installed to run commands.

    :return:
    """
    modules_that_should_be_optional = ["pyspark", "numpy" "pandas", "azure", "networkx"]
    # ideally, yaml should be optional too, because it is very large, but dynaconf currently imports it directly
    # (cf: https://github.com/dynaconf/dynaconf/issues/820)
    assert os.system(f"karadoc dev check_imports --fail-if-modules {' '.join(modules_that_should_be_optional)}") == 0
