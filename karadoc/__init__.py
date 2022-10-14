import os

# When running karadoc, this setting must always be set to true
# Otherwise, nodes of the setting tree that come from other files (or that override the "default" environment)
# will erase all the other branches.
# For an example, comment this and run the unit test `test_get_custom_conf_with_dynaconf_merge`

os.environ["MERGE_ENABLED_FOR_DYNACONF"] = "true"

# For some reason, the following code sample fails when unit tests are run within PyCharm,
# but does not fail when run outside PyCharm:
#
#    import karadoc
#    karadoc.cli.run_command(...)
#
# The import below fixes that issue.
from karadoc import cli  # noqa: E402

cli = cli

# This makes sure that the content libs/ folder is added to the python path in every situation
from karadoc.common.conf.package import _add_libs_folder_to_python_path  # noqa: E402

_add_libs_folder_to_python_path()
