def get_resource_folder_path(module):
    """Get the path of the associated resource folder for the given test module.

    * Each resource folder **MUST BE** used by only one unit test file
    * The path of the resource folder **MUST** match the path of the test file that uses it.
      For instance, the test file `/tests/karadoc/common/commands/test_run.py` uses the resource folder
      `/tests/resources/karadoc/common/commands/test_run`.

    Example::

        # If this snippet is used in the file /tests/karadoc/common/commands/test_run.py
        from tests.karadoc.test_utils import get_resource_folder_path
        test_resource_folder_path = get_resource_folder_path(__name__)
        # Then test_resource_folder_path will be /tests/resources/karadoc/common/commands/test_run

    :param module:
    :return:
    """
    return module.replace("tests.karadoc", "tests.resources.karadoc").replace(".", "/")
