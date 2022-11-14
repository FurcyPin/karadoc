# `tests/` folder structure

The `resources/` folder contains all resources used by unit tests.
It is organized using the following conventions:

* Each resource folder **MUST BE** used by only one unit test file
* The path of the resource folder **MUST** match the path of the test file that uses it.
  For instance, the test file `/tests/karadoc/common/commands/test_run.py` uses the resource folder
  `/tests/resources/karadoc/common/commands/test_run`.
* Each python test file can automatically get the corresponding name with the following code snippet:

```python
from tests.karadoc.test_utils import get_resource_folder_path
test_resource_folder_path = get_resource_folder_path(__name__)
```
