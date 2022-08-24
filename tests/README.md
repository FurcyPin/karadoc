# `tests/` folder structure

The `resources/` folder contains all resources used by unit tests.
It is organized using the following conventions:

* Each resource folder **MUST BE** used by only one unit test file
* The path of the resource folder **MUST BE** the match the path of the test file that uses it.
  For instance, the test file `/tests/karadoc/common/commands/test_run.py` uses the resource folder
  `/tests/resources/karadoc/common/commands/test_run/model`.

